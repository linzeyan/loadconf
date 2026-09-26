// Package conn_streamload loads data into Apache Doris and StarRocks over HTTP
// Stream Load from [config.StreamLoad]. It only needs the standard library.
//
//	c, err := conn_streamload.New(cfg.StreamLoad.MustGet("doris"))
//	res, err := c.LoadJSON(ctx, "events", rows)
//
// Besides single loads it offers two-phase commit ([Client.Begin]) and a
// batching [Writer]. Queries go over the MySQL protocol (conn_sql).
//
// The frontend (FE) answers a load with a redirect to a backend (BE). The
// client follows it itself so that the credentials reach the BE, like curl
// --location-trusted, and sends Expect: 100-continue so that the body is
// only sent to the BE. Loads rotate over the configured FEs and fail over
// to the next one; a retried load keeps its label, which the server uses to
// load the data at most once.
//
// Bodies that implement io.Seeker (*bytes.Reader, *os.File, ...) can be
// sent again for retries. Other readers, and files that cannot seek such as
// pipes, are sent at most once: a retry after the body was read fails.
package conn_streamload

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// Client loads into one database. It is safe for concurrent use.
type Client struct {
	cfg   config.StreamLoad
	fes   []*url.URL
	next  atomic.Uint64
	hc    *http.Client
	log   *slog.Logger
	label func(table string) string
}

// Option customizes a Client.
type Option func(*Client)

// WithHTTPClient sets the HTTP client. Its CheckRedirect is replaced on a
// copy, since the Client follows redirects itself.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		cp := *hc
		c.hc = &cp
	}
}

// WithLogger logs retries at Warn to l instead of slog.Default(); nil
// disables logging.
func WithLogger(l *slog.Logger) Option {
	return func(c *Client) {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}
		c.log = l
	}
}

// WithLabelFunc sets the function that generates labels. Labels may use
// letters, digits, '-', '_' and ':' and at most 128 characters.
func WithLabelFunc(fn func(table string) string) Option {
	return func(c *Client) { c.label = fn }
}

// New returns a Client for cfg.
func New(cfg config.StreamLoad, opts ...Option) (*Client, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	c := &Client{cfg: cfg, log: slog.Default()}
	for i, a := range cfg.Addrs {
		u, err := feURL(a, cfg.TLS.Enabled)
		if err != nil {
			return nil, fmt.Errorf("stream load: addrs[%d]: %w", i, err)
		}
		c.fes = append(c.fes, u)
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.hc == nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		// The FE answers 100-continue requests with a redirect at once; the
		// timeout only matters for an FE that ignores Expect.
		tr.ExpectContinueTimeout = 5 * time.Second
		if cfg.TLS.Enabled {
			tlsCfg, err := cfg.TLS.Config()
			if err != nil {
				return nil, fmt.Errorf("stream load tls: %w", err)
			}
			tr.TLSClientConfig = tlsCfg
		}
		c.hc = &http.Client{Transport: tr}
	}
	c.hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if c.label == nil {
		c.label = c.defaultLabel
	}
	return c, nil
}

func feURL(addr string, tls bool) (*url.URL, error) {
	if !strings.Contains(addr, "://") {
		scheme := "http://"
		if tls {
			scheme = "https://"
		}
		addr = scheme + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		// Not printed: its userinfo cannot be located to redact it.
		return nil, errors.New("not host:port or a URL")
	}
	u.User = nil // the base URL never carries it, and errors must not show it
	// An empty host would dial localhost, url.Parse lets stray brackets
	// through ("]" joins to "]:8030", which cannot be split again), and port
	// 0 or one above 65535 cannot be dialed: all are configuration mistakes.
	port := cmp.Or(u.Port(), "8030")
	host := net.JoinHostPort(u.Hostname(), port)
	if _, _, err := net.SplitHostPort(host); err != nil || u.Hostname() == "" {
		return nil, fmt.Errorf("invalid addr %q", u)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return nil, fmt.Errorf("invalid port in addr %q", u)
	}
	return &url.URL{Scheme: u.Scheme, Host: host}, nil
}

// defaultLabel returns {label_prefix}_{table}_{UTC time}_{random}, with
// label_prefix defaulting to "loadconf".
func (c *Client) defaultLabel(table string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	head := sanitizeLabel(cmp.Or(c.cfg.LabelPrefix, "loadconf") + "_" + table)
	if len(head) > 90 {
		head = head[:90]
	}
	return head + "_" + time.Now().UTC().Format("20060102T150405") + "_" + hex.EncodeToString(b[:])
}

func sanitizeLabel(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == ':':
			return r
		}
		return '_'
	}, s)
}

// Database returns the database loads go to.
func (c *Client) Database() string { return c.cfg.Database }

func (c *Client) table(table string) (string, error) {
	if table = cmp.Or(table, c.cfg.Table); table == "" {
		return "", errors.New("stream load: no table given and none configured")
	}
	return table, nil
}

// retry runs fn up to max_retries+1 times, each attempt bounded by the
// timeout. Attempts rotate over the FEs unless fe is pinned. fn reports
// whether its error may be retried.
func (c *Client) retry(ctx context.Context, op, label string, pinned *url.URL,
	fn func(ctx context.Context, fe *url.URL) (retry bool, err error),
) error {
	start := c.next.Add(1) - 1
	var err error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			c.log.LogAttrs(ctx, slog.LevelWarn, "stream load retry", slog.String("op", op),
				slog.String("label", label), slog.Int("attempt", attempt), slog.Any("error", err))
			if werr := sleep(ctx, backoff(c.cfg.RetryBackoff, attempt)); werr != nil {
				return err
			}
		}
		fe := pinned
		if fe == nil {
			fe = c.fes[(start+uint64(attempt))%uint64(len(c.fes))]
		}
		actx, cancel := ctx, context.CancelFunc(func() {})
		if c.cfg.Timeout > 0 {
			actx, cancel = context.WithTimeout(ctx, c.cfg.Timeout)
		}
		var retry bool
		retry, err = fn(actx, fe)
		cancel()
		if err == nil || !retry || ctx.Err() != nil {
			return err
		}
	}
	return err
}

// backoff doubles base per attempt, up to 30s (or base when larger).
func backoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	limit := max(base, 30*time.Second)
	d := base << min(attempt-1, 16)
	if d <= 0 || d > limit {
		return limit
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// response is an HTTP response read in full (up to 1 MiB).
type response struct {
	status int
	body   []byte
}

// send sends one request to fe and follows redirects with the credentials.
func (c *Client) send(ctx context.Context, fe *url.URL, method, path string, h http.Header, body *payload) (res *response, err error) {
	if body != nil {
		// The caller may reuse the body once the load returns, but the
		// transport can still be reading it (see payload.reader): wait for
		// its Close, after the response was read so that the transport can
		// finish with the connection.
		defer func() {
			if werr := body.wait(ctx); werr != nil && err == nil {
				res, err = nil, werr
			}
		}()
	}
	u := &url.URL{Scheme: fe.Scheme, Host: fe.Host, Path: path}
	for redirects := 0; ; redirects++ {
		req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header = h.Clone()
		if body != nil {
			// Taken only once nothing can fail before Do: the next request
			// waits for the transport to close this body (payload.reader).
			if req.Body, req.ContentLength, err = body.reader(ctx); err != nil {
				return nil, err
			}
			req.Header.Set("Expect", "100-continue")
		}
		req.SetBasicAuth(c.cfg.Username, c.cfg.Password.Value())

		resp, err := c.hc.Do(req)
		if err != nil {
			return nil, err
		}
		switch resp.StatusCode {
		case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
			loc := resp.Header.Get("Location")
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
			_ = resp.Body.Close()
			if loc == "" {
				return nil, fmt.Errorf("redirect from %s without a location", u.Host)
			}
			if redirects >= 5 {
				return nil, errors.New("too many redirects")
			}
			next, err := u.Parse(loc)
			if err != nil || (next.Scheme != "http" && next.Scheme != "https") {
				return nil, fmt.Errorf("invalid redirect location from %s", u.Host)
			}
			u = next
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		_ = resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return &response{status: resp.StatusCode, body: data}, nil
	}
}

var errNotRewindable = errors.New("the body was already sent and cannot be read again; pass an io.ReadSeeker such as *bytes.Reader or *os.File to allow retries")

// payload makes a request body re-readable when it is an io.Seeker.
type payload struct {
	r      io.Reader
	seeker io.Seeker
	start  int64
	size   int64
	used   bool
	read   int64
	// sent is closed when the transport closes the last request's body.
	sent chan struct{}
}

func newPayload(r io.Reader) (*payload, error) {
	if r == nil {
		r = bytes.NewReader(nil)
	}
	p := &payload{r: r, size: -1}
	if s, ok := r.(io.Seeker); ok {
		// A pipe, socket or terminal is an *os.File too, but cannot seek:
		// it is sent once like any other reader.
		start, err := s.Seek(0, io.SeekCurrent)
		if err != nil {
			return p, nil
		}
		end, err := s.Seek(0, io.SeekEnd)
		if err != nil {
			return p, nil
		}
		if _, err := s.Seek(start, io.SeekStart); err != nil {
			return nil, err
		}
		p.seeker, p.start, p.size = s, start, end-start
	}
	return p, nil
}

// reader returns the body of the next request and its size (-1: unknown).
func (p *payload) reader(ctx context.Context) (io.ReadCloser, int64, error) {
	if p.size == 0 {
		return http.NoBody, 0, nil
	}
	// The transport may go on reading a body after the response arrived (a
	// server that answered early and closed the connection). As the
	// http.RoundTripper contract asks, the reader is only rewound or read
	// again after that body was closed.
	if err := p.wait(ctx); err != nil {
		return nil, 0, err
	}
	var r io.Reader
	if p.seeker != nil {
		if p.used {
			if _, err := p.seeker.Seek(p.start, io.SeekStart); err != nil {
				return nil, 0, err
			}
		}
		// Hide the Seeker (and WriterTo) so net/http streams the reader
		// as is.
		r = io.LimitReader(p.r, p.size)
	} else {
		if p.read > 0 {
			return nil, 0, errNotRewindable
		}
		r = countingReader{p}
	}
	p.used = true
	p.sent = make(chan struct{})
	return &requestBody{Reader: r, sent: p.sent}, p.size, nil
}

// wait waits until the transport closed the last request's body.
func (p *payload) wait(ctx context.Context) error {
	if p.sent == nil {
		return nil
	}
	select {
	case <-p.sent:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// requestBody is a payload lent to one request until the transport closes
// it.
type requestBody struct {
	io.Reader
	once sync.Once
	sent chan struct{}
}

func (b *requestBody) Close() error {
	b.once.Do(func() { close(b.sent) })
	return nil
}

type countingReader struct{ p *payload }

func (c countingReader) Read(b []byte) (int, error) {
	n, err := c.p.r.Read(b)
	c.p.read += int64(n)
	return n, err
}
