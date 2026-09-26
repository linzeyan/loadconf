package conn_mqtt_rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// ErrNotFound matches (via errors.Is) API errors with status 404.
var ErrNotFound = errors.New("not found")

// APIError is a non-2xx response. EMQX errors carry Code and Message;
// publish errors carry ReasonCode (an MQTT reason code) and Message.
type APIError struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	ReasonCode int    `json:"reason_code"`
	// Body is the raw response body when it was not a JSON error object.
	Body string `json:"-"`
}

func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "http %d", e.StatusCode)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	if e.ReasonCode != 0 {
		fmt.Fprintf(&b, " reason_code=%d", e.ReasonCode)
	}
	switch {
	case e.Message != "":
		b.WriteString(": " + e.Message)
	case e.Body != "":
		b.WriteString(": " + e.Body)
	}
	return b.String()
}

func (e *APIError) Is(target error) bool {
	return target == ErrNotFound && e.StatusCode == http.StatusNotFound
}

// Option customizes a client.
type Option func(*httpClient)

// WithHTTPClient replaces the HTTP client built from the config (timeout,
// TLS). Use it for custom transports, proxies or instrumentation.
func WithHTTPClient(hc *http.Client) Option { return func(c *httpClient) { c.hc = hc } }

const maxBodySize = 32 << 20

// httpClient sends JSON requests with auth, headers and retries.
type httpClient struct {
	base       string // without trailing slash
	hc         *http.Client
	headers    map[string]string
	maxRetries int
	backoff    time.Duration
	auth       authenticator
}

type authenticator interface {
	apply(ctx context.Context, req *http.Request) error
	// renew is called once after a 401 with the rejected request's header;
	// it reports whether to retry.
	renew(ctx context.Context, sent http.Header, resp *APIError) bool
}

func newHTTPClient(cfg config.MQTTREST, basePath string, opts []Option) (*httpClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	if basePath != "" && !strings.HasSuffix(base, basePath) {
		base += basePath
	}
	c := &httpClient{
		base:       base,
		headers:    cfg.Headers,
		maxRetries: cfg.MaxRetries,
		backoff:    cfg.RetryBackoff,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.hc == nil {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if tlsCfg != nil {
			transport.TLSClientConfig = tlsCfg
		}
		c.hc = &http.Client{Timeout: cfg.Timeout, Transport: transport}
	}

	switch cfg.Auth.ResolvedType() {
	case config.HTTPAuthBasic:
		c.auth = basicAuth{user: cfg.Auth.Username, pass: cfg.Auth.Password.Value()}
	case config.HTTPAuthBearer:
		c.auth = bearerAuth(cfg.Auth.Token.Value())
	case config.HTTPAuthLogin:
		c.auth = &loginAuth{client: c, user: cfg.Auth.Username, pass: cfg.Auth.Password.Value()}
	default:
		c.auth = noAuth{}
	}
	return c, nil
}

// Do sends a JSON request to path (relative to the API base) and decodes a
// 2xx JSON response into out when out is non-nil. Other statuses return an
// *APIError. GET, HEAD and DELETE are retried on network errors, 429 and
// 5xx responses.
func (c *httpClient) Do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	status, data, err := c.roundTrip(ctx, method, path, query, body)
	if err != nil {
		return err
	}
	if status < 200 || status > 299 {
		return apiError(status, data)
	}
	if out != nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}
	return nil
}

// roundTrip returns the status and body of the final attempt.
func (c *httpClient) roundTrip(ctx context.Context, method, path string, query url.Values, body any) (int, []byte, error) {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return 0, nil, fmt.Errorf("encode request: %w", err)
		}
	}
	target := c.base + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}

	idempotent := method == http.MethodGet || method == http.MethodHead || method == http.MethodDelete
	renewed := false
	for attempt := 0; ; attempt++ {
		status, data, sent, err := c.send(ctx, method, target, payload, true)
		retryable := err != nil || status == http.StatusTooManyRequests || status >= 500
		if err == nil && status == http.StatusUnauthorized && !renewed {
			renewed = true
			if c.auth.renew(ctx, sent, apiError(status, data)) {
				attempt--
				continue
			}
		}
		if !retryable || !idempotent || attempt >= c.maxRetries || ctx.Err() != nil {
			return status, data, err
		}
		select {
		case <-ctx.Done():
			return status, data, err
		case <-time.After(c.backoff << attempt):
		}
	}
}

// send returns the response status and body, and the header sent so that
// renew can tell which token a 401 rejected.
func (c *httpClient) send(ctx context.Context, method, target string, payload []byte, withAuth bool) (int, []byte, http.Header, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if withAuth {
		if err := c.auth.apply(ctx, req); err != nil {
			return 0, nil, nil, err
		}
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, data, req.Header, nil
}

func apiError(status int, data []byte) *APIError {
	e := &APIError{StatusCode: status}
	if json.Unmarshal(data, e) != nil || (e.Code == "" && e.Message == "" && e.ReasonCode == 0) {
		e.Body = strings.TrimSpace(string(data))
		if len(e.Body) > 512 {
			e.Body = e.Body[:512] + "..."
		}
	}
	return e
}

type noAuth struct{}

func (noAuth) apply(context.Context, *http.Request) error         { return nil }
func (noAuth) renew(context.Context, http.Header, *APIError) bool { return false }

type basicAuth struct{ user, pass string }

func (a basicAuth) apply(_ context.Context, req *http.Request) error {
	req.SetBasicAuth(a.user, a.pass)
	return nil
}
func (basicAuth) renew(context.Context, http.Header, *APIError) bool { return false }

type bearerAuth string

func (a bearerAuth) apply(_ context.Context, req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+string(a))
	return nil
}
func (bearerAuth) renew(context.Context, http.Header, *APIError) bool { return false }
