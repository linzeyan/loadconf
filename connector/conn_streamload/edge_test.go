package conn_streamload

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// quiet drops the retry warnings the client logs by default: these tests
// retry on purpose and the noise would bury real failures.
func quiet() Option { return WithLogger(slog.New(slog.DiscardHandler)) }

func hostOf(s *httptest.Server) string { return strings.TrimPrefix(s.URL, "http://") }

// redirector is an FE that sends every request on to to(), keeping path and
// query, as a Doris FE does after picking a BE.
func redirector(t testing.TB, to func() string) *httptest.Server {
	t.Helper()
	fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", to()+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(fe.Close)
	return fe
}

// deadAddr is the address of a server that no longer listens, so dialing it
// fails at once with connection refused.
func deadAddr() string {
	s := httptest.NewServer(http.NotFoundHandler())
	addr := hostOf(s)
	s.Close()
	return addr
}

// hangUp drops the connection without an HTTP response, like a crashed node.
func hangUp(w http.ResponseWriter) {
	if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
		_ = conn.Close()
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// inProcess returns a client whose FE answers every request itself with
// Success and hands the request to seen. Fuzzing and benchmarking through
// sockets would measure the kernel rather than the encoders.
func inProcess(tb testing.TB, cfg config.StreamLoad, seen func(h http.Header, body []byte)) *Client {
	tb.Helper()
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Body != nil {
			if seen != nil {
				body, _ := io.ReadAll(r.Body)
				seen(r.Header, body)
			} else {
				_, _ = io.Copy(io.Discard, r.Body)
			}
			_ = r.Body.Close()
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: r,
			Body: io.NopCloser(strings.NewReader(`{"Status":"Success"}`))}, nil
	})}
	c, err := New(cfg, WithHTTPClient(hc), quiet())
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

func TestEdgeBEClosesConnectionMidBody(t *testing.T) {
	payload := bytes.Repeat([]byte(`{"k":"0123456789abcdef"},`), 40_000) // ~1 MiB
	var (
		mu     sync.Mutex
		labels []string
		bodies [][]byte
	)
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		first := len(labels) == 0
		labels = append(labels, r.Header.Get("label"))
		mu.Unlock()
		if first {
			// Take part of the body, then vanish like a BE that crashed mid-load.
			_, _ = io.CopyN(io.Discard, r.Body, 32<<10)
			hangUp(w)
			return
		}
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		_, _ = io.WriteString(w, `{"Status":"Success","NumberLoadedRows":1}`)
	}))
	t.Cleanup(be.Close)
	fe := redirector(t, func() string { return be.URL })

	c := newClient(t, testCfg([]string{hostOf(fe)}), quiet())
	if _, err := c.Load(context.Background(), "", bytes.NewReader(payload), LoadOptions{Label: "mid"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// The retry must resend the whole body under the same label: a partial
	// body would load truncated rows, a new label could load them twice.
	if !slices.Equal(labels, []string{"mid", "mid"}) {
		t.Errorf("labels = %v", labels)
	}
	if len(bodies) != 1 || !bytes.Equal(bodies[0], payload) {
		t.Errorf("retried body: %d bodies, first %d bytes, want %d", len(bodies), len(bodies[0]), len(payload))
	}
}

func TestEdgeRedirectChains(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	// fe0 forwards to another FE, which picks the BE: two hops, credentials
	// on both, as a follower FE forwarding to the master does.
	fe0 := redirector(t, func() string { return cl.fes[0].URL })
	c := newClient(t, testCfg([]string{hostOf(fe0)}), quiet())
	if _, err := c.Load(context.Background(), "", strings.NewReader(`[{"id":1}]`)); err != nil {
		t.Fatal(err)
	}
	be := cl.requests("be")
	if len(be) != 1 || be[0].auth() != "root:pw" || be[0].body != `[{"id":1}]` {
		t.Errorf("be = %+v", be)
	}

	// A relative Location resolves against the FE that sent it.
	var beHits atomic.Int32
	self := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/be/") {
			beHits.Add(1)
			b, _ := io.ReadAll(r.Body)
			if u, p, _ := r.BasicAuth(); u != "root" || p != "pw" || string(b) != "[]" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, `{"Status":"Success"}`)
			return
		}
		w.Header().Set("Location", "/be"+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(self.Close)
	c = newClient(t, testCfg([]string{hostOf(self)}), quiet())
	if _, err := c.Load(context.Background(), "", strings.NewReader("[]")); err != nil || beHits.Load() != 1 {
		t.Errorf("relative redirect: %v, be hits %d", err, beHits.Load())
	}
}

func TestEdgeRedirectLoopIsBounded(t *testing.T) {
	var hits atomic.Int32
	var a, b *httptest.Server
	loop := func(to func() *httptest.Server) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Location", to().URL+r.URL.RequestURI())
			w.WriteHeader(http.StatusTemporaryRedirect)
		})
	}
	a = httptest.NewServer(loop(func() *httptest.Server { return b }))
	b = httptest.NewServer(loop(func() *httptest.Server { return a }))
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)

	cfg := testCfg([]string{hostOf(a)})
	cfg.MaxRetries = 1
	done := make(chan error, 1)
	go func() {
		_, err := newClient(t, cfg, quiet()).Load(context.Background(), "", strings.NewReader("[]"))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "too many redirects") {
			t.Errorf("err = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a redirect loop never ended")
	}
	// Each attempt may follow a handful of redirects; a loop must not cost
	// more than that per attempt.
	if n := hits.Load(); n < 2 || n > 2*11 {
		t.Errorf("%d requests for a loop over 2 attempts", n)
	}
}

func TestEdgeRedirectWithoutLocation(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(broken.Close)

	cfg := testCfg([]string{hostOf(broken)})
	cfg.MaxRetries = 0
	_, err := newClient(t, cfg, quiet()).Load(context.Background(), "", strings.NewReader("[]"))
	if err == nil || !strings.Contains(err.Error(), "without a location") {
		t.Errorf("err = %v", err)
	}

	// A broken FE is a reason to try the next one, not to give up.
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	c := newClient(t, testCfg(append([]string{hostOf(broken)}, cl.addrs()...)), quiet())
	if _, err := c.Load(context.Background(), "", strings.NewReader("[]"), LoadOptions{Label: "l"}); err != nil {
		t.Errorf("failover past an FE without Location: %v", err)
	}
}

func TestEdgeMalformedLoadResponses(t *testing.T) {
	for _, body := range []string{
		`{"Status":"Succ`, `{"Status":"Success"`, `<html>502 Bad Gateway</html>`, ``, `null`, `[]`, `{}`, `"Success"`,
	} {
		cl := newCluster(t, 1, func(req, int) (int, string) { return 200, body })
		res, err := newClient(t, testCfg(cl.addrs()), quiet()).Load(context.Background(), "", strings.NewReader("[]"))
		// A 200 whose body does not say the load succeeded must never be
		// reported as loaded.
		var le *LoadError
		if res != nil || !errors.As(err, &le) {
			t.Errorf("body %q: res = %+v err = %v", body, res, err)
		}
	}
}

func TestEdgeServerErrorsThenSuccessResendSameBytes(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		switch n {
		case 0:
			return 500, "internal"
		case 1:
			return 502, "<html>bad gateway</html>"
		}
		return 200, success(r, 1)
	})
	// The body starts at a non-zero offset: every retry must resend from
	// there, not from the start of the reader.
	r := bytes.NewReader([]byte(`HEADER[{"id":1}]`))
	_, _ = r.Seek(6, io.SeekStart)
	if _, err := newClient(t, testCfg(cl.addrs()), quiet()).Load(context.Background(), "", r, LoadOptions{Label: "5xx"}); err != nil {
		t.Fatal(err)
	}
	be := cl.requests("be")
	if len(be) != 3 {
		t.Fatalf("be requests = %d", len(be))
	}
	for _, rq := range be {
		if rq.body != `[{"id":1}]` || rq.header.Get("label") != "5xx" {
			t.Errorf("attempt body %q label %q", rq.body, rq.header.Get("label"))
		}
	}
}

func TestEdgeAllFEsDown(t *testing.T) {
	cfg := testCfg([]string{deadAddr(), deadAddr(), deadAddr()})
	cfg.MaxRetries = 4
	c := newClient(t, cfg, quiet())
	ctx := context.Background()

	start := time.Now()
	_, err := c.Load(ctx, "", strings.NewReader("[]"))
	var ue *url.Error
	if !errors.As(err, &ue) {
		t.Errorf("err = %v, want the dial error", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %v with 1ms backoff", d)
	}

	// The writer reports the dropped batch once and stays usable.
	w, err := c.NewWriter("", WriterOptions{MaxRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "batch of 1 rows dropped") {
		t.Errorf("write = %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Errorf("close after a reported failure = %v", err)
	}

	cfg.Flavor = config.StreamLoadStarRocks
	if _, err := newClient(t, cfg, quiet()).Begin(ctx, ""); err == nil {
		t.Error("begin with every FE down should fail")
	}
}

func TestEdgeCancelDuringRetryBackoff(t *testing.T) {
	answered := make(chan struct{}, 1)
	cl := newCluster(t, 1, func(req, int) (int, string) {
		select {
		case answered <- struct{}{}:
		default:
		}
		return 503, "busy"
	})
	cfg := testCfg(cl.addrs())
	cfg.MaxRetries, cfg.RetryBackoff = 3, time.Hour
	c := newClient(t, cfg, quiet())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-answered
		cancel()
	}()
	start := time.Now()
	if _, err := c.Load(ctx, "", strings.NewReader("[]")); err == nil {
		t.Error("a cancelled load must fail")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Load slept %v through a cancelled context", d)
	}
	if n := len(cl.requests("be")); n != 1 {
		t.Errorf("be requests = %d, want no attempt after the cancel", n)
	}
}

func TestEdgeLoadResultStatuses(t *testing.T) {
	for _, tc := range []struct {
		status    int
		body      string
		twoPC     bool
		ok, retry bool
	}{
		{200, `{"Status":"Success"}`, false, true, false},
		{200, `{"Status":"success"}`, false, true, false},
		// Committed; becomes visible shortly. Retrying would be pointless.
		{200, `{"Status":"Publish Timeout"}`, false, true, false},
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"VISIBLE"}`, false, true, false},
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"committed"}`, false, true, false},
		// Outside 2PC a pre-committed job is not visible data.
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"PRECOMMITTED"}`, false, false, false},
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"PRECOMMITTED"}`, true, true, false},
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"RUNNING"}`, false, false, true},
		// Unknown or failed state of the earlier job: cannot claim the rows.
		{200, `{"Status":"Label Already Exists","ExistingJobStatus":"CANCELLED"}`, false, false, false},
		{200, `{"Status":"Label Already Exists"}`, false, false, false},
		{200, `{"Status":"Fail","Message":"too many filtered rows"}`, false, false, false},
		{503, `busy`, false, false, true},
		{404, `no such table`, false, false, false},
	} {
		res, retry, err := loadResult("l", &response{status: tc.status, body: []byte(tc.body)}, tc.twoPC)
		if (err == nil) != tc.ok || retry != tc.retry {
			t.Errorf("%d %s twoPC=%v: ok=%v retry=%v err=%v", tc.status, tc.body, tc.twoPC, err == nil, retry, err)
		}
		if err == nil && res.Label != "l" {
			t.Errorf("label = %q", res.Label)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for the concurrent writes of a handler.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestEdgeRetriesAreLoggedToDefault(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		if n < 2 {
			return 503, "busy"
		}
		return 200, success(r, 1)
	})
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cfg := testCfg(cl.addrs())
	cfg.Password = config.NewSecret("hunter2-secret")
	if _, err := newClient(t, cfg).Load(context.Background(), "", strings.NewReader("[]"), LoadOptions{Label: "logged"}); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// One warning per retry, with what an operator needs to find the load.
	if strings.Count(out, "stream load retry") != 2 || !strings.Contains(out, "level=WARN") ||
		!strings.Contains(out, "label=logged") || !strings.Contains(out, "attempt=2") {
		t.Errorf("log = %s", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("the password leaked into the log: %s", out)
	}
}

func TestEdgeWithHTTPClientDoesNotMutateCaller(t *testing.T) {
	hc := &http.Client{Timeout: time.Minute}
	_ = newClient(t, testCfg([]string{"fe"}), WithHTTPClient(hc))
	// The Client follows redirects itself; changing the caller's client
	// would break redirects everywhere else it is used.
	if hc.CheckRedirect != nil {
		t.Error("WithHTTPClient changed the caller's CheckRedirect")
	}
}

// labelDedupCluster loads each label at most once, like the server: a label
// seen again answers Label Already Exists. Every lostEvery-th load is
// stored but its answer replaced by a 503, which forces a retry.
type labelDedupCluster struct {
	*cluster
	mu     sync.Mutex
	labels map[string]bool
	rows   map[string]int
	loads  int
	bodies []string
}

func newLabelDedupCluster(t *testing.T, lostEvery int) *labelDedupCluster {
	d := &labelDedupCluster{labels: map[string]bool{}, rows: map[string]int{}}
	d.cluster = newCluster(t, 1, func(r req, _ int) (int, string) {
		d.mu.Lock()
		defer d.mu.Unlock()
		label := r.header.Get("label")
		if d.labels[label] {
			return 200, `{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED"}`
		}
		var rows []map[string]any
		if err := json.Unmarshal([]byte(r.body), &rows); err != nil {
			return 200, `{"Status":"Fail","Message":"invalid json"}`
		}
		d.labels[label] = true
		d.bodies = append(d.bodies, r.body)
		for _, row := range rows {
			d.rows[fmt.Sprint(row["g"], "/", row["i"])]++
		}
		d.loads++
		if lostEvery > 0 && d.loads%lostEvery == 0 {
			return 503, "loaded, but the answer is lost"
		}
		return 200, success(r, len(rows))
	})
	return d
}

func TestEdgeWriterConcurrentRowsExactlyOnce(t *testing.T) {
	const goroutines, perGoroutine, maxBytes, maxRows = 16, 300, 900, 23
	d := newLabelDedupCluster(t, 5)
	var flushed atomic.Int64
	w, err := newClient(t, testCfg(d.addrs()), quiet()).NewWriter("", WriterOptions{
		MaxRows: maxRows, MaxBytes: maxBytes, FlushInterval: 3 * time.Millisecond,
		OnFlush: func(_ *Result, rows int, err error) {
			if err == nil {
				flushed.Add(int64(rows))
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				if err := w.WriteJSON(ctx, map[string]int{"g": g, "i": i}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	// Retries after a lost answer keep the label, so the server loads every
	// row exactly once; a new label per attempt would duplicate rows.
	if len(d.rows) != goroutines*perGoroutine || flushed.Load() != goroutines*perGoroutine {
		t.Errorf("distinct rows = %d, flushed = %d, want %d", len(d.rows), flushed.Load(), goroutines*perGoroutine)
	}
	for k, n := range d.rows {
		if n != 1 {
			t.Errorf("row %s loaded %d times", k, n)
		}
	}
	for _, b := range d.bodies {
		if n := strings.Count(b, "{"); len(b) > maxBytes && n > 1 || n > maxRows {
			t.Errorf("batch of %d rows and %d bytes exceeds the limits", n, len(b))
		}
	}
}

func TestEdgeWriterIgnoresFixedLabel(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	w, err := newClient(t, testCfg(cl.addrs())).NewWriter("", WriterOptions{MaxRows: 1, LoadOptions: LoadOptions{Label: "fixed"}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := w.WriteJSON(context.Background(), map[string]int{"id": i}); err != nil {
			t.Fatal(err)
		}
	}
	// One label for every batch would make the server drop each batch after
	// the first as a duplicate.
	seen := map[string]bool{}
	for _, r := range cl.requests("be") {
		l := r.header.Get("label")
		if l == "fixed" || seen[l] {
			t.Errorf("batch label %q reused", l)
		}
		seen[l] = true
	}
}

func TestEdgeWriterCancelDuringFlush(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock) // the fake BE must return before the server can close
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		if n == 0 {
			close(arrived)
			<-release
		}
		return 200, success(r, 1)
	})
	var (
		mu        sync.Mutex
		failed    int
		failedErr error
	)
	w, err := newClient(t, testCfg(cl.addrs()), quiet()).NewWriter("", WriterOptions{MaxRows: 1,
		OnFlush: func(_ *Result, rows int, err error) {
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				failed, failedErr = rows, err
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Write(ctx, []byte(`{"id":1}`)) }()
	<-arrived
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "batch of 1 rows dropped") {
			t.Errorf("cancelled write = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Write ignored its cancelled context while loading")
	}
	unblock()

	// The writer stays usable, and the dropped batch is reported once: to the
	// cancelled Write and OnFlush, not again to later callers.
	bg := context.Background()
	if err := w.Write(bg, []byte(`{"id":2}`)); err != nil {
		t.Errorf("write after a cancelled flush = %v", err)
	}
	if err := w.Close(bg); err != nil {
		t.Errorf("close = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if failed != 1 || !errors.Is(failedErr, context.Canceled) {
		t.Errorf("OnFlush saw %d rows, %v", failed, failedErr)
	}
}

func TestEdgeWriterFlushCloseIdempotent(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	w, err := newClient(t, testCfg(cl.addrs())).NewWriter("", WriterOptions{FlushInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for range 2 {
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(cl.requests("")); n != 0 {
		t.Errorf("an empty flush made %d requests", n)
	}
	if err := w.Write(ctx, []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := w.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(cl.requests("be")); n != 1 {
		t.Errorf("flushing twice loaded %d times", n)
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := w.Close(ctx); err != nil {
				t.Errorf("close = %v", err)
			}
		})
	}
	wg.Wait()
	if err := w.Flush(ctx); !errors.Is(err, ErrClosed) {
		t.Errorf("flush after close = %v", err)
	}
	if err := w.WriteJSON(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Errorf("write after close = %v", err)
	}
	select {
	case <-w.done:
	default:
		t.Error("the interval flusher still runs after Close")
	}
	if n := len(cl.requests("be")); n != 1 {
		t.Errorf("loads = %d after close", n)
	}
}

func TestEdgeWriterFlushesExactlyAtMaxRows(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 3) })
	w, err := newClient(t, testCfg(cl.addrs())).NewWriter("", WriterOptions{MaxRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 3 {
		if err := w.Write(ctx, fmt.Appendf(nil, `{"id":%d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	// The Write that fills the batch waits for its load (backpressure), so
	// the load has happened when it returns.
	if be := cl.requests("be"); len(be) != 1 || be[0].body != `[{"id":0},{"id":1},{"id":2}]` {
		t.Fatalf("be = %+v", be)
	}
	if err := w.Close(ctx); err != nil || len(cl.requests("be")) != 1 {
		t.Errorf("close loaded again: %v", err)
	}
}

func TestEdgeWriterMaxBytesJSON(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	c := newClient(t, testCfg(cl.addrs()))
	ctx := context.Background()
	bodies := func() []string {
		var out []string
		for _, r := range cl.requests("be") {
			out = append(out, r.body)
		}
		cl.mu.Lock()
		cl.reqs = nil
		cl.mu.Unlock()
		return out
	}
	row := []byte(`{"a":1}`)
	full := `[{"a":1},{"a":1}]` // exactly max_bytes

	// A batch that fits max_bytes exactly is not split.
	w, _ := c.NewWriter("", WriterOptions{MaxBytes: int64(len(full))})
	for range 3 {
		if err := w.Write(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close(ctx)
	if got := bodies(); !slices.Equal(got, []string{full, `[{"a":1}]`}) {
		t.Errorf("exact max_bytes: bodies = %q", got)
	}

	// A row larger than max_bytes goes alone, after what was buffered.
	big := []byte(`{"blob":"` + strings.Repeat("x", 40) + `"}`)
	w, _ = c.NewWriter("", WriterOptions{MaxBytes: 16})
	for _, r := range [][]byte{row, big, row} {
		if err := w.Write(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close(ctx)
	if got := bodies(); !slices.Equal(got, []string{`[{"a":1}]`, "[" + string(big) + "]", `[{"a":1}]`}) {
		t.Errorf("oversized row: bodies = %q", got)
	}
}

func TestEdgeLoadJSONEmpty(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 0) })
	c := newClient(t, testCfg(cl.addrs()))
	ctx := context.Background()

	var none []map[string]any
	if _, err := c.LoadJSON(ctx, "", none); err == nil {
		t.Error("a nil slice marshals to null, which the server cannot strip as an array")
	}
	if n := len(cl.requests("")); n != 0 {
		t.Errorf("a rejected LoadJSON made %d requests", n)
	}
	for _, rows := range []any{[]map[string]any{}, [0]int{}} {
		if _, err := c.LoadJSON(ctx, "", rows); err != nil {
			continue // refusing an empty load is fine
		}
		be := cl.requests("be")
		if last := be[len(be)-1]; last.body != "[]" || last.header.Get("strip_outer_array") != "true" {
			t.Errorf("empty load sent %q %v", last.body, last.header)
		}
	}
}

func TestEdgeLoadCSVSeparatorsAndEmptyFields(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	cfg := testCfg(cl.addrs())
	cfg.Format = "csv"
	cfg.Headers = map[string]string{"column_separator": "|"}
	c := newClient(t, cfg)
	ctx := context.Background()
	comma := LoadOptions{Headers: map[string]string{"column_separator": ","}}

	for _, tc := range []struct {
		name    string
		opts    []LoadOptions
		records [][]string
		want    string
		sep     string
	}{
		{"configured separator", nil, [][]string{{"a", "b"}, {"c", "d"}}, "a|b\nc|d\n", "|"},
		{"option overrides config", []LoadOptions{comma}, [][]string{{"a|x", "b"}}, "a|x,b\n", ","},
		{"all fields empty", nil, [][]string{{"", "", ""}}, "||\n", "|"},
		{"trailing empty field", nil, [][]string{{"a", ""}}, "a|\n", "|"},
		{"leading empty field", nil, [][]string{{"", "a"}}, "|a\n", "|"},
		{"tab is data when not the separator", nil, [][]string{{"a\tb", "c"}}, "a\tb|c\n", "|"},
		{"multi-byte separator", []LoadOptions{{Headers: map[string]string{"column_separator": "¦"}}}, [][]string{{"x", "y"}}, "x¦y\n", "¦"},
	} {
		if _, err := c.LoadCSV(ctx, "", tc.records, tc.opts...); err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		be := cl.requests("be")
		last := be[len(be)-1]
		// The body must be split with the separator the server is told about.
		if last.body != tc.want || last.header.Get("column_separator") != tc.sep || last.header.Get("format") != "csv" {
			t.Errorf("%s: body %q sep %q", tc.name, last.body, last.header.Get("column_separator"))
		}
	}

	before := len(cl.requests(""))
	for _, rec := range [][]string{{"a|b"}, {"a\nb"}, {"a\rb"}, {"ok", "x|"}} {
		if _, err := c.LoadCSV(ctx, "", [][]string{{"fine"}, rec}); err == nil {
			t.Errorf("record %q should be refused: it would split into other columns or rows", rec)
		}
	}
	if n := len(cl.requests("")); n != before {
		t.Errorf("refused records still made %d requests", n-before)
	}
}

func TestEdgeFEIgnoringExpectSwallowsNonSeekableBody(t *testing.T) {
	var beHits atomic.Int32
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { beHits.Add(1) }))
	t.Cleanup(be.Close)
	var feHits atomic.Int32
	fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		feHits.Add(1)
		_, _ = io.ReadAll(r.Body) // an FE that reads the body before redirecting
		w.Header().Set("Location", be.URL+r.URL.RequestURI())
		w.WriteHeader(http.StatusTemporaryRedirect)
	}))
	t.Cleanup(fe.Close)

	_, err := newClient(t, testCfg([]string{hostOf(fe)}), quiet()).Load(context.Background(), "", onlyReader{strings.NewReader(`[{"a":1}]`)})
	// The body is gone; sending an empty or partial one to the BE would load
	// nothing (or garbage) and look like success. It must fail, once.
	if err == nil || !strings.Contains(err.Error(), "io.ReadSeeker") {
		t.Errorf("err = %v", err)
	}
	if beHits.Load() != 0 || feHits.Load() != 1 {
		t.Errorf("fe hits %d, be hits %d", feHits.Load(), beHits.Load())
	}
}

func TestEdgeTxnFinishedStaysFinished(t *testing.T) {
	for _, flavor := range []string{config.StreamLoadDoris, config.StreamLoadStarRocks} {
		t.Run(flavor, func(t *testing.T) {
			cl := newCluster(t, 1, func(r req, _ int) (int, string) {
				if strings.HasSuffix(r.path, "/_stream_load") {
					return 200, success(r, 1)
				}
				return 200, `{"Status":"OK","TxnId":42}`
			})
			cfg := testCfg(cl.addrs())
			cfg.Flavor = flavor
			c := newClient(t, cfg)
			ctx := context.Background()

			for _, finish := range []string{"commit", "abort"} {
				tx, err := c.Begin(ctx, "")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Load(ctx, strings.NewReader(`[{"id":1}]`)); err != nil {
					t.Fatal(err)
				}
				if finish == "commit" {
					err = tx.Commit(ctx)
				} else {
					err = tx.Abort(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				before := len(cl.requests(""))
				// Commit after abort, abort after commit and double commit are
				// answered locally: sent to the server, they could only fail
				// ambiguously or act on a finished transaction.
				_, lerr := tx.Load(ctx, strings.NewReader("[]"))
				for name, err := range map[string]error{
					"commit": tx.Commit(ctx), "abort": tx.Abort(ctx), "prepare": tx.Prepare(ctx), "load": lerr,
				} {
					if !errors.Is(err, ErrTxnDone) {
						t.Errorf("%s after %s = %v", name, finish, err)
					}
				}
				if n := len(cl.requests("")) - before; n != 0 {
					t.Errorf("%d requests after %s", n, finish)
				}
			}
		})
	}
}

func TestEdgeStarRocksTxnPinnedFEDown(t *testing.T) {
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = io.WriteString(w, `{"Status":"OK","TxnId":7}`)
	}))
	t.Cleanup(be.Close)
	var down atomic.Bool
	var hits [2]atomic.Int32
	var fes []string
	for i := range 2 {
		fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits[i].Add(1)
			if i == 0 && down.Load() {
				hangUp(w)
				return
			}
			w.Header().Set("Location", be.URL+r.URL.RequestURI())
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		t.Cleanup(fe.Close)
		fes = append(fes, hostOf(fe))
	}
	cfg := testCfg(fes)
	cfg.Flavor = config.StreamLoadStarRocks
	c := newClient(t, cfg, quiet())
	ctx := context.Background()

	tx, err := c.Begin(ctx, "") // the first round-robin pick: fe0
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Load(ctx, strings.NewReader(`[{"id":1}]`)); err != nil {
		t.Fatal(err)
	}
	if hits[1].Load() != 0 {
		t.Fatalf("the transaction left fe0")
	}

	down.Store(true)
	if _, err := tx.Load(ctx, strings.NewReader(`[{"id":2}]`)); err == nil {
		t.Error("load through a dead FE should fail")
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("commit through a dead FE should fail")
	}
	// The transaction lives on the FE it began on; another FE does not
	// know it.
	if n := hits[1].Load(); n != 0 {
		t.Errorf("%d transaction requests went to fe1", n)
	}

	// A failed Commit leaves the transaction open, so it can still be
	// committed (or aborted) once the FE is back.
	down.Store(false)
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("commit after the FE came back = %v", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, ErrTxnDone) {
		t.Errorf("second commit = %v", err)
	}
}

// TestEdgeWithLoggerNilDisablesLogging: nil means "no logging", as
// config.WithLogger(nil) (used across this repo's tests) and the kafka/es
// connectors treat it. A nil logger used as is would panic only when a load
// first needs a retry, typically in production.
func TestEdgeWithLoggerNilDisablesLogging(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		if n == 0 {
			return 503, "busy"
		}
		return 200, success(r, 1)
	})
	c := newClient(t, testCfg(cl.addrs()), WithLogger(nil))
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Load panicked on its first retry: %v", r)
		}
	}()
	if _, err := c.Load(context.Background(), "", strings.NewReader("[]")); err != nil {
		t.Error(err)
	}
}

// TestEdgeLoadPipeFileSentOnce: *os.File implements io.Seeker, but a pipe,
// socket or terminal (os.Stdin in `producer | loader`) cannot seek. Such a
// file is loaded like any non-seekable reader (sent once) instead of
// failing with "illegal seek" before any request.
func TestEdgeLoadPipeFileSentOnce(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	pr, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pr.Close()
	if _, err := pw.WriteString(`[{"id":1}]`); err != nil {
		t.Fatal(err)
	}
	_ = pw.Close()

	if _, err := newClient(t, testCfg(cl.addrs())).Load(context.Background(), "", pr); err != nil {
		t.Fatalf("Load(pipe) = %v; want it sent once like any non-seekable reader", err)
	}
	if be := cl.requests("be"); len(be) != 1 || be[0].body != `[{"id":1}]` {
		t.Errorf("be = %+v", be)
	}
}

// TestEdgeRetryDoesNotShareBodyWithEarlierRequest (meaningful under
// -race): when a BE answers before it has read the whole body (an early
// 503 or label check) and closes the connection, net/http hands over the
// response without waiting for its writeLoop goroutine, which is still
// reading the request body (transport.go: alive is false, so wroteRequest
// is skipped). The RoundTripper contract says callers must wait for the
// body's Close before reusing it. A retry must therefore rewind only after
// that Close; seeking at once races with the old writeLoop's Read and
// leaves the retry's bytes unguaranteed.
func TestEdgeRetryDoesNotShareBodyWithEarlierRequest(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 1<<19) // 8 MiB
	for name, body := range map[string]io.Reader{
		"ReaderAt":   bytes.NewReader(payload),
		"SeekerOnly": struct{ io.ReadSeeker }{bytes.NewReader(payload)},
	} {
		t.Run(name, func(t *testing.T) {
			var (
				n   atomic.Int32
				mu  sync.Mutex
				got []byte
			)
			retried := make(chan struct{})
			be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				k := n.Add(1)
				if k == 1 {
					// Read a little (so the client streams the body), answer at
					// once and keep the connection until the retry arrives: the
					// old request is still being written while the client
					// retries.
					_, _ = io.CopyN(io.Discard, r.Body, 64<<10)
					w.Header().Set("Content-Length", "4")
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = io.WriteString(w, "busy")
					_ = http.NewResponseController(w).Flush()
					select {
					case <-retried:
					case <-time.After(3 * time.Second):
					}
					return
				}
				if k == 2 {
					close(retried)
				}
				b, _ := io.ReadAll(r.Body)
				mu.Lock()
				got = b
				mu.Unlock()
				_, _ = io.WriteString(w, `{"Status":"Success"}`)
			}))
			t.Cleanup(be.Close)
			fe := redirector(t, func() string { return be.URL })
			cfg := testCfg([]string{hostOf(fe)})
			cfg.MaxRetries, cfg.RetryBackoff = 1, 0

			if _, err := newClient(t, cfg, quiet()).Load(context.Background(), "", body, LoadOptions{Label: "early"}); err != nil {
				t.Fatalf("retry after an early answer: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if !bytes.Equal(got, payload) {
				t.Errorf("retried body: %d bytes, want the original %d", len(got), len(payload))
			}
		})
	}
}

// TestEdgeWriterNextBatchAfterEarlyAnswer (meaningful under -race): a BE
// that answers a batch before reading it (its label already loaded) closes
// the connection, and net/http hands over that answer while its writeLoop
// still reads the batch. The Writer then writes the next batch into the same
// buffer, so the load must return only once the transport closed the body.
func TestEdgeWriterNextBatchAfterEarlyAnswer(t *testing.T) {
	var n atomic.Int32
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 1 {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `{"Status":"Success"}`)
			return
		}
		// Answer after a little of the body (so the client streams it) and
		// keep taking the rest until the client hangs up: the answer carries
		// Connection: close, and the client goes on sending meanwhile.
		_, _ = io.CopyN(io.Discard, r.Body, 64<<10)
		const answer = `{"Status":"Label Already Exists","ExistingJobStatus":"FINISHED"}`
		w.Header().Set("Content-Length", fmt.Sprint(len(answer)))
		_, _ = io.WriteString(w, answer)
		_ = http.NewResponseController(w).Flush()
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(be.Close)
	fe := redirector(t, func() string { return be.URL })
	w, err := newClient(t, testCfg([]string{hostOf(fe)}), quiet()).NewWriter("", WriterOptions{MaxRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	// A batch this large is still being sent when the answer comes, and the
	// next one overwrites every byte the transport may still read.
	row := []byte(`{"k":"` + strings.Repeat("x", 8<<20) + `"}`)
	ctx := context.Background()
	for range 2 {
		if err := w.Write(ctx, row); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestEdgeWriterCanceledFlushGivesUpBuffer (meaningful under -race): when
// ctx ends while a load waits for the transport to let go of the batch, the
// Write returns at once, and the transport may still read that batch: the
// next one must go into another buffer.
func TestEdgeWriterCanceledFlushGivesUpBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	var wg sync.WaitGroup
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		// Answer at once and read the body only when released, like a
		// transport still sending it.
		wg.Go(func() {
			<-release
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
		})
		cancel()
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: r,
			Body: io.NopCloser(strings.NewReader(`{"Status":"Success"}`))}, nil
	})}
	c, err := New(testCfg([]string{"fe"}), WithHTTPClient(hc), quiet())
	if err != nil {
		t.Fatal(err)
	}
	w, err := c.NewWriter("", WriterOptions{MaxRows: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, []byte(`{"batch":1}`)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write whose ctx ended while the body was held = %v, want context.Canceled", err)
	}
	close(release)
	if err := w.Write(context.Background(), []byte(`{"batch":2}`)); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

// TestEdgeLoadCSVMultiCharSeparatorKeepsColumns: with a multi-character
// separator such as "||" (Doris and StarRocks accept those), a field ending
// in a prefix of it yields an ambiguous line even without containing it:
// ["a|", "b"] becomes "a|||b", which the server splits at the first "||"
// into ["a", "|b"]. Such a field is refused like one containing the
// separator, rather than silently moving data to another column.
func TestEdgeLoadCSVMultiCharSeparatorKeepsColumns(t *testing.T) {
	cfg := testCfg([]string{"fe"})
	cfg.Format = "csv"
	var body string
	c := inProcess(t, cfg, func(_ http.Header, b []byte) { body = string(b) })
	rec := []string{"a|", "b"}
	if _, err := c.LoadCSV(context.Background(), "", [][]string{rec}, LoadOptions{Headers: map[string]string{"column_separator": "||"}}); err != nil {
		return // refusing the field is a correct outcome
	}
	if got := strings.Split(strings.TrimSuffix(body, "\n"), "||"); !slices.Equal(got, rec) {
		t.Errorf("body %q splits into %q, want %q", body, got, rec)
	}
}

// TestEdgeLoadCSVHexSeparatorJoinsWithByte: Doris and StarRocks read a
// column_separator written as \xNN as the byte 0xNN; that is the documented
// way to pass Hive's \x01. Joining fields with the header text itself would
// put the four characters `\x01` in the body, and the server would find one
// column per line.
func TestEdgeLoadCSVHexSeparatorJoinsWithByte(t *testing.T) {
	cfg := testCfg([]string{"fe"})
	cfg.Format = "csv"
	var body string
	c := inProcess(t, cfg, func(_ http.Header, b []byte) { body = string(b) })
	if _, err := c.LoadCSV(context.Background(), "", [][]string{{"a", "b"}}, LoadOptions{Headers: map[string]string{"column_separator": `\x01`}}); err != nil {
		t.Fatal(err)
	}
	if want := "a\x01b\n"; body != want {
		t.Errorf("body = %q, want %q", body, want)
	}
}

// TestEdgeTxnCommitAgainAfterLostResponse: when every attempt of a Commit
// loses its answer, the transaction is committed but the call fails and the
// Txn stays open. Calling Commit again, the natural recovery, gets "already
// visible" on its first attempt and must succeed, as an in-call retry
// would: a *LoadError for committed data makes a caller that loads again
// under a new label duplicate the rows.
func TestEdgeTxnCommitAgainAfterLostResponse(t *testing.T) {
	var commits atomic.Int32
	be := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_stream_load") {
			_, _ = io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, `{"TxnId":42,"Status":"Success"}`)
			return
		}
		if commits.Add(1) <= 3 {
			hangUp(w) // committed, but the answer never arrives
			return
		}
		_, _ = io.WriteString(w, `{"status":"Fail","msg":"transaction [42] is already visible, not pre-committed."}`)
	}))
	t.Cleanup(be.Close)
	fe := redirector(t, func() string { return be.URL })
	c := newClient(t, testCfg([]string{hostOf(fe)}), quiet()) // max_retries 2
	ctx := context.Background()

	tx, err := c.Begin(ctx, "", LoadOptions{Label: "tx"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Load(ctx, strings.NewReader(`[{"id":1}]`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("every commit answer was lost; the first Commit should fail")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Errorf("Commit again = %v; the transaction is committed", err)
	}
}

// TestEdgeDorisTxnLoadFailsOver: Doris 2PC loads are keyed by label and
// Commit/Abort rotate over FEs, so a Doris Txn's load fails over to the next
// FE like any load, as the package promises. Pinned to the FE picked at
// Begin, it would fail for 1/N of transactions whenever one of N FEs is
// down. (Only StarRocks transactions stay on their FE.)
func TestEdgeDorisTxnLoadFailsOver(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	c := newClient(t, testCfg(append([]string{deadAddr()}, cl.addrs()...)), quiet())
	ctx := context.Background()
	tx, err := c.Begin(ctx, "", LoadOptions{Label: "tx"}) // round robin starts at the dead FE
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Load(ctx, strings.NewReader(`[{"id":1}]`)); err != nil {
		t.Errorf("Txn.Load with one live FE = %v", err)
	}
}

// An FE addr New rejects must be reported without its credentials: New's
// errors end up in logs.
func TestEdgeNewAddrErrorRedactsPassword(t *testing.T) {
	for _, addr := range []string{"http://u:hunter2@:8030", "u:hunter2@fe:99999", "u:hunter2@fe:%zz"} {
		_, err := New(testCfg([]string{addr}), quiet())
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("New(%q) = %v, want an error without the password", addr, err)
		}
	}
}
