package conn_streamload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// req is a request seen by the fake cluster.
type req struct {
	server       string // fe0, fe1, ... or be
	method, path string
	header       http.Header
	body         string
}

func (r req) auth() string {
	u, p, _ := (&http.Request{Header: r.header}).BasicAuth()
	return u + ":" + p
}

// cluster is a fake Doris/StarRocks: FEs redirect every request to one BE,
// whose answers come from handle (n counts the BE requests).
type cluster struct {
	mu     sync.Mutex
	reqs   []req
	fes    []*httptest.Server
	be     *httptest.Server
	n      int
	handle func(r req, n int) (int, string)
}

func newCluster(t *testing.T, fes int, handle func(r req, n int) (int, string)) *cluster {
	t.Helper()
	c := &cluster{handle: handle}
	c.be = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rq := req{"be", r.Method, r.URL.Path, r.Header.Clone(), string(body)}
		c.mu.Lock()
		c.reqs = append(c.reqs, rq)
		n := c.n
		c.n++
		handle := c.handle
		c.mu.Unlock()
		status, resp := handle(rq, n)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(c.be.Close)
	for i := range fes {
		name := fmt.Sprintf("fe%d", i)
		fe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c.mu.Lock()
			c.reqs = append(c.reqs, req{name, r.Method, r.URL.Path, r.Header.Clone(), ""})
			c.mu.Unlock()
			w.Header().Set("Location", c.be.URL+r.URL.RequestURI())
			w.WriteHeader(http.StatusTemporaryRedirect)
		}))
		t.Cleanup(fe.Close)
		c.fes = append(c.fes, fe)
	}
	return c
}

func (c *cluster) addrs() []string {
	var out []string
	for _, fe := range c.fes {
		out = append(out, strings.TrimPrefix(fe.URL, "http://"))
	}
	return out
}

func (c *cluster) requests(server string) []req {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []req
	for _, r := range c.reqs {
		if server == "" || r.server == server {
			out = append(out, r)
		}
	}
	return out
}

func testCfg(addrs []string) config.StreamLoad {
	return config.StreamLoad{
		Flavor: "doris", Addrs: addrs, Username: "root", Password: config.NewSecret("pw"), Database: "db", Table: "events", Format: "json",
		Headers: map[string]string{"strict_mode": "true"},
		Timeout: 5 * time.Second, MaxRetries: 2, RetryBackoff: time.Millisecond,
		Batch: config.StreamLoadBatch{MaxRows: 100, MaxBytes: config.MiB},
	}
}

func success(r req, rows int) string {
	return fmt.Sprintf(`{"TxnId":42,"Label":%q,"Status":"Success","Message":"OK","NumberTotalRows":%d,"NumberLoadedRows":%d,"LoadBytes":%d,"LoadTimeMs":3}`,
		r.header.Get("label"), rows, rows, len(r.body))
}

func newClient(t *testing.T, cfg config.StreamLoad, opts ...Option) *Client {
	t.Helper()
	c, err := New(cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestLoadJSONFollowsRedirectWithAuth(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 2) })
	c := newClient(t, testCfg(cl.addrs()))
	rows := []map[string]any{{"id": 1, "name": "a"}, {"id": 2, "name": "b"}}
	res, err := c.LoadJSON(context.Background(), "", rows, LoadOptions{Headers: map[string]string{"columns": "id,name"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != "Success" || res.NumberLoadedRows != 2 || res.TxnID != 42 || !strings.HasPrefix(res.Label, "loadconf_events_") {
		t.Errorf("result = %+v", res)
	}

	fe, be := cl.requests("fe0"), cl.requests("be")
	if len(fe) != 1 || len(be) != 1 {
		t.Fatalf("fe = %d be = %d requests", len(fe), len(be))
	}
	for _, r := range []req{fe[0], be[0]} {
		if r.method != http.MethodPut || r.path != "/api/db/events/_stream_load" || r.auth() != "root:pw" {
			t.Errorf("%s: %s %s auth %q", r.server, r.method, r.path, r.auth())
		}
	}
	h := be[0].header
	if h.Get("format") != "json" || h.Get("strip_outer_array") != "true" || h.Get("strict_mode") != "true" ||
		h.Get("columns") != "id,name" || h.Get("label") != res.Label {
		t.Errorf("headers = %v", h)
	}
	if be[0].body != `[{"id":1,"name":"a"},{"id":2,"name":"b"}]` {
		t.Errorf("body = %s", be[0].body)
	}
}

func TestLoadRetriesWithSameLabelAndFailsOver(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		if n == 0 {
			return 503, "busy"
		}
		return 200, success(r, 1)
	})
	dead := httptest.NewServer(http.NotFoundHandler())
	deadAddr := strings.TrimPrefix(dead.URL, "http://")
	dead.Close()

	cfg := testCfg(append([]string{deadAddr}, cl.addrs()...))
	cfg.MaxRetries = 3
	c := newClient(t, cfg)
	res, err := c.Load(context.Background(), "t", strings.NewReader(`[{"id":1}]`), LoadOptions{Label: "fixed"})
	if err != nil {
		t.Fatal(err)
	}
	be := cl.requests("be")
	if len(be) != 2 || be[0].header.Get("label") != "fixed" || be[1].header.Get("label") != "fixed" || res.Label != "fixed" {
		t.Errorf("be requests = %+v", be)
	}
	if be[1].body != `[{"id":1}]` {
		t.Errorf("retried body = %q", be[1].body)
	}
}

func TestLoadLabelAlreadyExists(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		state := "RUNNING"
		if n > 0 {
			state = "FINISHED"
		}
		return 200, `{"Status":"Label Already Exists","ExistingJobStatus":"` + state + `","Message":"label exists"}`
	})
	c := newClient(t, testCfg(cl.addrs()))
	res, err := c.Load(context.Background(), "", strings.NewReader("[]"), LoadOptions{Label: "dup"})
	if err != nil || res.Status != "Label Already Exists" || res.ExistingJobStatus != "FINISHED" {
		t.Fatalf("res = %+v err = %v", res, err)
	}
	if n := len(cl.requests("be")); n != 2 {
		t.Errorf("be requests = %d", n)
	}
}

func TestLoadErrors(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		return 200, `{"TxnId":7,"Status":"Fail","Message":"too many filtered rows","NumberTotalRows":3,"NumberFilteredRows":3,"ErrorURL":"http://be/api/_load_error_log?file=x"}`
	})
	c := newClient(t, testCfg(cl.addrs()))
	_, err := c.Load(context.Background(), "", strings.NewReader("[]"), LoadOptions{Label: "bad"})
	var le *LoadError
	if !errors.As(err, &le) || le.Status != "Fail" || le.Result == nil || le.Result.NumberFilteredRows != 3 ||
		!strings.Contains(err.Error(), "too many filtered rows") || !strings.Contains(err.Error(), "_load_error_log") {
		t.Fatalf("err = %v", err)
	}
	if n := len(cl.requests("be")); n != 1 {
		t.Errorf("a data error must not be retried: %d requests", n)
	}

	cl.mu.Lock()
	cl.handle = func(req, int) (int, string) { return 401, "Unauthorized" }
	cl.mu.Unlock()
	_, err = c.Load(context.Background(), "", strings.NewReader("[]"))
	if !errors.As(err, &le) || le.HTTPStatus != 401 || strings.Contains(err.Error(), "pw") {
		t.Errorf("err = %v", err)
	}

	if _, err := c.LoadJSON(context.Background(), "", map[string]int{"a": 1}); err == nil {
		t.Error("LoadJSON of a map should fail")
	}
	noTable := testCfg(cl.addrs())
	noTable.Table = ""
	if _, err := newClient(t, noTable).Load(context.Background(), "", nil); err == nil {
		t.Error("a load without a table should fail")
	}
	// Two option sets have no obvious merge order, so the call must fail
	// rather than silently drop one of them.
	if _, err := c.Load(context.Background(), "", strings.NewReader("[]"), LoadOptions{Label: "a"}, LoadOptions{Label: "b"}); err == nil {
		t.Error("two LoadOptions should fail")
	}
}

// onlyReader hides Seek.
type onlyReader struct{ io.Reader }

func TestLoadNonSeekableBody(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 1) })
	c := newClient(t, testCfg(cl.addrs()))
	// The FE redirects before the body is sent (Expect: 100-continue).
	if _, err := c.Load(context.Background(), "", onlyReader{strings.NewReader(`[{"a":1}]`)}); err != nil {
		t.Fatal(err)
	}
	if be := cl.requests("be"); len(be) != 1 || be[0].body != `[{"a":1}]` {
		t.Fatalf("be = %+v", be)
	}

	cl.mu.Lock()
	cl.handle = func(req, int) (int, string) { return 500, "boom" }
	cl.mu.Unlock()
	_, err := c.Load(context.Background(), "", onlyReader{strings.NewReader(`[{"a":1}]`)})
	if err == nil || !strings.Contains(err.Error(), "io.ReadSeeker") {
		t.Errorf("err = %v", err)
	}
	if n := len(cl.requests("be")); n != 2 {
		t.Errorf("be requests = %d, want the first load plus one attempt", n)
	}
}

func TestLoadCSV(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, 2) })
	cfg := testCfg(cl.addrs())
	cfg.Format = "csv"
	c := newClient(t, cfg)
	_, err := c.LoadCSV(context.Background(), "", [][]string{{"1", "a"}, {"2", "b c"}}, LoadOptions{Headers: map[string]string{"column_separator": ","}})
	if err != nil {
		t.Fatal(err)
	}
	be := cl.requests("be")[0]
	if be.body != "1,a\n2,b c\n" || be.header.Get("format") != "csv" || be.header.Get("column_separator") != "," {
		t.Errorf("body = %q headers = %v", be.body, be.header)
	}
	if _, err := c.LoadCSV(context.Background(), "", [][]string{{"x\ty"}}); err == nil {
		t.Error("a field with the default separator should fail")
	}
}

func TestNewAndLabels(t *testing.T) {
	cfg := testCfg([]string{"fe1", "https://fe2:8443/ignored", "[::1]"})
	cfg.LabelPrefix = "etl job"
	c := newClient(t, cfg)
	var hosts []string
	for _, u := range c.fes {
		hosts = append(hosts, u.String())
	}
	if strings.Join(hosts, " ") != "http://fe1:8030 https://fe2:8443 http://[::1]:8030" {
		t.Errorf("fes = %v", hosts)
	}
	l := c.label(strings.Repeat("t", 200))
	if !strings.HasPrefix(l, "etl_job_ttt") || len(l) > 128 || strings.ContainsAny(l, " .") {
		t.Errorf("label = %q (%d)", l, len(l))
	}
	if c.label("x") == c.label("x") {
		t.Error("labels must be unique")
	}
	c = newClient(t, cfg, WithLabelFunc(func(table string) string { return "custom-" + table }))
	if c.label("x") != "custom-x" {
		t.Error("WithLabelFunc ignored")
	}

	cfg.TLS.Enabled = true
	if c := newClient(t, cfg); c.fes[0].Scheme != "https" {
		t.Errorf("tls scheme = %s", c.fes[0])
	}
	for _, bad := range []config.StreamLoad{{}, {Flavor: "doris", Addrs: []string{"h"}, Format: "json"}, {Flavor: "mysql", Addrs: []string{"h"}, Database: "d", Format: "json"}} {
		if _, err := New(bad); err == nil {
			t.Errorf("New(%+v) should fail", bad)
		}
	}
}

func TestBackoff(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 6: 30 * time.Second, 60: 30 * time.Second} {
		if got := backoff(time.Second, attempt); got != want {
			t.Errorf("backoff(1s, %d) = %v, want %v", attempt, got, want)
		}
	}
	if backoff(0, 3) != 0 || backoff(time.Minute, 2) != time.Minute {
		t.Error("backoff edge cases")
	}
}

func TestPayloadSeekFromCurrentOffset(t *testing.T) {
	r := bytes.NewReader([]byte("skipbody"))
	_, _ = r.Seek(4, io.SeekStart)
	p, err := newPayload(r)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		rd, size, err := p.reader(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(rd)
		_ = rd.Close()
		if string(b) != "body" || size != 4 {
			t.Errorf("read %q size %d", b, size)
		}
	}
}
