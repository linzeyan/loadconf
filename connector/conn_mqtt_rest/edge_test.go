package conn_mqtt_rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// wireReq is a request as the broker routes it: the raw path split on "/"
// and then each segment unescaped, which is how EMQX decodes client ids and
// topics in paths.
type wireReq struct {
	method string
	segs   []string
	query  url.Values
	body   []byte
	auth   string
}

// wireServer records requests at the wire level and answers from resp.
type wireServer struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []wireReq
}

func newWireServer(tb testing.TB, resp func(r *http.Request) (int, string)) *wireServer {
	s := &wireServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _, _ := strings.Cut(r.RequestURI, "?")
		var segs []string
		for p := range strings.SplitSeq(raw, "/") {
			u, err := url.PathUnescape(p)
			if err != nil {
				u = "<bad escape " + p + ">"
			}
			segs = append(segs, u)
		}
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.reqs = append(s.reqs, wireReq{r.Method, segs, r.URL.Query(), body, r.Header.Get("Authorization")})
		s.mu.Unlock()
		status, out := resp(r)
		reply(w, status, out)
	}))
	tb.Cleanup(s.Close)
	return s
}

func (s *wireServer) last() wireReq {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reqs[len(s.reqs)-1]
}

func emqxAt(tb testing.TB, baseURL string, mut ...func(*config.MQTTREST)) *EMQX {
	tb.Helper()
	cfg := config.MQTTREST{
		Provider: config.MQTTRESTEMQX, BaseURL: baseURL,
		Auth:    config.HTTPAuth{Username: "key", Password: config.NewSecret("secret")},
		Timeout: 5 * time.Second, MaxRetries: 2, RetryBackoff: time.Millisecond,
	}
	for _, m := range mut {
		m(&cfg)
	}
	c, err := NewEMQX(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// awkward are client ids and topics that collide with URL syntax or MQTT
// wildcards; each must reach the broker unchanged.
var awkward = []string{
	"a/b", "a+b", "#", "sensor/+/temp/#", "%2F", "100%", "with space", "日本/語", "emoji😀",
	"?q=1&x=2", "frag#ment", "a;b,c", "tab\there", "$share/g/t", "a%20b", `back\slash`, ".", "..", "",
}

func TestEdgePathEscapingRoundTrips(t *testing.T) {
	s := newWireServer(t, func(r *http.Request) (int, string) {
		switch {
		case r.Method != http.MethodGet:
			return 204, ""
		case strings.HasSuffix(r.URL.Path, "/subscriptions") && r.URL.Path != "/api/v5/subscriptions":
			return 200, "[]"
		}
		return 200, "{}"
	})
	c := emqxAt(t, s.URL)
	ctx := context.Background()
	api := []string{"", "api", "v5"}
	for _, id := range awkward {
		for _, tc := range []struct {
			name string
			call func() error
			want []string
		}{
			{"GetClient", func() error { _, err := c.GetClient(ctx, id); return err }, []string{"clients", id}},
			{"KickClient", func() error { return c.KickClient(ctx, id) }, []string{"clients", id}},
			{"ClientSubscriptions", func() error { _, err := c.ClientSubscriptions(ctx, id); return err }, []string{"clients", id, "subscriptions"}},
			{"Subscribe", func() error { return c.Subscribe(ctx, id, Subscription{Topic: id, QoS: 1}) }, []string{"clients", id, "subscribe"}},
			{"Unsubscribe", func() error { return c.Unsubscribe(ctx, id, id, "x") }, []string{"clients", id, "unsubscribe", "bulk"}},
			{"GetRetained", func() error { _, err := c.GetRetained(ctx, id); return err }, []string{"mqtt", "retainer", "message", id}},
			{"DeleteRetained", func() error { return c.DeleteRetained(ctx, id) }, []string{"mqtt", "retainer", "message", id}},
		} {
			if err := tc.call(); err != nil {
				t.Errorf("%s(%q): %v", tc.name, id, err)
				continue
			}
			// Another decoded path would act on another client or topic.
			if got, want := s.last().segs, append(slices.Clone(api), tc.want...); !slices.Equal(got, want) {
				t.Errorf("%s(%q): broker saw %q, want %q", tc.name, id, got, want)
			}
		}
		// Topics in bodies and queries reach the broker unchanged too.
		_ = c.Subscribe(ctx, "c", Subscription{Topic: id})
		var sub map[string]any
		if err := json.Unmarshal(s.last().body, &sub); err != nil || sub["topic"] != id {
			t.Errorf("subscribe body topic = %v (%v), want %q", sub["topic"], err, id)
		}
		if _, err := c.ListSubscriptions(ctx, SubscriptionQuery{Topic: id, MatchTopic: id, ClientID: id}); err != nil {
			t.Error(err)
		} else if q := s.last().query; q.Get("topic") != id || q.Get("match_topic") != id || q.Get("clientid") != id {
			t.Errorf("subscription query = %v, want %q", q, id)
		}
		if _, err := c.ListClients(ctx, ClientQuery{ClientIDs: []string{id, "b"}, LikeClientID: id}); err != nil {
			t.Error(err)
		} else if q := s.last().query; !slices.Equal(q["clientid"], []string{id, "b"}) || q.Get("like_clientid") != id {
			t.Errorf("clients query = %v, want %q", q, id)
		}
		if _, err := c.ListRetained(ctx, RetainedQuery{Topic: id}); err != nil || s.last().query.Get("topic") != id {
			t.Errorf("retained query = %v (%v), want %q", s.last().query, err, id)
		}
	}
}

func TestEdgePagination(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()
	for _, tc := range []struct {
		name, body string
		n          int
		hasNext    bool
		count      int // -1: not reported
	}{
		{"no items", `{"data":[],"meta":{"page":3,"limit":100,"hasnext":false,"count":0}}`, 0, false, 0},
		{"last page without count", `{"data":[{"clientid":"c"}],"meta":{"page":2,"limit":1,"hasnext":false}}`, 1, false, -1},
		// Missing hasnext must read as "no next page": a caller paging
		// until HasNext is false would otherwise never stop.
		{"hasnext missing", `{"data":[{"clientid":"c"}],"meta":{"page":1,"limit":1}}`, 1, false, -1},
		{"meta missing", `{"data":[{"clientid":"c"}]}`, 1, false, -1},
		{"data null", `{"data":null,"meta":{"hasnext":true,"count":5}}`, 0, true, 5},
	} {
		f.on("GET /api/v5/clients", fixed(200, tc.body))
		page, err := c.ListClients(ctx, ClientQuery{})
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		count := -1
		if page.Meta.Count != nil {
			count = *page.Meta.Count
		}
		if len(page.Data) != tc.n || page.Meta.HasNext != tc.hasNext || count != tc.count {
			t.Errorf("%s: %+v", tc.name, page)
		}
	}

	// Paging with Page and Limit visits every item once and stops.
	items := []string{"a", "b", "c", "d", "e"}
	f.on("GET /api/v5/clients", func(w http.ResponseWriter, r *http.Request) {
		var page, limit int
		_, _ = fmt.Sscan(r.URL.Query().Get("page"), &page)
		_, _ = fmt.Sscan(r.URL.Query().Get("limit"), &limit)
		lo, hi := min((page-1)*limit, len(items)), min(page*limit, len(items))
		var data []Client
		for _, id := range items[lo:hi] {
			data = append(data, Client{ClientID: id})
		}
		out, _ := json.Marshal(Page[Client]{Data: data, Meta: PageMeta{Page: page, Limit: limit, HasNext: hi < len(items)}})
		reply(w, 200, string(out))
	})
	var got []string
	for p := 1; p < 10; p++ {
		page, err := c.ListClients(ctx, ClientQuery{PageQuery: PageQuery{Page: p, Limit: 2}})
		if err != nil {
			t.Fatal(err)
		}
		for _, cl := range page.Data {
			got = append(got, cl.ClientID)
		}
		if !page.Meta.HasNext {
			break
		}
	}
	if !slices.Equal(got, items) {
		t.Errorf("paged = %v", got)
	}

	// Zero and negative page settings leave the broker defaults.
	if _, err := c.ListClients(ctx, ClientQuery{PageQuery: PageQuery{Page: -1, Limit: 0}}); err != nil || f.last().Query != "" {
		t.Errorf("query = %q, %v", f.last().Query, err)
	}
}

// tokenBroker issues tokens on /login and accepts only those it considers
// valid; a revoked token is answered 401 with the code given to revoke.
type tokenBroker struct {
	*fakeServer
	mu        sync.Mutex
	valid     map[string]bool
	revoked   map[string]string
	logins    int
	rejectAll bool
	published int
}

func newTokenBroker(t *testing.T) *tokenBroker {
	b := &tokenBroker{fakeServer: newFakeServer(t), valid: map[string]bool{}, revoked: map[string]string{}}
	b.on("POST /api/v5/login", func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		b.logins++
		tok := fmt.Sprintf("tok%d", b.logins)
		b.valid[tok] = true
		b.mu.Unlock()
		reply(w, 200, `{"token":"`+tok+`","role":"administrator"}`)
	})
	b.on("GET /api/v5/nodes", func(w http.ResponseWriter, r *http.Request) {
		if b.check(w, r) {
			reply(w, 200, "[]")
		}
	})
	b.on("POST /api/v5/publish", func(w http.ResponseWriter, r *http.Request) {
		if b.check(w, r) {
			b.mu.Lock()
			b.published++
			b.mu.Unlock()
			reply(w, 200, `{"id":"1"}`)
		}
	})
	return b
}

func (b *tokenBroker) check(w http.ResponseWriter, r *http.Request) bool {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.valid[tok] && !b.rejectAll {
		return true
	}
	reply(w, 401, `{"code":"`+cmpOr(b.revoked[tok], "TOKEN_TIME_OUT")+`","message":"token rejected"}`)
	return false
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func (b *tokenBroker) revoke(tok, code string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.valid, tok)
	b.revoked[tok] = code
}

func (b *tokenBroker) stats() (logins, published int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.logins, b.published
}

func loginClient(t *testing.T, b *tokenBroker) *EMQX {
	return emqxClient(t, b.fakeServer, func(cfg *config.MQTTREST) {
		cfg.Auth = config.HTTPAuth{Type: config.HTTPAuthLogin, Username: "admin", Password: config.NewSecret("public")}
	})
}

func TestEdgeLoginTokenExpiresMidSequence(t *testing.T) {
	b := newTokenBroker(t)
	c := loginClient(t, b)
	ctx := context.Background()

	if _, err := c.Nodes(ctx); err != nil {
		t.Fatal(err)
	}
	b.revoke("tok1", "TOKEN_TIME_OUT")
	if _, err := c.Nodes(ctx); err != nil || b.last().Auth != "Bearer tok2" {
		t.Fatalf("after expiry: %v, auth %q", err, b.last().Auth)
	}
	// A POST rejected for its token was not processed, so it is sent again
	// once with the new token: published exactly once.
	b.revoke("tok2", "BAD_TOKEN")
	if _, err := c.Publish(ctx, Message{Topic: "t", Payload: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if logins, published := b.stats(); logins != 3 || published != 1 || b.last().Auth != "Bearer tok3" {
		t.Errorf("logins = %d published = %d auth = %q", logins, published, b.last().Auth)
	}

	// A broker that rejects fresh tokens too: one renewal, then the error,
	// not a login loop.
	b.mu.Lock()
	b.rejectAll = true
	b.mu.Unlock()
	var apiErr *APIError
	if _, err := c.Nodes(ctx); !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Errorf("err = %v", err)
	}
	if logins, _ := b.stats(); logins != 4 {
		t.Errorf("logins = %d, want one renewal", logins)
	}
}

func TestEdgeLoginFailures(t *testing.T) {
	f := newFakeServer(t)
	var logins atomic.Int32
	f.on("POST /api/v5/login", func(w http.ResponseWriter, _ *http.Request) {
		logins.Add(1)
		reply(w, 401, `{"code":"BAD_USERNAME_OR_PWD","message":"Auth failed"}`)
	})
	f.on("GET /api/v5/nodes", fixed(200, "[]"))
	c := emqxClient(t, f, func(cfg *config.MQTTREST) {
		cfg.Auth = config.HTTPAuth{Type: config.HTTPAuthLogin, Username: "admin", Password: config.NewSecret("wrong")}
	})
	_, err := c.Nodes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "emqx login") || !strings.Contains(err.Error(), "BAD_USERNAME_OR_PWD") {
		t.Errorf("err = %v", err)
	}
	if n := logins.Load(); n < 1 || n > 3 {
		t.Errorf("logins = %d, want at most one per attempt", n)
	}

	// A login answer without a token is an error, not an empty bearer.
	f.on("POST /api/v5/login", fixed(200, `{"role":"viewer"}`))
	if _, err := c.Nodes(context.Background()); err == nil || !strings.Contains(err.Error(), "no token") {
		t.Errorf("err = %v", err)
	}

	// API-key (basic) auth has nothing to renew: a 401 is returned as is.
	f.on("GET /api/v5/stats", fixed(401, `{"code":"UNAUTHORIZED","message":"bad key"}`))
	n := f.count()
	var apiErr *APIError
	if _, err := emqxClient(t, f).Stats(context.Background()); !errors.As(err, &apiErr) || apiErr.Code != "UNAUTHORIZED" || f.count() != n+1 {
		t.Errorf("basic 401: %v, %d requests", err, f.count()-n)
	}
}

func TestEdgeConcurrentFirstLoginLogsInOnce(t *testing.T) {
	b := newTokenBroker(t)
	c := loginClient(t, b)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if _, err := c.Nodes(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if logins, _ := b.stats(); logins != 1 {
		t.Errorf("logins = %d, want 1", logins)
	}
}

// TestEdgeLoginConcurrentExpiryLogsInOnce: a request that went out with the
// old token and is rejected after another request already logged in again
// must keep the fresh token. Clearing the shared token on every
// TOKEN_TIME_OUT would log in once more per request in flight. One login
// per expiry: 2 in total here.
func TestEdgeLoginConcurrentExpiryLogsInOnce(t *testing.T) {
	f := newFakeServer(t)
	var logins atomic.Int32
	f.on("POST /api/v5/login", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, 200, fmt.Sprintf(`{"token":"tok%d"}`, logins.Add(1)))
	})
	var (
		expired     atomic.Bool
		mu          sync.Mutex
		held        int
		bothIn      = make(chan struct{})
		freshServed = make(chan struct{})
		freshOnce   sync.Once
	)
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok1" {
			reply(w, 200, "[]")
			freshOnce.Do(func() { close(freshServed) })
			return
		}
		if !expired.Load() {
			reply(w, 200, "[]")
			return
		}
		mu.Lock()
		held++
		n := held
		if n == 2 {
			close(bothIn)
		}
		mu.Unlock()
		<-bothIn
		if n == 2 {
			// This request's rejection arrives after the other one renewed.
			<-freshServed
		}
		reply(w, 401, `{"code":"TOKEN_TIME_OUT","message":"expired"}`)
	})
	c := emqxClient(t, f, func(cfg *config.MQTTREST) {
		cfg.Auth = config.HTTPAuth{Type: config.HTTPAuthLogin, Username: "admin", Password: config.NewSecret("public")}
	})
	ctx := context.Background()
	if _, err := c.Nodes(ctx); err != nil {
		t.Fatal(err)
	}
	expired.Store(true)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			if _, err := c.Nodes(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := logins.Load(); n != 2 {
		t.Errorf("logins = %d, want 2 (initial + one renewal for one expiry)", n)
	}
}

func TestEdgePostNeverRetried(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f) // max_retries 2
	ctx := context.Background()
	hangUp := func(w http.ResponseWriter, _ *http.Request) {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			_ = conn.Close()
		}
	}
	// A retried POST can publish, subscribe or kick twice: the first
	// attempt may have been processed even when its answer was an error.
	for _, tc := range []struct {
		route string
		h     func(http.ResponseWriter, *http.Request)
		call  func() error
	}{
		{"POST /api/v5/publish", fixed(503, `{"reason_code":131,"message":"failed_to_dispatch"}`),
			func() error { _, err := c.Publish(ctx, Message{Topic: "t"}); return err }},
		{"POST /api/v5/publish", hangUp,
			func() error { _, err := c.Publish(ctx, Message{Topic: "t"}); return err }},
		{"POST /api/v5/publish/bulk", fixed(500, `{"code":"INTERNAL_ERROR"}`),
			func() error { _, err := c.PublishBatch(ctx, []Message{{Topic: "a"}, {Topic: "b"}}); return err }},
		{"POST /api/v5/clients/c/subscribe", fixed(502, "bad gateway"),
			func() error { return c.Subscribe(ctx, "c", Subscription{Topic: "t"}) }},
		{"POST /api/v5/clients/c/subscribe/bulk", fixed(503, "busy"),
			func() error { return c.Subscribe(ctx, "c", Subscription{Topic: "a"}, Subscription{Topic: "b"}) }},
		{"POST /api/v5/clients/c/unsubscribe", fixed(503, "busy"),
			func() error { return c.Unsubscribe(ctx, "c", "t") }},
		{"POST /api/v5/clients/kickout/bulk", fixed(429, "slow down"),
			func() error { return c.KickClients(ctx, "a", "b") }},
	} {
		f.on(tc.route, tc.h)
		n := f.count()
		if err := tc.call(); err == nil {
			t.Errorf("%s: no error", tc.route)
		}
		if got := f.count() - n; got != 1 {
			t.Errorf("%s: %d requests, want 1", tc.route, got)
		}
	}

	// Idempotent calls are retried on the same answers.
	f.on("DELETE /api/v5/clients/c", fixed(503, "busy"))
	n := f.count()
	if err := c.KickClient(ctx, "c"); err == nil || f.count()-n != 3 {
		t.Errorf("DELETE: %v, %d requests, want 3", err, f.count()-n)
	}
	var calls atomic.Int32
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			reply(w, 429, `{"code":"TOO_MANY_REQUESTS"}`)
			return
		}
		reply(w, 200, "[]")
	})
	if _, err := c.Nodes(ctx); err != nil || calls.Load() != 2 {
		t.Errorf("GET after 429: %v, %d calls", err, calls.Load())
	}

	// The generic publisher follows the same rule.
	f.on("POST /gw/pub", fixed(503, "busy"))
	g, err := NewGeneric(config.MQTTREST{Provider: config.MQTTRESTGeneric, BaseURL: f.URL + "/gw", MaxRetries: 3,
		RetryBackoff: time.Millisecond, Publish: config.MQTTRESTPublish{Path: "/pub", TopicField: "topic", PayloadField: "payload"}})
	if err != nil {
		t.Fatal(err)
	}
	n = f.count()
	if _, err := g.Publish(ctx, Message{Topic: "t"}); err == nil || f.count()-n != 1 {
		t.Errorf("generic publish: %v, %d requests", err, f.count()-n)
	}
}

func TestEdgeRetryBackoffHonoursContext(t *testing.T) {
	f := newFakeServer(t)
	arrived := make(chan struct{}, 8)
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, _ *http.Request) {
		arrived <- struct{}{}
		reply(w, 503, `{"code":"SERVICE_UNAVAILABLE","message":"busy"}`)
	})
	c := emqxClient(t, f, func(cfg *config.MQTTREST) { cfg.RetryBackoff, cfg.MaxRetries = time.Hour, 5 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-arrived
		cancel()
	}()
	start := time.Now()
	if _, err := c.Nodes(ctx); err == nil {
		t.Error("a cancelled call must fail")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("slept %v through a cancelled context", d)
	}
	if n := f.count(); n != 1 {
		t.Errorf("requests = %d, want none after the cancel", n)
	}
}

func TestEdgeMalformedResponses(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()

	f.on("GET /api/v5/clients/c1", fixed(200, `{"clientid":"c1","connected":tr`))
	if _, err := c.GetClient(ctx, "c1"); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Errorf("truncated client = %v", err)
	}
	f.on("GET /api/v5/clients/c2", fixed(200, `{"clientid":"c2","connected_at":"yesterday"}`))
	if _, err := c.GetClient(ctx, "c2"); err == nil || !strings.Contains(err.Error(), "parse time") {
		t.Errorf("bad time = %v", err)
	}
	f.on("GET /api/v5/nodes", fixed(200, `{"not":"a list"}`))
	if _, err := c.Nodes(ctx); err == nil {
		t.Error("an object for a list should fail")
	}
	f.on("POST /api/v5/publish", fixed(200, `<html>ok</html>`))
	if _, err := c.Publish(ctx, Message{Topic: "t"}); err == nil {
		t.Error("an HTML publish answer should fail")
	}
	// A bulk answer that is not a list says nothing about the messages.
	f.on("POST /api/v5/publish/bulk", fixed(200, `{"id":"1"}`))
	var apiErr *APIError
	if res, err := c.PublishBatch(ctx, []Message{{Topic: "a"}}); res != nil || !errors.As(err, &apiErr) || apiErr.StatusCode != 200 {
		t.Errorf("bulk object = %v, %v", res, err)
	}
	// Error bodies are kept for diagnosis but bounded.
	f.on("GET /api/v5/clients/ghost", fixed(404, "<html>"+strings.Repeat("x", 4000)+"</html>"))
	_, err := c.GetClient(ctx, "ghost")
	if !errors.Is(err, ErrNotFound) || !errors.As(err, &apiErr) || len(apiErr.Body) > 515 || !strings.HasPrefix(apiErr.Body, "<html>") {
		t.Errorf("404 html = %v", err)
	}
}

func TestEdgeGenericBatchOneByOne(t *testing.T) {
	f := newFakeServer(t)
	var n atomic.Int32
	f.on("POST /gw/pub", func(w http.ResponseWriter, _ *http.Request) {
		if f.last().Body["topic"] == "bad" { // fakeServer already read the body
			reply(w, 400, `{"message":"bad topic"}`)
			return
		}
		reply(w, 200, fmt.Sprintf(`{"id":%d}`, n.Add(1)))
	})
	g, err := NewGeneric(config.MQTTREST{Provider: config.MQTTRESTGeneric, BaseURL: f.URL + "/gw",
		Publish: config.MQTTRESTPublish{Path: "/pub", TopicField: "topic", PayloadField: "payload"}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := g.PublishBatch(context.Background(), []Message{{Topic: "a"}, {Topic: "bad"}, {Topic: "c"}})
	// One failure must not hide the outcome of the others: each message
	// gets its own result, in order, and every message is tried once.
	if len(res) != 3 || res[0].Err != nil || res[0].ID != "1" || res[1].Err == nil || res[2].Err != nil || res[2].ID != "2" {
		t.Errorf("results = %+v", res)
	}
	if err == nil || !strings.Contains(err.Error(), "message 1 (bad)") || f.count() != 3 {
		t.Errorf("err = %v, requests = %d", err, f.count())
	}
}

// TestEdgeGenericPublishNonJSON2xx: Generic.Publish documents "any 2xx
// response is a success", including a plain-text answer such as
// "accepted" (with an empty ID). An error there would make a caller that
// retries publish the message twice, and PublishBatch report every such
// message failed.
func TestEdgeGenericPublishNonJSON2xx(t *testing.T) {
	f := newFakeServer(t)
	f.on("POST /gw/pub", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "accepted")
	})
	g, err := NewGeneric(config.MQTTREST{Provider: config.MQTTRESTGeneric, BaseURL: f.URL + "/gw",
		Publish: config.MQTTRESTPublish{Path: "/pub", TopicField: "topic", PayloadField: "payload"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Publish(context.Background(), Message{Topic: "t", Payload: []byte("x")}); err != nil {
		t.Errorf("Publish = %v; the gateway answered 202", err)
	}
}

// TestEdgeGenericPlainBinaryPayloadNotAltered: with payload_encoding plain,
// the payload travels as a JSON string, and encoding/json replaces invalid
// UTF-8 with U+FFFD. Either the exact bytes arrive or Publish refuses the
// payload; sending 0xff/0xfe as U+FFFD and reporting success is not an
// option.
func TestEdgeGenericPlainBinaryPayloadNotAltered(t *testing.T) {
	f := newFakeServer(t)
	f.on("POST /gw/pub", fixed(200, `{}`))
	g, err := NewGeneric(config.MQTTREST{Provider: config.MQTTRESTGeneric, BaseURL: f.URL + "/gw",
		Publish: config.MQTTRESTPublish{Path: "/pub", TopicField: "topic", PayloadField: "payload", PayloadEncoding: "plain"}})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{0xff, 0x00, 0xfe, 'a'}
	if _, err := g.Publish(context.Background(), Message{Topic: "t", Payload: payload}); err != nil {
		return // refusing the payload is a correct outcome
	}
	if got, _ := f.last().Body["payload"].(string); got != string(payload) {
		t.Errorf("broker got %q, want %q", got, payload)
	}
}
