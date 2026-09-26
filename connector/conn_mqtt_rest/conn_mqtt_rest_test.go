package conn_mqtt_rest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

type recorded struct {
	Method, RawPath, Query, Auth, ContentType string
	Body                                      map[string]any
	BodyList                                  []any
}

// fakeServer records requests and answers from a route table keyed by
// "METHOD /escaped/path".
type fakeServer struct {
	*httptest.Server
	mu     sync.Mutex
	reqs   []recorded
	routes map[string]func(w http.ResponseWriter, r *http.Request)
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{routes: map[string]func(http.ResponseWriter, *http.Request){}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		rec := recorded{Method: r.Method, RawPath: r.URL.EscapedPath(), Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization"), ContentType: r.Header.Get("Content-Type")}
		_ = json.Unmarshal(data, &rec.Body)
		_ = json.Unmarshal(data, &rec.BodyList)
		f.mu.Lock()
		f.reqs = append(f.reqs, rec)
		h := f.routes[r.Method+" "+rec.RawPath]
		f.mu.Unlock()
		if h == nil {
			reply(w, http.StatusNotFound, `{"code":"NOT_FOUND","message":"no route"}`)
			return
		}
		h(w, r)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeServer) on(route string, h func(w http.ResponseWriter, r *http.Request)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.routes[route] = h
}

func (f *fakeServer) last() recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reqs[len(f.reqs)-1]
}

func (f *fakeServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func reply(w http.ResponseWriter, status int, body string) {
	if body != "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func fixed(status int, body string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) { reply(w, status, body) }
}

func emqxClient(t *testing.T, f *fakeServer, mut ...func(*config.MQTTREST)) *EMQX {
	t.Helper()
	cfg := config.MQTTREST{
		Provider:     config.MQTTRESTEMQX,
		BaseURL:      f.URL,
		Auth:         config.HTTPAuth{Username: "key", Password: config.NewSecret("secret")},
		Timeout:      5 * time.Second,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
	}
	for _, m := range mut {
		m(&cfg)
	}
	c, err := NewEMQX(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPublish(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()

	f.on("POST /api/v5/publish", fixed(200, `{"id":"0006A1B2C3D4E5F6"}`))
	res, err := c.Publish(ctx, Message{Topic: "a/b", Payload: []byte("hello"), QoS: 1, Retain: true,
		Properties: &Properties{MessageExpiry: time.Minute, UserProperties: map[string]string{"k": "v"}}})
	if err != nil || res.ID != "0006A1B2C3D4E5F6" || res.NoSubscribers {
		t.Fatalf("res = %+v, %v", res, err)
	}
	req := f.last()
	if req.Auth != "Basic a2V5OnNlY3JldA==" || req.ContentType != "application/json" {
		t.Errorf("headers: auth=%q content-type=%q", req.Auth, req.ContentType)
	}
	b := req.Body
	props, _ := b["properties"].(map[string]any)
	if b["topic"] != "a/b" || b["payload"] != "hello" || b["payload_encoding"] != "plain" || b["qos"] != 1.0 || b["retain"] != true ||
		props["message_expiry_interval"] != 60.0 || props["user_properties"].(map[string]any)["k"] != "v" {
		t.Errorf("body = %v", b)
	}

	// Binary payloads are sent base64 encoded.
	_, _ = c.Publish(ctx, Message{Topic: "bin", Payload: []byte{0xff, 0x00}})
	if b := f.last().Body; b["payload_encoding"] != "base64" || b["payload"] != "/wA=" {
		t.Errorf("binary body = %v", b)
	}

	f.on("POST /api/v5/publish", fixed(202, `{"reason_code":16,"message":"no_matching_subscribers"}`))
	res, err = c.Publish(ctx, Message{Topic: "nobody"})
	if err != nil || !res.NoSubscribers || res.ReasonCode != ReasonNoMatchingSubscribers {
		t.Errorf("no subscribers = %+v, %v", res, err)
	}

	f.on("POST /api/v5/publish", fixed(400, `{"reason_code":144,"message":"topic_name_invalid"}`))
	var apiErr *APIError
	if _, err = c.Publish(ctx, Message{Topic: "a/#"}); !errors.As(err, &apiErr) || apiErr.ReasonCode != 144 {
		t.Errorf("invalid topic err = %v", err)
	}

	// Publishes are not retried.
	f.on("POST /api/v5/publish", fixed(503, `{"reason_code":131,"message":"failed_to_dispatch"}`))
	n := f.count()
	if _, err = c.Publish(ctx, Message{Topic: "x"}); err == nil || f.count() != n+1 {
		t.Errorf("err = %v, requests = %d", err, f.count()-n)
	}
}

func TestPublishBatch(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	msgs := []Message{{Topic: "a"}, {Topic: "b"}, {Topic: "c"}}

	f.on("POST /api/v5/publish/bulk", fixed(503, `[{"id":"1"},{"reason_code":16,"message":"no_matching_subscribers"},{"reason_code":131,"message":"failed_to_dispatch"}]`))
	res, err := c.PublishBatch(context.Background(), msgs)
	if len(res) != 3 || res[0].ID != "1" || !res[1].NoSubscribers || res[1].Err != nil || res[2].Err == nil {
		t.Fatalf("results = %+v", res)
	}
	if err == nil || !strings.Contains(err.Error(), "message 2 (c)") {
		t.Errorf("err = %v", err)
	}
	if body := f.last().BodyList; len(body) != 3 {
		t.Errorf("body = %v", body)
	}

	f.on("POST /api/v5/publish/bulk", fixed(400, `{"code":"BAD_REQUEST","message":"bad"}`))
	if res, err := c.PublishBatch(context.Background(), msgs); err == nil || res != nil {
		t.Errorf("invalid batch = %v, %v", res, err)
	}

	n := f.count()
	if res, err := c.PublishBatch(context.Background(), nil); res != nil || err != nil || f.count() != n {
		t.Error("empty batch must not call the API")
	}
}

func TestClients(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()

	f.on("GET /api/v5/clients", fixed(200, `{"data":[{"clientid":"c1","username":null,"connected":true,"connected_at":"2026-01-02T03:04:05.678+08:00","created_at":1767294245678,"subscriptions_cnt":2,"subscriptions_max":"infinity"}],"meta":{"page":1,"limit":10,"hasnext":false}}`))
	clean := true
	page, err := c.ListClients(ctx, ClientQuery{PageQuery: PageQuery{Limit: 10}, ClientIDs: []string{"c1", "c 2"}, LikeUsername: "bo", ConnState: "connected", CleanStart: &clean})
	if err != nil {
		t.Fatal(err)
	}
	cl := page.Data[0]
	if cl.ClientID != "c1" || !cl.Connected || cl.SubscriptionsCnt != 2 || cl.ConnectedAt.Year() != 2026 || cl.CreatedAt.IsZero() || page.Meta.Count != nil {
		t.Errorf("page = %+v", page)
	}
	q := f.last().Query
	for _, want := range []string{"clientid=c1", "clientid=c+2", "like_username=bo", "conn_state=connected", "clean_start=true", "limit=10"} {
		if !strings.Contains(q, want) {
			t.Errorf("query %q missing %q", q, want)
		}
	}

	f.on("GET /api/v5/clients/dev%2F1%2Bx", fixed(200, `{"clientid":"dev/1+x"}`))
	got, err := c.GetClient(ctx, "dev/1+x")
	if err != nil || got.ClientID != "dev/1+x" {
		t.Errorf("get = %+v, %v (path %s)", got, err, f.last().RawPath)
	}

	f.on("GET /api/v5/clients/ghost", fixed(404, `{"code":"CLIENTID_NOT_FOUND","message":"Client ID not found"}`))
	if _, err := c.GetClient(ctx, "ghost"); !errors.Is(err, ErrNotFound) || !strings.Contains(err.Error(), "CLIENTID_NOT_FOUND") {
		t.Errorf("not found err = %v", err)
	}

	f.on("DELETE /api/v5/clients/c1", fixed(204, ""))
	if err := c.KickClient(ctx, "c1"); err != nil {
		t.Error(err)
	}
	f.on("POST /api/v5/clients/kickout/bulk", fixed(204, ""))
	if err := c.KickClients(ctx, "a", "b"); err != nil || len(f.last().BodyList) != 2 {
		t.Errorf("bulk kick: %v %v", err, f.last().BodyList)
	}
}

func TestSubscriptions(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()

	f.on("POST /api/v5/clients/c1/subscribe", fixed(200, `{"clientid":"c1","topic":"t/1","qos":1,"nl":0,"rap":0,"rh":0,"node":"n1"}`))
	if err := c.Subscribe(ctx, "c1", Subscription{Topic: "t/1", QoS: 1}); err != nil {
		t.Fatal(err)
	}
	if b := f.last().Body; b["topic"] != "t/1" || b["qos"] != 1.0 {
		t.Errorf("subscribe body = %v", b)
	}

	f.on("POST /api/v5/clients/c1/subscribe/bulk", fixed(200, `{"succeed":[{"topic":"a"}],"failed":[{"data":["c1","$bad",{"qos":0}],"reason":"invalid_topic"}]}`))
	err := c.Subscribe(ctx, "c1", Subscription{Topic: "a"}, Subscription{Topic: "$bad"})
	if err == nil || !strings.Contains(err.Error(), "invalid_topic") {
		t.Errorf("partial failure err = %v", err)
	}
	f.on("POST /api/v5/clients/c1/subscribe/bulk", fixed(200, `[{"topic":"a"},{"topic":"b"}]`))
	if err := c.Subscribe(ctx, "c1", Subscription{Topic: "a"}, Subscription{Topic: "b"}); err != nil {
		t.Errorf("bulk subscribe: %v", err)
	}

	f.on("POST /api/v5/clients/c1/unsubscribe/bulk", fixed(204, ""))
	if err := c.Unsubscribe(ctx, "c1", "a", "b"); err != nil || f.last().BodyList[1].(map[string]any)["topic"] != "b" {
		t.Errorf("unsubscribe: %v %v", err, f.last().BodyList)
	}

	f.on("GET /api/v5/clients/c1/subscriptions", fixed(200, `[{"topic":"a","qos":1,"clientid":"c1"}]`))
	if subs, err := c.ClientSubscriptions(ctx, "c1"); err != nil || len(subs) != 1 || subs[0].QoS != 1 {
		t.Errorf("client subs = %v, %v", subs, err)
	}

	f.on("GET /api/v5/subscriptions", fixed(200, `{"data":[{"topic":"$share/g/t","clientid":"c1","qos":0,"durable":false}],"meta":{"page":1,"limit":100,"hasnext":true,"count":7}}`))
	qos := byte(0)
	page, err := c.ListSubscriptions(ctx, SubscriptionQuery{MatchTopic: "t/+", QoS: &qos})
	if err != nil || *page.Meta.Count != 7 || !page.Meta.HasNext || !strings.Contains(f.last().Query, "match_topic=t%2F%2B") {
		t.Errorf("subs = %+v, %v, query %s", page, err, f.last().Query)
	}

	f.on("GET /api/v5/topics", fixed(200, `{"data":[{"topic":"t/1","node":"n1"}],"meta":{"page":1,"limit":100,"hasnext":false,"count":1}}`))
	if routes, err := c.ListTopics(ctx, TopicQuery{Node: "n1"}); err != nil || routes.Data[0].Node != "n1" {
		t.Errorf("topics = %+v, %v", routes, err)
	}
}

func TestRetainedNodesStats(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	ctx := context.Background()

	f.on("GET /api/v5/mqtt/retainer/message/a%2Fb", fixed(200, `{"msgid":"ab","topic":"a/b","qos":1,"publish_at":"2026-01-02T03:04:05+00:00","payload":"aGVsbG8="}`))
	m, err := c.GetRetained(ctx, "a/b")
	if err != nil || string(m.Payload) != "hello" || m.PublishAt.IsZero() {
		t.Errorf("retained = %+v, %v", m, err)
	}
	f.on("DELETE /api/v5/mqtt/retainer/message/a%2F%23", fixed(204, ""))
	if err := c.DeleteRetained(ctx, "a/#"); err != nil {
		t.Error(err)
	}
	f.on("GET /api/v5/mqtt/retainer/messages", fixed(200, `{"data":[{"msgid":"ab","topic":"a/b"}],"meta":{"page":1,"limit":100,"hasnext":false}}`))
	if page, err := c.ListRetained(ctx, RetainedQuery{Topic: "a/#"}); err != nil || len(page.Data) != 1 {
		t.Errorf("list retained = %+v, %v", page, err)
	}

	f.on("GET /api/v5/nodes", fixed(200, `[{"node":"emqx@n1","node_status":"running","memory_total":"7.63G","uptime":1000}]`))
	if nodes, err := c.Nodes(ctx); err != nil || nodes[0].MemoryTotal != "7.63G" {
		t.Errorf("nodes = %+v, %v", nodes, err)
	}
	f.on("GET /api/v5/stats", fixed(200, `{"connections.count":3,"topics.max":10}`))
	if stats, err := c.Stats(ctx); err != nil || stats["connections.count"] != 3 || f.last().Query != "aggregate=true" {
		t.Errorf("stats = %v, %v", stats, err)
	}
}

func TestRetries(t *testing.T) {
	f := newFakeServer(t)
	c := emqxClient(t, f)
	var calls atomic.Int32
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) < 3 {
			reply(w, 503, `{"code":"INTERNAL_ERROR","message":"busy"}`)
			return
		}
		reply(w, 200, `[]`)
	})
	if _, err := c.Nodes(context.Background()); err != nil || calls.Load() != 3 {
		t.Errorf("err = %v calls = %d", err, calls.Load())
	}

	calls.Store(0)
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		reply(w, 500, `{"code":"INTERNAL_ERROR","message":"down"}`)
	})
	if _, err := c.Nodes(context.Background()); err == nil || calls.Load() != 3 {
		t.Errorf("err = %v calls = %d (want 1 + 2 retries)", err, calls.Load())
	}
}

func TestLoginAuth(t *testing.T) {
	f := newFakeServer(t)
	var logins atomic.Int32
	f.on("POST /api/v5/login", func(w http.ResponseWriter, r *http.Request) {
		n := logins.Add(1)
		reply(w, 200, `{"token":"tok`+string(rune('0'+n))+`","role":"administrator"}`)
	})
	var expired atomic.Bool
	f.on("GET /api/v5/nodes", func(w http.ResponseWriter, r *http.Request) {
		if expired.CompareAndSwap(true, false) {
			reply(w, 401, `{"code":"TOKEN_TIME_OUT","message":"expired"}`)
			return
		}
		reply(w, 200, `[]`)
	})
	c := emqxClient(t, f, func(cfg *config.MQTTREST) {
		cfg.Auth = config.HTTPAuth{Type: config.HTTPAuthLogin, Username: "admin", Password: config.NewSecret("public")}
	})

	if _, err := c.Nodes(context.Background()); err != nil || f.last().Auth != "Bearer tok1" {
		t.Fatalf("err = %v auth = %q", err, f.last().Auth)
	}
	expired.Store(true)
	if _, err := c.Nodes(context.Background()); err != nil || f.last().Auth != "Bearer tok2" || logins.Load() != 2 {
		t.Errorf("renew: err = %v auth = %q logins = %d", err, f.last().Auth, logins.Load())
	}
}

func TestBaseURLAndNew(t *testing.T) {
	f := newFakeServer(t)
	f.on("GET /api/v5/nodes", fixed(200, `[]`))
	c := emqxClient(t, f, func(cfg *config.MQTTREST) { cfg.BaseURL = f.URL + "/api/v5/" })
	if _, err := c.Nodes(context.Background()); err != nil || f.last().RawPath != "/api/v5/nodes" {
		t.Errorf("path = %s, %v", f.last().RawPath, err)
	}

	b, err := New(config.MQTTREST{Provider: config.MQTTRESTEMQX, BaseURL: f.URL})
	if _, ok := b.(Admin); err != nil || !ok {
		t.Errorf("New(emqx) = %T, %v", b, err)
	}
	if _, err := New(config.MQTTREST{Provider: config.MQTTRESTGeneric, BaseURL: f.URL}); err == nil {
		t.Error("generic without publish.path should fail validation")
	}
}

func TestGeneric(t *testing.T) {
	f := newFakeServer(t)
	cfg := config.MQTTREST{
		Provider: config.MQTTRESTGeneric,
		BaseURL:  f.URL + "/gw",
		Auth:     config.HTTPAuth{Token: config.NewSecret("tok")},
		Headers:  map[string]string{"X-Tenant": "t1"},
		Timeout:  5 * time.Second,
		Publish: config.MQTTRESTPublish{
			Method: "PUT", Path: "/v1/messages",
			TopicField: "destination", PayloadField: "data", QoSField: "", RetainField: "retained",
			PayloadEncoding: "base64", EncodingField: "encoding",
			Extra: map[string]any{"source": "svc"},
		},
	}
	g, err := NewGeneric(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.on("PUT /gw/v1/messages", fixed(201, `{"id":42}`))
	res, err := g.Publish(context.Background(), Message{Topic: "a/b", Payload: []byte("hi"), QoS: 1, Retain: true})
	if err != nil || res.ID != "42" {
		t.Fatalf("res = %+v, %v", res, err)
	}
	req := f.last()
	b := req.Body
	if req.Auth != "Bearer tok" || b["destination"] != "a/b" || b["data"] != "aGk=" || b["encoding"] != "base64" || b["retained"] != true || b["source"] != "svc" {
		t.Errorf("request = %+v", req)
	}
	if _, hasQoS := b["qos"]; hasQoS {
		t.Error("empty qos_field must omit qos")
	}

	// Without batch_path, batches are sent one by one.
	results, err := g.PublishBatch(context.Background(), []Message{{Topic: "x"}, {Topic: "y"}})
	if err != nil || len(results) != 2 || f.last().Body["destination"] != "y" {
		t.Errorf("batch = %+v, %v", results, err)
	}

	cfg.Publish.BatchPath = "/v1/messages:batch"
	g, _ = NewGeneric(cfg)
	f.on("PUT /gw/v1/messages:batch", fixed(200, ""))
	if _, err := g.PublishBatch(context.Background(), []Message{{Topic: "x"}, {Topic: "y"}}); err != nil || len(f.last().BodyList) != 2 {
		t.Errorf("batch path: %v %v", err, f.last().BodyList)
	}

	var out map[string]any
	f.on("GET /gw/other", fixed(200, `{"ok":true}`))
	if err := g.Do(context.Background(), http.MethodGet, "/other", nil, nil, &out); err != nil || out["ok"] != true {
		t.Errorf("do = %v, %v", out, err)
	}
}
