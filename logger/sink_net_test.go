package logger_test

import (
	"bufio"
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

func netLogger(t *testing.T, out config.LogOutput, opts ...logger.Option) *logger.Logger {
	t.Helper()
	out.Name = cmp.Or(out.Name, "net")
	cfg := logConfig("debug", out)
	cfg.Service = "api"
	l, err := logger.New(context.Background(), cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func udpListener(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	return c
}

func readUDP(t *testing.T, c *net.UDPConn) []byte {
	t.Helper()
	buf := make([]byte, 65536)
	n, _, err := c.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	return buf[:n]
}

// tcpListener accepts one connection and returns everything it receives
// until it is closed.
func tcpListener(t *testing.T) (string, func() []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	data := make(chan []byte, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			data <- nil
			return
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		b, _ := io.ReadAll(c)
		data <- b
	}()
	return ln.Addr().String(), func() []byte {
		select {
		case b := <-data:
			return b
		case <-time.After(5 * time.Second):
			t.Fatal("no tcp data")
			return nil
		}
	}
}

var rfc5424 = regexp.MustCompile(`^<(\d+)>1 (\S+) (\S+) api (\d+) - - (\{.*\})$`)

func TestSyslogUDP(t *testing.T) {
	c := udpListener(t)
	l := netLogger(t, config.LogOutput{Type: "syslog", Network: "udp", Addr: c.LocalAddr().String(), Timeout: time.Second})
	defer l.Close()
	l.Warn("disk low", "free", "1G")

	m := rfc5424.FindSubmatch(readUDP(t, c))
	if m == nil {
		t.Fatal("not an RFC 5424 message")
	}
	// local0 (16) * 8 + warning (4).
	if string(m[1]) != "132" {
		t.Errorf("pri = %s", m[1])
	}
	if _, err := time.Parse(time.RFC3339Nano, string(m[2])); err != nil {
		t.Errorf("timestamp: %v", err)
	}
	var rec map[string]any
	if err := json.Unmarshal(m[5], &rec); err != nil || rec["msg"] != "disk low" || rec["free"] != "1G" {
		t.Errorf("body = %s (%v)", m[5], err)
	}
}

func TestSyslogTCPFraming(t *testing.T) {
	addr, received := tcpListener(t)
	l := netLogger(t, config.LogOutput{Type: "syslog", Network: "tcp", Addr: addr, Facility: "user", Timeout: time.Second})
	l.Error("one")
	l.Info("two")
	_ = l.Close()

	// Octet counting: "LEN MSG" back to back.
	r := bufio.NewReader(bytes.NewReader(received()))
	for _, want := range []struct {
		pri int
		msg string
	}{{8 + 3, "one"}, {8 + 6, "two"}} {
		lenStr, err := r.ReadString(' ')
		if err != nil {
			t.Fatal(err)
		}
		n, _ := strconv.Atoi(strings.TrimSpace(lenStr))
		frame := make([]byte, n)
		if _, err := io.ReadFull(r, frame); err != nil {
			t.Fatal(err)
		}
		m := rfc5424.FindSubmatch(frame)
		if m == nil || string(m[1]) != strconv.Itoa(want.pri) || !bytes.Contains(m[5], []byte(`"msg":"`+want.msg+`"`)) {
			t.Errorf("frame = %q", frame)
		}
	}

	addr, received = tcpListener(t)
	l = netLogger(t, config.LogOutput{Type: "syslog", Network: "tcp", Addr: addr, SyslogFormat: "rfc3164", Tag: "worker", Timeout: time.Second})
	l.Info("legacy")
	_ = l.Close()
	line := string(received())
	if !regexp.MustCompile(`^<134>[A-Z][a-z]{2} [ \d]\d \d\d:\d\d:\d\d \S+ worker\[\d+\]: \{.*"msg":"legacy".*\}\n$`).MatchString(line) {
		t.Errorf("rfc3164 line = %q", line)
	}
}

func TestSyslogErrors(t *testing.T) {
	for name, out := range map[string]config.LogOutput{
		"facility": {Type: "syslog", Network: "udp", Addr: "127.0.0.1:1", Facility: "kitchen"},
		"udp tls":  {Type: "syslog", Network: "udp", Addr: "127.0.0.1:1", TLS: config.TLS{Enabled: true}},
		"network":  {Type: "syslog", Network: "sctp", Addr: "127.0.0.1:1"},
	} {
		out.Name = "s"
		if _, err := logger.New(context.Background(), logConfig("info", out)); err == nil {
			t.Errorf("%s: should fail", name)
		}
	}
}

func gelfDecode(t *testing.T, b []byte) map[string]any {
	t.Helper()
	if len(b) > 2 && b[0] == 0x1f && b[1] == 0x8b {
		zr, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		b, _ = io.ReadAll(zr)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("gelf %q: %v", b, err)
	}
	return m
}

func TestGELFUDP(t *testing.T) {
	c := udpListener(t)
	l := netLogger(t, config.LogOutput{Type: "gelf", Addr: c.LocalAddr().String(), Compress: true, Timeout: time.Second})
	defer l.Close()
	l.Error("failed", "id", 7, "req", map[string]any{"method": "GET"}, "ok", false)

	m := gelfDecode(t, readUDP(t, c))
	if m["version"] != "1.1" || m["short_message"] != "failed" || m["level"] != float64(3) || m["_level_name"] != "ERROR" ||
		m["_service"] != "api" || m["_id_"] != float64(7) || m["_req_method"] != "GET" || m["_ok"] != "false" {
		t.Errorf("gelf = %v", m)
	}
	if full, _ := m["full_message"].(string); !strings.HasPrefix(full, "failed\n") || !strings.Contains(full, "TestGELFUDP") {
		t.Errorf("full_message = %q", full)
	}
	if ts, _ := m["timestamp"].(float64); time.Since(time.Unix(int64(ts), 0)) > time.Minute {
		t.Errorf("timestamp = %v", m["timestamp"])
	}
}

func TestGELFChunking(t *testing.T) {
	c := udpListener(t)
	l := netLogger(t, config.LogOutput{Type: "gelf", Addr: c.LocalAddr().String(), ChunkSize: 200, Timeout: time.Second})
	defer l.Close()
	big := strings.Repeat("x", 1000)
	l.Info("big", "payload", big)

	var (
		id     []byte
		chunks [][]byte
		count  int
	)
	for {
		b := readUDP(t, c)
		if b[0] != 0x1e || b[1] != 0x0f {
			t.Fatalf("not a chunk: %q", b[:12])
		}
		if id == nil {
			id, count = b[2:10], int(b[11])
			chunks = make([][]byte, count)
		} else if !bytes.Equal(id, b[2:10]) || int(b[11]) != count {
			t.Fatal("chunk id or count changed")
		}
		if len(b) > 200 {
			t.Errorf("chunk of %d bytes", len(b))
		}
		chunks[b[10]] = b[12:]
		done := true
		for _, ch := range chunks {
			done = done && ch != nil
		}
		if done {
			break
		}
	}
	m := gelfDecode(t, bytes.Join(chunks, nil))
	if m["_payload"] != big || count < 5 {
		t.Errorf("reassembled %d chunks: %v", count, m["short_message"])
	}
}

func TestGELFTCP(t *testing.T) {
	addr, received := tcpListener(t)
	l := netLogger(t, config.LogOutput{Type: "gelf", Network: "tcp", Addr: addr, Timeout: time.Second})
	l.Info("a")
	l.Info("b")
	_ = l.Close()
	frames := bytes.Split(bytes.TrimSuffix(received(), []byte{0}), []byte{0})
	if len(frames) != 2 || gelfDecode(t, frames[0])["short_message"] != "a" || gelfDecode(t, frames[1])["short_message"] != "b" {
		t.Errorf("frames = %q", frames)
	}
}

// bulkServer is a fake Elasticsearch bulk endpoint.
type bulkServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []bulkRequest
	respond  func(n int) (int, string)
}

type bulkRequest struct {
	header  http.Header
	actions []map[string]map[string]string
	docs    []map[string]any
}

func newBulkServer(t *testing.T, respond func(n int) (int, string)) *bulkServer {
	s := &bulkServer{respond: respond}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			body, _ = gzip.NewReader(r.Body)
		}
		data, _ := io.ReadAll(body)
		req := bulkRequest{header: r.Header.Clone()}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		for i := 0; i+1 < len(lines); i += 2 {
			var action map[string]map[string]string
			var doc map[string]any
			_ = json.Unmarshal([]byte(lines[i]), &action)
			_ = json.Unmarshal([]byte(lines[i+1]), &doc)
			req.actions = append(req.actions, action)
			req.docs = append(req.docs, doc)
		}
		if r.URL.Path != "/_bulk" || r.Method != http.MethodPost {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		s.mu.Lock()
		n := len(s.requests)
		s.requests = append(s.requests, req)
		s.mu.Unlock()
		status, resp := s.respond(n)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, resp)
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *bulkServer) all() []bulkRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]bulkRequest(nil), s.requests...)
}

func TestElasticsearchBulk(t *testing.T) {
	srv := newBulkServer(t, func(int) (int, string) { return 200, `{"errors":false,"items":[]}` })
	l := netLogger(t, config.LogOutput{
		Type: "elasticsearch", Addresses: []string{srv.URL + "/"}, Username: "elastic", Password: config.NewSecret("pw"), Compress: true,
		Headers: map[string]string{"X-Tenant": "t1"}, BatchSize: 2, FlushInterval: time.Hour, Timeout: 5 * time.Second,
	})
	l.Info("one", "user", "u1")
	l.Info("two")
	l.Warn("three")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	reqs := srv.all()
	if len(reqs) != 2 || len(reqs[0].docs) != 2 || len(reqs[1].docs) != 1 {
		t.Fatalf("batches = %d", len(reqs))
	}
	h := reqs[0].header
	if u, p, _ := (&http.Request{Header: h}).BasicAuth(); u != "elastic" || p != "pw" || h.Get("X-Tenant") != "t1" ||
		h.Get("Content-Type") != "application/x-ndjson" || h.Get("Content-Encoding") != "gzip" {
		t.Errorf("headers = %v", h)
	}
	if idx := reqs[0].actions[0]["create"]["_index"]; idx != "logs-api-default" {
		t.Errorf("index = %s", idx)
	}
	doc := reqs[0].docs[0]
	ts, _ := doc["@timestamp"].(string)
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil || doc["msg"] != "one" || doc["user"] != "u1" || doc["service"] != "api" {
		t.Errorf("doc = %v", doc)
	}
	if reqs[1].docs[0]["msg"] != "three" {
		t.Errorf("Close did not flush the last record: %v", reqs[1].docs)
	}
}

// Elasticsearch 9 maps host as an ECS object in logs-* data streams and
// rejects a record whose host is a string, so the sink nests it.
func TestElasticsearchNestsHost(t *testing.T) {
	srv := newBulkServer(t, func(int) (int, string) { return 200, `{"errors":false,"items":[]}` })
	l := netLogger(t, config.LogOutput{
		Type: "elasticsearch", Addresses: []string{srv.URL}, BatchSize: 10, FlushInterval: time.Hour, Timeout: 5 * time.Second,
	})
	l.Info("plain", "host", "web-1")
	l.Info("object", slog.Group("host", "name", "web-2", "ip", "10.0.0.2"))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	reqs := srv.all()
	if len(reqs) != 1 || len(reqs[0].docs) != 2 {
		t.Fatalf("requests = %d", len(reqs))
	}
	plain, object := reqs[0].docs[0], reqs[0].docs[1]
	if h, _ := plain["host"].(map[string]any); h["name"] != "web-1" || plain["msg"] != "plain" || plain["@timestamp"] == nil {
		t.Errorf("plain = %v", plain)
	}
	if h, _ := object["host"].(map[string]any); h["name"] != "web-2" || h["ip"] != "10.0.0.2" {
		t.Errorf("object host changed: %v", object["host"])
	}
}

func TestElasticsearchIndexAndErrors(t *testing.T) {
	var (
		mu   sync.Mutex
		errs []string
	)
	onError := logger.WithErrorHandler(func(_ string, err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err.Error())
	})
	srv := newBulkServer(t, func(n int) (int, string) {
		switch n {
		case 0:
			return 503, "unavailable"
		default:
			return 200, `{"errors":true,"items":[{"create":{"status":201}},{"create":{"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse field [n]"}}}]}`
		}
	})
	l := netLogger(t, config.LogOutput{
		Type: "elasticsearch", Addresses: []string{srv.URL}, APIKey: config.NewSecret("key123"), Index: "App-{service}-{date:2006.01}",
		BatchSize: 10, FlushInterval: time.Hour, Timeout: 5 * time.Second,
	}, onError)
	l.Info("a")
	l.Info("b", "n", "x")
	_ = l.Close()

	reqs := srv.all()
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want a retry after 503", len(reqs))
	}
	if reqs[0].header.Get("Authorization") != "ApiKey key123" {
		t.Errorf("auth = %q", reqs[0].header.Get("Authorization"))
	}
	if idx := reqs[0].actions[0]["create"]["_index"]; idx != "app-api-"+time.Now().UTC().Format("2006.01") {
		t.Errorf("index = %s", idx)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(errs) != 1 || !strings.Contains(errs[0], "1 of 2 records rejected") || !strings.Contains(errs[0], "mapper_parsing_exception") {
		t.Errorf("errors = %v", errs)
	}

	if _, err := logger.New(context.Background(), logConfig("info", config.LogOutput{
		Name: "es", Type: "elasticsearch", Addresses: []string{srv.URL}, Index: "logs-{nope}",
	})); err == nil || !strings.Contains(err.Error(), "unknown placeholder") {
		t.Errorf("bad index: %v", err)
	}
}

func TestElasticsearchQueueFull(t *testing.T) {
	release := make(chan struct{})
	srv := newBulkServer(t, func(int) (int, string) {
		<-release
		return 200, `{"errors":false}`
	})
	var (
		mu   sync.Mutex
		errs []string
	)
	l := netLogger(t, config.LogOutput{
		Type: "elasticsearch", Addresses: []string{srv.URL}, BatchSize: 1, QueueSize: 1, FlushInterval: 10 * time.Millisecond, Timeout: 5 * time.Second,
	}, logger.WithErrorHandler(func(_ string, err error) {
		mu.Lock()
		defer mu.Unlock()
		errs = append(errs, err.Error())
	}))
	for i := range 20 {
		l.Info("burst", "i", i)
	}
	close(release)
	_ = l.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(errs) == 0 || !strings.Contains(strings.Join(errs, ";"), "queue full") {
		t.Errorf("errors = %v", errs)
	}
}

func TestGELFMessageFlattening(t *testing.T) {
	b, err := logger.GELFMessage([]byte(`{"time":"2026-01-02T03:04:05.000Z","level":"INFO","msg":"m","a":{"b c":{"d":1}},"list":[1,2],"n":null}`), "h", "")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["_a_b_c_d"] != float64(1) || m["_list"] != "[1,2]" || m["timestamp"] != 1767323045.0 || m["host"] != "h" {
		t.Errorf("gelf = %v", m)
	}
	if _, ok := m["_n"]; ok {
		t.Error("null fields should be dropped")
	}
	if _, ok := m["_service"]; ok {
		t.Error("no service, no _service")
	}
}
