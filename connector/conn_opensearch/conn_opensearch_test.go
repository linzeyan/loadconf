package conn_opensearch

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opensearch-project/opensearch-go/v4"

	"github.com/linzeyan/loadconf/config"
)

func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func TestSlogLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	lg := SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	req := httptest.NewRequest(http.MethodGet, "https://user:secret@es:9200/_search?q=x", nil)

	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 200}, nil, time.Now(), 5*time.Millisecond)
	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 503}, nil, time.Now(), time.Millisecond)
	_ = lg.LogRoundTrip(req, &http.Response{}, errors.New("connection refused"), time.Now(), time.Millisecond)
	_ = lg.LogRoundTrip(nil, nil, errors.New("no connection"), time.Time{}, 0)
	if lg.RequestBodyEnabled() || lg.ResponseBodyEnabled() {
		t.Error("bodies must not be logged")
	}

	if strings.Contains(buf.String(), "secret") {
		t.Errorf("log leaks credentials: %s", buf.String())
	}
	recs := decodeRecords(t, &buf)
	if len(recs) != 4 {
		t.Fatalf("got %d records", len(recs))
	}
	for i, want := range []string{"DEBUG", "WARN", "ERROR", "ERROR"} {
		if recs[i]["level"] != want {
			t.Errorf("record %d level = %v, want %s", i, recs[i]["level"], want)
		}
	}
	if r := recs[0]; r["method"] != "GET" || r["url"] != "https://es:9200/_search?q=x" || r["status"] != float64(200) || r["duration"] == nil {
		t.Errorf("record = %v", r)
	}
	if _, ok := recs[2]["status"]; ok || recs[2]["error"] != "connection refused" {
		t.Errorf("error record = %v", recs[2])
	}

	// Disabled levels are skipped.
	buf.Reset()
	lg = SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 200}, nil, time.Now(), 0)
	if buf.Len() != 0 {
		t.Errorf("debug record written: %s", buf.String())
	}
}

const infoBody = `{"name":"n1","cluster_name":"test","version":{"distribution":"opensearch","number":"3.2.0"},"tagline":"The OpenSearch Project: https://opensearch.org/"}`

type seen struct {
	mu      sync.Mutex
	headers []http.Header
	paths   []string
}

func (s *seen) last() http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.paths) - 1; i >= 0; i-- {
		if s.paths[i] == "/" {
			return s.headers[i]
		}
	}
	return nil
}

func fakeOS(s *seen, status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if s != nil {
			s.mu.Lock()
			s.headers = append(s.headers, r.Header.Clone())
			s.paths = append(s.paths, r.URL.Path)
			s.mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/" {
			// Discovery and health checks of the client.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}
}

func TestConfigMapping(t *testing.T) {
	oc, err := Config(config.OpenSearch{
		Addresses:             []string{"https://os1:9200", "https://os2:9200"},
		Username:              "admin",
		Password:              config.NewSecret("pw"),
		ResponseHeaderTimeout: 3 * time.Second,
		TLS:                   config.TLS{Enabled: true, ServerName: "os.internal", MinVersion: "1.3"},
		Options: map[string]any{
			"header":                  map[string]any{"x-team": "data"},
			"compress_request_body":   true,
			"max_retries":             5,
			"retry_on_status":         []any{429, 503},
			"disable_retry":           true,
			"discover_nodes_on_start": true,
			"discover_nodes_interval": "1m",
		},
	}, func(c *opensearch.Config) { c.RequestTimeout = 7 * time.Second })
	if err != nil {
		t.Fatal(err)
	}
	if len(oc.Addresses) != 2 || oc.Username != "admin" || oc.Password != "pw" || oc.Header.Get("X-Team") != "data" ||
		!oc.CompressRequestBody || oc.MaxRetries != 5 || len(oc.RetryOnStatus) != 2 || !oc.DisableRetry ||
		oc.DiscoverNodesOnStart == nil || !*oc.DiscoverNodesOnStart || oc.DiscoverNodesInterval != time.Minute ||
		oc.RequestTimeout != 7*time.Second {
		t.Errorf("config = %+v", oc)
	}
	tr, ok := oc.Transport.(*http.Transport)
	if !ok || tr.TLSClientConfig == nil || tr.TLSClientConfig.ServerName != "os.internal" || tr.ResponseHeaderTimeout != 3*time.Second {
		t.Fatalf("transport = %#v", oc.Transport)
	}
	if tr == http.DefaultTransport {
		t.Error("the default transport must not be modified")
	}

	oc, err = Config(config.OpenSearch{Addresses: []string{"http://os:9200"}})
	if err != nil || oc.Transport != nil || oc.DiscoverNodesOnStart != nil {
		t.Errorf("plain config = %+v, %v", oc, err)
	}
}

var noRetry = map[string]any{"disable_retry": true}

func TestConfigRejectsElasticOnlySettings(t *testing.T) {
	_, err := Config(config.OpenSearch{
		Addresses: []string{"http://os:9200"}, APIKey: config.NewSecret("k"), CertificateFingerprint: "ab",
	})
	if err == nil || !strings.Contains(err.Error(), "api_key is not supported") || !strings.Contains(err.Error(), "certificate_fingerprint is not supported") {
		t.Errorf("err = %v", err)
	}
	if _, err := Config(config.OpenSearch{CloudID: "x:y"}); err == nil || !strings.Contains(err.Error(), "cloud_id is not supported") {
		t.Errorf("err = %v", err)
	}
	if _, err := Config(config.OpenSearch{}); err == nil {
		t.Error("empty config should fail validation")
	}
	if _, err := Config(config.OpenSearch{Addresses: []string{"http://os:9200"}, Options: map[string]any{"password": "x"}}); err == nil ||
		!strings.Contains(err.Error(), "options.password: set by its own config key") {
		t.Errorf("owned option: %v", err)
	}
}

func TestOpen(t *testing.T) {
	var s seen
	srv := httptest.NewServer(fakeOS(&s, http.StatusOK, infoBody))
	defer srv.Close()

	c, err := Open(context.Background(), config.OpenSearch{
		Addresses: []string{srv.URL}, Username: "admin", Password: config.NewSecret("pw"),
		Options: map[string]any{"header": map[string]any{"X-Trace": "1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	h := s.last()
	if h.Get("Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("admin:pw")) || h.Get("X-Trace") != "1" {
		t.Errorf("headers = %v", h)
	}
}

func TestOpenFailures(t *testing.T) {
	srv := httptest.NewServer(fakeOS(nil, http.StatusUnauthorized, `{"error":"unauthorized"}`))
	defer srv.Close()
	_, err := Open(context.Background(), config.OpenSearch{Addresses: []string{srv.URL}, Options: noRetry})
	if err == nil || !strings.Contains(err.Error(), "opensearch ping") {
		t.Errorf("err = %v", err)
	}

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	}))
	defer slow.Close()
	start := time.Now()
	_, err = Open(context.Background(), config.OpenSearch{
		Addresses: []string{"http://user:secret@" + strings.TrimPrefix(slow.URL, "http://")}, PingTimeout: 50 * time.Millisecond, Options: noRetry,
	})
	if err == nil || time.Since(start) > time.Second {
		t.Errorf("ping timeout not honored: %v after %v", err, time.Since(start))
	}
	if err != nil && strings.Contains(err.Error(), "secret") {
		t.Errorf("error leaks credentials: %v", err)
	}
}

func TestWithLogger(t *testing.T) {
	srv := httptest.NewServer(fakeOS(nil, http.StatusOK, infoBody))
	defer srv.Close()
	var buf syncBuffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c, err := Open(context.Background(), config.OpenSearch{Addresses: []string{srv.URL}}, WithLogger(l))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if !strings.Contains(buf.String(), `"msg":"opensearch request"`) {
		t.Errorf("no request logged: %s", buf.String())
	}
}

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
