package conn_es9

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/linzeyan/loadconf/config"
)

const infoBody = `{"name":"n1","cluster_name":"test","version":{"number":"9.5.2"},"tagline":"You Know, for Search"}`

// recorder remembers the headers of the last request a fake server saw.
type recorder struct {
	mu     sync.Mutex
	header http.Header
	method string
}

func (r *recorder) last() (string, http.Header) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.method, r.header
}

// fakeES answers every request like an Elasticsearch node with status and
// body; product controls the X-Elastic-Product header.
func fakeES(rec *recorder, status int, body string, product bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if rec != nil {
			rec.mu.Lock()
			rec.method, rec.header = r.Method, r.Header.Clone()
			rec.mu.Unlock()
		}
		if product {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

func TestConfigMapping(t *testing.T) {
	ec, err := Config(config.Elasticsearch{
		Addresses:             []string{"https://es1:9200", "https://es2:9200"},
		APIKey:                config.NewSecret("a2V5"),
		ResponseHeaderTimeout: 2 * time.Second,
		TLS:                   config.TLS{Enabled: true, ServerName: "es.local", MinVersion: "1.3"},
		Options: map[string]any{
			"header":                  map[string]any{"x-tenant": "t1"},
			"compress_request_body":   true,
			"max_retries":             5,
			"retry_on_status":         "429,503",
			"discover_nodes_on_start": true,
			"discover_nodes_interval": "1m",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ec.Addresses) != 2 || ec.APIKey != "a2V5" || ec.Header.Get("X-Tenant") != "t1" || !ec.CompressRequestBody ||
		ec.MaxRetries != 5 || len(ec.RetryOnStatus) != 2 || !ec.DiscoverNodesOnStart || ec.DiscoverNodesInterval != time.Minute {
		t.Errorf("config = %+v", ec)
	}
	tr, ok := ec.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T", ec.Transport)
	}
	if tr.TLSClientConfig == nil || tr.TLSClientConfig.ServerName != "es.local" || tr.TLSClientConfig.MinVersion != tls.VersionTLS13 ||
		tr.ResponseHeaderTimeout != 2*time.Second || tr == http.DefaultTransport {
		t.Errorf("transport tls = %+v timeout = %v", tr.TLSClientConfig, tr.ResponseHeaderTimeout)
	}

	ec, err = Config(config.Elasticsearch{CloudID: "dep:ZXhhbXBsZS5jb20kYWJjJGRlZg==", Username: "u", Password: config.NewSecret("p")},
		func(c *elasticsearch.Config) { c.EnableMetrics = true })
	if err != nil {
		t.Fatal(err)
	}
	if ec.CloudID == "" || ec.Username != "u" || ec.Password != "p" || ec.Transport != nil || !ec.EnableMetrics {
		t.Errorf("config = %+v", ec)
	}
}

func TestConfigErrors(t *testing.T) {
	fp := strings.Repeat("ab", sha256.Size)
	for name, tc := range map[string]struct {
		cfg  config.Elasticsearch
		opts []Option
	}{
		"no address":           {cfg: config.Elasticsearch{}},
		"two auth methods":     {cfg: config.Elasticsearch{Addresses: []string{"http://es:9200"}, Username: "u", APIKey: config.NewSecret("k")}},
		"fingerprint with tls": {cfg: config.Elasticsearch{Addresses: []string{"https://es:9200"}, CertificateFingerprint: fp, TLS: config.TLS{Enabled: true}}},
		"bad fingerprint":      {cfg: config.Elasticsearch{Addresses: []string{"https://es:9200"}, CertificateFingerprint: "abc"}},
		"missing ca file":      {cfg: config.Elasticsearch{Addresses: []string{"https://es:9200"}, TLS: config.TLS{Enabled: true, CAFile: "/nonexistent.pem"}}},
		"unknown option":       {cfg: config.Elasticsearch{Addresses: []string{"http://es:9200"}, Options: map[string]any{"max_retry": 1}}},
		"owned option":         {cfg: config.Elasticsearch{Addresses: []string{"http://es:9200"}, Options: map[string]any{"api_key": "k"}}},
		"fingerprint with custom round tripper": {
			cfg:  config.Elasticsearch{Addresses: []string{"https://es:9200"}, CertificateFingerprint: fp},
			opts: []Option{func(c *elasticsearch.Config) { c.Transport = roundTripper(http.DefaultTransport.RoundTrip) }},
		},
	} {
		if _, err := Config(tc.cfg, tc.opts...); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

var noRetry = map[string]any{"disable_retry": true}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOpen(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(fakeES(rec, http.StatusOK, infoBody, true))
	defer srv.Close()

	es, err := Open(context.Background(), config.Elasticsearch{
		Addresses: []string{srv.URL},
		APIKey:    config.NewSecret("a2V5"),
		Options:   map[string]any{"header": map[string]any{"X-Opaque-Id": "svc"}},
	}, func(c *elasticsearch.Config) { c.EnableCompatibilityMode = true })
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close(context.Background())

	method, h := rec.last()
	if method != http.MethodGet || h.Get("Authorization") != "APIKey a2V5" || h.Get("X-Opaque-Id") != "svc" {
		t.Errorf("request = %s %v", method, h)
	}
	if accept := h.Get("Accept"); accept != "application/vnd.elasticsearch+json;compatible-with=9" {
		t.Errorf("accept = %q", accept)
	}

	tc, err := NewTypedClient(config.Elasticsearch{Addresses: []string{srv.URL}, Username: "u", Password: config.NewSecret("p")})
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := tc.Ping().IsSuccess(context.Background()); !ok || err != nil {
		t.Errorf("typed ping = %v, %v", ok, err)
	}
	if method, h := rec.last(); method != http.MethodHead || !strings.HasPrefix(h.Get("Authorization"), "Basic ") {
		t.Errorf("typed request = %s %v", method, h)
	}
}

func TestOpenErrors(t *testing.T) {
	unauthorized := httptest.NewServer(fakeES(nil, http.StatusUnauthorized,
		`{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}`, true))
	defer unauthorized.Close()
	notES := httptest.NewServer(fakeES(nil, http.StatusOK, infoBody, false))
	defer notES.Close()

	for name, tc := range map[string]struct {
		addr string
		want string
	}{
		"unauthorized":      {unauthorized.URL, "status 401 Unauthorized: {\"error\":{\"type\":\"security_exception\""},
		"not elasticsearch": {notES.URL, "not Elasticsearch"},
		"unreachable":       {"http://user:secret@127.0.0.1:1", "elasticsearch ping [http://127.0.0.1:1]"},
	} {
		_, err := Open(context.Background(), config.Elasticsearch{Addresses: []string{tc.addr}, PingTimeout: 2 * time.Second, Options: noRetry})
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: error leaks the password: %v", name, err)
		}
	}
}

func TestCertificateFingerprint(t *testing.T) {
	srv := httptest.NewTLSServer(fakeES(nil, http.StatusOK, infoBody, true))
	defer srv.Close()
	sum := sha256.Sum256(srv.Certificate().Raw)
	var colon []string
	for _, b := range sum {
		colon = append(colon, strings.ToUpper(hex.EncodeToString([]byte{b})))
	}

	// The fingerprint dialer must also work on the transport built for
	// ResponseHeaderTimeout.
	es, err := Open(context.Background(), config.Elasticsearch{
		Addresses:              []string{srv.URL},
		CertificateFingerprint: strings.Join(colon, ":"),
		ResponseHeaderTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = es.Close(context.Background())

	_, err = Open(context.Background(), config.Elasticsearch{
		Addresses:              []string{srv.URL},
		CertificateFingerprint: strings.Repeat("00", sha256.Size),
		Options:                noRetry,
	})
	if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Errorf("err = %v", err)
	}
}

func TestTLSCAFile(t *testing.T) {
	srv := httptest.NewTLSServer(fakeES(nil, http.StatusOK, infoBody, true))
	defer srv.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}

	es, err := Open(context.Background(), config.Elasticsearch{Addresses: []string{srv.URL}, TLS: config.TLS{Enabled: true, CAFile: caFile}})
	if err != nil {
		t.Fatal(err)
	}
	_ = es.Close(context.Background())

	if _, err := Open(context.Background(), config.Elasticsearch{Addresses: []string{srv.URL}, Options: noRetry}); err == nil {
		t.Error("untrusted certificate should fail")
	}
}
