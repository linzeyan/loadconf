package conn_trino

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/trinodb/trino-go-client/trino"

	"github.com/linzeyan/loadconf/config"
)

func parse(t *testing.T, cfg config.Trino) *trino.Config {
	t.Helper()
	dsn, err := DSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := trino.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("trino rejects %s: %v", dsn, err)
	}
	return c
}

func TestDSNFields(t *testing.T) {
	c := parse(t, config.Trino{
		Host: "trino", User: "app", Password: config.NewSecret("p@ss:w/rd"), Catalog: "hive", Schema: "sales",
		// The ';' of session_properties must survive the DSN round trip.
		Params: map[string]string{
			"source": "etl", "clientTags": "batch,nightly", "timezone": "Asia/Taipei", "query_timeout": "10m",
			"session_properties": "query_max_run_time:1h;hive.insert_existing_partitions_behavior:OVERWRITE",
		},
		TLS: config.TLS{Enabled: true},
	})
	u, _ := url.Parse(c.ServerURI)
	if u.Scheme != "https" || u.Host != "trino:8080" || u.User.Username() != "app" {
		t.Errorf("server = %s", c.ServerURI)
	}
	if pw, _ := u.User.Password(); pw != "p@ss:w/rd" {
		t.Errorf("password = %q", pw)
	}
	if c.Catalog != "hive" || c.Schema != "sales" || c.Source != "etl" || strings.Join(c.ClientTags, ",") != "batch,nightly" {
		t.Errorf("config = %+v", c)
	}
	if c.SessionProperties["query_max_run_time"] != "1h" || c.SessionProperties["hive.insert_existing_partitions_behavior"] != "OVERWRITE" {
		t.Errorf("session = %v", c.SessionProperties)
	}
	if c.TimeZone != "Asia/Taipei" || c.QueryTimeout == nil || *c.QueryTimeout != 10*time.Minute || c.CustomClientName != "" {
		t.Errorf("params: tz = %q timeout = %v client = %q", c.TimeZone, c.QueryTimeout, c.CustomClientName)
	}
}

func TestDSNOverride(t *testing.T) {
	c := parse(t, config.Trino{
		DSN:         config.NewSecret("https://old:secret@t1:8443?catalog=a&schema=b&session_properties=x:1%3By:2"),
		User:        "new",
		Schema:      "c",
		Params:      map[string]string{"session_properties": "y:3"},
		AccessToken: config.NewSecret("jwt"),
	})
	u, _ := url.Parse(c.ServerURI)
	if u.Host != "t1:8443" || u.User.Username() != "new" || c.Catalog != "a" || c.Schema != "c" || c.AccessToken != "jwt" {
		t.Errorf("server = %s config = %+v", c.ServerURI, c)
	}
	if pw, _ := u.User.Password(); pw != "secret" {
		t.Errorf("password = %q", pw)
	}
	// A param replaces the DSN parameter as a whole, so an override states
	// every session property it wants.
	if len(c.SessionProperties) != 1 || c.SessionProperties["y"] != "3" {
		t.Errorf("session = %v", c.SessionProperties)
	}
}

func TestDSNErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Trino
		want string
	}{
		"empty":        {config.Trino{}, "either dsn or host is required"},
		"no user":      {config.Trino{Host: "h"}, "user is required"},
		"plain pass":   {config.Trino{Host: "h", User: "u", Password: config.NewSecret("hunter2")}, "require tls.enabled"},
		"dsn pass":     {config.Trino{DSN: config.NewSecret("http://u@h:8080"), Password: config.NewSecret("hunter2")}, "password requires https"},
		"dsn token":    {config.Trino{DSN: config.NewSecret("http://u@h:8080"), AccessToken: config.NewSecret("hunter2")}, "access token requires https"},
		"scheme":       {config.Trino{DSN: config.NewSecret("trino://u:hunter2@h")}, "must start with http"},
		"bad dsn":      {config.Trino{DSN: config.NewSecret("https://u:hunter2@h:bad")}, "trino dsn"},
		"semicolon":    {config.Trino{DSN: config.NewSecret("https://u:hunter2@h?session_properties=a:1;b:2")}, "invalid semicolon"},
		"bad param":    {config.Trino{Host: "h", User: "u", Params: map[string]string{"query_timeout": "soon"}}, "trino dsn"},
		"bad tls file": {config.Trino{Host: "h", User: "u", TLS: config.TLS{Enabled: true, CAFile: "/nonexistent.pem"}}, "trino tls"},
	} {
		_, err := DSN(tc.cfg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: error leaks the secret: %v", name, err)
		}
	}
}

// coordinator answers every statement with a single row holding 1.
func coordinator(t *testing.T, tlsServer bool) (*httptest.Server, func() []*http.Request) {
	var (
		mu   sync.Mutex
		reqs []*http.Request
	)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		reqs = append(reqs, r.Clone(context.Background()))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statement":
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			_, _ = w.Write([]byte(`{"id":"q1","nextUri":"` + scheme + "://" + r.Host + `/v1/statement/q1/1","stats":{"state":"QUEUED"}}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statement/q1/1":
			_, _ = w.Write([]byte(`{"id":"q1","columns":[{"name":"_col0","type":"integer","typeSignature":{"rawType":"integer","arguments":[]}}],"data":[[1]],"stats":{"state":"FINISHED"}}`))
		default:
			http.NotFound(w, r)
		}
	})
	var srv *httptest.Server
	if tlsServer {
		srv = httptest.NewTLSServer(h)
	} else {
		srv = httptest.NewServer(h)
	}
	t.Cleanup(srv.Close)
	return srv, func() []*http.Request {
		mu.Lock()
		defer mu.Unlock()
		return append([]*http.Request(nil), reqs...)
	}
}

func TestOpen(t *testing.T) {
	srv, reqs := coordinator(t, false)
	db, err := Open(context.Background(), config.Trino{
		DSN: config.NewSecret(srv.URL), User: "app", Catalog: "hive",
		Params:  map[string]string{"source": "etl", "session_properties": "query_max_run_time:1h"},
		SQLPool: config.SQLPool{MaxOpenConns: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Stats().MaxOpenConnections != 4 {
		t.Errorf("max open = %d", db.Stats().MaxOpenConnections)
	}
	got := reqs()
	if len(got) == 0 {
		t.Fatal("no request")
	}
	h := got[0].Header
	if h.Get("X-Trino-User") != "app" || h.Get("X-Trino-Catalog") != "hive" || h.Get("X-Trino-Source") != "etl" ||
		h.Get("X-Trino-Session") != "query_max_run_time=1h" {
		t.Errorf("headers = %v", h)
	}
}

func TestOpenCustomTLS(t *testing.T) {
	srv, reqs := coordinator(t, true)
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Trino{DSN: config.NewSecret(srv.URL), User: "app", Password: config.NewSecret("hunter2"), TLS: config.TLS{Enabled: true, CAFile: ca}}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if user, pw, _ := reqs()[0].BasicAuth(); user != "app" || pw != "hunter2" {
		t.Errorf("basic auth = %q %q", user, pw)
	}

	// The client name is stable, so opening again does not grow the registry.
	d1, _ := DSN(cfg)
	d2, _ := DSN(cfg)
	if d1 != d2 || !strings.Contains(d1, "custom_client=loadconf-conn_trino-") {
		t.Errorf("dsn = %s / %s", d1, d2)
	}

	// Without the CA the certificate is rejected, and the error hides the password.
	cfg.TLS.CAFile = ""
	_, err = Open(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "ping trino https://") || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %v", err)
	}
	if _, err := New(cfg); err != nil {
		t.Errorf("New: %v", err)
	}
}
