package conn_clickhouse

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/linzeyan/loadconf/config"
)

func TestOptionsFromFields(t *testing.T) {
	l := slog.New(slog.DiscardHandler)
	var hooked bool
	co, err := Options(config.ClickHouse{
		Addrs: []string{"ch1:9000", "ch2:9000"}, Protocol: "native", Database: "analytics", Username: "app", Password: config.NewSecret("pw"),
		Compression: "zstd",
		TLS:         config.TLS{Enabled: true, ServerName: "ch.internal"},
		SQLPool:     config.SQLPool{MaxOpenConns: 8, MaxIdleConns: 4, ConnMaxLifetime: 10 * time.Minute},
		Options: map[string]any{
			"settings":           map[string]any{"max_execution_time": 60, "async_insert": true, "load_balancing": "nearest_hostname"},
			"conn_open_strategy": 1, "block_buffer_size": 10, "dial_timeout": "2s", "read_timeout": "1m",
		},
	}, WithLogger(l), WithOptions(func(o *clickhouse.Options) { hooked = true; o.FreeBufOnConnRelease = true }))
	if err != nil {
		t.Fatal(err)
	}
	if co.Protocol != clickhouse.Native || strings.Join(co.Addr, ",") != "ch1:9000,ch2:9000" {
		t.Errorf("protocol = %v addr = %v", co.Protocol, co.Addr)
	}
	if co.Auth != (clickhouse.Auth{Database: "analytics", Username: "app", Password: "pw"}) {
		t.Errorf("auth = %+v", co.Auth)
	}
	if co.Settings["max_execution_time"] != 60 || co.Settings["async_insert"] != true || co.Settings["load_balancing"] != "nearest_hostname" {
		t.Errorf("settings = %v", co.Settings)
	}
	if co.Compression == nil || co.Compression.Method != clickhouse.CompressionZSTD || co.Compression.Level != 3 {
		t.Errorf("compression = %+v", co.Compression)
	}
	if co.ConnOpenStrategy != clickhouse.ConnOpenRoundRobin || co.BlockBufferSize != 10 || co.DialTimeout != 2*time.Second || co.ReadTimeout != time.Minute {
		t.Errorf("options = %+v", co)
	}
	if co.TLS == nil || co.TLS.ServerName != "ch.internal" {
		t.Errorf("tls = %+v", co.TLS)
	}
	if co.MaxOpenConns != 8 || co.MaxIdleConns != 4 || co.ConnMaxLifetime != 10*time.Minute {
		t.Errorf("pool = %d %d %v", co.MaxOpenConns, co.MaxIdleConns, co.ConnMaxLifetime)
	}
	if co.Logger != l || !hooked || !co.FreeBufOnConnRelease {
		t.Errorf("logger/hook not applied")
	}
}

func TestOptionsDSNOverride(t *testing.T) {
	co, err := Options(config.ClickHouse{
		DSN:      config.NewSecret("clickhouse://u:old@h1:9000,h2:9000/db1?dial_timeout=1s&compress=gzip&compress_level=6&max_execution_time=10&max_threads=4&max_open_conns=3"),
		Password: config.NewSecret("new"), Database: "db2",
		Options: map[string]any{"compression": map[string]any{"level": 9}, "settings": map[string]any{"max_execution_time": 30}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(co.Addr, ",") != "h1:9000,h2:9000" || co.Auth != (clickhouse.Auth{Database: "db2", Username: "u", Password: "new"}) {
		t.Errorf("addr = %v auth = %+v", co.Addr, co.Auth)
	}
	// Option settings merge into the DSN's instead of replacing them.
	if co.DialTimeout != time.Second || co.MaxOpenConns != 3 || co.Settings["max_execution_time"] != 30 || co.Settings["max_threads"] != 4 {
		t.Errorf("dsn values lost: %+v", co)
	}
	if co.Compression == nil || co.Compression.Method != clickhouse.CompressionGZIP || co.Compression.Level != 9 {
		t.Errorf("compression = %+v", co.Compression)
	}

	// The DSN's level survives a compression method from the config.
	co, _ = Options(config.ClickHouse{DSN: config.NewSecret("clickhouse://h:9000?compress=gzip&compress_level=6"), Compression: "deflate"})
	if co.Compression.Method != clickhouse.CompressionDeflate || co.Compression.Level != 6 {
		t.Errorf("compression = %+v", co.Compression)
	}

	co, err = Options(config.ClickHouse{DSN: config.NewSecret("https://h:8443?secure=true"), Protocol: "http", Options: map[string]any{"http_url_path": "/proxy"}})
	if err != nil || co.Protocol != clickhouse.HTTP || co.TLS == nil || co.HttpUrlPath != "/proxy" {
		t.Errorf("https dsn: %+v %v", co, err)
	}
}

func TestOptionsErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.ClickHouse
		want string
	}{
		"empty":       {config.ClickHouse{}, "either dsn or addrs is required"},
		"compression": {config.ClickHouse{Addrs: []string{"h:9000"}, Compression: "snappy"}, "unsupported compression"},
		"http proto":  {config.ClickHouse{DSN: config.NewSecret("clickhouse://h:9000"), Protocol: "http"}, "protocol http needs an http"},
		"http tls":    {config.ClickHouse{DSN: config.NewSecret("http://h:8123"), TLS: config.TLS{Enabled: true}}, "needs an https:// dsn"},
		"bad dsn":     {config.ClickHouse{DSN: config.NewSecret("clickhouse://u:hunter2@h:bad/db")}, "clickhouse dsn"},
		// The driver's detail is withheld: it can quote credentials.
		"bad setting": {config.ClickHouse{DSN: config.NewSecret("clickhouse://u:hunter2@h:9000?dial_timeout=soon")}, "clickhouse dsn"},
		"tls files":   {config.ClickHouse{Addrs: []string{"h:9440"}, TLS: config.TLS{Enabled: true, CAFile: "/nonexistent/ca.pem"}}, "clickhouse tls"},
		// A password in options would skip the Secret type and its redaction.
		"owned option":   {config.ClickHouse{Addrs: []string{"h:9000"}, Options: map[string]any{"auth": map[string]any{"password": "hunter2"}}}, "options.auth"},
		"unknown option": {config.ClickHouse{Addrs: []string{"h:9000"}, Options: map[string]any{"dial_timout": "1s"}}, "options.dial_timout"},
	} {
		_, err := Options(tc.cfg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: error leaks the password: %v", name, err)
		}
	}
}

func TestOpenHTTPRequest(t *testing.T) {
	var (
		mu  sync.Mutex
		req *http.Request
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if req == nil {
			req = r.Clone(context.Background())
		}
		mu.Unlock()
		http.Error(w, "Code: 516. DB::Exception: Authentication failed", http.StatusUnauthorized)
	}))
	defer srv.Close()

	cfg := config.ClickHouse{
		Addrs: []string{strings.TrimPrefix(srv.URL, "http://")}, Protocol: "http", Database: "analytics",
		Username: "app", Password: config.NewSecret("hunter2"), PingTimeout: 5 * time.Second,
		Options: map[string]any{
			"http_url_path": "/ch", "http_headers": map[string]any{"X-Tenant": "t1"}, "settings": map[string]any{"max_threads": 4},
		},
	}
	_, err := Open(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "ping clickhouse http://") || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("err = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if req == nil {
		t.Fatal("no request")
	}
	if req.URL.Path != "/ch" || req.URL.Query().Get("database") != "analytics" || req.URL.Query().Get("max_threads") != "4" {
		t.Errorf("url = %s", req.URL)
	}
	if user, pw, _ := req.BasicAuth(); user != "app" || pw != "hunter2" || req.Header.Get("X-Tenant") != "t1" {
		t.Errorf("headers = %v", req.Header)
	}

	// database/sql goes the same way.
	req = nil
	mu.Unlock()
	_, err = OpenDB(context.Background(), cfg)
	mu.Lock()
	if err == nil || req == nil || req.Header.Get("X-Tenant") != "t1" {
		t.Errorf("OpenDB err = %v", err)
	}
}

func TestOpenNative(t *testing.T) {
	cfg := config.ClickHouse{Addrs: []string{"127.0.0.1:1"}, Password: config.NewSecret("hunter2"), PingTimeout: 2 * time.Second,
		Options: map[string]any{"dial_timeout": "1s"},
		SQLPool: config.SQLPool{MaxOpenConns: 6, MaxIdleConns: 2, ConnMaxIdleTime: time.Minute}}
	if _, err := Open(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "ping clickhouse native://127.0.0.1:1") ||
		strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %v", err)
	}

	conn, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	db, err := NewDB(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Stats().MaxOpenConnections != 6 {
		t.Errorf("max open = %d", db.Stats().MaxOpenConnections)
	}
	if _, err := Connector(cfg); err != nil {
		t.Error(err)
	}
}
