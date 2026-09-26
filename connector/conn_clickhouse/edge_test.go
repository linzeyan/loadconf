package conn_clickhouse

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/linzeyan/loadconf/config"
)

func TestOptionsOwnedKeysInAnySpelling(t *testing.T) {
	// Options are matched case-insensitively, so the owned-key check must be
	// too; otherwise "AUTH" would smuggle a password past the Secret type
	// and "TLS" would silently replace the verified TLS config.
	for name, opts := range map[string]map[string]any{
		"protocol upper":     {"PROTOCOL": 1},
		"addr mixed":         {"Addr": []string{"evil:9000"}},
		"auth upper":         {"AUTH": map[string]any{"Password": "hunter2"}},
		"auth nested upper":  {"auth": map[string]any{"PASSWORD": "hunter2"}},
		"tls mixed":          {"Tls": map[string]any{"InsecureSkipVerify": true}},
		"compression method": {"compression": map[string]any{"METHOD": 1}},
		"max open conns":     {"Max_Open_Conns": 100},
		"max idle conns":     {"MAX_IDLE_CONNS": 100},
		"conn max lifetime":  {"conn_max_lifetime": "1h"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Options(config.ClickHouse{Addrs: []string{"h:9000"}, Options: opts})
			if err == nil || !strings.Contains(err.Error(), "not in options") {
				t.Errorf("err = %v, want an owned-key error", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsDefaultLogger(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	def := slog.New(slog.NewTextHandler(&buf, nil))
	slog.SetDefault(def)
	t.Cleanup(func() { slog.SetDefault(prev) })

	for name, opts := range map[string][]Option{"no option": nil, "nil logger": {WithLogger(nil)}} {
		co, err := Options(config.ClickHouse{Addrs: []string{"h:9000"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if co.Logger != def {
			t.Errorf("%s: logger = %v, want slog.Default()", name, co.Logger)
		}
	}
}

func TestOptionsAdversarialCredentials(t *testing.T) {
	for _, s := range []string{"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p%zz", "it's", "has space", "密碼🔑", "x@evil:1/db?secure=true"} {
		t.Run(s, func(t *testing.T) {
			// A DSN written with proper escaping must come back exactly.
			dsn := (&url.URL{Scheme: "clickhouse", User: url.UserPassword("u"+s, s), Host: "h:9000", Path: "/db" + s}).String()
			co, err := Options(config.ClickHouse{DSN: config.NewSecret(dsn)})
			if err != nil {
				t.Fatal(err)
			}
			if co.Auth != (clickhouse.Auth{Username: "u" + s, Password: s, Database: "db" + s}) || strings.Join(co.Addr, ",") != "h:9000" || co.TLS != nil {
				t.Errorf("dsn %s: auth %+v addr %v tls %v", dsn, co.Auth, co.Addr, co.TLS != nil)
			}
			// Fields bypass the DSN syntax entirely.
			co, err = Options(config.ClickHouse{DSN: config.NewSecret(dsn), Username: "f" + s, Password: config.NewSecret("f" + s), Database: "f" + s})
			if err != nil {
				t.Fatal(err)
			}
			if co.Auth != (clickhouse.Auth{Username: "f" + s, Password: "f" + s, Database: "f" + s}) {
				t.Errorf("fields: auth %+v", co.Auth)
			}
		})
	}
}

func TestOptionsTLSAndSchemes(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg      config.ClickHouse
		wantErr  bool
		wantSNI  string
		wantHTTP bool
	}{
		"native tls no server name":  {config.ClickHouse{Addrs: []string{"[::1]:9440"}, TLS: config.TLS{Enabled: true}}, false, "", false},
		"native tls with name":       {config.ClickHouse{Addrs: []string{"10.0.0.1:9440"}, TLS: config.TLS{Enabled: true, ServerName: "ch"}}, false, "ch", false},
		"https dsn replaces dsn tls": {config.ClickHouse{DSN: config.NewSecret("https://h:8443?secure=true&skip_verify=true"), TLS: config.TLS{Enabled: true, ServerName: "ch"}}, false, "ch", true},
		"upper-case HTTP dsn":        {config.ClickHouse{DSN: config.NewSecret("HTTP://h:8123"), TLS: config.TLS{Enabled: true}}, true, "", true},
		"tcp dsn is native":          {config.ClickHouse{DSN: config.NewSecret("tcp://h:9000"), Protocol: "http"}, true, "", false},
		"protocol HTTP any case":     {config.ClickHouse{Addrs: []string{"h:8123"}, Protocol: "HTTP"}, false, "", true},
	} {
		t.Run(name, func(t *testing.T) {
			co, err := Options(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if err != nil {
				return
			}
			if (co.Protocol == clickhouse.HTTP) != tc.wantHTTP {
				t.Errorf("protocol = %v", co.Protocol)
			}
			if tc.cfg.TLS.Enabled {
				// TLS from config replaces the DSN's; skip_verify from the
				// DSN must not survive.
				if co.TLS == nil || co.TLS.ServerName != tc.wantSNI || co.TLS.InsecureSkipVerify {
					t.Errorf("tls = %+v", co.TLS)
				}
			}
		})
	}
}

func TestOptionsErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.ClickHouse{
		"bad escape":           {DSN: config.NewSecret("clickhouse://u:" + pw + "%zz@h:9000")},
		"control char":         {DSN: config.NewSecret("clickhouse://u:" + pw + "\x7f@h:9000")},
		"bad port":             {DSN: config.NewSecret("clickhouse://u:" + pw + "@h:port/db")},
		"bad param":            {DSN: config.NewSecret("clickhouse://u:" + pw + "@h:9000?read_timeout=soon")},
		"no host":              {DSN: config.NewSecret("clickhouse://u:" + pw + "@/db")},
		"https without tls":    {DSN: config.NewSecret("https://u:" + pw + "@h:8443")},
		"field and bad option": {Addrs: []string{"h:9000"}, Password: config.NewSecret(pw), Options: map[string]any{"dial_timeout": "soon"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Options(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

// TestDSNHTTPProxyErrorHidesCredentials: clickhouse-go formats a bad
// http_proxy URL into its error with %s, so there is no *url.Error to unwrap;
// the proxy's credentials must still stay out of the returned error (and so
// out of the logs).
func TestDSNHTTPProxyErrorHidesCredentials(t *testing.T) {
	_, err := Options(config.ClickHouse{DSN: config.NewSecret("http://h:8123?http_proxy=" + url.QueryEscape("http://u:proxysecret@proxy:bad"))})
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "proxysecret") {
		t.Errorf("error leaks proxy credentials: %v", err)
	}
}

func TestOptionsDSNWithFieldsAndOptionsPrecedence(t *testing.T) {
	// Typed fields beat options, which beat the DSN.
	co, err := Options(config.ClickHouse{
		DSN:     config.NewSecret("clickhouse://u:p@old:9000/olddb?dial_timeout=1s&max_open_conns=3"),
		Addrs:   []string{"new1:9000", "[::1]:9000"},
		Options: map[string]any{"dial_timeout": "2s"},
		SQLPool: config.SQLPool{MaxOpenConns: 9},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(co.Addr, ",") != "new1:9000,[::1]:9000" || co.DialTimeout.String() != "2s" || co.MaxOpenConns != 9 || co.Auth.Database != "olddb" {
		t.Errorf("addr %v dial %v max open %d db %q", co.Addr, co.DialTimeout, co.MaxOpenConns, co.Auth.Database)
	}
}
