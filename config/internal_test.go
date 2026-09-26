package config

import (
	"crypto/tls"
	"testing"
)

func TestSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"MaxOpenConns": "max_open_conns",
		"DSN":          "dsn",
		"ClientID":     "client_id",
		"HTTPServer":   "http_server",
		"APIKey":       "api_key",
		"V2Enabled":    "v2_enabled",
		"TLS":          "tls",
		"name":         "name",
		// Proper nouns stay one word, as config files spell them.
		"MySQL":       "mysql",
		"OrdersMySQL": "orders_mysql",
		"MySQLDSN":    "mysql_dsn",
		"PostgreSQL":  "postgresql",
		"SQLServer":   "sqlserver",
		"SQLite":      "sqlite",
		"ClickHouse":  "clickhouse",
		"MongoDB":     "mongodb",
		"OpenSearch":  "opensearch",
		"IPv6Addr":    "ipv6_addr",
	} {
		if got := snakeCase(in); got != want {
			t.Errorf("snakeCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTLSConfig(t *testing.T) {
	if c, err := (TLS{}).Config(); c != nil || err != nil {
		t.Errorf("disabled TLS = %v, %v", c, err)
	}
	c, err := TLS{Enabled: true, MinVersion: "1.3", ServerName: "h"}.Config()
	if err != nil || c.MinVersion != tls.VersionTLS13 || c.ServerName != "h" {
		t.Errorf("tls = %+v, %v", c, err)
	}
	if _, err := (TLS{Enabled: true, CertFile: "c.pem"}).Config(); err == nil {
		t.Error("cert without key should fail")
	}
	if _, err := (TLS{Enabled: true, CAFile: "/nonexistent"}).Config(); err == nil {
		t.Error("missing CA should fail")
	}
}

func TestEnvSourceParsing(t *testing.T) {
	s := &envSource{prefix: "APP", sep: "__", environ: func() []string {
		return []string{
			"APP_A__B=1",
			"APP_A=scalar-loses-to-mapping",
			"APP_=x",
			"APP_BAD____KEY=x",
			"OTHER_A=1",
			"APP_EQ=a=b",
		}
	}}
	m, err := s.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := m["a"].(map[string]any); !ok || a["b"] != "1" {
		t.Errorf("a = %#v", m["a"])
	}
	if m["eq"] != "a=b" || len(m) != 2 {
		t.Errorf("m = %#v", m)
	}
}
