package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

func TestByteSize(t *testing.T) {
	for in, want := range map[string]config.ByteSize{
		"512":    512,
		"1KB":    1000,
		"1kib":   1024,
		"64MiB":  64 << 20,
		"1.5GB":  1_500_000_000,
		"2 GiB":  2 << 30,
		"1TiB":   1 << 40,
		" 10mb ": 10_000_000,
		"3M":     3 << 20,
	} {
		got, err := config.ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MB", "10XB", "-1MB", "1e3", "99999999TB"} {
		if _, err := config.ParseByteSize(in); err == nil {
			t.Errorf("ParseByteSize(%q) should fail", in)
		}
	}
	for b, want := range map[config.ByteSize]string{0: "0B", 1000: "1000B", 1024: "1KiB", 64 << 20: "64MiB", 3 << 30: "3GiB"} {
		if b.String() != want {
			t.Errorf("%d.String() = %s, want %s", int64(b), b, want)
		}
	}

	type conf struct {
		A config.ByteSize `config:"a"`
		B config.ByteSize `config:"b" default:"8MiB"`
		C config.ByteSize `config:"c"`
	}
	cfg := mustYAML[conf](t, "a: 2048\nc: 1GiB\n")
	if cfg.A != 2048 || cfg.B != 8<<20 || cfg.C != 1<<30 {
		t.Fatalf("got %+v", cfg)
	}
	cfg, err := config.Load[conf](t.Context(), quiet(), config.From(config.Bytes("json", []byte(`{"a": 4096}`))))
	if err != nil || cfg.A != 4096 {
		t.Fatalf("json number: %+v %v", cfg, err)
	}
	if _, err := loadYAML[conf](t, "a: -1\n"); err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("negative size: %v", err)
	}
	if _, err := loadYAML[conf](t, "a: lots\n"); err == nil || !strings.Contains(err.Error(), `a: invalid byte size "lots"`) {
		t.Fatalf("bad size: %v", err)
	}
}

type moreConnConf struct {
	SQLServer  config.Named[config.SQLServer]     `config:"sqlserver"`
	Oracle     config.Named[config.Oracle]        `config:"oracle"`
	SQLite     config.Named[config.SQLite]        `config:"sqlite"`
	Cassandra  config.Named[config.Cassandra]     `config:"cassandra"`
	ClickHouse config.Named[config.ClickHouse]    `config:"clickhouse"`
	StreamLoad config.Named[config.StreamLoad]    `config:"stream_load"`
	Trino      config.Named[config.Trino]         `config:"trino"`
	ES         config.Named[config.Elasticsearch] `config:"elasticsearch"`
	OpenSearch config.Named[config.OpenSearch]    `config:"opensearch"`
	Log        config.Log                         `config:"log"`
}

func TestMoreConnDefaults(t *testing.T) {
	cfg := mustYAML[moreConnConf](t, `
sqlserver:
  main: {host: mssql, database: app}
oracle:
  main: {host: ora, service: ORCLPDB1}
sqlite:
  local: {path: data/app.db}
cassandra:
  main: {hosts: [c1, c2], local_dc: dc1}
clickhouse:
  olap: {addrs: ["ch:9000"]}
stream_load:
  doris: {addrs: ["fe:8030"], database: db}
trino:
  main: {host: trino, user: svc}
elasticsearch:
  main: {addresses: ["https://es:9200"], api_key: key}
opensearch:
  main: {addresses: ["http://os:9200"]}
`)
	if s := cfg.SQLServer.MustGet("main"); s.Port != 1433 || s.PingTimeout != 3*time.Second || s.MaxOpenConns != 0 {
		t.Errorf("sqlserver = %+v", s)
	}
	if o := cfg.Oracle.MustGet("main"); o.Port != 1521 || o.MaxIdleConns != 0 {
		t.Errorf("oracle = %+v", o)
	}
	if s := cfg.SQLite.MustGet("local"); s.BusyTimeout != 5*time.Second || s.JournalMode != "wal" || !s.ForeignKeys {
		t.Errorf("sqlite = %+v", s)
	}
	if c := cfg.Cassandra.MustGet("main"); c.Port != 9042 || c.LocalDC != "dc1" {
		t.Errorf("cassandra = %+v", c)
	}
	if c := cfg.ClickHouse.MustGet("olap"); c.Protocol != "native" || c.PingTimeout != 3*time.Second {
		t.Errorf("clickhouse = %+v", c)
	}
	sl := cfg.StreamLoad.MustGet("doris")
	if sl.Flavor != config.StreamLoadDoris || sl.Username != "root" || sl.Format != "json" || sl.Timeout != 5*time.Minute ||
		sl.Batch.MaxRows != 100000 || sl.Batch.MaxBytes != 64*config.MiB || sl.Batch.FlushInterval != 5*time.Second {
		t.Errorf("stream_load = %+v", sl)
	}
	if tr := cfg.Trino.MustGet("main"); tr.Port != 8080 || tr.PingTimeout != 5*time.Second {
		t.Errorf("trino = %+v", tr)
	}
	if e := cfg.ES.MustGet("main"); e.PingTimeout != 5*time.Second || e.APIKey.Value() != "key" {
		t.Errorf("elasticsearch = %+v", e)
	}
	if o := cfg.OpenSearch.MustGet("main"); o.PingTimeout != 5*time.Second {
		t.Errorf("opensearch = %+v", o)
	}
	l := cfg.Log
	if l.Level != "info" || l.Format != "json" || l.StackLevel != "error" || l.Outputs.Len() != 0 {
		t.Errorf("log = %+v", l)
	}
}

func TestMoreConnValidation(t *testing.T) {
	_, err := loadYAML[moreConnConf](t, `
sqlserver:
  a: {max_open_conns: 1, max_idle_conns: 2}
oracle:
  a: {service: s, sid: x}
sqlite:
  a: {mode: rwx, journal_mode: fast, synchronous: sometimes, tx_lock: now}
cassandra:
  a: {compression: zstd}
clickhouse:
  a: {protocol: grpc, compression: lzma}
stream_load:
  a: {flavor: selectdb, addrs: ["ftp://fe"], format: parquet, batch: {max_rows: -1}}
trino:
  a: {host: t, password: p}
elasticsearch:
  a: {addresses: ["es:9200"], cloud_id: c, username: u, api_key: k}
  b: {}
log:
  level: loud
  format: xml
  stack_level: always
  outputs:
    file: {}
    syslog: {network: tcp, syslog_format: bsd}
    graylog: {type: gelf, network: quic}
    es: {type: elasticsearch, username: u, api_key: k}
    otel: {type: otlp, protocol: thrift, level: chatty}
    off: {type: file, disabled: true}
    custom: {type: kafka, options: {topic: logs}}
`)
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		"sqlserver.a: either dsn or host is required",
		"sqlserver.a: max_idle_conns (2) exceeds max_open_conns (1)",
		"oracle.a: one of dsn, connect_string or host is required",
		"oracle.a: service and sid are mutually exclusive",
		"sqlite.a: path is required",
		`sqlite.a: unsupported mode "rwx"`,
		`sqlite.a: unsupported journal_mode "fast"`,
		`sqlite.a: unsupported synchronous "sometimes"`,
		`sqlite.a: unsupported tx_lock "now"`,
		"cassandra.a: hosts is required",
		`cassandra.a: unsupported compression "zstd"`,
		"clickhouse.a: either dsn or addrs is required",
		`clickhouse.a: unsupported protocol "grpc"`,
		`clickhouse.a: unsupported compression "lzma"`,
		`stream_load.a: unsupported flavor "selectdb"`,
		`stream_load.a: addr "ftp://fe" must be host:port or an http(s) URL`,
		"stream_load.a: database is required",
		`stream_load.a: unsupported format "parquet"`,
		"stream_load.a: retry and batch settings must not be negative",
		"trino.a: user is required",
		"trino.a: password and access_token require tls.enabled",
		"elasticsearch.a: addresses and cloud_id are mutually exclusive",
		`elasticsearch.a: address "es:9200" must be an http(s) URL`,
		"elasticsearch.a: username, api_key and service_token are mutually exclusive",
		"elasticsearch.b: either addresses or cloud_id is required",
		`log: unsupported level "loud"`,
		`log: unsupported format "xml"`,
		`log: unsupported stack_level "always"`,
		"log.outputs.file: path is required for a file output",
		"log.outputs.syslog: addr is required when network is set",
		`log.outputs.syslog: unsupported syslog_format "bsd"`,
		`log.outputs.graylog: unsupported network "quic" for gelf`,
		"log.outputs.graylog: addr is required for a gelf output",
		"log.outputs.es: addresses is required for an elasticsearch output",
		"log.outputs.es: username and api_key are mutually exclusive",
		`log.outputs.otel: unsupported protocol "thrift" for otlp`,
		`log.outputs.otel: unsupported level "chatty"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
	for _, unwanted := range []string{"log.outputs.off", "log.outputs.custom"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Errorf("unexpected error for %s:\n%v", unwanted, err)
		}
	}
}

func TestLogOutputs(t *testing.T) {
	cfg := mustYAML[moreConnConf](t, `
log:
  level: debug
  service: orders
  fields: {env: prod}
  outputs:
    stdout: {format: text}
    file: {path: logs/app.log, max_size: 10MiB, max_age: 168h}
    graylog: {type: GELF, addr: "graylog:12201"}
`)
	l := cfg.Log
	if l.Outputs.Len() != 3 || l.Fields["env"] != "prod" {
		t.Fatalf("log = %+v", l)
	}
	for name, want := range map[string]string{"stdout": config.LogStdout, "file": config.LogFile, "graylog": config.LogGELF} {
		if got := l.Outputs.MustGet(name).ResolvedType(); got != want {
			t.Errorf("%s type = %s, want %s", name, got, want)
		}
	}
	f := l.Outputs.MustGet("file")
	if f.MaxSize != 10*config.MiB || f.MaxAge != 168*time.Hour || f.Timeout != 5*time.Second || f.QueueSize != 10000 {
		t.Errorf("file = %+v", f)
	}
	if g := l.Outputs.MustGet("graylog"); g.MaxSize != 100*config.MiB || g.BatchSize != 500 {
		t.Errorf("graylog defaults = %+v", g)
	}
}
