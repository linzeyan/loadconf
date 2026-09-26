package conn_scylla

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocql/lz4"

	"github.com/linzeyan/loadconf/config"
)

func TestIsScyllaFork(t *testing.T) {
	if !IsScyllaFork() {
		t.Error("the module's replace directive should link Scylla's fork")
	}
}

// fallbackPolicy returns the type name of the token-aware policy's fallback
// and, for a DC-aware fallback, its local DC.
func fallbackPolicy(t *testing.T, p gocql.HostSelectionPolicy) (string, string) {
	t.Helper()
	v := reflect.ValueOf(p)
	if v.Type().String() != "*gocql.tokenAwareHostPolicy" {
		t.Fatalf("policy = %v", v.Type())
	}
	fallback := v.Elem().FieldByName("fallback").Elem()
	var dc string
	if f := fallback.Elem().FieldByName("local"); f.IsValid() {
		dc = f.String()
	}
	return fallback.Type().String(), dc
}

func TestClusterConfigDefaults(t *testing.T) {
	cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"s1", "s2"}})
	if err != nil {
		t.Fatal(err)
	}
	def := gocql.NewCluster()
	if !reflect.DeepEqual(cc.Hosts, []string{"s1", "s2"}) || cc.Port != def.Port || cc.Consistency != def.Consistency ||
		cc.SerialConsistency != 0 || cc.Timeout != def.Timeout || cc.ConnectTimeout != def.ConnectTimeout ||
		cc.WriteTimeout != def.WriteTimeout || cc.NumConns != def.NumConns || cc.PageSize != def.PageSize ||
		cc.ReconnectInterval != def.ReconnectInterval || cc.ProtoVersion != 0 || cc.DisableShardAwarePort {
		t.Errorf("defaults changed: %+v", cc)
	}
	if cc.Authenticator != nil || cc.RetryPolicy != nil || cc.Compressor != nil || cc.SslOpts != nil {
		t.Errorf("unexpected driver settings: %+v", cc)
	}
	if typ, _ := fallbackPolicy(t, cc.PoolConfig.HostSelectionPolicy); typ != "*gocql.roundRobinHostPolicy" {
		t.Errorf("fallback = %s", typ)
	}
}

func TestClusterConfigMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg   config.Cassandra
		check func(*testing.T, *gocql.ClusterConfig)
	}{
		"connection": {
			config.Cassandra{Port: 19042, Keyspace: "app", Options: map[string]any{"proto_version": 4, "disable_initial_host_lookup": true}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Port != 19042 || cc.Keyspace != "app" || cc.ProtoVersion != 4 || !cc.DisableInitialHostLookup {
					t.Errorf("config = %+v", cc)
				}
			},
		},
		"auth": {
			config.Cassandra{Username: "u", Password: config.NewSecret("p@ss")},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if a, ok := cc.Authenticator.(gocql.PasswordAuthenticator); !ok || a.Username != "u" || a.Password != "p@ss" {
					t.Errorf("authenticator = %#v", cc.Authenticator)
				}
			},
		},
		"consistency mixed case": {
			config.Cassandra{Options: map[string]any{"consistency": "LOCAL_ONE", "serial_consistency": "LOCAL_SERIAL"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Consistency != gocql.LocalOne || cc.SerialConsistency != gocql.LocalSerial {
					t.Errorf("consistency = %v serial = %v", cc.Consistency, cc.SerialConsistency)
				}
			},
		},
		"consistency any": {
			config.Cassandra{Options: map[string]any{"consistency": "ANY", "serial_consistency": "SERIAL"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Consistency != gocql.Any || cc.SerialConsistency != gocql.Serial {
					t.Errorf("consistency = %v serial = %v", cc.Consistency, cc.SerialConsistency)
				}
			},
		},
		"timeouts and pool": {
			config.Cassandra{Options: map[string]any{"timeout": "2s", "connect_timeout": "3s", "write_timeout": "1s",
				"num_conns": 4, "page_size": 100, "reconnect_interval": "10s"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Timeout != 2*time.Second || cc.ConnectTimeout != 3*time.Second || cc.WriteTimeout != time.Second ||
					cc.NumConns != 4 || cc.PageSize != 100 || cc.ReconnectInterval != 10*time.Second {
					t.Errorf("config = %+v", cc)
				}
			},
		},
		"retries": {
			config.Cassandra{Retries: 2},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if p, ok := cc.RetryPolicy.(*gocql.SimpleRetryPolicy); !ok || p.NumRetries != 2 {
					t.Errorf("retry policy = %#v", cc.RetryPolicy)
				}
			},
		},
		"snappy": {
			config.Cassandra{Compression: "SNAPPY", Options: map[string]any{"proto_version": 4}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if _, ok := cc.Compressor.(gocql.SnappyCompressor); !ok {
					t.Errorf("compressor = %T", cc.Compressor)
				}
			},
		},
		"lz4 with protocol v5": {
			config.Cassandra{Compression: "lz4", Options: map[string]any{"proto_version": 5}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if _, ok := cc.Compressor.(lz4.LZ4Compressor); !ok || cc.Compressor.Name() != "lz4" {
					t.Errorf("compressor = %T", cc.Compressor)
				}
				if _, ok := cc.Compressor.(gocql.SegmentCompressor); !ok {
					t.Error("lz4 must support v5 segments")
				}
			},
		},
		"local dc": {
			config.Cassandra{LocalDC: "dc1"},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if typ, dc := fallbackPolicy(t, cc.PoolConfig.HostSelectionPolicy); typ != "*gocql.dcAwareRR" || dc != "dc1" {
					t.Errorf("fallback = %s local = %q", typ, dc)
				}
			},
		},
		"tls verified": {
			config.Cassandra{TLS: config.TLS{Enabled: true, ServerName: "scylla.local", MinVersion: "1.3"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.SslOpts == nil || cc.SslOpts.Config == nil || !cc.SslOpts.EnableHostVerification ||
					cc.SslOpts.ServerName != "scylla.local" || cc.SslOpts.MinVersion != 0x0304 {
					t.Errorf("ssl opts = %+v", cc.SslOpts)
				}
			},
		},
		"tls insecure": {
			config.Cassandra{TLS: config.TLS{Enabled: true, InsecureSkipVerify: true}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.SslOpts == nil || cc.SslOpts.EnableHostVerification || !cc.SslOpts.InsecureSkipVerify {
					t.Errorf("ssl opts = %+v", cc.SslOpts)
				}
			},
		},
		"tls disabled": {
			config.Cassandra{TLS: config.TLS{CAFile: "/does/not/matter"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.SslOpts != nil {
					t.Errorf("ssl opts = %+v", cc.SslOpts)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc.cfg.Hosts = []string{"s1"}
			cc, err := ClusterConfig(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, cc)
			// The driver's own validation must accept what was built.
			if err := cc.Validate(); err != nil {
				t.Errorf("driver validation: %v", err)
			}
		})
	}
}

func TestClusterConfigErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Cassandra
		want string
	}{
		"no hosts":           {config.Cassandra{}, "hosts is required"},
		"bad consistency":    {config.Cassandra{Hosts: []string{"s"}, Options: map[string]any{"consistency": "MOST"}}, "options.consistency"},
		"unknown option":     {config.Cassandra{Hosts: []string{"s"}, Options: map[string]any{"num_conn": 2}}, "unknown options: options.num_conn"},
		"owned option":       {config.Cassandra{Hosts: []string{"s"}, Options: map[string]any{"hosts": "x"}}, "options.hosts: set by its own config key"},
		"bad compression":    {config.Cassandra{Hosts: []string{"s"}, Compression: "zstd"}, "compression"},
		"snappy on v5":       {config.Cassandra{Hosts: []string{"s"}, Compression: "snappy", Options: map[string]any{"proto_version": 5}}, "snappy is not supported"},
		"missing ca file":    {config.Cassandra{Hosts: []string{"s"}, TLS: config.TLS{Enabled: true, CAFile: "/no/such/ca.pem"}}, "ca_file"},
		"cert without key":   {config.Cassandra{Hosts: []string{"s"}, TLS: config.TLS{Enabled: true, KeyFile: "k.pem"}}, "cert_file and key_file"},
		"bad tls version":    {config.Cassandra{Hosts: []string{"s"}, TLS: config.TLS{Enabled: true, MinVersion: "1.0"}}, "min_version"},
		"missing client key": {config.Cassandra{Hosts: []string{"s"}, TLS: config.TLS{Enabled: true, CertFile: "c.pem", KeyFile: "k.pem"}}, "client certificate"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ClusterConfig(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestOptionsApplyLast(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"s"}, LocalDC: "dc1"}, WithLogger(l),
		func(cc *gocql.ClusterConfig) { cc.DisableShardAwarePort = true })
	if err != nil {
		t.Fatal(err)
	}
	if !cc.DisableShardAwarePort {
		t.Error("option not applied")
	}
	sl, ok := cc.Logger.(slogLogger)
	if !ok || sl.level != slog.LevelWarn {
		t.Fatalf("logger = %#v", cc.Logger)
	}
	cc.Logger.Printf("unable to dial %q: %v\n", "s", "refused")
	if !strings.Contains(buf.String(), `level=WARN msg="unable to dial \"s\": refused"`) {
		t.Errorf("log output = %q", buf.String())
	}
}

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	l := SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})), slog.LevelInfo)
	for name, tc := range map[string]struct {
		log  func()
		want string
	}{
		"print":   {func() { l.Print("a", 1, 2, "b") }, "a1 2b"},
		"printf":  {func() { l.Printf("gocql: pool %q: %v\n", "h", errors.New("boom")) }, `gocql: pool "h": boom`},
		"println": {func() { l.Println("a", 1) }, "a 1"},
	} {
		buf.Reset()
		tc.log()
		var got map[string]any
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("%s: %v in %q", name, err, buf.String())
		}
		src, _ := got["source"].(map[string]any)
		if got["level"] != "INFO" || got["msg"] != tc.want || !strings.HasSuffix(src["file"].(string), "conn_scylla_test.go") {
			t.Errorf("%s: record = %v", name, got)
		}
	}
}

func TestSlogLoggerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	SlogLogger(slog.New(h), slog.LevelDebug).Printf("hidden %d", 1)
	SlogLogger(slog.New(h), slog.LevelError).Println("shown")
	if out := buf.String(); strings.Contains(out, "hidden") || !strings.Contains(out, "level=ERROR msg=shown") {
		t.Errorf("output = %q", out)
	}
	if _, ok := SlogLogger(nil, slog.LevelInfo).(slogLogger); !ok {
		t.Error("nil logger should fall back to slog.Default")
	}
}

func TestOpenFailsFast(t *testing.T) {
	_, err := Open(context.Background(), config.Cassandra{Hosts: []string{"127.0.0.1"}, Port: 1, Password: config.NewSecret("secret"), Options: map[string]any{"connect_timeout": "1s"}},
		WithLogger(slog.New(slog.DiscardHandler)))
	if err == nil || !strings.Contains(err.Error(), "connect scylla [127.0.0.1]") || strings.Contains(err.Error(), "secret") {
		t.Errorf("got %v", err)
	}
	if _, err := Open(context.Background(), config.Cassandra{}); err == nil {
		t.Error("invalid config should fail")
	}
}

func TestOpenHonorsContext(t *testing.T) {
	// A server that accepts connections but never answers keeps session
	// creation busy until ConnectTimeout.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close()
		}
	}()
	addr := ln.Addr().(*net.TCPAddr)
	cfg := config.Cassandra{Hosts: []string{addr.IP.String()}, Port: addr.Port, Options: map[string]any{"connect_timeout": "2s"}}
	quiet := WithLogger(slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = Open(ctx, cfg, quiet)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("context not honored: %v", time.Since(start))
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, cfg, quiet); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want canceled", err)
	}
}
