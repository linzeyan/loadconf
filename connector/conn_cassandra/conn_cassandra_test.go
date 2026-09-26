package conn_cassandra

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

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"

	"github.com/linzeyan/loadconf/config"
)

func TestClusterConfigDefaults(t *testing.T) {
	cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"c1", "c2:9043"}})
	if err != nil {
		t.Fatal(err)
	}
	def := gocql.NewCluster()
	if !reflect.DeepEqual(cc.Hosts, []string{"c1", "c2:9043"}) || cc.Port != def.Port || cc.Consistency != def.Consistency ||
		cc.SerialConsistency != 0 || cc.Timeout != def.Timeout || cc.ConnectTimeout != def.ConnectTimeout ||
		cc.NumConns != def.NumConns || cc.PageSize != def.PageSize || cc.ReconnectInterval != def.ReconnectInterval ||
		cc.ProtoVersion != 0 {
		t.Errorf("defaults changed: %+v", cc)
	}
	if cc.Authenticator != nil || cc.RetryPolicy != nil || cc.Compressor != nil || cc.SslOpts != nil ||
		cc.PoolConfig.HostSelectionPolicy != nil {
		t.Errorf("unexpected driver settings: %+v", cc)
	}
	// Driver logs reach the application's slog logger without any wiring.
	if _, ok := cc.Logger.(slogLogger); !ok {
		t.Errorf("logger = %T, want the slog adapter", cc.Logger)
	}
}

func TestClusterConfigMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg   config.Cassandra
		check func(*testing.T, *gocql.ClusterConfig)
	}{
		"connection": {
			config.Cassandra{Port: 9142, Keyspace: "app", Options: map[string]any{"proto_version": 4, "disable_initial_host_lookup": true}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Port != 9142 || cc.Keyspace != "app" || cc.ProtoVersion != 4 || !cc.DisableInitialHostLookup {
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
		"consistency": {
			config.Cassandra{Options: map[string]any{"consistency": "LOCAL_QUORUM", "serial_consistency": "LOCAL_SERIAL"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.Consistency != gocql.LocalQuorum || cc.SerialConsistency != gocql.LocalSerial {
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
			config.Cassandra{Retries: 3},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if p, ok := cc.RetryPolicy.(*gocql.SimpleRetryPolicy); !ok || p.NumRetries != 3 {
					t.Errorf("retry policy = %#v", cc.RetryPolicy)
				}
			},
		},
		"snappy": {
			config.Cassandra{Compression: "Snappy"},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if _, ok := cc.Compressor.(snappy.SnappyCompressor); !ok {
					t.Errorf("compressor = %T", cc.Compressor)
				}
			},
		},
		"lz4": {
			config.Cassandra{Compression: "lz4"},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if _, ok := cc.Compressor.(lz4.LZ4Compressor); !ok || cc.Compressor.Name() != "lz4" {
					t.Errorf("compressor = %T", cc.Compressor)
				}
			},
		},
		"local dc": {
			config.Cassandra{LocalDC: "dc1"},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				p := reflect.ValueOf(cc.PoolConfig.HostSelectionPolicy)
				if p.Type().String() != "*gocql.tokenAwareHostPolicy" {
					t.Fatalf("policy = %v", p.Type())
				}
				fallback := p.Elem().FieldByName("fallback").Elem()
				if fallback.Type().String() != "*gocql.dcAwareRR" || fallback.Elem().FieldByName("local").String() != "dc1" {
					t.Errorf("fallback = %v", fallback.Type())
				}
			},
		},
		"tls verified": {
			config.Cassandra{TLS: config.TLS{Enabled: true, ServerName: "cass.local", MinVersion: "1.3"}},
			func(t *testing.T, cc *gocql.ClusterConfig) {
				if cc.SslOpts == nil || cc.SslOpts.Config == nil || !cc.SslOpts.EnableHostVerification ||
					cc.SslOpts.ServerName != "cass.local" || cc.SslOpts.MinVersion != 0x0304 {
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
			tc.cfg.Hosts = []string{"c1"}
			cc, err := ClusterConfig(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, cc)
		})
	}
}

func TestClusterConfigErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Cassandra
		want string
	}{
		"no hosts":           {config.Cassandra{}, "hosts is required"},
		"bad consistency":    {config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{"consistency": "MOST"}}, "options.consistency"},
		"unknown option":     {config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{"num_conn": 2}}, "unknown options: options.num_conn"},
		"owned option":       {config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{"keyspace": "k"}}, "options.keyspace: set by its own config key"},
		"bad compression":    {config.Cassandra{Hosts: []string{"c"}, Compression: "zstd"}, "compression"},
		"missing ca file":    {config.Cassandra{Hosts: []string{"c"}, TLS: config.TLS{Enabled: true, CAFile: "/no/such/ca.pem"}}, "ca_file"},
		"cert without key":   {config.Cassandra{Hosts: []string{"c"}, TLS: config.TLS{Enabled: true, CertFile: "c.pem"}}, "cert_file and key_file"},
		"bad tls version":    {config.Cassandra{Hosts: []string{"c"}, TLS: config.TLS{Enabled: true, MinVersion: "1.1"}}, "min_version"},
		"missing client key": {config.Cassandra{Hosts: []string{"c"}, TLS: config.TLS{Enabled: true, CertFile: "c.pem", KeyFile: "k.pem"}}, "client certificate"},
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
	cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{"num_conns": 4}}, WithLogger(l),
		func(cc *gocql.ClusterConfig) { cc.NumConns = 1 })
	if err != nil {
		t.Fatal(err)
	}
	if cc.NumConns != 1 {
		t.Errorf("option did not override: num conns = %d", cc.NumConns)
	}
	if _, ok := cc.Logger.(slogLogger); !ok {
		t.Fatalf("logger = %T", cc.Logger)
	}
	cc.Logger.Info("hello")
	if !strings.Contains(buf.String(), "msg=hello") {
		t.Errorf("log output = %q", buf.String())
	}
}

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	l := SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})))
	fields := []gocql.LogField{
		gocql.NewLogFieldString("host_addr", "10.0.0.1"),
		gocql.NewLogFieldInt("port", 9042),
		gocql.NewLogFieldBool("up", true),
		gocql.NewLogFieldIP("ip", net.IPv4(10, 0, 0, 2)),
		gocql.NewLogFieldError("err", errors.New("boom")),
		{Name: "nil"},
	}
	for level, logf := range map[string]func(string, ...gocql.LogField){
		"DEBUG": l.Debug, "INFO": l.Info, "WARN": l.Warning, "ERROR": l.Error,
	} {
		buf.Reset()
		logf("msg "+level, fields...)
		var got map[string]any
		if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
			t.Fatalf("%s: %v in %q", level, err, buf.String())
		}
		src, _ := got["source"].(map[string]any)
		if got["level"] != level || got["msg"] != "msg "+level || got["host_addr"] != "10.0.0.1" || got["port"] != float64(9042) ||
			got["up"] != true || got["ip"] != "10.0.0.2" || got["err"] != "boom" || got["nil"] != nil ||
			!strings.HasSuffix(src["file"].(string), "conn_cassandra_test.go") {
			t.Errorf("%s: record = %v", level, got)
		}
	}
}

func TestSlogLoggerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	l := SlogLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	l.Debug("debug")
	l.Info("info")
	l.Warning("warn")
	if out := buf.String(); strings.Contains(out, "debug") || strings.Contains(out, "info") || !strings.Contains(out, "msg=warn") {
		t.Errorf("output = %q", out)
	}
	if _, ok := SlogLogger(nil).(slogLogger); !ok {
		t.Error("nil logger should fall back to slog.Default")
	}
}

func TestOpenFailsFast(t *testing.T) {
	_, err := Open(context.Background(), config.Cassandra{Hosts: []string{"127.0.0.1"}, Port: 1, Password: config.NewSecret("secret"), Options: map[string]any{"connect_timeout": "1s"}})
	if err == nil || !strings.Contains(err.Error(), "connect cassandra [127.0.0.1]") || strings.Contains(err.Error(), "secret") {
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

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = Open(ctx, cfg)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want deadline exceeded", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("context not honored: %v", time.Since(start))
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, cfg); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want canceled", err)
	}
}
