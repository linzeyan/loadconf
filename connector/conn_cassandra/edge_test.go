package conn_cassandra

import (
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds characters that break naive quoting in URLs, CQL identifiers
// or SASL PLAIN framing; credentials from a secret store may contain any.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "it's", `say "hi"`, `back\slash`,
	"100%", "a;b", "has space", " lead", "trail ", "密碼🔑", `x"; DROP KEYSPACE ks; --`,
}

func TestClusterConfigAdversarialValues(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"c1"}, Keyspace: "ks" + s, Username: "u" + s, Password: config.NewSecret(s), LocalDC: "dc" + s})
			if err != nil {
				t.Fatal(err)
			}
			// Values reach the driver untouched: no trimming, unescaping or
			// case folding that would log in as someone else.
			a, ok := cc.Authenticator.(gocql.PasswordAuthenticator)
			if !ok || a.Username != "u"+s || a.Password != s || cc.Keyspace != "ks"+s {
				t.Fatalf("authenticator %#v keyspace %q", cc.Authenticator, cc.Keyspace)
			}
			resp, _, err := a.Challenge([]byte("org.apache.cassandra.auth.PasswordAuthenticator"))
			if err != nil || string(resp) != "\x00u"+s+"\x00"+s {
				t.Errorf("sasl response %q, err %v", resp, err)
			}
		})
	}
}

func TestClusterConfigHostsAndPorts(t *testing.T) {
	hosts := []string{"[::1]:9043", "::1", "fe80::1%en0", "c1:9042", "c1", "密碼.example"}
	for name, tc := range map[string]struct{ port, want int }{
		"unset uses driver default": {0, 9042},
		"negative ignored":          {-1, 9042},
		"max":                       {65535, 65535},
		"min":                       {1, 1},
	} {
		t.Run(name, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: hosts, Port: tc.port})
			if err != nil {
				t.Fatal(err)
			}
			// Host strings are the driver's to parse; rewriting them would
			// break IPv6 zones or per-host ports.
			if cc.Port != tc.want || !reflect.DeepEqual(cc.Hosts, hosts) {
				t.Errorf("port %d hosts %v", cc.Port, cc.Hosts)
			}
		})
	}
}

func TestClusterConfigCompressionSpellings(t *testing.T) {
	for in, want := range map[string]string{"": "", "LZ4": "lz4", "Lz4": "lz4", "SNAPPY": "snappy", " lz4": "error", "lz4 ": "error", "none": "error", "zstd": "error"} {
		t.Run(in, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"c"}, Compression: in})
			got := "error"
			if err == nil {
				switch cc.Compressor.(type) {
				case nil:
					got = ""
				case lz4.LZ4Compressor:
					got = "lz4"
				case snappy.SnappyCompressor:
					got = "snappy"
				}
			}
			if got != want {
				t.Errorf("compressor = %q, want %q (err %v)", got, want, err)
			}
		})
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for key, want := range map[string]string{
		"HOSTS":    "options.hosts: set by its own config key",
		"Keyspace": "options.keyspace: set by its own config key",
		"PORT":     "options.port: set by its own config key",
		"Ssl_Opts": "options.ssl_opts: set by its own config key",
		// The Go field spelling is not a config key at all.
		"SslOpts": "unknown options: options.SslOpts",
	} {
		t.Run(key, func(t *testing.T) {
			_, err := ClusterConfig(config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{key: "x"}})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want %q", err, want)
			}
		})
	}
}

func TestOptionsCannotInstallDriverObjects(t *testing.T) {
	// Authenticator, logger and policies are interfaces: options must fail
	// loudly rather than drop the value or bypass the Secret-typed password.
	for key, v := range map[string]any{
		"authenticator": map[string]any{"username": "u", "password": "hunter2"},
		"auth_provider": "hunter2",
		"logger":        "stdout",
		"retry_policy":  map[string]any{"num_retries": 3},
		"pool_config":   map[string]any{"host_selection_policy": "round_robin"},
	} {
		t.Run(key, func(t *testing.T) {
			_, err := ClusterConfig(config.Cassandra{Hosts: []string{"c"}, Options: map[string]any{key: v}})
			if err == nil || !strings.Contains(err.Error(), "options."+key) {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestAuthenticatorOnlyWithCredentials(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Cassandra
		want bool
	}{
		"none":          {config.Cassandra{}, false},
		"user only":     {config.Cassandra{Username: "u"}, true},
		"password only": {config.Cassandra{Password: config.NewSecret("p")}, true},
	} {
		t.Run(name, func(t *testing.T) {
			tc.cfg.Hosts = []string{"c"}
			cc, err := ClusterConfig(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if got := cc.Authenticator != nil; got != tc.want {
				t.Errorf("authenticator = %#v", cc.Authenticator)
			}
		})
	}
}

func TestClusterConfigFreshPolicyPerCall(t *testing.T) {
	// gocql panics or misroutes when one policy is shared by two sessions,
	// so every call must build a new one.
	cfg := config.Cassandra{Hosts: []string{"c"}, LocalDC: "dc1"}
	a, err := ClusterConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ClusterConfig(cfg)
	if a.PoolConfig.HostSelectionPolicy == b.PoolConfig.HostSelectionPolicy {
		t.Error("host selection policy is shared between configs")
	}
}

func TestClusterConfigTLSServerName(t *testing.T) {
	for name, tc := range map[string]struct {
		tls        config.TLS
		serverName string
		verify     bool
	}{
		// An empty ServerName lets gocql verify each node against its own
		// address; a fixed one would break multi-host clusters.
		"no server name": {config.TLS{Enabled: true}, "", true},
		"server name":    {config.TLS{Enabled: true, ServerName: "db.internal"}, "db.internal", true},
		"insecure":       {config.TLS{Enabled: true, ServerName: "db.internal", InsecureSkipVerify: true}, "db.internal", false},
	} {
		t.Run(name, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"[::1]:9142", "c2"}, TLS: tc.tls})
			if err != nil {
				t.Fatal(err)
			}
			o := cc.SslOpts
			if o == nil || o.Config == nil || o.ServerName != tc.serverName || o.EnableHostVerification != tc.verify || o.InsecureSkipVerify == tc.verify {
				t.Errorf("ssl opts = %+v", o)
			}
		})
	}
}

func TestClusterConfigErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Cassandra{
		"no hosts":        {Password: config.NewSecret(pw)},
		"bad option":      {Hosts: []string{"c"}, Password: config.NewSecret(pw), Options: map[string]any{"timeout": "soon"}},
		"unknown option":  {Hosts: []string{"c"}, Password: config.NewSecret(pw), Options: map[string]any{"pasword": pw}},
		"bad compression": {Hosts: []string{"c"}, Password: config.NewSecret(pw), Compression: "zstd"},
		"bad tls":         {Hosts: []string{"c"}, Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ClusterConfig(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestDefaultAndNilLoggerUseSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for name, opts := range map[string][]Option{"no option": nil, "nil logger": {WithLogger(nil)}} {
		buf.Reset()
		cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"c"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		cc.Logger.Warning("cassandra-default-marker", gocql.NewLogFieldString("host", "c"))
		if !strings.Contains(buf.String(), "level=WARN msg=cassandra-default-marker host=c") {
			t.Errorf("%s: output = %q", name, buf.String())
		}
	}
}
