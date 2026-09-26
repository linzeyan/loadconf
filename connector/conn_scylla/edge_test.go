package conn_scylla

import (
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocql/lz4"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds characters that break naive quoting in URLs, CQL identifiers
// or SASL PLAIN framing; credentials from a secret store may contain any.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "it's", `say "hi"`, `back\slash`,
	"100%", "%s%v", "a;b", "has space", " lead", "trail ", "密碼🔑", `x"; DROP KEYSPACE ks; --`,
}

func TestClusterConfigAdversarialValues(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"s1"}, Port: 9042, Keyspace: "ks" + s, Username: "u" + s, Password: config.NewSecret(s), LocalDC: "dc" + s})
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
			if _, dc := fallbackPolicy(t, cc.PoolConfig.HostSelectionPolicy); dc != "dc"+s {
				t.Errorf("local dc = %q", dc)
			}
			cc.Logger = SlogLogger(slog.New(slog.DiscardHandler), slog.LevelWarn)
			if err := cc.Validate(); err != nil {
				t.Errorf("driver validation: %v", err)
			}
		})
	}
}

func TestClusterConfigPortsAgainstDriverValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		hosts []string
		port  int
		ok    bool
	}{
		"max port":           {[]string{"s1", "[::1]"}, 65535, true},
		"ipv6 with zone":     {[]string{"fe80::1%en0"}, 9042, true},
		"mixed host ports":   {[]string{"s1", "s2:19042"}, 9042, true},
		"unset learns ports": {[]string{"s1:19042", "[::1]:19042"}, 0, true},
		// Only when Port is unset (the config default is 9042) does the fork
		// insist that host ports agree.
		"unset mixed ports": {[]string{"s1", "s2:19042"}, 0, false},
		// config does not range-check the port; the fork does, so it still
		// fails before dialing.
		"port too large": {[]string{"s1"}, 65536, false},
	} {
		t.Run(name, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: tc.hosts, Port: tc.port}, WithLogger(slog.New(slog.DiscardHandler)))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cc.Hosts, tc.hosts) {
				t.Errorf("hosts = %v", cc.Hosts)
			}
			if err := cc.Validate(); (err == nil) != tc.ok {
				t.Errorf("driver validation = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func TestClusterConfigSnappyNeedsProtocolBelow5(t *testing.T) {
	for name, tc := range map[string]struct {
		compression string
		options     map[string]any
		ok          bool
	}{
		"snappy negotiated":      {"snappy", nil, true},
		"snappy v4":              {"Snappy", map[string]any{"proto_version": 4}, true},
		"snappy v5":              {"SNAPPY", map[string]any{"proto_version": 5}, false},
		"snappy v5 key any case": {"snappy", map[string]any{"PROTO_VERSION": "5"}, false},
		"snappy v6":              {"snappy", map[string]any{"proto_version": 6}, false},
		"lz4 v5":                 {"LZ4", map[string]any{"proto_version": 5}, true},
		"none v5":                {"", map[string]any{"proto_version": 5}, true},
	} {
		t.Run(name, func(t *testing.T) {
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"s"}, Compression: tc.compression, Options: tc.options})
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok %v", err, tc.ok)
			}
			if err != nil {
				return
			}
			if tc.compression != "" && !strings.EqualFold(cc.Compressor.Name(), tc.compression) {
				t.Errorf("compressor = %q", cc.Compressor.Name())
			}
			if _, isLZ4 := cc.Compressor.(lz4.LZ4Compressor); isLZ4 != strings.EqualFold(tc.compression, "lz4") {
				t.Errorf("compressor = %T", cc.Compressor)
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
			_, err := ClusterConfig(config.Cassandra{Hosts: []string{"s"}, Options: map[string]any{key: "x"}})
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
		"dns_resolver":  "8.8.8.8",
	} {
		t.Run(key, func(t *testing.T) {
			_, err := ClusterConfig(config.Cassandra{Hosts: []string{"s"}, Options: map[string]any{key: v}})
			if err == nil || !strings.Contains(err.Error(), "options."+key) {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestClusterConfigFreshTokenAwarePolicyPerCall(t *testing.T) {
	// The fork needs token awareness for shard routing, and one policy
	// must not be shared by two sessions.
	for _, dc := range []string{"", "dc1"} {
		cfg := config.Cassandra{Hosts: []string{"s"}, LocalDC: dc}
		a, err := ClusterConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := ClusterConfig(cfg)
		if a.PoolConfig.HostSelectionPolicy == b.PoolConfig.HostSelectionPolicy {
			t.Errorf("dc %q: host selection policy is shared between configs", dc)
		}
		fallbackPolicy(t, a.PoolConfig.HostSelectionPolicy)
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
			cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"[::1]:9142", "s2:9142"}, TLS: tc.tls}, WithLogger(slog.New(slog.DiscardHandler)))
			if err != nil {
				t.Fatal(err)
			}
			o := cc.SslOpts
			if o == nil || o.Config == nil || o.ServerName != tc.serverName || o.EnableHostVerification != tc.verify || o.InsecureSkipVerify == tc.verify {
				t.Fatalf("ssl opts = %+v", o)
			}
			if err := cc.Validate(); err != nil {
				t.Errorf("driver validation: %v", err)
			}
		})
	}
}

func TestClusterConfigErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Cassandra{
		"no hosts":        {Password: config.NewSecret(pw)},
		"bad option":      {Hosts: []string{"s"}, Password: config.NewSecret(pw), Options: map[string]any{"timeout": "soon"}},
		"unknown option":  {Hosts: []string{"s"}, Password: config.NewSecret(pw), Options: map[string]any{"pasword": pw}},
		"bad compression": {Hosts: []string{"s"}, Password: config.NewSecret(pw), Compression: "zstd"},
		"snappy on v5":    {Hosts: []string{"s"}, Password: config.NewSecret(pw), Compression: "snappy", Options: map[string]any{"proto_version": 5}},
		"bad tls":         {Hosts: []string{"s"}, Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
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

func TestDefaultAndNilLoggerUseSlogDefaultAtWarn(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for name, opts := range map[string][]Option{"no option": nil, "nil logger": {WithLogger(nil)}} {
		buf.Reset()
		cc, err := ClusterConfig(config.Cassandra{Hosts: []string{"s"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		// The fork formats with %v verbs and ends lines with '\n'; both must
		// come out as one clean message.
		cc.Logger.Printf("scylla-default-marker %d%%\n\n", 100)
		if !strings.Contains(buf.String(), `level=WARN msg="scylla-default-marker 100%"`+"\n") {
			t.Errorf("%s: output = %q", name, buf.String())
		}
	}
}

func TestSlogLoggerKeepsInnerNewlines(t *testing.T) {
	var buf bytes.Buffer
	SlogLogger(slog.New(slog.NewJSONHandler(&buf, nil)), slog.LevelWarn).Print("line1\nline2\n")
	if !strings.Contains(buf.String(), `"msg":"line1\nline2"`) {
		t.Errorf("output = %s", buf.String())
	}
}
