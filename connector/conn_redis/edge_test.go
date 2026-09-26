package conn_redis

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/linzeyan/loadconf/config"
)

// clientKind names the go-redis client New builds; a failover (sentinel)
// client is a *redis.Client too, so the options tell them apart.
func clientKind(t *testing.T, cfg config.Redis) string {
	t.Helper()
	c, err := New(cfg)
	if err != nil {
		return "error"
	}
	defer c.Close()
	o, _ := Options(cfg)
	switch c.(type) {
	case *redis.ClusterClient:
		return "cluster"
	case *redis.Client:
		if o.MasterName != "" {
			return "sentinel"
		}
		return "single"
	}
	return "other"
}

func TestModeInferenceEdgeCases(t *testing.T) {
	two := []string{"a:1", "b:2"}
	for name, tc := range map[string]struct {
		cfg  config.Redis
		want string
	}{
		"master name beats several addrs":   {config.Redis{Addrs: two, MasterName: "m"}, "sentinel"},
		"master name with one sentinel":     {config.Redis{Addrs: []string{"s:26379"}, MasterName: "m", DB: 2}, "sentinel"},
		"explicit cluster ignores master":   {config.Redis{Mode: "cluster", Addrs: []string{"a:1"}, MasterName: "m"}, "cluster"},
		"explicit single ignores master":    {config.Redis{Mode: "single", Addrs: []string{"a:1"}, MasterName: "m"}, "single"},
		"one addr":                          {config.Redis{Addrs: []string{"a:1"}, DB: 15}, "single"},
		"inferred cluster rejects db":       {config.Redis{Addrs: two, DB: 1}, "error"},
		"explicit single rejects two addrs": {config.Redis{Mode: "single", Addrs: two}, "error"},
		"sentinel needs master name":        {config.Redis{Mode: "sentinel", Addrs: two}, "error"},
		"mode is case-sensitive":            {config.Redis{Mode: "Cluster", Addrs: two}, "error"},
		"no addrs":                          {config.Redis{MasterName: "m"}, "error"},
		"sentinel keeps db":                 {config.Redis{Mode: "sentinel", Addrs: two, MasterName: "m", DB: 3}, "sentinel"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := clientKind(t, tc.cfg); got != tc.want {
				t.Errorf("client = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestSentinelRoutingOptionsKeepFailover(t *testing.T) {
	// route_by_latency in sentinel mode must give a failover cluster client
	// (reads from replicas; go-redis returns it as a *redis.ClusterClient),
	// never a plain cluster client that treats the sentinels as data nodes:
	// that depends on MasterName being kept and IsClusterMode unset.
	o, err := Options(config.Redis{Addrs: []string{"s1:26379", "s2:26379"}, MasterName: "m", Options: map[string]any{"route_by_latency": true}})
	if err != nil {
		t.Fatal(err)
	}
	if o.IsClusterMode || o.MasterName != "m" || !o.RouteByLatency {
		t.Errorf("options = %+v", o)
	}
	c := redis.NewUniversalClient(o)
	defer c.Close()
	if _, ok := c.(*redis.ClusterClient); !ok {
		t.Fatalf("client = %T", c)
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for name, opts := range map[string]map[string]any{
		"password upper":          {"PASSWORD": "hunter2"},
		"sentinel password mixed": {"Sentinel_Password": "hunter2"},
		"username":                {"UserName": "admin"},
		"addrs":                   {"Addrs": []string{"evil:6379"}},
		"db":                      {"DB": 5},
		"master name":             {"MASTER_NAME": "evil"},
		"cluster mode":            {"is_cluster_mode": true},
		"tls config":              {"TLS_Config": map[string]any{"InsecureSkipVerify": true}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Options(config.Redis{Addrs: []string{"a:6379"}, Options: opts})
			if err == nil || !strings.Contains(err.Error(), "set by its own config key") {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsTypedFieldsAlwaysWin(t *testing.T) {
	o, err := Options(config.Redis{
		Addrs: []string{"[::1]:6379"}, Username: "u", Password: config.NewSecret("p@ss:/"), DB: 2,
		SentinelUsername: "su", SentinelPassword: config.NewSecret("sp"),
		TLS:     config.TLS{ServerName: "ignored-when-disabled"},
		Options: map[string]any{"client_name": "svc", "protocol": 2, "pool_size": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Addrs[0] != "[::1]:6379" || o.Username != "u" || o.Password != "p@ss:/" || o.DB != 2 || o.SentinelUsername != "su" || o.SentinelPassword != "sp" {
		t.Errorf("options = %+v", o)
	}
	// Disabled TLS must yield no TLS config at all, not an empty one that
	// would switch the client to TLS.
	if o.TLSConfig != nil || o.ClientName != "svc" || o.Protocol != 2 || o.PoolSize != 3 {
		t.Errorf("tls %v client name %q protocol %d pool %d", o.TLSConfig, o.ClientName, o.Protocol, o.PoolSize)
	}
}

func TestOptionsErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Redis{
		"bad option value": {Addrs: []string{"a:6379"}, Password: config.NewSecret(pw), Options: map[string]any{"pool_size": "many"}},
		"unknown option":   {Addrs: []string{"a:6379"}, Password: config.NewSecret(pw), Options: map[string]any{"pasword": pw}},
		"bad tls":          {Addrs: []string{"a:6379"}, Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
		"invalid mode":     {Mode: "bogus", Addrs: []string{"a:6379"}, Password: config.NewSecret(pw)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Options(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			// An unknown key is named, but its value must not be printed.
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestSlogLoggerNilAndFormatting(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	p := SlogLogger(nil, slog.LevelWarn)
	p.Printf(context.Background(), "redis: 100%% of %d conns closed\n", 3)
	p.Printf(context.Background(), "keep redis: inner prefix")
	out := buf.String()
	if !strings.Contains(out, `level=WARN msg="100% of 3 conns closed"`) || !strings.Contains(out, `msg="keep redis: inner prefix"`) {
		t.Errorf("output = %s", out)
	}
}
