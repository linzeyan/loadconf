package conn_redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/linzeyan/loadconf/config"
)

func TestClientTypeByMode(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Redis
		want any
	}{
		"single":           {config.Redis{Addrs: []string{"a:6379"}, DB: 3}, &redis.Client{}},
		"cluster inferred": {config.Redis{Addrs: []string{"a:1", "b:2"}}, &redis.ClusterClient{}},
		"cluster one seed": {config.Redis{Mode: config.RedisCluster, Addrs: []string{"a:1"}}, &redis.ClusterClient{}},
		"sentinel":         {config.Redis{Addrs: []string{"s1:26379", "s2:26379"}, MasterName: "mymaster"}, &redis.Client{}},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := New(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			switch tc.want.(type) {
			case *redis.Client:
				if _, ok := c.(*redis.Client); !ok {
					t.Errorf("got %T", c)
				}
			case *redis.ClusterClient:
				if _, ok := c.(*redis.ClusterClient); !ok {
					t.Errorf("got %T", c)
				}
			}
		})
	}
}

func TestOptionsMapping(t *testing.T) {
	o, err := Options(config.Redis{
		Mode:             config.RedisSingle,
		Addrs:            []string{"a:6379"},
		MasterName:       "ignored-in-single-mode",
		Password:         config.NewSecret("pw"),
		DB:               1,
		TLS:              config.TLS{Enabled: true, ServerName: "redis.local"},
		SentinelPassword: config.NewSecret("spw"),
		Options:          map[string]any{"pool_size": 7, "dial_timeout": "1s", "route_by_latency": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.MasterName != "" || o.Password != "pw" || o.DB != 1 || o.PoolSize != 7 || o.DialTimeout != time.Second ||
		!o.RouteByLatency || o.TLSConfig == nil || o.TLSConfig.ServerName != "redis.local" || o.SentinelPassword != "spw" {
		t.Errorf("options = %+v", o)
	}

	if _, err := Options(config.Redis{}); err == nil {
		t.Error("invalid config should fail")
	}
}

func TestOptionsRejectOwnedAndUnknownKeys(t *testing.T) {
	// A password in options would bypass the Secret type and compete with
	// the typed field.
	_, err := Options(config.Redis{Addrs: []string{"a:6379"}, Options: map[string]any{"password": "x", "pool_sise": 1}})
	if err == nil || !strings.Contains(err.Error(), "options.password: set by its own config key") ||
		!strings.Contains(err.Error(), "unknown options: options.pool_sise") {
		t.Errorf("err = %v", err)
	}
}

func TestOpenFailsFast(t *testing.T) {
	_, err := Open(context.Background(), config.Redis{
		Addrs: []string{"127.0.0.1:1"}, PingTimeout: 500 * time.Millisecond, Options: map[string]any{"max_retries": -1},
	}, func(o *redis.UniversalOptions) { o.DialTimeout = 200 * time.Millisecond })
	if err == nil {
		t.Error("expected ping failure")
	}
}
