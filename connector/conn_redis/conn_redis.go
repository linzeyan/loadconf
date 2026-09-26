// Package conn_redis opens go-redis clients from [config.Redis].
//
//	rdb, err := conn_redis.Open(ctx, cfg.Redis.MustGet("cache"))
//	defer rdb.Close()
//
// go-redis logs through a process-wide logger; route it to slog with
//
//	redis.SetLogger(conn_redis.SlogLogger(logger, slog.LevelWarn))
package conn_redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the go-redis options after they are built from the config,
// e.g. to set a Dialer or hooks the config cannot express.
type Option func(*redis.UniversalOptions)

// Options converts cfg into go-redis universal options: cfg.Options first,
// then the typed fields.
func Options(cfg config.Redis) (*redis.UniversalOptions, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	tlsCfg, err := cfg.TLS.Config()
	if err != nil {
		return nil, err
	}
	o := &redis.UniversalOptions{}
	if err := config.DecodeOptions(cfg.Options, o, "addrs", "is_cluster_mode", "master_name",
		"username", "password", "db", "sentinel_username", "sentinel_password", "tls_config"); err != nil {
		return nil, err
	}
	mode := cfg.ResolvedMode()
	o.Addrs = cfg.Addrs
	o.IsClusterMode = mode == config.RedisCluster
	o.Username, o.Password = cfg.Username, cfg.Password.Value()
	o.DB = cfg.DB
	o.SentinelUsername, o.SentinelPassword = cfg.SentinelUsername, cfg.SentinelPassword.Value()
	o.TLSConfig = tlsCfg
	// go-redis picks the client type from MasterName, so only pass it along
	// in sentinel mode.
	if mode == config.RedisSentinel {
		o.MasterName = cfg.MasterName
	}
	return o, nil
}

// New creates a client without connecting: a *redis.Client, a
// *redis.ClusterClient or a sentinel-backed failover client depending on the
// mode.
func New(cfg config.Redis, opts ...Option) (redis.UniversalClient, error) {
	o, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(o)
	}
	return redis.NewUniversalClient(o), nil
}

// Open creates a client and verifies it with PING, bounded by
// cfg.PingTimeout. The client is closed if the ping fails.
func Open(ctx context.Context, cfg config.Redis, opts ...Option) (redis.UniversalClient, error) {
	c, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis ping %v: %w", cfg.Addrs, err)
	}
	return c, nil
}
