// Package conn_scylla opens ScyllaDB sessions from [config.Cassandra] with
// Scylla's shard-aware gocql fork (github.com/scylladb/gocql).
//
// # Required replace directive
//
// The fork keeps the import path github.com/gocql/gocql, and replace
// directives only apply in the main module. Every application that uses this
// package must therefore add the same line to its own go.mod:
//
//	replace github.com/gocql/gocql => github.com/scylladb/gocql v1.19.0
//
// Without it the build silently falls back to upstream gocql v1.7.0: it
// compiles and talks to ScyllaDB over plain CQL, but without shard-aware
// routing, per-shard connections or the shard-aware port. [IsScyllaFork]
// reports which driver was linked and [Open] logs a warning through the
// cluster logger when it is not the fork.
//
// # Usage
//
//	session, err := conn_scylla.Open(ctx, cfg.Cassandra.MustGet("scylla"))
//	defer session.Close()
//
// Driver logs go to slog.Default() at Warn; [WithLogger] picks another
// logger.
//
// Queries are always routed token-aware, which the fork requires for shard
// awareness; LocalDC selects a DC-aware fallback. The shard-aware port is used
// automatically when the nodes expose it. With ScyllaDB the driver opens one
// connection per shard and ignores NumConns.
package conn_scylla

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocql/lz4"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the cluster config after it is built from the config, e.g.
// to set a Dialer, observers or a custom host selection policy.
type Option func(*gocql.ClusterConfig)

// WithLogger routes driver logs to l instead of slog.Default(), at
// [slog.LevelWarn] through [SlogLogger].
func WithLogger(l *slog.Logger) Option {
	return func(c *gocql.ClusterConfig) { c.Logger = SlogLogger(l, slog.LevelWarn) }
}

// IsScyllaFork reports whether github.com/gocql/gocql resolves to Scylla's
// fork, i.e. whether the application's go.mod has the replace directive.
func IsScyllaFork() bool {
	_, ok := reflect.TypeFor[gocql.ClusterConfig]().FieldByName("DisableShardAwarePort")
	return ok
}

// ClusterConfig converts cfg into a gocql cluster config: the driver
// defaults, then cfg.Options, then the typed fields. The host selection
// policy is always token-aware (over a DC-aware round robin when LocalDC is
// set, else a plain round robin) as the fork needs it for shard awareness.
//
// The returned config carries a fresh host selection policy, which gocql does
// not allow to share between sessions: build a new config per session.
func ClusterConfig(cfg config.Cassandra, opts ...Option) (*gocql.ClusterConfig, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cc := gocql.NewCluster(cfg.Hosts...)
	if err := config.DecodeOptions(cfg.Options, cc, "hosts", "port", "keyspace", "ssl_opts"); err != nil {
		return nil, err
	}
	compression := strings.ToLower(cfg.Compression)
	if compression == "snappy" && cc.ProtoVersion >= 5 {
		return nil, fmt.Errorf("compression snappy is not supported with proto_version %d (use lz4)", cc.ProtoVersion)
	}
	if cfg.Port > 0 {
		cc.Port = cfg.Port
	}
	cc.Keyspace = cfg.Keyspace
	if cfg.Username != "" || cfg.Password.Value() != "" {
		cc.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password.Value()}
	}
	fallback := gocql.RoundRobinHostPolicy()
	if cfg.LocalDC != "" {
		fallback = gocql.DCAwareRoundRobinPolicy(cfg.LocalDC)
	}
	cc.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	if cfg.Retries > 0 {
		cc.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cfg.Retries}
	}
	switch compression {
	case "snappy":
		cc.Compressor = gocql.SnappyCompressor{}
	case "lz4":
		cc.Compressor = lz4.LZ4Compressor{}
	}

	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		// gocql derives ServerName from each host address when it is empty.
		cc.SslOpts = &gocql.SslOptions{Config: tlsCfg, EnableHostVerification: !cfg.TLS.InsecureSkipVerify}
	}
	cc.Logger = SlogLogger(nil, slog.LevelWarn)

	for _, opt := range opts {
		opt(cc)
	}
	return cc, nil
}

// Open builds the cluster config and creates a session, which connects to the
// cluster and discovers its topology.
//
// gocql cannot cancel session creation, so it runs in the background while
// Open waits for it or for ctx: when ctx ends first, Open returns ctx.Err()
// and a session that is still created later is closed right away. Bound the
// attempt with a context deadline or options.connect_timeout.
func Open(ctx context.Context, cfg config.Cassandra, opts ...Option) (*gocql.Session, error) {
	cc, err := ClusterConfig(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if !IsScyllaFork() {
		var logger gocql.StdLogger = log.Default()
		if cc.Logger != nil {
			logger = cc.Logger
		}
		logger.Println("conn_scylla: github.com/gocql/gocql is not Scylla's fork, shard awareness is off; " +
			"add `replace github.com/gocql/gocql => github.com/scylladb/gocql v1.19.0` to go.mod")
	}
	return createSession(ctx, cc)
}

func createSession(ctx context.Context, cc *gocql.ClusterConfig) (*gocql.Session, error) {
	target := fmt.Sprintf("scylla %v", cc.Hosts)
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect %s: %w", target, err)
	}
	type result struct {
		session *gocql.Session
		err     error
	}
	done := make(chan result, 1)
	go func() {
		s, err := cc.CreateSession()
		done <- result{s, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return nil, fmt.Errorf("connect %s: %w", target, r.err)
		}
		return r.session, nil
	case <-ctx.Done():
		go func() {
			if r := <-done; r.session != nil {
				r.session.Close()
			}
		}()
		return nil, fmt.Errorf("connect %s: %w", target, ctx.Err())
	}
}
