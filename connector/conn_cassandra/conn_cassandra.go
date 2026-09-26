// Package conn_cassandra opens Apache Cassandra sessions
// (cassandra-gocql-driver v2) from [config.Cassandra].
//
//	session, err := conn_cassandra.Open(ctx, cfg.Cassandra.MustGet("main"))
//	defer session.Close()
//
// Driver logs go to slog.Default(); [WithLogger] picks another logger.
//
// For ScyllaDB prefer github.com/linzeyan/loadconf/connector/conn_scylla, which uses Scylla's
// shard-aware driver fork.
package conn_cassandra

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the cluster config after it is built from the config, e.g.
// to set a Tracer, observers or a custom host selection policy.
type Option func(*gocql.ClusterConfig)

// WithLogger routes driver logs to l instead of slog.Default() through
// [SlogLogger].
func WithLogger(l *slog.Logger) Option {
	return func(c *gocql.ClusterConfig) { c.Logger = SlogLogger(l) }
}

// ClusterConfig converts cfg into a gocql cluster config: the driver
// defaults, then cfg.Options, then the typed fields. With LocalDC set,
// queries are routed token-aware over a DC-aware round robin that prefers
// that data center.
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
	if cfg.Port > 0 {
		cc.Port = cfg.Port
	}
	cc.Keyspace = cfg.Keyspace
	if cfg.Username != "" || cfg.Password.Value() != "" {
		cc.Authenticator = gocql.PasswordAuthenticator{Username: cfg.Username, Password: cfg.Password.Value()}
	}
	if cfg.LocalDC != "" {
		cc.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
	}
	if cfg.Retries > 0 {
		cc.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cfg.Retries}
	}
	switch strings.ToLower(cfg.Compression) {
	case "snappy":
		// Cassandra does not offer snappy with protocol v5; the driver then
		// falls back to no compression.
		cc.Compressor = snappy.SnappyCompressor{}
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
	cc.Logger = SlogLogger(nil)

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
	return createSession(ctx, cc)
}

func createSession(ctx context.Context, cc *gocql.ClusterConfig) (*gocql.Session, error) {
	target := fmt.Sprintf("cassandra %v", cc.Hosts)
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
