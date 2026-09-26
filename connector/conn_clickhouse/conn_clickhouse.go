// Package conn_clickhouse opens ClickHouse connections with
// github.com/ClickHouse/clickhouse-go/v2 from [config.ClickHouse], either as
// the driver's native API or as a database/sql pool:
//
//	conn, err := conn_clickhouse.Open(ctx, cfg.ClickHouse.MustGet("olap"))
//	db, err := conn_clickhouse.OpenDB(ctx, cfg.ClickHouse.MustGet("olap"))
//
// Both work over the native protocol (9000/9440) or HTTP (8123/8443). The
// driver logs to slog.Default(); [WithLogger] picks another logger.
package conn_clickhouse

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/linzeyan/loadconf/config"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	hooks  []func(*clickhouse.Options)
	logger *slog.Logger
}

// WithOptions adjusts the driver options after they are built from the
// config, e.g. to set DialContext, GetJWT or ClientInfo.
func WithOptions(fn func(*clickhouse.Options)) Option {
	return func(o *options) { o.hooks = append(o.hooks, fn) }
}

// WithLogger sets the driver's structured logger (clickhouse.Options.Logger)
// instead of slog.Default().
func WithLogger(l *slog.Logger) Option { return func(o *options) { o.logger = l } }

var compressions = map[string]clickhouse.CompressionMethod{
	"none":    clickhouse.CompressionNone,
	"lz4":     clickhouse.CompressionLZ4,
	"lz4hc":   clickhouse.CompressionLZ4HC,
	"zstd":    clickhouse.CompressionZSTD,
	"gzip":    clickhouse.CompressionGZIP,
	"deflate": clickhouse.CompressionDeflate,
	"br":      clickhouse.CompressionBrotli,
}

// Options converts cfg into driver options. The DSN is parsed first, then
// cfg.Options and the fields that are set override it. With a DSN its scheme selects the
// protocol (clickhouse:// or tcp:// native, http:// or https:// HTTP), so
// protocol http needs an http(s) DSN and tls.enabled cannot be combined with
// an http:// DSN.
func Options(cfg config.ClickHouse, opts ...Option) (*clickhouse.Options, error) {
	o := buildOptions(opts)
	return newOptions(cfg, o)
}

func buildOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func newOptions(cfg config.ClickHouse, o options) (*clickhouse.Options, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	httpProto := strings.EqualFold(cfg.Protocol, "http")

	var co *clickhouse.Options
	if dsn := cfg.DSN.Value(); dsn != "" {
		var err error
		if co, err = clickhouse.ParseDSN(dsn); err != nil {
			// The driver's errors quote the DSN, or parts of it such as an
			// http_proxy URL with its credentials.
			return nil, errors.New("clickhouse dsn: invalid (details withheld: they may contain credentials)")
		}
		scheme := strings.ToLower(dsn[:max(strings.Index(dsn, "://"), 0)])
		if httpProto && co.Protocol != clickhouse.HTTP {
			return nil, errors.New("clickhouse: protocol http needs an http:// or https:// dsn")
		}
		if cfg.TLS.Enabled && scheme == "http" {
			return nil, errors.New("clickhouse: tls.enabled needs an https:// dsn")
		}
	} else {
		co = &clickhouse.Options{Settings: clickhouse.Settings{}}
		if httpProto {
			co.Protocol = clickhouse.HTTP
		}
	}

	// Options merge into what the DSN set (settings and headers key by key);
	// the typed fields below override both.
	if err := config.DecodeOptions(cfg.Options, co, "protocol", "addr", "auth", "tls", "compression.method",
		"max_open_conns", "max_idle_conns", "conn_max_lifetime"); err != nil {
		return nil, fmt.Errorf("clickhouse: %w", err)
	}

	if len(cfg.Addrs) > 0 {
		co.Addr = cfg.Addrs
	}
	if cfg.Database != "" {
		co.Auth.Database = cfg.Database
	}
	if cfg.Username != "" {
		co.Auth.Username = cfg.Username
	}
	if pw := cfg.Password.Value(); pw != "" {
		co.Auth.Password = pw
	}
	if cfg.Compression != "" {
		if co.Compression == nil {
			co.Compression = &clickhouse.Compression{}
		}
		co.Compression.Method = compressions[strings.ToLower(cfg.Compression)]
		// Level 0 means no compression at all for gzip and deflate.
		if co.Compression.Level == 0 {
			co.Compression.Level = 3
		}
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, fmt.Errorf("clickhouse tls: %w", err)
		}
		co.TLS = tlsCfg
	}

	if cfg.MaxOpenConns > 0 {
		co.MaxOpenConns = cfg.MaxOpenConns
	}
	if cfg.MaxIdleConns > 0 {
		co.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.ConnMaxLifetime > 0 {
		co.ConnMaxLifetime = cfg.ConnMaxLifetime
	}
	co.Logger = o.logger
	if co.Logger == nil {
		co.Logger = slog.Default()
	}
	for _, fn := range o.hooks {
		fn(co)
	}
	return co, nil
}

// New builds the driver's native API, a pool sized by the pool settings,
// without connecting: the driver dials on first use.
func New(cfg config.ClickHouse, opts ...Option) (chdriver.Conn, error) {
	conn, _, err := newConn(cfg, opts)
	return conn, err
}

// Open is [New] plus a ping within cfg.PingTimeout. The connection is closed
// if the ping fails.
func Open(ctx context.Context, cfg config.ClickHouse, opts ...Option) (chdriver.Conn, error) {
	conn, co, err := newConn(cfg, opts)
	if err != nil {
		return nil, err
	}
	if err := ping(ctx, cfg.PingTimeout, conn.Ping); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse %s: %w", target(co), err)
	}
	return conn, nil
}

// Connector returns a database/sql connector for sql.OpenDB.
func Connector(cfg config.ClickHouse, opts ...Option) (driver.Connector, error) {
	co, err := Options(cfg, opts...)
	if err != nil {
		return nil, err
	}
	return clickhouse.Connector(co), nil
}

func newConn(cfg config.ClickHouse, opts []Option) (chdriver.Conn, *clickhouse.Options, error) {
	co, err := newOptions(cfg, buildOptions(opts))
	if err != nil {
		return nil, nil, err
	}
	conn, err := clickhouse.Open(co)
	if err != nil {
		return nil, nil, fmt.Errorf("open clickhouse %s: %w", target(co), err)
	}
	return conn, co, nil
}

// NewDB builds a database/sql pool with the pool settings applied, without
// connecting.
func NewDB(cfg config.ClickHouse, opts ...Option) (*sql.DB, error) {
	db, _, err := newDB(cfg, opts)
	return db, err
}

// OpenDB is [NewDB] plus a ping within cfg.PingTimeout. The pool is closed
// if the ping fails.
func OpenDB(ctx context.Context, cfg config.ClickHouse, opts ...Option) (*sql.DB, error) {
	db, co, err := newDB(cfg, opts)
	if err != nil {
		return nil, err
	}
	if err := ping(ctx, cfg.PingTimeout, db.PingContext); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping clickhouse %s: %w", target(co), err)
	}
	return db, nil
}

func newDB(cfg config.ClickHouse, opts []Option) (*sql.DB, *clickhouse.Options, error) {
	co, err := newOptions(cfg, buildOptions(opts))
	if err != nil {
		return nil, nil, err
	}
	db := clickhouse.OpenDB(co)
	cfg.SQLPool.Apply(db)
	return db, co, nil
}

func ping(ctx context.Context, timeout time.Duration, fn func(context.Context) error) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return fn(ctx)
}

func target(co *clickhouse.Options) string {
	return co.Protocol.String() + "://" + strings.Join(co.Addr, ",")
}
