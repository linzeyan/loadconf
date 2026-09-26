// Package conn_gorm_sqlite opens *gorm.DB for SQLite on top of the pool built by
// conn_sqlite (the cgo-free github.com/ncruces/go-sqlite3 driver), so the
// PRAGMAs, in-memory handling and pool settings are shared.
//
//	db, err := conn_gorm_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))
//
// Queries log to slog.Default() through conn_gorm_log unless the gorm.Config
// passed with WithGormConfig has its own Logger.
package conn_gorm_sqlite

import (
	"context"

	"github.com/ncruces/go-sqlite3/gormlite"
	"gorm.io/gorm"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm_log"
	"github.com/linzeyan/loadconf/connector/conn_sqlite"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	gorm *gorm.Config
	sql  []conn_sqlite.Option
}

// WithGormConfig sets the gorm config (logger, naming strategy, ...). A nil
// config means the default one.
func WithGormConfig(c *gorm.Config) Option {
	if c == nil {
		c = &gorm.Config{}
	}
	return func(o *options) { o.gorm = c }
}

// WithSQLOptions passes options to the underlying conn_sqlite pool.
func WithSQLOptions(opts ...conn_sqlite.Option) Option {
	return func(o *options) { o.sql = append(o.sql, opts...) }
}

// Open opens a gorm SQLite handle.
func Open(ctx context.Context, cfg config.SQLite, opts ...Option) (*gorm.DB, error) {
	o := options{gorm: &gorm.Config{}}
	for _, opt := range opts {
		opt(&o)
	}
	sqlDB, err := conn_sqlite.Open(ctx, cfg, o.sql...)
	if err != nil {
		return nil, err
	}
	// gorm's own default logger prints colored text to stdout, which breaks
	// structured logs.
	if o.gorm.Logger == nil {
		c := *o.gorm // the caller's Config stays as given, as gorm.Open leaves it
		c.Logger = conn_gorm_log.New(nil, conn_gorm_log.DefaultConfig())
		o.gorm = &c
	}
	db, err := gorm.Open(gormlite.OpenDB(sqlDB), o.gorm)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}
