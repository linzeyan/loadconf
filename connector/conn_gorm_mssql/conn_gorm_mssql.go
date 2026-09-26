// Package conn_gorm_mssql opens *gorm.DB for Microsoft SQL Server on top of the
// pool built by conn_mssql, so DSN handling and pool settings are shared.
//
//	db, err := conn_gorm_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
//
// Queries log to slog.Default() through conn_gorm_log unless the gorm.Config
// passed with WithGormConfig has its own Logger. It is a separate module from conn_gorm so that MySQL/PostgreSQL users do
// not pull in the SQL Server driver.
package conn_gorm_mssql

import (
	"context"
	"database/sql"

	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm_log"
	"github.com/linzeyan/loadconf/connector/conn_mssql"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	gorm      *gorm.Config
	sql       []conn_mssql.Option
	dialector []func(*sqlserver.Config)
	// noPing builds the pool with conn_mssql.New instead of Open. Only tests
	// set it: gorm's dialector may query the server while initializing, so
	// there is no exported way to open without connecting.
	noPing bool
}

// WithGormConfig sets the gorm config (logger, naming strategy, ...). A nil
// config means the default one.
func WithGormConfig(c *gorm.Config) Option {
	if c == nil {
		c = &gorm.Config{}
	}
	return func(o *options) { o.gorm = c }
}

// WithSQLOptions passes options to the underlying conn_mssql pool.
func WithSQLOptions(opts ...conn_mssql.Option) Option {
	return func(o *options) { o.sql = append(o.sql, opts...) }
}

// WithDialector adjusts the gorm SQL Server dialector config.
func WithDialector(fn func(*sqlserver.Config)) Option {
	return func(o *options) { o.dialector = append(o.dialector, fn) }
}

// Open opens a gorm SQL Server handle.
func Open(ctx context.Context, cfg config.SQLServer, opts ...Option) (*gorm.DB, error) {
	o := options{gorm: &gorm.Config{}}
	for _, opt := range opts {
		opt(&o)
	}
	var sqlDB *sql.DB
	var err error
	if o.noPing {
		sqlDB, err = conn_mssql.New(cfg, o.sql...)
	} else {
		sqlDB, err = conn_mssql.Open(ctx, cfg, o.sql...)
	}
	if err != nil {
		return nil, err
	}
	dc := sqlserver.Config{Conn: sqlDB}
	for _, fn := range o.dialector {
		fn(&dc)
	}
	// gorm's own default logger prints colored text to stdout, which breaks
	// structured logs.
	if o.gorm.Logger == nil {
		c := *o.gorm // the caller's Config stays as given, as gorm.Open leaves it
		c.Logger = conn_gorm_log.New(nil, conn_gorm_log.DefaultConfig())
		o.gorm = &c
	}
	db, err := gorm.Open(sqlserver.New(dc), o.gorm)
	if err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}
