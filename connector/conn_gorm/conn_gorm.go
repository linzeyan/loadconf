// Package conn_gorm opens *gorm.DB for MySQL and PostgreSQL on top of the
// pools built by conn_sql, so DSN handling and pool settings are shared.
//
//	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"))
//
// Queries log to slog.Default() through conn_gorm_log: failed ones at Error,
// ones slower than 200ms at Warn. A gorm.Config with its own Logger, passed
// with WithGormConfig, replaces that. SQL Server and SQLite live in the
// conn_gorm_mssql and conn_gorm_sqlite modules.
//
// Read replicas are added with WithMySQLReplicas or WithPostgresReplicas:
//
//	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"),
//		conn_gorm.WithMySQLReplicas(cfg.MySQL.MustGet("orders_replica")))
//	defer conn_gorm.Close(db)
package conn_gorm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm_log"
	"github.com/linzeyan/loadconf/connector/conn_sql"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	gorm     *gorm.Config
	sql      []conn_sql.Option
	mysql    []func(*gormmysql.Config)
	postgres []func(*gormpostgres.Config)

	mysqlReplicas    []config.MySQL
	postgresReplicas []config.Postgres

	// noPing builds the pool with conn_sql's New instead of Open. Only tests
	// set it: gorm's dialectors may query the server while initializing, so
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

// WithSQLOptions passes options to the underlying conn_sql pool.
func WithSQLOptions(opts ...conn_sql.Option) Option {
	return func(o *options) { o.sql = append(o.sql, opts...) }
}

// WithMySQLDialector adjusts the gorm MySQL dialector config.
func WithMySQLDialector(fn func(*gormmysql.Config)) Option {
	return func(o *options) { o.mysql = append(o.mysql, fn) }
}

// WithPostgresDialector adjusts the gorm PostgreSQL dialector config.
func WithPostgresDialector(fn func(*gormpostgres.Config)) Option {
	return func(o *options) { o.postgres = append(o.postgres, fn) }
}

// WithMySQLReplicas sends reads to the replicas through gorm's dbresolver
// plugin, picking one at random per query. Writes, transactions and reads
// with Clauses(dbresolver.Write) stay on the primary; use that clause to
// read a row just written, since replicas lag behind. Each replica is opened
// like the primary: its own pool settings, the parseTime default,
// WithSQLOptions and WithMySQLDialector. Close the handle with [Close].
func WithMySQLReplicas(replicas ...config.MySQL) Option {
	return func(o *options) { o.mysqlReplicas = append(o.mysqlReplicas, replicas...) }
}

// WithPostgresReplicas is [WithMySQLReplicas] for PostgreSQL.
func WithPostgresReplicas(replicas ...config.Postgres) Option {
	return func(o *options) { o.postgresReplicas = append(o.postgresReplicas, replicas...) }
}

func buildOptions(opts []Option) options {
	o := options{gorm: &gorm.Config{}}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// OpenMySQL opens a gorm MySQL handle. Unless the DSN or params set
// parseTime, it is enabled since gorm maps DATETIME columns to time.Time.
func OpenMySQL(ctx context.Context, cfg config.MySQL, opts ...Option) (*gorm.DB, error) {
	o := buildOptions(opts)
	open := func(cfg config.MySQL) (gorm.Dialector, *sql.DB, error) {
		var dsnConfig *mysql.Config
		sqlOpts := append([]conn_sql.Option{conn_sql.WithMySQLConfig(func(mc *mysql.Config) {
			if !mysqlParseTimeSet(cfg) {
				mc.ParseTime = true
			}
			dsnConfig = mc
		})}, o.sql...)
		var sqlDB *sql.DB
		var err error
		if o.noPing {
			sqlDB, err = conn_sql.NewMySQL(cfg, sqlOpts...)
		} else {
			sqlDB, err = conn_sql.OpenMySQL(ctx, cfg, sqlOpts...)
		}
		if err != nil {
			return nil, nil, err
		}
		dc := gormmysql.Config{Conn: sqlDB, DSNConfig: dsnConfig}
		for _, fn := range o.mysql {
			fn(&dc)
		}
		return gormmysql.New(dc), sqlDB, nil
	}
	return openAll(cfg, o.mysqlReplicas, open, o.gorm)
}

// OpenPostgres opens a gorm PostgreSQL handle.
func OpenPostgres(ctx context.Context, cfg config.Postgres, opts ...Option) (*gorm.DB, error) {
	o := buildOptions(opts)
	open := func(cfg config.Postgres) (gorm.Dialector, *sql.DB, error) {
		var sqlDB *sql.DB
		var err error
		if o.noPing {
			sqlDB, err = conn_sql.NewPostgres(cfg, o.sql...)
		} else {
			sqlDB, err = conn_sql.OpenPostgres(ctx, cfg, o.sql...)
		}
		if err != nil {
			return nil, nil, err
		}
		dc := gormpostgres.Config{Conn: sqlDB}
		for _, fn := range o.postgres {
			fn(&dc)
		}
		return gormpostgres.New(dc), sqlDB, nil
	}
	return openAll(cfg, o.postgresReplicas, open, o.gorm)
}

// openAll opens the primary and every replica, then gorm on top; on any
// failure it closes the pools opened so far.
func openAll[C any](primary C, replicas []C, open func(C) (gorm.Dialector, *sql.DB, error), gcfg *gorm.Config) (*gorm.DB, error) {
	var pools []*sql.DB
	fail := func(err error) (*gorm.DB, error) {
		for _, p := range pools {
			_ = p.Close()
		}
		return nil, err
	}
	dialector, pool, err := open(primary)
	if err != nil {
		return nil, err
	}
	pools = append(pools, pool)
	replicaDialectors := make([]gorm.Dialector, 0, len(replicas))
	for i, rc := range replicas {
		d, p, err := open(rc)
		if err != nil {
			return fail(fmt.Errorf("replica %d: %w", i, err))
		}
		pools = append(pools, p)
		replicaDialectors = append(replicaDialectors, d)
	}

	// gorm's own default logger prints colored text to stdout, which breaks
	// structured logs.
	if gcfg.Logger == nil {
		c := *gcfg // the caller's Config stays as given, as gorm.Open leaves it
		c.Logger = conn_gorm_log.New(nil, conn_gorm_log.DefaultConfig())
		gcfg = &c
	}
	db, err := gorm.Open(dialector, gcfg)
	if err != nil {
		return fail(err)
	}
	if len(replicaDialectors) > 0 {
		if err := db.Use(dbresolver.Register(dbresolver.Config{Replicas: replicaDialectors, Policy: dbresolver.RandomPolicy{}})); err != nil {
			return fail(fmt.Errorf("replicas: %w", err))
		}
	}
	return db, nil
}

// Close closes every pool of db: the primary's and, unlike closing db.DB(),
// those of the replicas.
func Close(db *gorm.DB) error {
	resolver, ok := db.Config.Plugins[(&dbresolver.DBResolver{}).Name()].(*dbresolver.DBResolver)
	if !ok {
		sqlDB, err := db.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	var errs []error
	_ = resolver.Call(func(p gorm.ConnPool) error {
		if c, ok := p.(interface{ Close() error }); ok {
			errs = append(errs, c.Close())
		}
		return nil
	})
	return errors.Join(errs...)
}

func mysqlParseTimeSet(cfg config.MySQL) bool {
	if _, ok := cfg.Params["parseTime"]; ok {
		return true
	}
	// Only the query counts, found the way go-sql-driver finds it (from the
	// first '?' after the last '/'), so a password or database name holding
	// "parseTime=" does not.
	dsn := cfg.DSN.Value()
	_, query, _ := strings.Cut(dsn[strings.LastIndexByte(dsn, '/')+1:], "?")
	for kv := range strings.SplitSeq(query, "&") {
		if k, _, ok := strings.Cut(kv, "="); ok && k == "parseTime" {
			return true
		}
	}
	return false
}
