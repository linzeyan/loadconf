// Package conn_sql opens database/sql pools for MySQL (go-sql-driver/mysql)
// and PostgreSQL (pgx) from [config.MySQL] and [config.Postgres].
//
//	db, err := conn_sql.OpenMySQL(ctx, cfg.MySQL.MustGet("orders"))
//	defer db.Close()
//
// Driver logs go to slog.Default(); WithLogger, MySQLLogger and PgxTracer
// pick another logger or level.
//
// Databases speaking the MySQL protocol (TiDB, OceanBase, Doris, StarRocks)
// open with OpenMySQL, those speaking the PostgreSQL protocol (CockroachDB,
// YugabyteDB, Redshift, TimescaleDB, Greenplum) with OpenPostgres.
package conn_sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"

	"github.com/linzeyan/loadconf/config"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	mysql    []func(*mysql.Config)
	postgres []func(*pgx.ConnConfig)
}

// WithMySQLConfig adjusts the driver config after it is built from the
// config, e.g. to set a Logger or DialFunc.
func WithMySQLConfig(fn func(*mysql.Config)) Option {
	return func(o *options) { o.mysql = append(o.mysql, fn) }
}

// WithPostgresConfig adjusts the pgx config after it is built from the
// config, e.g. to set a Tracer.
func WithPostgresConfig(fn func(*pgx.ConnConfig)) Option {
	return func(o *options) { o.postgres = append(o.postgres, fn) }
}

func buildOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// MySQLConfig converts cfg into a go-sql-driver config. Params are applied
// through the DSN so that driver options such as parseTime or loc are
// recognized; explicit fields override the DSN.
func MySQLConfig(cfg config.MySQL) (*mysql.Config, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	dsn := cfg.DSN.Value()
	if dsn == "" {
		dsn = "/"
	}
	if len(cfg.Params) > 0 {
		q := url.Values{}
		for k, v := range cfg.Params {
			q.Set(k, v)
		}
		// go-sql-driver starts the query at the first '?' after the last
		// '/', so a '?' in the password does not open one.
		sep := "?"
		if strings.Contains(dsn[strings.LastIndexByte(dsn, '/')+1:], "?") {
			sep = "&"
		}
		// The driver unescapes only some params; charset it splits on a raw
		// ','. A literal ',' reads the same in the others, so keep it.
		dsn += sep + strings.ReplaceAll(q.Encode(), "%2C", ",")
	}
	mc, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql dsn: %w", err)
	}
	mc.Logger = MySQLLogger(nil, slog.LevelWarn)

	if cfg.Host != "" {
		mc.Net = "tcp"
		mc.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(portOr(cfg.Port, 3306)))
	}
	if cfg.User != "" {
		mc.User = cfg.User
	}
	if cfg.Password.Value() != "" {
		mc.Passwd = cfg.Password.Value()
	}
	if cfg.Database != "" {
		mc.DBName = cfg.Database
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName, _, _ = net.SplitHostPort(mc.Addr)
		}
		mc.TLS = tlsCfg
	}
	return mc, nil
}

// NewMySQL builds a pool with the pool settings applied, without connecting:
// database/sql dials on first use.
func NewMySQL(cfg config.MySQL, opts ...Option) (*sql.DB, error) {
	db, _, err := newMySQL(cfg, buildOptions(opts))
	return db, err
}

// OpenMySQL is [NewMySQL] plus a ping within cfg.PingTimeout. The pool is
// closed if the ping fails.
func OpenMySQL(ctx context.Context, cfg config.MySQL, opts ...Option) (*sql.DB, error) {
	db, target, err := newMySQL(cfg, buildOptions(opts))
	if err != nil {
		return nil, err
	}
	return ping(ctx, db, cfg.PingTimeout, target)
}

func newMySQL(cfg config.MySQL, o options) (*sql.DB, string, error) {
	mc, err := MySQLConfig(cfg)
	if err != nil {
		return nil, "", err
	}
	for _, fn := range o.mysql {
		fn(mc)
	}
	connector, err := mysql.NewConnector(mc)
	if err != nil {
		return nil, "", err
	}
	db := sql.OpenDB(connector)
	ApplyPool(db, cfg.SQLPool)
	return db, "mysql " + mc.Addr + "/" + mc.DBName, nil
}

// PostgresConnString appends the fields and Params that are set to cfg's DSN
// (URL or keyword/value form), or builds a URL when there is no DSN.
//
// The DSN itself is left as written: libpq keeps the last value of a repeated
// key, and in a URL query parameters override the host, userinfo and path.
// Editing it in place would need libpq's grammar, which net/url does not
// follow ('#', '+', multiple hosts).
func PostgresConnString(cfg config.Postgres) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	set := make([][2]string, 0, 5+len(cfg.Params))
	if cfg.Host != "" {
		set = append(set, [2]string{"host", cfg.Host}, [2]string{"port", strconv.Itoa(portOr(cfg.Port, 5432))})
	}
	for _, kv := range [][2]string{{"user", cfg.User}, {"password", cfg.Password.Value()}, {"dbname", cfg.Database}} {
		if kv[1] != "" {
			set = append(set, kv)
		}
	}
	// Params come last so that they win, sorted so that the same config
	// always gives the same string.
	for _, k := range slices.Sorted(maps.Keys(cfg.Params)) {
		set = append(set, [2]string{k, cfg.Params[k]})
	}

	dsn := cfg.DSN.Value()
	if dsn == "" {
		dsn = "postgres://"
	}
	var b strings.Builder
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		b.WriteString(dsn)
		sep := "?"
		if q, ok := uriQuery(dsn); ok {
			sep = "&"
			if q == "" || strings.HasSuffix(q, "&") {
				sep = ""
			}
		}
		for _, kv := range set {
			b.WriteString(sep)
			b.WriteString(uriEscape(kv[0]))
			b.WriteByte('=')
			b.WriteString(uriEscape(kv[1]))
			sep = "&"
		}
		return b.String(), nil
	}
	dsn, err := closeKV(dsn)
	if err != nil {
		return "", err
	}
	b.WriteString(dsn)
	for _, kv := range set {
		b.WriteByte(' ')
		b.WriteString(kv[0])
		b.WriteByte('=')
		b.WriteString(quoteKV(kv[1]))
	}
	return b.String(), nil
}

// uriEscape escapes a libpq URI query key or value. libpq decodes %XX but
// takes '+' literally, so a space must be %20.
func uriEscape(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

// uriQuery returns the query of a libpq URI, found the way pgconn's parser
// finds it: the userinfo ends at an '@' before the first '/', and a host in
// brackets may hold a '?'.
func uriQuery(uri string) (query string, ok bool) {
	_, p, _ := strings.Cut(uri, "://")
	if i := strings.IndexAny(p, "@/"); i >= 0 && p[i] == '@' {
		p = p[i+1:]
	}
	for {
		if strings.HasPrefix(p, "[") {
			if i := strings.IndexByte(p, ']'); i >= 0 {
				p = p[i+1:]
			}
		}
		i := strings.IndexAny(p, "/?,")
		if i < 0 {
			return "", false
		}
		if p[i] != ',' {
			_, query, ok = strings.Cut(p[i:], "?")
			return query, ok
		}
		p = p[i+1:]
	}
}

var errKVSyntax = errors.New("postgres dsn: invalid keyword/value syntax")

// closeKV ends a keyword/value DSN so that appended text starts a new
// setting. libpq takes the next word as the value of a trailing "key=", and a
// trailing backslash escapes the space before it, so either would swallow the
// first override. The scan follows pgconn's tokenizer.
func closeKV(dsn string) (string, error) {
	const space = " \t\n\r\v\f"
	s := strings.TrimLeft(dsn, space)
	for s != "" {
		_, v, found := strings.Cut(s, "=")
		if !found {
			return "", errKVSyntax
		}
		v = strings.TrimLeft(v, space)
		if v == "" {
			return dsn + "''", nil
		}
		quoted := v[0] == '\''
		if quoted {
			v = v[1:]
		}
		for {
			if v == "" {
				if quoted {
					// Appended quotes could close it and shift every
					// override by one token.
					return "", errKVSyntax
				}
				return dsn, nil
			}
			c := v[0]
			v = v[1:]
			if quoted && c == '\'' || !quoted && strings.IndexByte(space, c) >= 0 {
				break
			}
			if c == '\\' {
				if v == "" {
					if quoted {
						return "", errKVSyntax
					}
					// libpq drops a backslash that escapes the end; without
					// it the value may be empty ("key=\"), so scan again.
					return closeKV(dsn[:len(dsn)-1])
				}
				v = v[1:]
			}
		}
		s = strings.TrimLeft(v, space)
	}
	return dsn, nil
}

func quoteKV(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// PostgresConfig converts cfg into a pgx connection config.
func PostgresConfig(cfg config.Postgres) (*pgx.ConnConfig, error) {
	connString, err := PostgresConnString(cfg)
	if err != nil {
		return nil, err
	}
	cc, err := pgx.ParseConfig(connString)
	if err != nil {
		// pgconn redacts passwords only where it recognizes them: not a
		// quoted one with an escaped quote, nor one that a malformed URL
		// puts in another position.
		return nil, errors.New("postgres: invalid connection settings (details withheld: they may contain the password)")
	}
	cc.Tracer = PgxTracer(nil, tracelog.LogLevelWarn)
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		if tlsCfg.ServerName == "" {
			tlsCfg.ServerName = cc.Host
		}
		cc.TLSConfig = tlsCfg
		cc.Fallbacks = nil // no silent downgrade to plaintext
	}
	return cc, nil
}

// NewPostgres builds a pool through pgx's database/sql driver with the pool
// settings applied, without connecting.
func NewPostgres(cfg config.Postgres, opts ...Option) (*sql.DB, error) {
	db, _, err := newPostgres(cfg, buildOptions(opts))
	return db, err
}

// OpenPostgres is [NewPostgres] plus a ping within cfg.PingTimeout. The pool
// is closed if the ping fails.
func OpenPostgres(ctx context.Context, cfg config.Postgres, opts ...Option) (*sql.DB, error) {
	db, target, err := newPostgres(cfg, buildOptions(opts))
	if err != nil {
		return nil, err
	}
	return ping(ctx, db, cfg.PingTimeout, target)
}

func newPostgres(cfg config.Postgres, o options) (*sql.DB, string, error) {
	cc, err := PostgresConfig(cfg)
	if err != nil {
		return nil, "", err
	}
	for _, fn := range o.postgres {
		fn(cc)
	}
	db := stdlib.OpenDB(*cc)
	ApplyPool(db, cfg.SQLPool)
	return db, fmt.Sprintf("postgres %s/%s", net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))), cc.Database), nil
}

// ApplyPool applies pool settings; zero values keep the database/sql
// defaults.
func ApplyPool(db *sql.DB, p config.SQLPool) { p.Apply(db) }

func ping(ctx context.Context, db *sql.DB, pingTimeout time.Duration, target string) (*sql.DB, error) {
	if pingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pingTimeout)
		defer cancel()
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", target, err)
	}
	return db, nil
}

func portOr(port, def int) int {
	if port == 0 {
		return def
	}
	return port
}
