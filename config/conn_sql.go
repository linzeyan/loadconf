package config

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// SQLPool configures a database/sql connection pool. Zero values keep the
// pool's own defaults (database/sql: unlimited open, 2 idle, no expiry), so a
// lone max_open_conns never conflicts with an idle count it did not set.
type SQLPool struct {
	MaxOpenConns    int           `config:"max_open_conns"`
	MaxIdleConns    int           `config:"max_idle_conns"`
	ConnMaxLifetime time.Duration `config:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `config:"conn_max_idle_time"`
}

func (p SQLPool) Validate() error {
	if p.MaxOpenConns < 0 || p.MaxIdleConns < 0 || p.ConnMaxLifetime < 0 || p.ConnMaxIdleTime < 0 {
		return errors.New("pool settings must not be negative")
	}
	if p.MaxOpenConns > 0 && p.MaxIdleConns > p.MaxOpenConns {
		return fmt.Errorf("max_idle_conns (%d) exceeds max_open_conns (%d)", p.MaxIdleConns, p.MaxOpenConns)
	}
	return nil
}

// Apply sets the non-zero pool settings on db.
func (p SQLPool) Apply(db *sql.DB) {
	if p.MaxOpenConns > 0 {
		db.SetMaxOpenConns(p.MaxOpenConns)
	}
	if p.MaxIdleConns > 0 {
		db.SetMaxIdleConns(p.MaxIdleConns)
	}
	if p.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(p.ConnMaxLifetime)
	}
	if p.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(p.ConnMaxIdleTime)
	}
}

// MySQL configures a MySQL / MariaDB connection.
//
// Either DSN (go-sql-driver format) or Host is required. Fields set next to a
// DSN override the corresponding DSN parts, so a DSN without password can be
// completed by a Password from a secret store.
type MySQL struct {
	DSN      Secret `config:"dsn"`
	Host     string `config:"host"`
	Port     int    `config:"port"     default:"3306"`
	User     string `config:"user"`
	Password Secret `config:"password"`
	Database string `config:"database"`
	// Params are go-sql-driver DSN parameters, e.g. parseTime, loc, charset,
	// collation, timeout, readTimeout or writeTimeout.
	Params map[string]string `config:"params"`

	// PingTimeout bounds the connectivity check when opening.
	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`

	TLS     TLS `config:"tls"`
	SQLPool `config:",squash"`
}

func (m MySQL) Validate() error {
	var errs []error
	if m.DSN.Value() == "" && m.Host == "" {
		errs = append(errs, errors.New("either dsn or host is required"))
	}
	// The pool is squashed into this struct, so its hook is not reached on its own.
	return errors.Join(append(errs, m.SQLPool.Validate())...)
}

// Postgres configures a PostgreSQL connection.
//
// Either DSN (URL or keyword/value form) or Host is required. Fields set next
// to a DSN override the corresponding DSN parts.
type Postgres struct {
	DSN      Secret `config:"dsn"`
	Host     string `config:"host"`
	Port     int    `config:"port"     default:"5432"`
	User     string `config:"user"`
	Password Secret `config:"password"`
	Database string `config:"database"`
	// Params are libpq connection parameters, e.g. sslmode, connect_timeout
	// (in seconds) or target_session_attrs, or runtime parameters such as
	// search_path, application_name or statement_timeout.
	Params map[string]string `config:"params"`

	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`

	// TLS, when enabled, replaces the TLS setup derived from sslmode.
	TLS     TLS `config:"tls"`
	SQLPool `config:",squash"`
}

func (p Postgres) Validate() error {
	var errs []error
	if p.DSN.Value() == "" && p.Host == "" {
		errs = append(errs, errors.New("either dsn or host is required"))
	}
	return errors.Join(append(errs, p.SQLPool.Validate())...)
}
