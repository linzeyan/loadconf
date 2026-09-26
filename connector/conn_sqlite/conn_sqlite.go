// Package conn_sqlite opens SQLite databases with the cgo-free driver
// github.com/ncruces/go-sqlite3 from [config.SQLite].
//
//	db, err := conn_sqlite.Open(ctx, cfg.SQLite.MustGet("local"))
//	defer db.Close()
//
// In-memory databases (path ":memory:", file::memory: or mode memory) use
// the driver's memdb VFS, so every connection of the pool sees the same
// database. The pool then keeps its connections open, since the database is
// freed with the last one.
package conn_sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/ncruces/go-sqlite3"
	"github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/vfs/memdb" // registers the memdb VFS

	"github.com/linzeyan/loadconf/config"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	init []func(*sqlite3.Conn) error
}

// WithInit runs fn on every new connection after the PRAGMAs, e.g. to
// register functions or collations.
func WithInit(fn func(*sqlite3.Conn) error) Option {
	return func(o *options) { o.init = append(o.init, fn) }
}

// DSN converts cfg into a file: URI for the driver. The PRAGMAs are applied
// to every connection in this order: busy_timeout, journal_mode (skipped for
// read-only and in-memory databases), synchronous, foreign_keys, then
// cfg.Pragmas by name.
func DSN(cfg config.SQLite) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	mode := strings.ToLower(cfg.Mode)
	u := &url.URL{Scheme: "file", OmitHost: true, Path: cfg.Path}
	if strings.HasPrefix(cfg.Path, "file:") {
		var err error
		if u, err = url.Parse(cfg.Path); err != nil {
			return "", fmt.Errorf("sqlite path: %w", err)
		}
	}
	q := u.Query()
	// SQLite reads the URIs file::memory: and file:x?mode=memory as
	// in-memory too; without memdb each connection would get its own.
	name := u.Opaque + u.Path
	memory := name == ":memory:" || mode == "memory" || mode == "" && q.Get("mode") == "memory"

	if memory {
		if u.Opaque != "" {
			// url.Parse leaves an opaque path (file:x) escaped but decodes
			// u.Path; SQLite decodes both once. Undecoded, the name would be
			// escaped a second time below and miss the memdb database that
			// file:///x or memdb.Create give the same name.
			var err error
			if name, err = url.PathUnescape(u.Opaque); err != nil {
				return "", fmt.Errorf("sqlite path: %w", err)
			}
		}
		name = strings.TrimPrefix(name, "/")
		if name == ":memory:" || name == "" {
			// A private database: unique per Open, shared by its pool.
			name = "mem-" + rand.Text()
		}
		u = &url.URL{Scheme: "file", OmitHost: true, Path: "/" + name}
		q.Set("vfs", "memdb")
		q.Del("mode")
	} else if mode != "" {
		q.Set("mode", mode)
	}
	pragma := func(name, value string) { q.Add("_pragma", name+"("+value+")") }
	if cfg.BusyTimeout > 0 {
		pragma("busy_timeout", strconv.FormatInt(cfg.BusyTimeout.Milliseconds(), 10))
	}
	if cfg.JournalMode != "" && !memory && q.Get("mode") != "ro" {
		pragma("journal_mode", strings.ToLower(cfg.JournalMode))
	}
	if cfg.Synchronous != "" {
		pragma("synchronous", strings.ToLower(cfg.Synchronous))
	}
	pragma("foreign_keys", strconv.FormatBool(cfg.ForeignKeys))
	for _, name := range slices.Sorted(maps.Keys(cfg.Pragmas)) {
		if strings.ContainsAny(name, "()&=;") || strings.ContainsAny(cfg.Pragmas[name], "()&;") {
			return "", fmt.Errorf("invalid pragma %s(%s)", name, cfg.Pragmas[name])
		}
		pragma(name, cfg.Pragmas[name])
	}
	if cfg.TxLock != "" {
		q.Set("_txlock", strings.ToLower(cfg.TxLock))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// New builds a pool with the pool settings applied, without opening a
// connection: the file is created and the PRAGMAs run on first use.
func New(cfg config.SQLite, opts ...Option) (*sql.DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	dsn, err := DSN(cfg)
	if err != nil {
		return nil, err
	}
	var hooks []func(*sqlite3.Conn) error
	if len(o.init) > 0 {
		hooks = append(hooks, func(c *sqlite3.Conn) error {
			for _, fn := range o.init {
				if err := fn(c); err != nil {
					return err
				}
			}
			return nil
		})
	}
	db, err := driver.Open(dsn, hooks...)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", cfg.Path, err)
	}

	pool := cfg.SQLPool
	if strings.Contains(dsn, "vfs=memdb") {
		// Closing the last connection frees an in-memory database.
		pool.ConnMaxLifetime, pool.ConnMaxIdleTime = 0, 0
		pool.MaxIdleConns = max(pool.MaxIdleConns, 1)
	}
	pool.Apply(db)
	return db, nil
}

// Open is [New] plus opening a first connection, which applies the PRAGMAs,
// so a bad path or PRAGMA fails here. The pool is closed if that fails.
func Open(ctx context.Context, cfg config.SQLite, opts ...Option) (*sql.DB, error) {
	db, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open sqlite %s: %w", cfg.Path, err)
	}
	return db, nil
}
