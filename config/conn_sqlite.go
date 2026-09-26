package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// SQLite configures an SQLite database (ncruces/go-sqlite3, cgo-free).
type SQLite struct {
	// Path is a file path, a file: URI or ":memory:". An in-memory database
	// is private to one connection, so the pool is limited to one connection.
	Path string `config:"path"`
	// Mode is ro, rw or rwc (read-write, create if missing; the default).
	Mode string `config:"mode"`
	// BusyTimeout is how long a connection waits for a lock before failing
	// with SQLITE_BUSY.
	BusyTimeout time.Duration `config:"busy_timeout" default:"5s"`
	// JournalMode is delete, truncate, persist, memory, wal or off. Empty
	// keeps the database setting.
	JournalMode string `config:"journal_mode" default:"wal"`
	// Synchronous is off, normal, full or extra. Empty keeps the default
	// (full); normal is safe and faster with WAL.
	Synchronous string `config:"synchronous"`
	ForeignKeys bool   `config:"foreign_keys" default:"true"`
	// TxLock is the BEGIN mode of transactions: deferred (the default),
	// immediate or exclusive. immediate avoids SQLITE_BUSY when a read
	// transaction later writes.
	TxLock string `config:"tx_lock"`
	// Pragmas are extra PRAGMA settings applied to every connection, e.g.
	// cache_size: "-20000".
	Pragmas map[string]string `config:"pragmas"`

	SQLPool `config:",squash"`
}

var (
	sqliteModes       = []string{"", "ro", "rw", "rwc", "memory"}
	sqliteJournal     = []string{"", "delete", "truncate", "persist", "memory", "wal", "off"}
	sqliteSynchronous = []string{"", "off", "normal", "full", "extra", "0", "1", "2", "3"}
	sqliteTxLock      = []string{"", "deferred", "immediate", "exclusive"}
)

func (s SQLite) Validate() error {
	var errs []error
	if s.Path == "" {
		errs = append(errs, errors.New("path is required"))
	}
	check := func(name, v string, allowed []string) {
		if !slices.Contains(allowed, strings.ToLower(v)) {
			errs = append(errs, fmt.Errorf("unsupported %s %q", name, v))
		}
	}
	check("mode", s.Mode, sqliteModes)
	check("journal_mode", s.JournalMode, sqliteJournal)
	check("synchronous", s.Synchronous, sqliteSynchronous)
	check("tx_lock", s.TxLock, sqliteTxLock)
	return errors.Join(append(errs, s.SQLPool.Validate())...)
}
