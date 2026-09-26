package conn_sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"

	"github.com/linzeyan/loadconf/config"
)

func sqliteCfg(path string) config.SQLite {
	return config.SQLite{
		Path: path, BusyTimeout: 5 * time.Second, JournalMode: "wal", ForeignKeys: true,
		SQLPool: config.SQLPool{MaxOpenConns: 4, MaxIdleConns: 2, ConnMaxLifetime: time.Minute},
	}
}

func pragma[T any](t *testing.T, db *sql.DB, name string) T {
	t.Helper()
	var v T
	if err := db.QueryRow("PRAGMA " + name).Scan(&v); err != nil {
		t.Fatalf("PRAGMA %s: %v", name, err)
	}
	return v
}

func TestDSN(t *testing.T) {
	cfg := sqliteCfg("data/app db.sqlite")
	cfg.Mode, cfg.Synchronous, cfg.TxLock = "rwc", "NORMAL", "immediate"
	cfg.Pragmas = map[string]string{"cache_size": "-20000", "auto_vacuum": "incremental"}
	dsn, err := DSN(cfg)
	if err != nil {
		t.Fatal(err)
	}
	q, err := url.ParseQuery(dsn[strings.Index(dsn, "?")+1:])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(dsn, "file:data/app%20db.sqlite?") {
		t.Errorf("dsn = %s", dsn)
	}
	want := []string{"busy_timeout(5000)", "journal_mode(wal)", "synchronous(normal)", "foreign_keys(true)", "auto_vacuum(incremental)", "cache_size(-20000)"}
	if got := q["_pragma"]; strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("pragmas = %v, want %v", got, want)
	}
	if q.Get("mode") != "rwc" || q.Get("_txlock") != "immediate" {
		t.Errorf("query = %v", q)
	}

	dsn, _ = DSN(config.SQLite{Path: "file:/abs/x.db?cache=private&_timefmt=sqlite", Mode: "ro", JournalMode: "wal"})
	q, _ = url.ParseQuery(dsn[strings.Index(dsn, "?")+1:])
	if !strings.HasPrefix(dsn, "file:/abs/x.db?") || q.Get("cache") != "private" || q.Get("_timefmt") != "sqlite" || q.Get("mode") != "ro" {
		t.Errorf("file URI dsn = %s", dsn)
	}
	if strings.Contains(dsn, "journal_mode") {
		t.Errorf("read-only databases must not set journal_mode: %s", dsn)
	}

	for _, bad := range []config.SQLite{{}, {Path: "x", Mode: "rwx"}, {Path: "x", Pragmas: map[string]string{"a); DROP": "1"}}} {
		if _, err := DSN(bad); err == nil {
			t.Errorf("DSN(%+v) should fail", bad)
		}
	}
}

func TestOpenFile(t *testing.T) {
	dir := t.TempDir()
	cfg := sqliteCfg(filepath.Join(dir, "app.db"))
	cfg.Synchronous = "normal"
	cfg.Pragmas = map[string]string{"cache_size": "-4000"}
	var inits int
	db, err := Open(context.Background(), cfg, WithInit(func(*sqlite3.Conn) error { inits++; return nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if got := pragma[string](t, db, "journal_mode"); got != "wal" {
		t.Errorf("journal_mode = %s", got)
	}
	if got := pragma[int](t, db, "foreign_keys"); got != 1 {
		t.Errorf("foreign_keys = %d", got)
	}
	if got := pragma[int](t, db, "busy_timeout"); got != 5000 {
		t.Errorf("busy_timeout = %d", got)
	}
	if got := pragma[int](t, db, "synchronous"); got != 1 {
		t.Errorf("synchronous = %d, want 1 (normal)", got)
	}
	if got := pragma[int](t, db, "cache_size"); got != -4000 {
		t.Errorf("cache_size = %d", got)
	}
	if inits == 0 {
		t.Error("init hook not called")
	}
	if db.Stats().MaxOpenConnections != 4 {
		t.Errorf("max open = %d", db.Stats().MaxOpenConnections)
	}

	if _, err := db.Exec(`CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(pid INTEGER REFERENCES parent(id))`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO child VALUES (42)`); err == nil {
		t.Error("foreign keys are not enforced")
	}
	_ = db.Close()

	cfg.Mode = "ro"
	ro, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	if _, err := ro.Exec(`INSERT INTO parent VALUES (1)`); err == nil || !strings.Contains(err.Error(), "readonly") {
		t.Errorf("read-only write err = %v", err)
	}

	cfg.Path, cfg.Mode = filepath.Join(dir, "missing", "x.db"), "rw"
	if _, err := Open(context.Background(), cfg); err == nil {
		t.Error("opening a missing file in rw mode should fail")
	}
}

func TestOpenMemory(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, sqliteCfg(":memory:"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t(v TEXT)`); err != nil {
		t.Fatal(err)
	}
	// Two connections at once must see the same database.
	c1, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	c2, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, err := c1.ExecContext(ctx, `INSERT INTO t VALUES ('x')`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := c2.QueryRowContext(ctx, `SELECT count(*) FROM t`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("second connection sees %d rows, %v", n, err)
	}

	// Another private in-memory database is independent.
	other, err := Open(ctx, sqliteCfg(":memory:"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name = 't'`).Scan(&n); err != nil || n != 0 {
		t.Errorf("private databases are shared: %d %v", n, err)
	}

	// Named in-memory databases are shared by name within the process.
	named := config.SQLite{Path: "cache", Mode: "memory", ForeignKeys: true}
	a, err := Open(ctx, named)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(ctx, named)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err := a.Exec(`CREATE TABLE shared(v)`); err != nil {
		t.Fatal(err)
	}
	if err := b.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name = 'shared'`).Scan(&n); err != nil || n != 1 {
		t.Errorf("named database not shared: %d %v", n, err)
	}
}
