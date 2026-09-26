package conn_sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linzeyan/loadconf/config"
)

func TestOpenAdversarialFileNames(t *testing.T) {
	// Each name must reach SQLite as exactly that file: no character may be
	// taken as URI syntax (a query that sets mode=ro, a fragment, a
	// percent-escape decoded twice).
	for _, name := range []string{
		"has space.db", "q?mode=ro.db", "h#frag.db", "pct%41.db", "pct%zz.db", "amp&_pragma=x.db",
		"semi;colon.db", "it's.db", `dq"x.db`, "資料.db", "plus+sign.db", ":colon.db", "vfs=memdb.db",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			db, err := Open(context.Background(), config.SQLite{Path: path, ForeignKeys: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(`CREATE TABLE t(v)`); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, err := os.Stat(path); err != nil {
				entries, _ := os.ReadDir(filepath.Dir(path))
				t.Errorf("file %q not created (dir has %v): %v", name, entries, err)
			}
		})
	}
}

func TestDSNDoubleSlashPathIsNotAnAuthority(t *testing.T) {
	// "//tmp/x.db" is a valid POSIX path; written as file://tmp/x.db SQLite
	// would read "tmp" as a URI authority and refuse it.
	dir := t.TempDir()
	db, err := Open(context.Background(), config.SQLite{Path: "/" + dir + "/x.db", ForeignKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE t(v)`); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "x.db")); err != nil {
		t.Error(err)
	}
}

func TestDSNModeFromFileURI(t *testing.T) {
	// A read-only mode given in the URI must also skip journal_mode, which
	// would fail on a read-only database.
	dsn, err := DSN(config.SQLite{Path: "file:x.db?mode=ro", JournalMode: "wal"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(dsn, "journal_mode") {
		t.Errorf("dsn = %s", dsn)
	}
	// cfg.Mode replaces the URI's mode instead of adding a second one.
	dsn, _ = DSN(config.SQLite{Path: "file:x.db?mode=ro", Mode: "rw"})
	if q := query(t, dsn); len(q["mode"]) != 1 || q.Get("mode") != "rw" {
		t.Errorf("modes = %v", q["mode"])
	}
}

func TestDSNMemoryDropsFileMode(t *testing.T) {
	dsn, err := DSN(config.SQLite{Path: "file:cache?mode=rwc", Mode: "memory", JournalMode: "wal"})
	if err != nil {
		t.Fatal(err)
	}
	q := query(t, dsn)
	if q.Has("mode") || q.Get("vfs") != "memdb" || strings.Contains(dsn, "journal_mode") {
		t.Errorf("dsn = %s", dsn)
	}
}

func TestDSNPragmaValidation(t *testing.T) {
	// _pragma values become "PRAGMA name(value)" statements; anything that
	// could close the call or start another statement must be refused.
	for _, bad := range []map[string]string{
		{"cache_size": "1); DROP TABLE t; --"}, {"cache_size(1)": "2"}, {"a;b": "1"}, {"a=b": "1"},
		{"a&b": "1"}, {"x": "a&b"}, {"x": "("}, {"x": ")"},
	} {
		if dsn, err := DSN(config.SQLite{Path: "x.db", Pragmas: bad}); err == nil {
			t.Errorf("pragmas %v accepted: %s", bad, dsn)
		}
	}
	dsn, err := DSN(config.SQLite{Path: "x.db", Pragmas: map[string]string{"application_id": "42", "main.cache_size": "-2000"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := query(t, dsn)["_pragma"]; strings.Join(got, " ") != "foreign_keys(false) application_id(42) main.cache_size(-2000)" {
		t.Errorf("pragmas = %v", got)
	}
}

// TestFileURIMemoryDatabaseShared: the package promises that in-memory
// databases use memdb so that every connection of the pool sees the same
// database. That must hold for the canonical SQLite URIs file::memory: and
// file:x?mode=memory too; passed through, each pooled connection gets its
// own empty database and data silently "disappears" under concurrency.
func TestFileURIMemoryDatabaseShared(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{"file::memory:", "file:x?mode=memory"} {
		t.Run(path, func(t *testing.T) {
			db, err := Open(ctx, config.SQLite{Path: path, ForeignKeys: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
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
			if _, err := c1.ExecContext(ctx, `CREATE TABLE t(v)`); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := c2.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name = 't'`).Scan(&n); err != nil || n != 1 {
				t.Errorf("second connection sees %d tables (%v), want 1", n, err)
			}
		})
	}
}

// TestMemoryNameDropsURIQuery: in memory mode the database name is the path
// of a file: URI without its query, so "file:cache?cache=shared" and
// "file:cache" name the same in-memory database.
func TestMemoryNameDropsURIQuery(t *testing.T) {
	dsn, err := DSN(config.SQLite{Path: "file:cache?cache=shared", Mode: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/cache" {
		t.Errorf("memdb name = %q in %s, want /cache", u.Path, dsn)
	}
}

// TestMemoryNameDecodedOnce: SQLite percent-decodes a URI path once, so
// file:a%20b%2525 and file:///a%20b%2525 both name the memdb database
// "a b%25", as memdb.Create("a b%25", ...) does. Decoded zero times or twice,
// the opaque spelling opens another, empty database: pools configured with
// the two spellings, or a database seeded with memdb.Create, would not share
// data.
func TestMemoryNameDecodedOnce(t *testing.T) {
	ctx := context.Background()
	var dbs []*sql.DB
	for _, path := range []string{"file:a%20b%2525?mode=memory", "file:///a%20b%2525?mode=memory"} {
		dsn, err := DSN(config.SQLite{Path: path})
		if err != nil {
			t.Fatal(err)
		}
		rawPath, _, _ := strings.Cut(strings.TrimPrefix(dsn, "file:"), "?")
		if name, err := url.PathUnescape(rawPath); err != nil || name != "/a b%25" {
			t.Errorf("%s: SQLite reads name %q (%v), want \"/a b%%25\"", path, name, err)
		}
		db, err := Open(ctx, config.SQLite{Path: path})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		dbs = append(dbs, db)
	}
	if _, err := dbs[0].Exec(`CREATE TABLE t(v)`); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := dbs[1].QueryRow(`SELECT count(*) FROM sqlite_master WHERE name = 't'`).Scan(&n); err != nil || n != 1 {
		t.Errorf("file:///a%%20b%%2525 sees %d tables (%v), want the one created through file:a%%20b%%2525", n, err)
	}
}

func query(t *testing.T, dsn string) url.Values {
	t.Helper()
	_, raw, _ := strings.Cut(dsn, "?")
	q, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatalf("query of %s: %v", dsn, err)
	}
	return q
}
