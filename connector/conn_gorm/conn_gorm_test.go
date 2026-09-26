package conn_gorm

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"

	"github.com/linzeyan/loadconf/config"
)

// withoutPing builds the pool without pinging it.
func withoutPing() Option { return func(o *options) { o.noPing = true } }

// offline opens without touching a server.
func offline() []Option {
	return []Option{
		WithGormConfig(&gorm.Config{DisableAutomaticPing: true}),
		withoutPing(),
		WithMySQLDialector(func(c *gormmysql.Config) { c.SkipInitializeWithVersion = true }),
	}
}

func TestOpenMySQLDefaultsParseTime(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.MySQL
		want bool
	}{
		"default":          {config.MySQL{Host: "db"}, true},
		"param false":      {config.MySQL{Host: "db", Params: map[string]string{"parseTime": "false"}}, false},
		"dsn false":        {config.MySQL{DSN: config.NewSecret("u@tcp(db)/x?parseTime=false")}, false},
		"dsn without flag": {config.MySQL{DSN: config.NewSecret("u@tcp(db)/x")}, true},
	} {
		t.Run(name, func(t *testing.T) {
			var got *gormmysql.Config
			db, err := OpenMySQL(context.Background(), tc.cfg, append(offline(), WithMySQLDialector(func(c *gormmysql.Config) { got = c }))...)
			if err != nil {
				t.Fatal(err)
			}
			sqlDB, _ := db.DB()
			defer sqlDB.Close()
			if got.DSNConfig == nil || got.DSNConfig.ParseTime != tc.want {
				t.Errorf("parseTime = %v, want %v", got.DSNConfig.ParseTime, tc.want)
			}
		})
	}
}

func TestOpenPostgres(t *testing.T) {
	db, err := OpenPostgres(context.Background(), config.Postgres{Host: "pg", SQLPool: config.SQLPool{MaxOpenConns: 4}}, offline()...)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if sqlDB.Stats().MaxOpenConnections != 4 || db.Dialector.Name() != "postgres" {
		t.Errorf("max open = %d dialector = %s", sqlDB.Stats().MaxOpenConnections, db.Dialector.Name())
	}
}

func TestOpenFailure(t *testing.T) {
	if _, err := OpenMySQL(context.Background(), config.MySQL{Host: "127.0.0.1", Port: 1, PingTimeout: time.Second}); err == nil {
		t.Error("expected ping failure")
	}
	if _, err := OpenPostgres(context.Background(), config.Postgres{}); err == nil {
		t.Error("expected validation failure")
	}
}

// Reads must go to a replica and writes to the primary, or replicas buy
// nothing; Clauses(dbresolver.Write) must pin a read to the primary for
// read-your-writes. Close must release the replica pools too, which
// db.DB().Close() cannot reach.
func TestReplicasRouteReadsAndWrites(t *testing.T) {
	var dialected []*gormmysql.Config
	db, err := OpenMySQL(context.Background(), config.MySQL{Host: "primary"}, append(offline(),
		WithMySQLDialector(func(c *gormmysql.Config) { dialected = append(dialected, c) }),
		WithMySQLReplicas(config.MySQL{Host: "replica", SQLPool: config.SQLPool{MaxOpenConns: 3}}))...)
	if err != nil {
		t.Fatal(err)
	}
	primary, _ := db.DB()
	if len(dialected) != 2 {
		t.Fatalf("dialector hook ran %d times, want once for the primary and once for the replica", len(dialected))
	}
	for _, c := range dialected {
		if !c.DSNConfig.ParseTime {
			t.Errorf("%s: parseTime = false, want the same default as the primary", c.DSNConfig.Addr)
		}
	}

	type User struct{ ID int }
	dry := db.Session(&gorm.Session{DryRun: true, SkipDefaultTransaction: true})
	read, ok := dry.Find(&[]User{}).Statement.ConnPool.(*sql.DB)
	if !ok || read == primary {
		t.Fatalf("read went to %T %p, want a replica pool (primary %p)", read, read, primary)
	}
	if read.Stats().MaxOpenConnections != 3 {
		t.Errorf("replica pool settings not applied: max open = %d", read.Stats().MaxOpenConnections)
	}
	if got := dry.Create(&User{ID: 1}).Statement.ConnPool; got != gorm.ConnPool(primary) {
		t.Errorf("write went to %p, want the primary %p", got, primary)
	}
	if got := dry.Clauses(dbresolver.Write).Find(&[]User{}).Statement.ConnPool; got != gorm.ConnPool(primary) {
		t.Errorf("pinned read went to %p, want the primary %p", got, primary)
	}

	if err := Close(db); err != nil {
		t.Fatal(err)
	}
	for name, p := range map[string]*sql.DB{"primary": primary, "replica": read} {
		if err := p.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
			t.Errorf("%s pool still open after Close: %v", name, err)
		}
	}
}

// A replica that cannot be opened fails the whole open and releases the
// pools already opened.
func TestReplicaFailureClosesPrimary(t *testing.T) {
	var primary *sql.DB
	_, err := OpenMySQL(context.Background(), config.MySQL{Host: "primary"}, append(offline(),
		WithMySQLDialector(func(c *gormmysql.Config) {
			if primary == nil {
				primary = c.Conn.(*sql.DB)
			}
		}),
		WithMySQLReplicas(config.MySQL{}))...) // invalid: no host
	if err == nil || !strings.Contains(err.Error(), "replica 0") {
		t.Fatalf("err = %v", err)
	}
	if err := primary.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("primary pool left open: %v", err)
	}
}
