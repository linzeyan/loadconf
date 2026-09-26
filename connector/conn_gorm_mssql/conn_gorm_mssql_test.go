package conn_gorm_mssql

import (
	"context"
	"strings"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"

	"github.com/linzeyan/loadconf/config"
)

// withoutPing builds the pool without pinging it.
func withoutPing() Option { return func(o *options) { o.noPing = true } }

func TestOpenWithoutServer(t *testing.T) {
	cfg := config.SQLServer{Host: "127.0.0.1", Port: 1, Database: "app", SQLPool: config.SQLPool{MaxOpenConns: 5}}
	var dialected bool
	db, err := Open(context.Background(), cfg,
		WithGormConfig(&gorm.Config{DisableAutomaticPing: true}),
		withoutPing(),
		WithDialector(func(c *sqlserver.Config) { dialected = true; c.DefaultStringSize = 191 }),
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if !dialected || db.Dialector.Name() != "sqlserver" {
		t.Errorf("dialector = %s, hook called = %v", db.Dialector.Name(), dialected)
	}
	if got := sqlDB.Stats().MaxOpenConnections; got != 5 {
		t.Errorf("pool settings not applied: max open = %d", got)
	}
	if _, ok := sqlDB.Driver().(*mssql.Driver); !ok {
		t.Errorf("driver = %T", sqlDB.Driver())
	}

	// SQL generation works without a server.
	type User struct {
		ID   int
		Name string
	}
	stmt := db.Session(&gorm.Session{DryRun: true}).Where("name = ?", "x").Limit(3).Find(&[]User{}).Statement
	if sql := stmt.SQL.String(); !strings.Contains(sql, "FETCH NEXT 3 ROWS ONLY") {
		t.Errorf("sql = %s", sql)
	}
}

// A nil gorm config means the default config; it must not be dereferenced
// after the pool is already open.
func TestWithGormConfigNilUsesDefaults(t *testing.T) {
	db, err := Open(context.Background(), config.SQLServer{Host: "127.0.0.1", Port: 1}, withoutPing(), WithGormConfig(nil))
	if err != nil {
		return // gorm's automatic ping fails without a server
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()
}

func TestOpenErrors(t *testing.T) {
	if _, err := Open(context.Background(), config.SQLServer{}); err == nil {
		t.Error("an empty config should fail")
	}
	// gorm's automatic ping fails against a closed port and closes the pool.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := Open(ctx, config.SQLServer{Host: "127.0.0.1", Port: 1, Password: config.NewSecret("hunter2"), PingTimeout: time.Second,
		Params: map[string]string{"dial timeout": "1"}})
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %v", err)
	}
}
