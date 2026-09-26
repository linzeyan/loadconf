package conn_gorm

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_sql"
)

func TestDefaultLoggerIsSlog(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// gorm's own default logger prints colored text to stdout; the
	// connector must route gorm through slog.Default() instead.
	db, err := OpenPostgres(context.Background(), config.Postgres{Host: "pg"}, offline()...)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if _, ok := db.Config.Logger.(gorm.ParamsFilter); !ok {
		t.Errorf("logger = %T, want the conn_gorm_log adapter", db.Config.Logger)
	}
	db.Logger.Error(context.Background(), "gorm-default-marker")
	if !strings.Contains(buf.String(), `"msg":"gorm-default-marker"`) || !strings.Contains(buf.String(), `"component":"gorm"`) {
		t.Errorf("default logger output: %s", buf.String())
	}
}

func TestCustomLoggerKept(t *testing.T) {
	custom := gormlogger.Discard
	db, err := OpenMySQL(context.Background(), config.MySQL{Host: "db"},
		append(offline(), WithGormConfig(&gorm.Config{Logger: custom, DisableAutomaticPing: true}))...)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if db.Config.Logger != custom {
		t.Errorf("logger = %T, want the one passed in", db.Config.Logger)
	}
}

func TestParseTimeUserHookWins(t *testing.T) {
	// Options passed through WithSQLOptions run after the parseTime default,
	// so an application can still switch it off in code.
	var parseTime bool
	db, err := OpenMySQL(context.Background(), config.MySQL{Host: "db"}, append(offline(),
		WithSQLOptions(conn_sql.WithMySQLConfig(func(mc *mysql.Config) { mc.ParseTime = false })),
		WithMySQLDialector(func(c *gormmysql.Config) { parseTime = c.DSNConfig.ParseTime }))...)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if parseTime {
		t.Error("user hook could not disable parseTime")
	}
}

func TestOpenErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	_, err := OpenMySQL(context.Background(), config.MySQL{Host: "db", Password: config.NewSecret(pw), Params: map[string]string{"parseTime": "maybe"}}, offline()...)
	if err == nil || strings.Contains(err.Error(), pw) {
		t.Errorf("mysql err = %v", err)
	}
	_, err = OpenPostgres(context.Background(), config.Postgres{Host: "pg", Port: 70000, Password: config.NewSecret(pw)}, offline()...)
	if err == nil || strings.Contains(err.Error(), pw) {
		t.Errorf("postgres err = %v", err)
	}
}

// TestMySQLParseTimeInPasswordIsNotExplicit: only parseTime in the DSN query
// is an explicit setting. A password or database name containing
// "parseTime=" must still get the parseTime=true default, or gorm scans
// DATETIME columns into []byte.
func TestMySQLParseTimeInPasswordIsNotExplicit(t *testing.T) {
	var parseTime bool
	db, err := OpenMySQL(context.Background(), config.MySQL{DSN: config.NewSecret("app:parseTime=x@tcp(db:3306)/core")}, append(offline(),
		WithMySQLDialector(func(c *gormmysql.Config) { parseTime = c.DSNConfig.ParseTime }))...)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if !parseTime {
		t.Error("parseTime = false, want true")
	}
}

// TestWithGormConfigNilUsesDefaults: a nil config means the default config;
// it must not be dereferenced after the pool is already open.
func TestWithGormConfigNilUsesDefaults(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("OpenPostgres panicked: %v", r)
		}
	}()
	db, err := OpenPostgres(context.Background(), config.Postgres{Host: "pg"}, withoutPing(), WithGormConfig(nil))
	if err != nil {
		return
	}
	sqlDB, _ := db.DB()
	_ = sqlDB.Close()
}
