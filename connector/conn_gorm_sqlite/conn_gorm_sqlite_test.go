package conn_gorm_sqlite

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ncruces/go-sqlite3"
	"gorm.io/gorm"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/connector/conn_gorm_log"
	"github.com/linzeyan/loadconf/connector/conn_sqlite"
)

type Item struct {
	ID       uint `gorm:"primaryKey"`
	Name     string
	Price    float64
	Created  time.Time
	ParentID *uint
	Parent   *Item
}

func TestOpen(t *testing.T) {
	var logs bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{AddSource: true}))
	var inits int
	cfg := config.SQLite{Path: filepath.Join(t.TempDir(), "app.db"), BusyTimeout: time.Second, JournalMode: "wal", ForeignKeys: true}
	db, err := Open(context.Background(), cfg,
		WithGormConfig(&gorm.Config{Logger: conn_gorm_log.New(l, conn_gorm_log.Config{Level: "info"})}),
		WithSQLOptions(conn_sqlite.WithInit(func(*sqlite3.Conn) error { inits++; return nil })),
	)
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if inits == 0 {
		t.Error("conn_sqlite options not applied")
	}

	if err := db.AutoMigrate(&Item{}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	parent := Item{Name: "parent", Price: 1.5, Created: now}
	if err := db.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&Item{Name: "child", ParentID: &parent.ID, Created: now}).Error; err != nil {
		t.Fatal(err)
	}

	var got Item
	if err := db.Preload("Parent").Where("name = ?", "child").First(&got).Error; err != nil {
		t.Fatal(err)
	}
	if got.Parent == nil || got.Parent.Name != "parent" || !got.Created.Equal(now) {
		t.Errorf("got %+v", got)
	}

	// Foreign keys come from conn_sqlite's PRAGMAs.
	bad := uint(999)
	if err := db.Create(&Item{Name: "orphan", ParentID: &bad}).Error; err == nil {
		t.Error("foreign keys are not enforced")
	}
	if err := db.Where("name = ?", "missing").First(&Item{}).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("err = %v", err)
	}

	// Records through gorm point at this file, not at gorm or conn_gorm_log.
	out := logs.String()
	if !strings.Contains(out, `"msg":"query"`) || !strings.Contains(out, "conn_gorm_sqlite_test.go") {
		t.Errorf("logs = %s", out)
	}
	if strings.Contains(out, "gorm.io/") || strings.Contains(out, "gormlite@") || strings.Contains(out, "record not found") {
		t.Errorf("unexpected source or error: %s", out)
	}
	if !strings.Contains(out, `"msg":"query failed"`) || !strings.Contains(out, "FOREIGN KEY") {
		t.Errorf("failed insert not logged: %s", out)
	}
}

func TestOpenMemoryAndErrors(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, config.SQLite{Path: ":memory:", ForeignKeys: true})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if err := db.AutoMigrate(&Item{}); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := db.Model(&Item{}).Count(&n).Error; err != nil || n != 0 {
		t.Errorf("count = %d, %v", n, err)
	}

	if _, err := Open(ctx, config.SQLite{}); err == nil {
		t.Error("an empty path should fail")
	}
	if _, err := Open(ctx, config.SQLite{Path: filepath.Join(t.TempDir(), "no", "x.db"), Mode: "rw"}); err == nil {
		t.Error("a missing file in rw mode should fail")
	}
}

// A nil gorm config means the default config; it must not be dereferenced
// after the pool is already open.
func TestWithGormConfigNilUsesDefaults(t *testing.T) {
	db, err := Open(context.Background(), config.SQLite{Path: ":memory:"}, WithGormConfig(nil))
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if _, ok := db.Config.Logger.(gorm.ParamsFilter); !ok {
		t.Errorf("logger = %T, want the conn_gorm_log adapter", db.Config.Logger)
	}
}

// The default logger goes into the opened DB only: the caller's gorm config
// may be shared between Opens and must stay as given.
func TestDefaultLoggerLeavesCallerConfig(t *testing.T) {
	gcfg := &gorm.Config{}
	db, err := Open(context.Background(), config.SQLite{Path: ":memory:"}, WithGormConfig(gcfg))
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	if gcfg.Logger != nil {
		t.Errorf("caller's config Logger = %T, want it left nil", gcfg.Logger)
	}
	if _, ok := db.Config.Logger.(gorm.ParamsFilter); !ok {
		t.Errorf("logger = %T, want the conn_gorm_log adapter", db.Config.Logger)
	}
}

// Without a logger in the gorm config, failed queries must reach the
// application's structured logger, not gorm's colored stdout printer.
func TestDefaultLoggerIsSlogDefault(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(old)

	db, err := Open(context.Background(), config.SQLite{Path: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()
	_ = db.Exec("SELECT * FROM missing_table").Error
	if out := logs.String(); !strings.Contains(out, `"level":"ERROR"`) || !strings.Contains(out, "missing_table") {
		t.Errorf("want an ERROR record for the failed query in slog.Default(), got:\n%s", out)
	}
}
