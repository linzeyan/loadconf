package conn_gorm_log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/linzeyan/loadconf/config"
)

func newTest(level slog.Level, cfg Config) (gormlogger.Interface, *bytes.Buffer) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level, AddSource: true}))
	return New(l, cfg), &buf
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func query(sql string, rows int64) func() (string, int64) {
	return func() (string, int64) { return sql, rows }
}

func TestTrace(t *testing.T) {
	lg, buf := newTest(slog.LevelDebug, Config{SlowThreshold: 100 * time.Millisecond, Level: "info", Component: "gorm"})
	ctx := context.Background()
	now := time.Now()

	lg.Trace(ctx, now, query("SELECT 1", 1), nil)
	lg.Trace(ctx, now.Add(-time.Second), query("SELECT sleep(1)", -1), nil)
	lg.Trace(ctx, now, query("SELECT * FROM users WHERE id = 1", 0), gorm.ErrRecordNotFound)
	lg.Trace(ctx, now, query("INSERT INTO t VALUES (1)", 0), errors.New("duplicate key"))

	recs := records(t, buf)
	if len(recs) != 4 {
		t.Fatalf("records = %v", recs)
	}
	if r := recs[0]; r["level"] != "INFO" || r["msg"] != "query" || r["sql"] != "SELECT 1" || r["rows"] != float64(1) || r["component"] != "gorm" {
		t.Errorf("info = %v", r)
	}
	if r := recs[1]; r["level"] != "WARN" || r["msg"] != "slow query" || r["slow_threshold"] == nil || r["rows"] != nil {
		t.Errorf("slow = %v", r)
	}
	if r := recs[2]; r["level"] != "INFO" || r["error"] != nil {
		t.Errorf("record not found should not be an error: %v", r)
	}
	if r := recs[3]; r["level"] != "ERROR" || r["msg"] != "query failed" || r["error"] != "duplicate key" {
		t.Errorf("error = %v", r)
	}

	buf.Reset()
	New(slog.New(slog.NewJSONHandler(buf, nil)), Config{LogRecordNotFound: true}).
		Trace(ctx, now, query("SELECT 1", 0), gorm.ErrRecordNotFound)
	if !strings.Contains(buf.String(), `"error":"record not found"`) {
		t.Errorf("LogRecordNotFound: %s", buf.String())
	}
	src, _ := recs[0]["source"].(map[string]any)
	if file, _ := src["file"].(string); !strings.HasSuffix(file, "conn_gorm_log_test.go") {
		t.Errorf("source = %v", recs[0]["source"])
	}
}

func TestLevels(t *testing.T) {
	ctx := context.Background()
	called := false
	lazy := func() (string, int64) { called = true; return "SELECT 1", 1 }

	// warn (the default) logs errors and slow queries only.
	lg, buf := newTest(slog.LevelDebug, Config{SlowThreshold: time.Millisecond})
	lg.Trace(ctx, time.Now(), lazy, nil)
	lg.Info(ctx, "ignored %d", 1)
	lg.Warn(ctx, "migrating %s", "users")
	if called || !strings.Contains(buf.String(), `"msg":"migrating users"`) || strings.Contains(buf.String(), "ignored") {
		t.Errorf("warn level: called=%v %s", called, buf.String())
	}

	// A disabled slog level skips formatting the SQL.
	lg, buf = newTest(slog.LevelWarn, Config{Level: "info"})
	lg.Trace(ctx, time.Now(), lazy, nil)
	if called || buf.Len() != 0 {
		t.Errorf("slog level not honored: called=%v %s", called, buf.String())
	}

	// LogMode returns a copy.
	base, buf := newTest(slog.LevelDebug, Config{Level: "error"})
	silent := base.LogMode(gormlogger.Silent)
	silent.Error(ctx, "hidden")
	silent.Trace(ctx, time.Now(), lazy, errors.New("x"))
	base.Error(ctx, "shown")
	if called || strings.Contains(buf.String(), "hidden") || !strings.Contains(buf.String(), "shown") {
		t.Errorf("LogMode: called=%v %s", called, buf.String())
	}
	base.LogMode(gormlogger.Info).Info(ctx, "info on copy")
	base.Info(ctx, "not on base")
	if !strings.Contains(buf.String(), "info on copy") || strings.Contains(buf.String(), "not on base") {
		t.Errorf("LogMode must not change the original: %s", buf.String())
	}
}

func TestParamsFilter(t *testing.T) {
	lg, _ := newTest(slog.LevelInfo, Config{ParameterizedQueries: true})
	sql, params := lg.(gorm.ParamsFilter).ParamsFilter(context.Background(), "SELECT ?", 1)
	if sql != "SELECT ?" || params != nil {
		t.Errorf("parameterized: %s %v", sql, params)
	}
	lg, _ = newTest(slog.LevelInfo, Config{})
	if _, params = lg.(gorm.ParamsFilter).ParamsFilter(context.Background(), "SELECT ?", 1); len(params) != 1 {
		t.Errorf("params dropped: %v", params)
	}
}

func TestConfig(t *testing.T) {
	type app struct {
		Gorm Config `config:"gorm"`
	}
	cfg, err := config.Load[app](context.Background(), config.WithLogger(nil), config.From(config.Map(map[string]any{})))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gorm != DefaultConfig() {
		t.Errorf("defaults = %+v, want %+v", cfg.Gorm, DefaultConfig())
	}
	_, err = config.Load[app](context.Background(), config.WithLogger(nil), config.From(config.Map(map[string]any{"gorm": map[string]any{"level": "chatty"}})))
	if err == nil || !strings.Contains(err.Error(), `gorm: unsupported level "chatty"`) {
		t.Errorf("validation: %v", err)
	}
}
