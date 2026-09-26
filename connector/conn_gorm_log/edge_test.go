package conn_gorm_log

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/utils/tests"
)

func TestTraceDecisionTable(t *testing.T) {
	ctx := context.Background()
	wrappedNotFound := fmt.Errorf("find user: %w", gorm.ErrRecordNotFound)
	type event struct {
		name string
		slow bool
		err  error
	}
	events := []event{
		{"ok", false, nil},
		{"slow", true, nil},
		{"failed", false, errors.New("boom")},
		{"failed slow", true, errors.New("boom")},
		{"not found", false, gorm.ErrRecordNotFound},
		{"wrapped not found", false, wrappedNotFound},
		{"slow not found", true, gorm.ErrRecordNotFound},
	}
	// Expected slog level per event ("" = nothing logged), by gorm level.
	// Unknown and empty levels behave like warn, the documented default.
	want := map[string][]string{
		"silent":  {"", "", "", "", "", "", ""},
		"error":   {"", "", "ERROR", "ERROR", "", "", ""},
		"warn":    {"", "WARN", "ERROR", "ERROR", "", "", "WARN"},
		"WARN":    {"", "WARN", "ERROR", "ERROR", "", "", "WARN"},
		"":        {"", "WARN", "ERROR", "ERROR", "", "", "WARN"},
		"verbose": {"", "WARN", "ERROR", "ERROR", "", "", "WARN"},
		"info":    {"INFO", "WARN", "ERROR", "ERROR", "INFO", "INFO", "WARN"},
		"Info":    {"INFO", "WARN", "ERROR", "ERROR", "INFO", "INFO", "WARN"},
	}
	for level, wants := range want {
		for i, ev := range events {
			t.Run(level+"/"+ev.name, func(t *testing.T) {
				lg, buf := newTest(slog.LevelDebug, Config{Level: level, SlowThreshold: time.Hour})
				begin := time.Now()
				if ev.slow {
					begin = begin.Add(-2 * time.Hour)
				}
				called := false
				lg.Trace(ctx, begin, func() (string, int64) { called = true; return "SELECT 1", 1 }, ev.err)
				got := ""
				if buf.Len() > 0 {
					got = records(t, buf)[0]["level"].(string)
				}
				if got != wants[i] {
					t.Errorf("logged %q, want %q", got, wants[i])
				}
				// Building the SQL string is the expensive part; it must only
				// happen for records that are written.
				if called != (got != "") {
					t.Errorf("fc called = %v for logged level %q", called, got)
				}
			})
		}
	}
}

func TestTraceZeroSlowThresholdNeverSlow(t *testing.T) {
	lg, buf := newTest(slog.LevelDebug, Config{Level: "warn"})
	lg.Trace(context.Background(), time.Now().Add(-24*time.Hour), query("SELECT sleep(86400)", 1), nil)
	if buf.Len() != 0 {
		t.Errorf("zero slow_threshold logged a slow query: %s", buf.String())
	}
}

func TestLogRecordNotFoundWrapped(t *testing.T) {
	// gorm wraps errors from hooks; errors.Is must still classify them.
	lg, buf := newTest(slog.LevelDebug, Config{Level: "error", LogRecordNotFound: true})
	lg.Trace(context.Background(), time.Now(), query("SELECT 1", 0), fmt.Errorf("hook: %w", gorm.ErrRecordNotFound))
	if recs := records(t, buf); len(recs) != 1 || recs[0]["level"] != "ERROR" {
		t.Errorf("records = %v", recs)
	}
}

func TestTraceNilContext(t *testing.T) {
	lg, buf := newTest(slog.LevelDebug, Config{Level: "info"})
	//nolint:staticcheck // gorm passes stmt.Context, which callers may leave nil
	lg.Trace(nil, time.Now(), query("SELECT 1", 1), nil)
	lg.Info(nil, "hello %s", "world") //nolint:staticcheck
	if recs := records(t, buf); len(recs) != 2 || recs[1]["msg"] != "hello world" {
		t.Errorf("records = %v", recs)
	}
}

func TestMessageWithoutArgsKeepsPercent(t *testing.T) {
	// gorm passes preformatted messages; a literal % must not turn into
	// %!d(MISSING).
	lg, buf := newTest(slog.LevelDebug, Config{Level: "info"})
	lg.Info(context.Background(), "100% done")
	if recs := records(t, buf); recs[0]["msg"] != "100% done" {
		t.Errorf("msg = %v", recs[0]["msg"])
	}
}

func TestNilLoggerUsesSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	New(nil, DefaultConfig()).Error(context.Background(), "to default")
	recs := records(t, &buf)
	if len(recs) != 1 || recs[0]["msg"] != "to default" || recs[0]["component"] != "gorm" {
		t.Errorf("records = %v", recs)
	}
}

func TestEmptyComponentOmitted(t *testing.T) {
	lg, buf := newTest(slog.LevelDebug, Config{Level: "info"})
	lg.Info(context.Background(), "x")
	if _, ok := records(t, buf)[0]["component"]; ok {
		t.Errorf("empty component should add no attribute: %s", buf.String())
	}
}

type secretRow struct {
	ID    int
	Token string
}

// dryRun opens gorm without a database; DryRun still builds the SQL and
// traces it, which exercises the real gorm call path into the adapter.
func dryRun(t testing.TB, lg gormlogger.Interface) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(tests.DummyDialector{}, &gorm.Config{Logger: lg, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestThroughGorm(t *testing.T) {
	for _, parameterized := range []bool{false, true} {
		t.Run(fmt.Sprint("parameterized=", parameterized), func(t *testing.T) {
			lg, buf := newTest(slog.LevelDebug, Config{Level: "info", ParameterizedQueries: parameterized})
			var row secretRow
			dryRun(t, lg).Where("token = ?", "tok-hunter2").First(&row)

			recs := records(t, buf)
			if len(recs) != 1 {
				t.Fatalf("records = %v", recs)
			}
			sql, _ := recs[0]["sql"].(string)
			// ParameterizedQueries exists to keep bound values (tokens, PII)
			// out of logs; gorm only honors it through ParamsFilter.
			if leaked := strings.Contains(sql, "tok-hunter2"); leaked == parameterized {
				t.Errorf("sql = %q", sql)
			}
			// The source must be this test, not the gorm callback that
			// called Trace.
			src, _ := recs[0]["source"].(map[string]any)
			if file, _ := src["file"].(string); !strings.HasSuffix(file, "edge_test.go") {
				t.Errorf("source = %v", src)
			}
		})
	}
}

func TestLogModeKeepsParamsFilter(t *testing.T) {
	// db.Debug() swaps in LogMode(Info); the copy must still filter values.
	lg, buf := newTest(slog.LevelDebug, Config{Level: "error", ParameterizedQueries: true})
	var row secretRow
	dryRun(t, lg).Debug().Where("token = ?", "tok-hunter2").First(&row)
	if strings.Contains(buf.String(), "tok-hunter2") || !strings.Contains(buf.String(), `"msg":"query"`) {
		t.Errorf("debug session: %s", buf.String())
	}
}
