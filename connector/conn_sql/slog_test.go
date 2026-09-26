package conn_sql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"

	"github.com/linzeyan/loadconf/config"
)

func jsonLogger(level slog.Level) (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level, AddSource: true})), &buf
}

func lastRecord(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &m); err != nil {
		t.Fatalf("decode %q: %v", buf.String(), err)
	}
	return m
}

func sourceFile(m map[string]any) string {
	src, _ := m["source"].(map[string]any)
	file, _ := src["file"].(string)
	return file
}

func TestMySQLLogger(t *testing.T) {
	l, buf := jsonLogger(slog.LevelInfo)
	MySQLLogger(l, slog.LevelWarn).Print("closing bad idle connection: ", errors.New("EOF"))
	m := lastRecord(t, buf)
	if m["level"] != "WARN" || m["msg"] != "closing bad idle connection: EOF" || !strings.HasSuffix(sourceFile(m), "slog_test.go") {
		t.Errorf("record = %v", m)
	}

	buf.Reset()
	MySQLLogger(l, slog.LevelDebug).Print("hidden")
	if buf.Len() != 0 {
		t.Errorf("disabled level logged: %s", buf.String())
	}
}

func TestPgxTracer(t *testing.T) {
	l, buf := jsonLogger(slog.LevelDebug - 4)
	lg := PgxTracer(l, tracelog.LogLevelTrace).Logger
	lg.Log(context.Background(), tracelog.LogLevelInfo, "Query", map[string]any{"sql": "SELECT 1", "elapsed": time.Millisecond, "commandTag": "SELECT 1"})
	m := lastRecord(t, buf)
	if m["level"] != "INFO" || m["msg"] != "Query" || m["sql"] != "SELECT 1" || m["elapsed"] != float64(time.Millisecond) || !strings.HasSuffix(sourceFile(m), "slog_test.go") {
		t.Errorf("record = %v", m)
	}
	for lv, want := range map[tracelog.LogLevel]string{
		tracelog.LogLevelTrace: "DEBUG-4", tracelog.LogLevelDebug: "DEBUG", tracelog.LogLevelWarn: "WARN", tracelog.LogLevelError: "ERROR",
	} {
		lg.Log(context.Background(), lv, "x", nil)
		if got := lastRecord(t, buf)["level"]; got != want {
			t.Errorf("%v -> %v, want %s", lv, got, want)
		}
	}
}

func TestWithLogger(t *testing.T) {
	l, buf := jsonLogger(slog.LevelInfo)
	var mc *mysql.Config
	db, err := NewMySQL(config.MySQL{Host: "db"}, WithLogger(l),
		WithMySQLConfig(func(c *mysql.Config) { mc = c }))
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, ok := mc.Logger.(mysqlLogger); !ok {
		t.Errorf("mysql logger = %T", mc.Logger)
	}

	// A failed connect is traced at Error, pointing at the code that pinged.
	var pc *pgx.ConnConfig
	_, err = OpenPostgres(context.Background(), config.Postgres{Host: "127.0.0.1", Port: 1, Password: config.NewSecret("secret"), PingTimeout: time.Second},
		WithLogger(l), WithPostgresConfig(func(c *pgx.ConnConfig) { pc = c }))
	if err == nil {
		t.Fatal("connecting to a closed port should fail")
	}
	if _, ok := pc.Tracer.(*tracelog.TraceLog); !ok {
		t.Fatalf("tracer = %T", pc.Tracer)
	}
	m := lastRecord(t, buf)
	if m["level"] != "ERROR" || m["msg"] != "Connect" || m["err"] == nil || m["elapsed"] == nil {
		t.Errorf("record = %v", m)
	}
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("password logged: %s", buf.String())
	}
}
