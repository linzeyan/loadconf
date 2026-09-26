package conn_sql

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"
)

// WithLogger logs driver messages to l instead of slog.Default(): MySQL
// driver messages at Warn and failed PostgreSQL queries and connects at Error
// (a pgx tracer at tracelog.LogLevelWarn). Use MySQLLogger and PgxTracer
// directly for other levels.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.mysql = append(o.mysql, func(c *mysql.Config) { c.Logger = MySQLLogger(l, slog.LevelWarn) })
		o.postgres = append(o.postgres, func(c *pgx.ConnConfig) { c.Tracer = PgxTracer(l, tracelog.LogLevelWarn) })
	}
}

// MySQLLogger adapts l to the go-sql-driver/mysql logger, set per pool in
// mysql.Config.Logger:
//
//	conn_sql.WithMySQLConfig(func(c *mysql.Config) {
//		c.Logger = conn_sql.MySQLLogger(logger, slog.LevelWarn)
//	})
//
// The driver's messages (bad idle connections, auth plugin fallbacks, ...)
// carry no level, so all are logged at level. A nil l uses slog.Default().
func MySQLLogger(l *slog.Logger, level slog.Level) mysql.Logger {
	if l == nil {
		l = slog.Default()
	}
	return mysqlLogger{l: l, level: level}
}

type mysqlLogger struct {
	l     *slog.Logger
	level slog.Level
}

func (m mysqlLogger) Print(v ...any) {
	ctx := context.Background()
	if !m.l.Enabled(ctx, m.level) {
		return
	}
	r := slog.NewRecord(time.Now(), m.level, strings.TrimSpace(fmt.Sprint(v...)), callerPC())
	_ = m.l.Handler().Handle(ctx, r)
}

// PgxTracer returns a pgx tracer logging to l, set in pgx.ConnConfig.Tracer:
//
//	conn_sql.WithPostgresConfig(func(c *pgx.ConnConfig) {
//		c.Tracer = conn_sql.PgxTracer(logger, tracelog.LogLevelInfo)
//	})
//
// level is pgx's threshold: at Info every query is logged with its SQL and
// arguments, at Warn or Error only failures. pgx levels map to the slog
// levels of the same name; trace maps to Debug-4. The duration is logged as
// "elapsed". A nil l uses slog.Default().
func PgxTracer(l *slog.Logger, level tracelog.LogLevel) *tracelog.TraceLog {
	if l == nil {
		l = slog.Default()
	}
	cfg := tracelog.DefaultTraceLogConfig()
	cfg.TimeKey = "elapsed"
	return &tracelog.TraceLog{
		Logger: tracelog.LoggerFunc(func(ctx context.Context, lv tracelog.LogLevel, msg string, data map[string]any) {
			sl := pgxLevel(lv)
			if !l.Enabled(ctx, sl) {
				return
			}
			r := slog.NewRecord(time.Now(), sl, msg, callerPC())
			for _, k := range slices.Sorted(maps.Keys(data)) {
				r.AddAttrs(slog.Any(k, data[k]))
			}
			_ = l.Handler().Handle(ctx, r)
		}),
		LogLevel: level,
		Config:   cfg,
	}
}

func pgxLevel(lv tracelog.LogLevel) slog.Level {
	switch lv {
	case tracelog.LogLevelTrace:
		return slog.LevelDebug - 4
	case tracelog.LogLevelDebug:
		return slog.LevelDebug
	case tracelog.LogLevelInfo:
		return slog.LevelInfo
	case tracelog.LogLevelWarn:
		return slog.LevelWarn
	default:
		return slog.LevelError
	}
}

// skipPrefixes are the function prefixes of the drivers, database/sql, gorm
// and this package, which callerPC skips to find the application code.
var skipPrefixes = []string{
	"github.com/go-sql-driver/mysql.",
	"github.com/jackc/",
	"database/sql.",
	"gorm.io/",
	"github.com/linzeyan/loadconf/connector/",
	"runtime.",
}

// callerPC returns the first frame outside skipPrefixes; background driver
// work without application frames falls back to the frame that logged.
func callerPC() uintptr {
	var pcs [48]uintptr
	n := runtime.Callers(3, pcs[:]) // skip Callers, callerPC and the adapter
	for _, pc := range pcs[:n] {
		f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
		if strings.HasSuffix(f.File, "_test.go") ||
			!slices.ContainsFunc(skipPrefixes, func(p string) bool { return strings.HasPrefix(f.Function, p) }) {
			return pc
		}
	}
	if n > 0 {
		return pcs[0]
	}
	return 0
}
