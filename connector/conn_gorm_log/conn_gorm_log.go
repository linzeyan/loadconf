// Package conn_gorm_log adapts *slog.Logger to gorm's logger.Interface.
//
//	db, err := conn_gorm.OpenMySQL(ctx, cfg.MySQL.MustGet("main"),
//		conn_gorm.WithGormConfig(&gorm.Config{
//			Logger: conn_gorm_log.New(logger, cfg.GormLog),
//		}))
//
// [Config] carries config tags, so it can be part of the application config.
// Records point their source at the application code that ran the query,
// not at gorm.
package conn_gorm_log

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Config configures the adapter.
type Config struct {
	// SlowThreshold logs queries slower than this at Warn; 0 disables.
	SlowThreshold time.Duration `config:"slow_threshold" default:"200ms"`
	// Level is gorm's level: silent, error, warn (slow queries) or info
	// (every query, logged at slog Info).
	Level string `config:"level" default:"warn"`
	// LogRecordNotFound logs gorm.ErrRecordNotFound as an error; by default
	// such queries are logged like successful ones.
	LogRecordNotFound bool `config:"log_record_not_found"`
	// ParameterizedQueries logs SQL with placeholders instead of the bound
	// values, keeping them out of the logs.
	ParameterizedQueries bool `config:"parameterized_queries"`
	// Component is the value of a "component" attribute; empty omits it.
	Component string `config:"component" default:"gorm"`
}

var levels = map[string]gormlogger.LogLevel{
	"silent": gormlogger.Silent,
	"error":  gormlogger.Error,
	"warn":   gormlogger.Warn,
	"info":   gormlogger.Info,
}

func (c Config) Validate() error {
	if _, ok := levels[strings.ToLower(c.Level)]; !ok && c.Level != "" {
		return fmt.Errorf("unsupported level %q (want silent, error, warn or info)", c.Level)
	}
	if c.SlowThreshold < 0 {
		return errors.New("slow_threshold must not be negative")
	}
	return nil
}

// DefaultConfig returns the defaults a config file load applies. Note that
// a zero SlowThreshold disables slow query logging.
func DefaultConfig() Config {
	return Config{SlowThreshold: 200 * time.Millisecond, Level: "warn", Component: "gorm"}
}

// New returns a gorm logger writing to l (slog.Default() when nil). An
// empty or unknown cfg.Level means warn.
func New(l *slog.Logger, cfg Config) gormlogger.Interface {
	if l == nil {
		l = slog.Default()
	}
	if cfg.Component != "" {
		l = l.With(slog.String("component", cfg.Component))
	}
	level, ok := levels[strings.ToLower(cfg.Level)]
	if !ok {
		level = gormlogger.Warn
	}
	return &adapter{l: l, cfg: cfg, level: level}
}

type adapter struct {
	l     *slog.Logger
	cfg   Config
	level gormlogger.LogLevel
}

var _ gorm.ParamsFilter = (*adapter)(nil)

func (a *adapter) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	c := *a
	c.level = level
	return &c
}

func (a *adapter) Info(ctx context.Context, msg string, args ...any) {
	if a.level >= gormlogger.Info {
		a.log(ctx, slog.LevelInfo, format(msg, args))
	}
}

func (a *adapter) Warn(ctx context.Context, msg string, args ...any) {
	if a.level >= gormlogger.Warn {
		a.log(ctx, slog.LevelWarn, format(msg, args))
	}
}

func (a *adapter) Error(ctx context.Context, msg string, args ...any) {
	if a.level >= gormlogger.Error {
		a.log(ctx, slog.LevelError, format(msg, args))
	}
}

// Trace logs a failed query at Error, a slow one at Warn and, at the info
// level, every other one at Info, with the SQL, the affected rows and the
// elapsed time.
func (a *adapter) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if a.level <= gormlogger.Silent {
		return
	}
	elapsed := time.Since(begin)
	var (
		level slog.Level
		msg   string
		extra []slog.Attr
	)
	switch {
	case err != nil && a.level >= gormlogger.Error && (a.cfg.LogRecordNotFound || !errors.Is(err, gorm.ErrRecordNotFound)):
		level, msg = slog.LevelError, "query failed"
		extra = append(extra, slog.Any("error", err))
	case a.cfg.SlowThreshold > 0 && elapsed > a.cfg.SlowThreshold && a.level >= gormlogger.Warn:
		level, msg = slog.LevelWarn, "slow query"
		extra = append(extra, slog.Duration("slow_threshold", a.cfg.SlowThreshold))
	case a.level >= gormlogger.Info:
		level, msg = slog.LevelInfo, "query"
	default:
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !a.l.Enabled(ctx, level) {
		return
	}
	sql, rows := fc()
	attrs := []slog.Attr{slog.String("sql", sql), slog.Duration("elapsed", elapsed)}
	if rows >= 0 {
		attrs = append(attrs, slog.Int64("rows", rows))
	}
	a.log(ctx, level, msg, append(attrs, extra...)...)
}

// ParamsFilter drops the bound values when ParameterizedQueries is set.
func (a *adapter) ParamsFilter(_ context.Context, sql string, params ...any) (string, []any) {
	if a.cfg.ParameterizedQueries {
		return sql, nil
	}
	return sql, params
}

func (a *adapter) log(ctx context.Context, level slog.Level, msg string, attrs ...slog.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !a.l.Enabled(ctx, level) {
		return
	}
	r := slog.NewRecord(time.Now(), level, msg, callerPC())
	r.AddAttrs(attrs...)
	_ = a.l.Handler().Handle(ctx, r)
}

// skipPrefixes are the function prefixes of gorm, the dialectors and this
// package, which callerPC skips to find the application code.
var skipPrefixes = []string{
	"gorm.io/",
	"github.com/ncruces/go-sqlite3/gormlite.",
	"github.com/linzeyan/loadconf/connector/conn_gorm_log.",
}

// callerPC returns the first frame outside gorm, the dialectors and this
// package.
func callerPC() uintptr {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	for _, pc := range pcs[:n] {
		f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
		if strings.HasSuffix(f.File, "_test.go") {
			return pc
		}
		if !slices.ContainsFunc(skipPrefixes, func(p string) bool { return strings.HasPrefix(f.Function, p) }) {
			return pc
		}
	}
	return 0
}

func format(msg string, args []any) string {
	if len(args) == 0 {
		return msg
	}
	return fmt.Sprintf(msg, args...)
}
