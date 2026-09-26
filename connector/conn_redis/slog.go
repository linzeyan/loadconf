package conn_redis

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"
)

// Printer is the logger interface of go-redis (redis.SetLogger).
type Printer interface {
	Printf(ctx context.Context, format string, v ...any)
}

// SlogLogger adapts l to go-redis, which logs through one process-wide
// logger:
//
//	redis.SetLogger(conn_redis.SlogLogger(logger, slog.LevelWarn))
//
// go-redis messages carry no level, so all are logged at level; which
// messages go-redis emits is set with redis.SetLogLevel (errors only by
// default). The "redis: " prefix is dropped and the source points at the
// go-redis code that logged. A nil l uses slog.Default().
func SlogLogger(l *slog.Logger, level slog.Level) Printer {
	if l == nil {
		l = slog.Default()
	}
	return slogPrinter{l: l, level: level}
}

type slogPrinter struct {
	l     *slog.Logger
	level slog.Level
}

func (p slogPrinter) Printf(ctx context.Context, format string, v ...any) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !p.l.Enabled(ctx, p.level) {
		return
	}
	msg := strings.TrimSpace(strings.TrimPrefix(fmt.Sprintf(format, v...), "redis: "))
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip Callers and Printf
	r := slog.NewRecord(time.Now(), p.level, msg, pcs[0])
	_ = p.l.Handler().Handle(ctx, r)
}
