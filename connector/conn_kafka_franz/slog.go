package conn_kafka_franz

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SlogLogger adapts l to franz-go. The client's log level follows the level
// l is enabled at, checked on every call, so changing the level of l (e.g.
// with logger.Update) takes effect on running clients. A nil l uses
// slog.Default().
func SlogLogger(l *slog.Logger) kgo.Logger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l}
}

// WithLogger returns a client option that logs to l instead of
// slog.Default() through [SlogLogger]:
//
//	cl, err := conn_kafka_franz.Open(ctx, cfg, conn_kafka_franz.WithLogger(logger))
func WithLogger(l *slog.Logger) kgo.Opt { return kgo.WithLogger(SlogLogger(l)) }

type slogLogger struct{ l *slog.Logger }

var kgoLevels = []struct {
	kgo  kgo.LogLevel
	slog slog.Level
}{
	{kgo.LogLevelDebug, slog.LevelDebug},
	{kgo.LogLevelInfo, slog.LevelInfo},
	{kgo.LogLevelWarn, slog.LevelWarn},
	{kgo.LogLevelError, slog.LevelError},
}

func (s slogLogger) Level() kgo.LogLevel {
	ctx := context.Background()
	for _, lv := range kgoLevels {
		if s.l.Enabled(ctx, lv.slog) {
			return lv.kgo
		}
	}
	return kgo.LogLevelNone
}

func (s slogLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := slog.LevelDebug
	for _, lv := range kgoLevels {
		if lv.kgo == level {
			lvl = lv.slog
		}
	}
	ctx := context.Background()
	if !s.l.Enabled(ctx, lvl) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // skip Callers and Log
	r := slog.NewRecord(time.Now(), lvl, msg, pcs[0])
	r.Add(keyvals...)
	_ = s.l.Handler().Handle(ctx, r)
}
