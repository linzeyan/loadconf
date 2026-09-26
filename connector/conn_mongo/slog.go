package conn_mongo

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// SlogSink adapts l to the driver's log sink: informational messages are
// logged at Info, debugging messages at Debug and errors at Error, with the
// driver's key/value pairs as attributes. A nil l uses slog.Default().
func SlogSink(l *slog.Logger) options.LogSink {
	if l == nil {
		l = slog.Default()
	}
	return slogSink{l}
}

// WithLogger routes the driver's structured logs of all components
// (commands, topology, server selection, connections) to l, at
// options.LogLevelInfo or the more verbose options.LogLevelDebug.
func WithLogger(l *slog.Logger, level options.LogLevel) Option {
	return func(o *options.ClientOptions) {
		o.SetLoggerOptions(options.Logger().
			SetSink(SlogSink(l)).
			SetComponentLevel(options.LogComponentAll, level))
	}
}

type slogSink struct{ l *slog.Logger }

// The driver's verbosity levels: 1 informational, 2 debugging.
const mongoDebugVerbosity = 2

func (s slogSink) Info(verbosity int, msg string, keysAndValues ...any) {
	level := slog.LevelInfo
	if verbosity >= mongoDebugVerbosity {
		level = slog.LevelDebug
	}
	s.log(level, msg, keysAndValues)
}

func (s slogSink) Error(err error, msg string, keysAndValues ...any) {
	s.log(slog.LevelError, msg, append(keysAndValues, "error", err))
}

func (s slogSink) log(level slog.Level, msg string, kv []any) {
	ctx := context.Background()
	if !s.l.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip Callers, log and Info or Error
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.Add(kv...)
	_ = s.l.Handler().Handle(ctx, r)
}
