package conn_cassandra

import (
	"context"
	"log/slog"
	"runtime"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// SlogLogger adapts l to the gocql structured logger. gocql levels map to
// the slog levels of the same name (Warning to [slog.LevelWarn]) and log
// fields become attributes of their kind. A nil l uses [slog.Default].
func SlogLogger(l *slog.Logger) gocql.StructuredLogger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l}
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Error(msg string, fields ...gocql.LogField) {
	s.log(slog.LevelError, msg, fields)
}

func (s slogLogger) Warning(msg string, fields ...gocql.LogField) {
	s.log(slog.LevelWarn, msg, fields)
}

func (s slogLogger) Info(msg string, fields ...gocql.LogField) {
	s.log(slog.LevelInfo, msg, fields)
}

func (s slogLogger) Debug(msg string, fields ...gocql.LogField) {
	s.log(slog.LevelDebug, msg, fields)
}

func (s slogLogger) log(level slog.Level, msg string, fields []gocql.LogField) {
	ctx := context.Background()
	if !s.l.Enabled(ctx, level) {
		return
	}
	// Skip runtime.Callers, log and the level method so that the source
	// points at the driver code that logged.
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	for _, f := range fields {
		r.AddAttrs(slogAttr(f))
	}
	_ = s.l.Handler().Handle(ctx, r)
}

func slogAttr(f gocql.LogField) slog.Attr {
	switch f.Value.LogFieldValueType() {
	case gocql.LogFieldTypeBool:
		return slog.Bool(f.Name, f.Value.Bool())
	case gocql.LogFieldTypeInt64:
		return slog.Int64(f.Name, f.Value.Int64())
	case gocql.LogFieldTypeString:
		return slog.String(f.Name, f.Value.String())
	default:
		return slog.Any(f.Name, f.Value.Any())
	}
}
