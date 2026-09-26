package conn_mssql

import (
	"context"
	"log/slog"
	"strconv"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"
)

// SlogLogger adapts l to the driver's context logger; nil means
// slog.Default(). Install it once per process with [mssql.SetContextLogger].
// The driver only emits the categories enabled by the "log" connection
// param, a bit mask of [msdsn.Log] values (1 errors, 2 messages, 4 rows,
// 8 SQL, 16 params, 32 transactions, 64 debug, 128 retries).
//
// Errors are logged at Error, retries at Warn, server messages at Info and
// everything else at Debug, each with a "category" attribute.
func SlogLogger(l *slog.Logger) mssql.ContextLogger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l}
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) Log(ctx context.Context, category msdsn.Log, msg string) {
	if ctx == nil {
		ctx = context.Background()
	}
	level := slog.LevelDebug
	switch category {
	case msdsn.LogErrors:
		level = slog.LevelError
	case msdsn.LogRetries:
		level = slog.LevelWarn
	case msdsn.LogMessages:
		level = slog.LevelInfo
	}
	if !s.l.Enabled(ctx, level) {
		return
	}
	s.l.LogAttrs(ctx, level, msg, slog.String("category", categoryName(category)))
}

func categoryName(c msdsn.Log) string {
	switch c {
	case msdsn.LogErrors:
		return "errors"
	case msdsn.LogMessages:
		return "messages"
	case msdsn.LogRows:
		return "rows"
	case msdsn.LogSQL:
		return "sql"
	case msdsn.LogParams:
		return "params"
	case msdsn.LogTransaction:
		return "transaction"
	case msdsn.LogDebug:
		return "debug"
	case msdsn.LogRetries:
		return "retries"
	}
	return strconv.FormatUint(uint64(c), 10)
}
