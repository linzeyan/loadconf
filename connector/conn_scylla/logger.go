package conn_scylla

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// SlogLogger adapts l to gocql's unleveled logger: every message is logged
// at level, with the trailing newline gocql often adds removed. Outside
// gocql_debug builds the driver only logs problems such as failed dials, so
// [slog.LevelWarn] is a sensible level. A nil l uses [slog.Default].
func SlogLogger(l *slog.Logger, level slog.Level) gocql.StdLogger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l, level: level}
}

type slogLogger struct {
	l     *slog.Logger
	level slog.Level
}

func (s slogLogger) Print(v ...any) { s.log(func() string { return fmt.Sprint(v...) }) }

func (s slogLogger) Printf(format string, v ...any) {
	s.log(func() string { return fmt.Sprintf(format, v...) })
}

func (s slogLogger) Println(v ...any) { s.log(func() string { return fmt.Sprintln(v...) }) }

func (s slogLogger) log(msg func() string) {
	ctx := context.Background()
	if !s.l.Enabled(ctx, s.level) {
		return
	}
	// Skip runtime.Callers, log and the Print method so that the source
	// points at the driver code that logged.
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])
	r := slog.NewRecord(time.Now(), s.level, strings.TrimRight(msg(), "\n"), pcs[0])
	_ = s.l.Handler().Handle(ctx, r)
}
