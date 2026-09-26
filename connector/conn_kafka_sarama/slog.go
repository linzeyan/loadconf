package conn_kafka_sarama

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

// SlogLogger adapts l to sarama's process-wide loggers, logging every
// message at level:
//
//	sarama.Logger = conn_kafka_sarama.SlogLogger(logger, slog.LevelInfo)
//	sarama.DebugLogger = conn_kafka_sarama.SlogLogger(logger, slog.LevelDebug)
//
// Set them before creating clients; sarama reads them without locking. A
// nil l uses slog.Default().
func SlogLogger(l *slog.Logger, level slog.Level) sarama.StdLogger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l, level: level}
}

type slogLogger struct {
	l     *slog.Logger
	level slog.Level
}

// Each method checks the level before formatting: sarama logs on hot paths
// even when the level is off.
func (s slogLogger) Print(v ...any) {
	if s.enabled() {
		s.log(fmt.Sprint(v...))
	}
}

func (s slogLogger) Printf(format string, v ...any) {
	if s.enabled() {
		s.log(fmt.Sprintf(format, v...))
	}
}

func (s slogLogger) Println(v ...any) {
	if s.enabled() {
		s.log(fmt.Sprintln(v...))
	}
}

func (s slogLogger) enabled() bool { return s.l.Enabled(context.Background(), s.level) }

func (s slogLogger) log(msg string) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip Callers, log and Print*
	r := slog.NewRecord(time.Now(), s.level, strings.TrimSpace(msg), pcs[0])
	_ = s.l.Handler().Handle(context.Background(), r)
}
