package conn_mqtt

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// SetLogger routes paho's process-wide loggers to l: ERROR and CRITICAL at
// Error, WARN at Warn and DEBUG at Debug. DEBUG is very chatty and formats
// every message, so it is only installed when l is enabled at Debug when
// SetLogger is called. A nil l uses slog.Default().
//
// Call it before creating clients, since paho reads the loggers without
// locking. The returned function restores the previous loggers.
func SetLogger(l *slog.Logger) (restore func()) {
	if l == nil {
		l = slog.Default()
	}
	prevErr, prevCrit, prevWarn, prevDebug := mqtt.ERROR, mqtt.CRITICAL, mqtt.WARN, mqtt.DEBUG
	mqtt.ERROR = SlogLogger(l, slog.LevelError)
	mqtt.CRITICAL = SlogLogger(l, slog.LevelError)
	mqtt.WARN = SlogLogger(l, slog.LevelWarn)
	if l.Enabled(context.Background(), slog.LevelDebug) {
		mqtt.DEBUG = SlogLogger(l, slog.LevelDebug)
	}
	return func() {
		mqtt.ERROR, mqtt.CRITICAL, mqtt.WARN, mqtt.DEBUG = prevErr, prevCrit, prevWarn, prevDebug
	}
}

// SlogLogger adapts l to one paho logger, logging every message at level.
func SlogLogger(l *slog.Logger, level slog.Level) mqtt.Logger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l: l, level: level}
}

type slogLogger struct {
	l     *slog.Logger
	level slog.Level
}

// Each method checks the level before formatting, so a filtered call does
// not pay for fmt.
func (s slogLogger) Println(v ...any) {
	if s.enabled() {
		s.log(fmt.Sprintln(v...))
	}
}

func (s slogLogger) Printf(format string, v ...any) {
	if s.enabled() {
		s.log(fmt.Sprintf(format, v...))
	}
}

func (s slogLogger) enabled() bool { return s.l.Enabled(context.Background(), s.level) }

func (s slogLogger) log(msg string) {
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:]) // skip Callers, log and Print*
	r := slog.NewRecord(time.Now(), s.level, strings.TrimSpace(msg), pcs[0])
	_ = s.l.Handler().Handle(context.Background(), r)
}
