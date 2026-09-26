package logger

import (
	"fmt"
	"log/slog"
	"math"
	"strings"
)

// Levels beyond the four of log/slog. zap maps trace to debug.
const (
	LevelTrace = slog.Level(-8)
	LevelFatal = slog.Level(12)
)

// levelOff is above every level, for "none".
const levelOff = slog.Level(math.MaxInt32)

// ParseLevel parses trace, debug, info, warn (or warning), error or fatal,
// ignoring case.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "trace":
		return LevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "fatal":
		return LevelFatal, nil
	}
	return 0, fmt.Errorf("unknown log level %q", s)
}

// LevelName returns the upper-case name of l: TRACE, DEBUG, INFO, WARN,
// ERROR or FATAL, with an offset for levels in between, e.g. INFO+2.
func LevelName(l slog.Level) string {
	switch {
	case l < slog.LevelDebug:
		return offsetName("TRACE", l-LevelTrace)
	case l >= LevelFatal:
		return offsetName("FATAL", l-LevelFatal)
	}
	return l.String()
}

func offsetName(name string, off slog.Level) string {
	if off == 0 {
		return name
	}
	return fmt.Sprintf("%s%+d", name, int(off))
}

func parseStackLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "none":
		return levelOff, nil
	case "":
		return slog.LevelError, nil
	}
	return ParseLevel(s)
}
