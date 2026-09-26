package logger

import (
	"bytes"
	"encoding/json"
	"io"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// Record keys shared by all backends.
const (
	TimeKey    = "time"
	LevelKey   = "level"
	MessageKey = "msg"
	SourceKey  = "source"
	StackKey   = "stack"
)

// TimeFormat formats record times as configured by config.Log.TimeFormat
// and config.Log.UTC.
type TimeFormat struct {
	layout string
	unit   time.Duration // for epoch formats; 0 for layouts
	utc    bool
}

// RFC3339Milli is RFC 3339 with milliseconds, the default time format.
const RFC3339Milli = "2006-01-02T15:04:05.000Z07:00"

// NewTimeFormat returns the time format of cfg.
func NewTimeFormat(cfg config.Log) TimeFormat {
	f := TimeFormat{utc: cfg.UTC}
	switch strings.ToLower(cfg.TimeFormat) {
	case "", "rfc3339milli":
		f.layout = RFC3339Milli
	case "rfc3339":
		f.layout = time.RFC3339
	case "rfc3339nano":
		f.layout = time.RFC3339Nano
	case "unix":
		f.unit = time.Second
	case "unixmilli":
		f.unit = time.Millisecond
	case "unixnano":
		f.unit = time.Nanosecond
	default:
		f.layout = cfg.TimeFormat
	}
	return f
}

// Epoch reports whether times are formatted as integers.
func (f TimeFormat) Epoch() bool { return f.unit != 0 }

// String formats t as text; epoch formats yield decimal integers.
func (f TimeFormat) String(t time.Time) string {
	if f.unit != 0 {
		return strconv.FormatInt(f.Int(t), 10)
	}
	if f.utc {
		t = t.UTC()
	}
	return t.Format(f.layout)
}

// Int returns t as an integer in the epoch unit, or 0 for layouts.
func (f TimeFormat) Int(t time.Time) int64 {
	switch f.unit {
	case time.Second:
		return t.Unix()
	case time.Millisecond:
		return t.UnixMilli()
	case time.Nanosecond:
		return t.UnixNano()
	}
	return 0
}

// Stack formats the call stack starting at the frame of pc, or of the
// caller when pc is 0. Frames of the runtime are left out.
//
//	main.handle
//		/app/main.go:42
func Stack(pc uintptr) string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var want runtime.Frame
	if pc != 0 {
		want, _ = runtime.CallersFrames([]uintptr{pc}).Next()
	}
	s := formatFrames(frames, func(f runtime.Frame) bool {
		return pc == 0 || f.Function == want.Function && f.Line == want.Line && f.File == want.File
	})
	if s == "" && pc != 0 {
		// pc is not on this goroutine's stack, e.g. in an asynchronous
		// handler: fall back to the current stack.
		s = formatFrames(runtime.CallersFrames(pcs[:n]), func(runtime.Frame) bool { return true })
	}
	return s
}

// StackSkipping formats the call stack of the caller, starting at the first
// frame whose function is outside the given package path prefixes, e.g. the
// logging library that called it.
func StackSkipping(prefixes ...string) string {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	return formatFrames(runtime.CallersFrames(pcs[:n]), func(f runtime.Frame) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(f.Function, p) {
				return false
			}
		}
		return true
	})
}

func formatFrames(frames *runtime.Frames, start func(runtime.Frame) bool) string {
	var b strings.Builder
	started := false
	for {
		f, more := frames.Next()
		if !started && start(f) {
			started = true
		}
		if started && f.Function != "" && !strings.HasPrefix(f.Function, "runtime.") {
			b.WriteString(f.Function)
			b.WriteString("\n\t")
			b.WriteString(f.File)
			b.WriteByte(':')
			b.WriteString(strconv.Itoa(f.Line))
			b.WriteByte('\n')
		}
		if !more {
			break
		}
	}
	return strings.TrimSuffix(b.String(), "\n")
}

// PrettyJSON wraps w so that each JSON record written is indented. Other
// content passes through unchanged.
func PrettyJSON(w io.Writer) io.Writer { return &prettyWriter{w: w} }

type prettyWriter struct{ w io.Writer }

func (p *prettyWriter) Write(b []byte) (int, error) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, bytes.TrimRight(b, "\n"), "", "  "); err != nil {
		return p.w.Write(b)
	}
	buf.WriteByte('\n')
	if _, err := p.w.Write(buf.Bytes()); err != nil {
		return 0, err
	}
	return len(b), nil
}
