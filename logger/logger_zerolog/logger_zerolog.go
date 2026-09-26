// Package logger_zerolog builds a zerolog.Logger from [config.Log] on the
// outputs of github.com/linzeyan/loadconf/logger, so it shares their destinations, time
// format, static fields, source and stack settings and runtime level changes
// with the slog logger.
//
//	log, err := logger_zerolog.New(ctx, cfg.Log)
//	if err != nil { ... }
//	defer log.Close()
//	log.Info().Int("port", 8080).Msg("started")
//
// zerolog's global settings are left alone. Its message key ("message") and
// lower-case level values therefore stay as they are, since zerolog only
// configures them process-wide (zerolog.MessageFieldName, ...); the
// network outputs accept both. The time, "source" ("file:line") and "stack"
// fields are added by a hook. As in zerolog itself, "time" is reserved: the
// hook writes it after the record's fields, so the network outputs take a
// field of that name for the record time. Levels map to slog's by name, with
// panic and fatal as logger.LevelFatal. Fatal closes the outputs, flushing
// asynchronous ones, before exiting.
package logger_zerolog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// Option customizes New.
type Option func(*options)

type options struct {
	onError logger.ErrorHandler
	sinks   map[string]logger.SinkOpener
}

// WithErrorHandler receives write and delivery errors of the outputs.
func WithErrorHandler(h logger.ErrorHandler) Option { return func(o *options) { o.onError = h } }

// WithSink makes an output type beyond the built-in ones available, like
// logger.WithSink.
func WithSink(typ string, open logger.SinkOpener) Option {
	return func(o *options) {
		if o.sinks == nil {
			o.sinks = map[string]logger.SinkOpener{}
		}
		o.sinks[typ] = open
	}
}

// Logger is a zerolog.Logger with its outputs.
type Logger struct {
	zerolog.Logger
	outputs *logger.Outputs
}

// New builds a zerolog logger from cfg. Call Close before exit to flush
// asynchronous outputs.
func New(ctx context.Context, cfg config.Log, opts ...Option) (*Logger, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	outs, err := logger.OpenOutputs(ctx, cfg, o.onError, o.sinks)
	if err != nil {
		return nil, err
	}
	zc := zerolog.New(NewWriter(outs)).
		Sample(levelSampler{outs}).
		Hook(NewHook(outs)).
		With()
	for _, f := range outs.Fields() {
		zc = zc.Str(f.Key, f.Value)
	}
	return &Logger{Logger: zc.Logger(), outputs: outs}, nil
}

// NewWriter returns a zerolog.LevelWriter writing each record to the
// outputs of outs that accept its level, as text for text outputs. Its
// Close closes outs.
func NewWriter(outs *logger.Outputs) zerolog.LevelWriter {
	w := &writer{outs: outs}
	for _, out := range outs.List() {
		var dst io.Writer = out.Writer
		if out.Text {
			dst = zerolog.ConsoleWriter{
				Out:     out.Writer,
				NoColor: true,
				// The hook already formatted the time.
				FormatTimestamp: func(v any) string { return fmt.Sprint(v) },
			}
		}
		w.list = append(w.list, output{out, dst})
	}
	return w
}

type output struct {
	out *logger.Output
	w   io.Writer
}

type writer struct {
	outs *logger.Outputs
	list []output
}

func (w *writer) Write(p []byte) (int, error) { return w.WriteLevel(zerolog.NoLevel, p) }

func (w *writer) WriteLevel(l zerolog.Level, p []byte) (int, error) {
	n := len(p)
	// RawJSON writes its argument verbatim, so a record may span lines,
	// which splits it in files, Elasticsearch bulk bodies and syslog over
	// TCP. Valid JSON has no raw newline inside a string: compacting removes
	// them all.
	if i := bytes.IndexByte(p, '\n'); i >= 0 && i < n-1 {
		var buf bytes.Buffer
		if json.Compact(&buf, p) == nil {
			p = append(buf.Bytes(), '\n')
		}
	}
	sl := SlogLevel(l)
	for _, o := range w.list {
		if o.out.Enabled(sl) {
			_, _ = o.w.Write(p) // the outputs report their errors
		}
	}
	return n, nil
}

// Close closes the outputs; zerolog calls it before exiting on Fatal.
func (w *writer) Close() error { return w.outs.Close() }

// levelSampler drops records no output accepts before they are built.
type levelSampler struct{ outs *logger.Outputs }

func (s levelSampler) Sample(l zerolog.Level) bool { return s.outs.Enabled(SlogLevel(l)) }

// NewHook returns the hook adding the time and, as configured, the source
// and stack of each record.
func NewHook(outs *logger.Outputs) zerolog.Hook {
	tf := outs.TimeFormat()
	return zerolog.HookFunc(func(e *zerolog.Event, l zerolog.Level, _ string) {
		now := time.Now()
		if tf.Epoch() {
			e.Int64(logger.TimeKey, tf.Int(now))
		} else {
			e.Str(logger.TimeKey, tf.String(now))
		}
		addSource, addStack := outs.AddSource(), outs.StackEnabled(SlogLevel(l))
		if !addSource && !addStack {
			return
		}
		pc := callerPC()
		if addSource && pc != 0 {
			f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
			e.Str(logger.SourceKey, f.File+":"+strconv.Itoa(f.Line))
		}
		if addStack {
			e.Str(logger.StackKey, logger.Stack(pc))
		}
	})
}

// callerPC returns the first frame outside zerolog and this package.
func callerPC() uintptr {
	var pcs [32]uintptr
	n := runtime.Callers(3, pcs[:])
	for _, pc := range pcs[:n] {
		f, _ := runtime.CallersFrames([]uintptr{pc}).Next()
		if strings.HasSuffix(f.File, "_test.go") ||
			!strings.HasPrefix(f.Function, "github.com/rs/zerolog") && !strings.HasPrefix(f.Function, "github.com/linzeyan/loadconf/logger/logger_zerolog.") {
			return pc
		}
	}
	return 0
}

// SlogLevel maps a zerolog level to the slog level the outputs compare
// with.
func SlogLevel(l zerolog.Level) slog.Level {
	switch l {
	case zerolog.TraceLevel:
		return logger.LevelTrace
	case zerolog.DebugLevel:
		return slog.LevelDebug
	case zerolog.WarnLevel:
		return slog.LevelWarn
	case zerolog.ErrorLevel:
		return slog.LevelError
	case zerolog.FatalLevel, zerolog.PanicLevel:
		return logger.LevelFatal
	default: // info and records without a level
		return slog.LevelInfo
	}
}

// Outputs returns the outputs.
func (l *Logger) Outputs() *logger.Outputs { return l.outputs }

// Update applies the levels of cfg; other settings need a new Logger.
func (l *Logger) Update(cfg config.Log) error { return l.outputs.Update(cfg) }

// Sync flushes the outputs.
func (l *Logger) Sync() error { return l.outputs.Sync() }

// Close flushes and closes the outputs. The Logger must not be used after.
func (l *Logger) Close() error { return l.outputs.Close() }
