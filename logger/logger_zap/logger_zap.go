// Package logger_zap builds a *zap.Logger from [config.Log] on the outputs of
// github.com/linzeyan/loadconf/logger, so it shares their destinations, record keys
// (time, level, msg, source, stack), time format, static fields and runtime
// level changes with the slog logger.
//
//	log, err := logger_zap.New(ctx, cfg.Log)
//	if err != nil { ... }
//	defer log.Close()
//	log.Info("started", zap.Int("port", 8080))
//
// zap's levels map to slog's: debug, info, warn and error to their
// namesakes, dpanic, panic and fatal to logger.LevelFatal; a trace level
// enables debug. The source is "file:line", unlike slog's object. Fatal
// closes the outputs, flushing asynchronous ones, before exiting.
package logger_zap

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// Option customizes New.
type Option func(*options)

type options struct {
	onError logger.ErrorHandler
	sinks   map[string]logger.SinkOpener
	zap     []zap.Option
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

// WithZapOptions adds zap options, e.g. zap.AddCallerSkip or hooks.
func WithZapOptions(opts ...zap.Option) Option {
	return func(o *options) { o.zap = append(o.zap, opts...) }
}

// Logger is a *zap.Logger with its outputs.
type Logger struct {
	*zap.Logger
	outputs *logger.Outputs
}

// New builds a zap logger from cfg. Call Close before exit to flush
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
	zopts := []zap.Option{
		zap.AddStacktrace(zap.LevelEnablerFunc(func(l zapcore.Level) bool { return outs.StackEnabled(SlogLevel(l)) })),
		zap.WithFatalHook(fatalHook{outs}),
	}
	if outs.AddSource() {
		zopts = append(zopts, zap.AddCaller())
	}
	var fields []zap.Field
	for _, f := range outs.Fields() {
		fields = append(fields, zap.String(f.Key, f.Value))
	}
	if len(fields) > 0 {
		zopts = append(zopts, zap.Fields(fields...))
	}
	return &Logger{Logger: zap.New(NewCore(outs), append(zopts, o.zap...)...), outputs: outs}, nil
}

// NewCore returns a core writing to every output of outs at its level. It
// adds neither the static fields nor caller and stack settings, which are
// zap options (see New).
func NewCore(outs *logger.Outputs) zapcore.Core {
	tf := outs.TimeFormat()
	var cores []zapcore.Core
	for _, out := range outs.List() {
		enc := encoderConfig(tf, out.Text)
		var encoder zapcore.Encoder
		if out.Text {
			encoder = zapcore.NewConsoleEncoder(enc)
		} else {
			encoder = zapcore.NewJSONEncoder(enc)
		}
		cores = append(cores, zapcore.NewCore(encoder, zapcore.AddSync(out.Writer),
			zap.LevelEnablerFunc(func(l zapcore.Level) bool { return out.Enabled(SlogLevel(l)) })))
	}
	return zapcore.NewTee(cores...)
}

func encoderConfig(tf logger.TimeFormat, text bool) zapcore.EncoderConfig {
	c := zapcore.EncoderConfig{
		TimeKey:       logger.TimeKey,
		LevelKey:      logger.LevelKey,
		NameKey:       "logger",
		CallerKey:     logger.SourceKey,
		FunctionKey:   zapcore.OmitKey,
		MessageKey:    logger.MessageKey,
		StacktraceKey: logger.StackKey,
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel: func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			if l > zapcore.ErrorLevel {
				enc.AppendString(l.CapitalString())
				return
			}
			enc.AppendString(logger.LevelName(SlogLevel(l)))
		},
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			if tf.Epoch() {
				enc.AppendInt64(tf.Int(t))
				return
			}
			enc.AppendString(tf.String(t))
		},
		// Like slog: nanoseconds in JSON, "1.5s" in text.
		EncodeDuration: zapcore.NanosDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}
	if text {
		c.EncodeDuration = zapcore.StringDurationEncoder
	}
	return c
}

// SlogLevel maps a zap level to the slog level the outputs compare with.
func SlogLevel(l zapcore.Level) slog.Level {
	switch {
	case l <= zapcore.DebugLevel:
		return slog.LevelDebug
	case l == zapcore.InfoLevel:
		return slog.LevelInfo
	case l == zapcore.WarnLevel:
		return slog.LevelWarn
	case l == zapcore.ErrorLevel:
		return slog.LevelError
	default:
		return logger.LevelFatal
	}
}

// Outputs returns the outputs.
func (l *Logger) Outputs() *logger.Outputs { return l.outputs }

// Update applies the levels of cfg; other settings need a new Logger.
func (l *Logger) Update(cfg config.Log) error { return l.outputs.Update(cfg) }

// Sync flushes zap and the outputs.
func (l *Logger) Sync() error { return errors.Join(l.Logger.Sync(), l.outputs.Sync()) }

// Close flushes and closes the outputs. The Logger must not be used after.
func (l *Logger) Close() error {
	_ = l.Logger.Sync()
	return l.outputs.Close()
}

// fatalHook closes the outputs, flushing asynchronous ones, before Fatal
// exits.
type fatalHook struct{ outs *logger.Outputs }

func (h fatalHook) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {
	_ = h.outs.Close()
	os.Exit(1)
}
