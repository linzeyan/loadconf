// Package logger builds loggers from [config.Log]: *slog.Logger here, zap
// in github.com/linzeyan/loadconf/logger/logger_zap and zerolog in
// github.com/linzeyan/loadconf/logger/logger_zerolog. All three share the outputs (stdout,
// stderr, rotated files, syslog, GELF, Elasticsearch and, with
// github.com/linzeyan/loadconf/logger/sink_otlp, OTLP), the record keys (time, level, msg,
// source, stack), static fields and runtime level changes.
//
//	log, err := logger.New(ctx, cfg.Log)
//	if err != nil { ... }
//	defer log.Close()
//	log.SetDefault()
//
//	loader.OnChange(func(_, next *AppConf) { _ = log.Update(next.Log) })
//
// Structured outputs (everything but stdout, stderr and file) always receive
// JSON. Other output types, such as otlp, are passed with [WithSink].
package logger

import (
	"context"
	"log/slog"
	"slices"

	"github.com/linzeyan/loadconf/config"
)

// Option customizes [New].
type Option func(*options)

type options struct {
	onError  ErrorHandler
	sinks    map[string]SinkOpener
	ctxAttrs []func(context.Context) []slog.Attr
	replace  func(groups []string, a slog.Attr) slog.Attr
}

// WithErrorHandler receives write and delivery errors of outputs (default:
// stderr, at most once per output every 10 seconds).
func WithErrorHandler(h ErrorHandler) Option { return func(o *options) { o.onError = h } }

// WithSink makes an output type beyond the built-in ones available to this
// logger, e.g. WithSink("otlp", sink_otlp.Open). It is only opened when the
// config has an output of that type.
func WithSink(typ string, open SinkOpener) Option {
	return func(o *options) {
		if o.sinks == nil {
			o.sinks = map[string]SinkOpener{}
		}
		o.sinks[typ] = open
	}
}

// WithContextAttrs adds attributes taken from the context of each record,
// e.g. trace and span IDs (see sink_otlp.TraceAttrs) or a request ID.
func WithContextAttrs(fn func(context.Context) []slog.Attr) Option {
	return func(o *options) { o.ctxAttrs = append(o.ctxAttrs, fn) }
}

// WithReplaceAttr rewrites attributes after the built-in time and level
// formatting, like slog.HandlerOptions.ReplaceAttr.
func WithReplaceAttr(fn func(groups []string, a slog.Attr) slog.Attr) Option {
	return func(o *options) { o.replace = fn }
}

// Logger is a *slog.Logger with its outputs.
type Logger struct {
	*slog.Logger
	outputs *Outputs
}

// New builds a slog logger from cfg. Call Close before exit to flush
// asynchronous outputs.
func New(ctx context.Context, cfg config.Log, opts ...Option) (*Logger, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	outs, err := OpenOutputs(ctx, cfg, o.onError, o.sinks)
	if err != nil {
		return nil, err
	}
	return &Logger{Logger: slog.New(NewHandler(outs, opts...)), outputs: outs}, nil
}

// NewHandler returns a slog.Handler writing to outs, with the static fields
// of outs. It honors WithContextAttrs and WithReplaceAttr.
func NewHandler(outs *Outputs, opts ...Option) slog.Handler {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	tf := outs.TimeFormat()
	replace := func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 {
			switch a.Key {
			case slog.TimeKey:
				if a.Value.Kind() == slog.KindTime {
					if tf.Epoch() {
						a.Value = slog.Int64Value(tf.Int(a.Value.Time()))
					} else {
						a.Value = slog.StringValue(tf.String(a.Value.Time()))
					}
				}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(LevelName(l))
				}
			}
		}
		if o.replace != nil {
			a = o.replace(groups, a)
		}
		return a
	}

	handlers := make([]slog.Handler, 0, len(outs.List()))
	for _, out := range outs.List() {
		hopts := &slog.HandlerOptions{AddSource: outs.AddSource(), Level: out.Leveler(), ReplaceAttr: replace}
		if out.Text {
			handlers = append(handlers, slog.NewTextHandler(out.Writer, hopts))
		} else {
			handlers = append(handlers, slog.NewJSONHandler(out.Writer, hopts))
		}
	}
	var h slog.Handler = &rootHandler{next: slog.NewMultiHandler(handlers...), outs: outs, ctxAttrs: o.ctxAttrs}
	if fields := outs.Fields(); len(fields) > 0 {
		attrs := make([]slog.Attr, len(fields))
		for i, f := range fields {
			attrs[i] = slog.String(f.Key, f.Value)
		}
		h = h.WithAttrs(attrs)
	}
	return h
}

// Outputs returns the opened outputs.
func (l *Logger) Outputs() *Outputs { return l.outputs }

// Update applies the level settings of cfg; see [Outputs.Update].
func (l *Logger) Update(cfg config.Log) error { return l.outputs.Update(cfg) }

// SetDefault makes l the default logger of log/slog and the log package.
func (l *Logger) SetDefault() { slog.SetDefault(l.Logger) }

// Close flushes and closes the outputs.
func (l *Logger) Close() error { return l.outputs.Close() }

// rootHandler adds context attributes and stack traces before fanning out.
type rootHandler struct {
	next     slog.Handler
	outs     *Outputs
	ctxAttrs []func(context.Context) []slog.Attr
	// groups are applied to each record here rather than to next, which
	// would put the context attrs and the stack into the innermost group,
	// where neither ParseRecord nor sink_otlp look for them.
	groups []group
}

// group is a WithGroup call with the attrs added after it.
type group struct {
	name  string
	attrs []slog.Attr
}

func (h *rootHandler) Enabled(_ context.Context, l slog.Level) bool { return h.outs.Enabled(l) }

func (h *rootHandler) Handle(ctx context.Context, r slog.Record) error {
	stack := h.outs.StackEnabled(r.Level)
	if stack || len(h.ctxAttrs) > 0 || len(h.groups) > 0 {
		if len(h.groups) == 0 {
			r = r.Clone()
		} else {
			attrs := make([]slog.Attr, 0, r.NumAttrs())
			r.Attrs(func(a slog.Attr) bool {
				attrs = append(attrs, a)
				return true
			})
			for _, g := range slices.Backward(h.groups) {
				attrs = []slog.Attr{slog.GroupAttrs(g.name, append(slices.Clip(g.attrs), attrs...)...)}
			}
			r = slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
			r.AddAttrs(attrs...)
		}
		for _, fn := range h.ctxAttrs {
			r.AddAttrs(fn(ctx)...)
		}
		if stack {
			r.AddAttrs(slog.String(StackKey, Stack(r.PC)))
		}
	}
	return h.next.Handle(ctx, r)
}

func (h *rootHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	c := *h
	if len(h.groups) == 0 {
		c.next = h.next.WithAttrs(attrs)
		return &c
	}
	c.groups = slices.Clone(h.groups)
	last := &c.groups[len(c.groups)-1]
	last.attrs = append(slices.Clip(last.attrs), attrs...)
	return &c
}

func (h *rootHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.groups = append(slices.Clip(h.groups), group{name: name})
	return &c
}
