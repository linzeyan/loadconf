package logger

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// Output is an opened destination of a [config.Log] with its effective
// level: the higher of the logger level and the output level.
type Output struct {
	Name string
	Type string
	// Text reports that records should be encoded as text; otherwise they
	// are JSON.
	Text bool
	// Pretty reports that JSON records are indented by Writer.
	Pretty bool
	// Writer receives one encoded record per Write. Write errors are
	// reported to the error handler and not returned.
	Writer io.Writer

	sink   Sink
	own    slog.Level
	hasOwn bool
	level  slog.LevelVar
}

// Level returns the current minimum level.
func (o *Output) Level() slog.Level { return o.level.Level() }

// Leveler returns the minimum level as a slog.Leveler that follows updates.
func (o *Output) Leveler() slog.Leveler { return &o.level }

// Enabled reports whether a record at level l goes to this output.
func (o *Output) Enabled(l slog.Level) bool { return l >= o.level.Level() }

// Field is a static field added to every record.
type Field struct {
	Key   string
	Value string
}

// Outputs are the opened outputs of a [config.Log] and the settings shared
// by the slog, zap and zerolog loggers built from it. Levels can change at
// runtime with [Outputs.Update].
type Outputs struct {
	list       []*Output
	min        atomic.Int64
	stack      slog.LevelVar
	timeFormat TimeFormat
	fields     []Field
	env        SinkEnv
	addSource  bool
}

// ErrorHandler receives delivery errors of an output. It must not block.
type ErrorHandler func(output string, err error)

// OpenOutputs opens the outputs of cfg; stdout when cfg has none. Built-in
// types (stdout, stderr, file, syslog, gelf, elasticsearch) open directly;
// any other type needs its opener in sinks, keyed by type (case-insensitive).
// onError receives write and delivery errors; nil reports them to stderr at
// most once per output every 10 seconds.
func OpenOutputs(ctx context.Context, cfg config.Log, onError ErrorHandler, sinks map[string]SinkOpener) (*Outputs, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("logger: %w", err)
	}
	custom := make(map[string]SinkOpener, len(sinks))
	for typ, open := range sinks {
		typ = strings.ToLower(typ)
		if _, ok := builtinSink(typ); ok {
			return nil, fmt.Errorf("logger: sink type %q is built in and cannot be replaced", typ)
		}
		custom[typ] = open
	}
	if onError == nil {
		onError = stderrReporter()
	}
	host, _ := os.Hostname()
	o := &Outputs{
		timeFormat: NewTimeFormat(cfg),
		env:        SinkEnv{Service: cmp.Or(cfg.Service, filepath.Base(os.Args[0])), Hostname: host},
		addSource:  cfg.AddSource,
	}
	if cfg.Service != "" {
		o.fields = append(o.fields, Field{"service", cfg.Service})
	}
	if cfg.Hostname {
		o.fields = append(o.fields, Field{"host", host})
	}
	for _, k := range slices.Sorted(maps.Keys(cfg.Fields)) {
		o.fields = append(o.fields, Field{k, cfg.Fields[k]})
	}

	outs := cfg.Outputs
	if outs.Len() == 0 {
		outs = config.Named[config.LogOutput]{}
		outs.Set(config.LogStdout, config.LogOutput{Name: config.LogStdout})
	}
	for name, out := range outs.All() {
		if out.Disabled {
			continue
		}
		// The config loader validates outputs, but a config built in code
		// does not pass it, and some invalid outputs only fail later: an
		// Elasticsearch sink without addresses crashes its goroutine.
		if err := out.Validate(); err != nil {
			_ = o.Close()
			return nil, fmt.Errorf("logger: output %s: %w", name, err)
		}
		typ := out.ResolvedType()
		open, ok := builtinSink(typ)
		if !ok {
			open, ok = custom[typ]
		}
		if !ok {
			_ = o.Close()
			hint := ""
			if typ == config.LogOTLP {
				hint = `; pass WithSink("otlp", sink_otlp.Open) from github.com/linzeyan/loadconf/logger/sink_otlp`
			}
			return nil, fmt.Errorf("logger: output %s: unknown type %q (built-in types are stdout, stderr, file, syslog, gelf and elasticsearch; add others with WithSink)%s", name, typ, hint)
		}
		env := o.env
		env.OnError = func(err error) { onError(name, err) }
		sink, err := open(ctx, out, env)
		if err != nil {
			_ = o.Close()
			return nil, fmt.Errorf("logger: open output %s (%s): %w", name, typ, err)
		}
		text := textCapable(typ) && strings.EqualFold(cmp.Or(out.Format, cfg.Format), "text")
		pretty := !text && cfg.PrettyJSON && textCapable(typ)
		var w io.Writer = &reportingWriter{w: sink, report: env.OnError}
		if pretty {
			w = PrettyJSON(w)
		}
		output := &Output{Name: name, Type: typ, Text: text, Pretty: pretty, Writer: w, sink: sink}
		if out.Level != "" {
			output.own, _ = ParseLevel(out.Level)
			output.hasOwn = true
		}
		o.list = append(o.list, output)
	}
	if err := o.Update(cfg); err != nil {
		_ = o.Close()
		return nil, err
	}
	return o, nil
}

// List returns the outputs, sorted by name.
func (o *Outputs) List() []*Output { return o.list }

// Enabled reports whether any output takes records at level l.
func (o *Outputs) Enabled(l slog.Level) bool { return int64(l) >= o.min.Load() }

// StackEnabled reports whether records at level l carry a stack trace.
func (o *Outputs) StackEnabled(l slog.Level) bool { return l >= o.stack.Level() }

// AddSource reports whether records carry the call site.
func (o *Outputs) AddSource() bool { return o.addSource }

// TimeFormat returns the configured time format.
func (o *Outputs) TimeFormat() TimeFormat { return o.timeFormat }

// Fields returns the static fields: service, host and config.Log.Fields.
func (o *Outputs) Fields() []Field { return o.fields }

// Env returns the service and host names.
func (o *Outputs) Env() SinkEnv { return o.env }

// Update applies the levels of cfg: the logger level, the output levels and
// the stack level. Other changes, such as new outputs or formats, need a new
// logger.
func (o *Outputs) Update(cfg config.Log) error {
	base, err := ParseLevel(cfg.Level)
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	stack, err := parseStackLevel(cfg.StackLevel)
	if err != nil {
		return fmt.Errorf("logger: stack_level: %w", err)
	}
	own := map[string]*slog.Level{}
	for name, out := range cfg.Outputs.All() {
		if out.Level != "" {
			l, err := ParseLevel(out.Level)
			if err != nil {
				return fmt.Errorf("logger: output %s: %w", name, err)
			}
			own[name] = &l
		} else {
			own[name] = nil
		}
	}

	minLevel := int64(levelOff)
	for _, out := range o.list {
		if l, ok := own[out.Name]; ok {
			out.hasOwn = l != nil
			if l != nil {
				out.own = *l
			}
		}
		eff := base
		if out.hasOwn {
			eff = max(eff, out.own)
		}
		out.level.Set(eff)
		minLevel = min(minLevel, int64(eff))
	}
	o.min.Store(minLevel)
	o.stack.Set(stack)
	return nil
}

// Sync flushes outputs that support it.
func (o *Outputs) Sync() error {
	var errs []error
	for _, out := range o.list {
		if s, ok := out.sink.(Syncer); ok {
			if err := s.Sync(); err != nil {
				errs = append(errs, fmt.Errorf("output %s: %w", out.Name, err))
			}
		}
	}
	return errors.Join(errs...)
}

// Close flushes and closes all outputs.
func (o *Outputs) Close() error {
	var errs []error
	for _, out := range o.list {
		if err := out.sink.Close(); err != nil {
			errs = append(errs, fmt.Errorf("output %s: %w", out.Name, err))
		}
	}
	return errors.Join(errs...)
}

type reportingWriter struct {
	w      io.Writer
	report func(error)
}

func (r *reportingWriter) Write(b []byte) (int, error) {
	if _, err := r.w.Write(b); err != nil {
		r.report(err)
	}
	return len(b), nil
}

func stderrReporter() ErrorHandler {
	var mu sync.Mutex
	last := map[string]time.Time{}
	return func(output string, err error) {
		mu.Lock()
		now := time.Now()
		if now.Sub(last[output]) < 10*time.Second {
			mu.Unlock()
			return
		}
		last[output] = now
		mu.Unlock()
		fmt.Fprintf(os.Stderr, "logger: output %s: %v\n", output, err)
	}
}
