package config

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-playground/validator/v10"
)

// ErrNotWatchable is returned by [Loader.Watch] when no source can be watched.
var ErrNotWatchable = errors.New("config: no watchable source")

// Option configures a [Loader].
type Option func(*options)

type options struct {
	sources      []Source
	tagName      string
	strict       bool
	validator    *validator.Validate
	validatorSet bool
	logger       *slog.Logger
	debounce     time.Duration
}

// From appends sources. Later sources override earlier ones; `env` tags
// override all sources.
func From(sources ...Source) Option {
	return func(o *options) { o.sources = append(o.sources, sources...) }
}

// WithStrict fails loading when a source contains keys that match no field;
// without it such keys are logged at Warn and ignored. Env and DotEnv
// sources are exempt since they hold unrelated variables.
func WithStrict() Option { return func(o *options) { o.strict = true } }

// WithTagName sets the struct tag holding key names (default "config"), e.g.
// "yaml" to reuse existing yaml tags.
func WithTagName(name string) Option { return func(o *options) { o.tagName = name } }

// WithValidator sets the validator used for `validate` tags, e.g. one with
// custom rules registered. nil disables tag validation; Validator hooks still
// run. For string rules on [Secret] fields, register a custom type func that
// returns [Secret.Value], as the default validator does.
func WithValidator(v *validator.Validate) Option {
	return func(o *options) { o.validator, o.validatorSet = v, true }
}

// WithLogger sets the logger for load and reload events (default: whatever
// slog.Default() is when an event is logged). nil disables logging.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l == nil {
			l = slog.New(slog.DiscardHandler)
		}
		o.logger = l
	}
}

// WithDebounce sets how long Watch waits for changes to settle before
// reloading (default 500ms).
func WithDebounce(d time.Duration) Option { return func(o *options) { o.debounce = d } }

// log resolves the default logger per call rather than in New: the loader
// is created before the logger it configures, and reload events must reach
// that logger with their level intact.
func (o *options) log() *slog.Logger {
	if o.logger != nil {
		return o.logger
	}
	return slog.Default()
}

// Loader loads configuration of type T, which must be a struct, and keeps the
// current value for hot reload.
type Loader[T any] struct {
	opts    options
	current atomic.Pointer[T]

	mu       sync.Mutex
	onChange []func(old, new *T)
	onError  []func(error)
}

// New creates a Loader. Nothing is loaded until [Loader.Load] or
// [Loader.Watch] is called.
func New[T any](opts ...Option) *Loader[T] {
	o := options{tagName: DefaultTagName, debounce: 500 * time.Millisecond}
	for _, opt := range opts {
		opt(&o)
	}
	if !o.validatorSet {
		o.validator = newValidator(o.tagName)
	}
	return &Loader[T]{opts: o}
}

// Load creates a Loader and loads once.
func Load[T any](ctx context.Context, opts ...Option) (*T, error) {
	return New[T](opts...).Load(ctx)
}

// MustLoad is like Load but panics on error.
func MustLoad[T any](ctx context.Context, opts ...Option) *T {
	cfg, err := Load[T](ctx, opts...)
	if err != nil {
		panic(err)
	}
	return cfg
}

// Load loads, validates and stores the configuration as current.
func (l *Loader[T]) Load(ctx context.Context) (*T, error) {
	cfg, err := l.load(ctx)
	if err != nil {
		return nil, err
	}
	l.current.Store(cfg)
	return cfg, nil
}

// Current returns the last successfully loaded configuration, or nil before
// the first load. Treat it as read-only: it is shared between goroutines.
func (l *Loader[T]) Current() *T { return l.current.Load() }

// OnChange registers fn to be called after a reload produced a different
// configuration. Callbacks run sequentially on the Watch goroutine.
func (l *Loader[T]) OnChange(fn func(old, new *T)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onChange = append(l.onChange, fn)
}

// OnError registers fn to be called when a reload fails. The current
// configuration is kept.
func (l *Loader[T]) OnError(fn func(error)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onError = append(l.onError, fn)
}

// Watch loads the configuration if needed, then watches all watchable
// sources and reloads on change until ctx is done. It returns nil when ctx is
// done, [ErrNotWatchable] if no source supports watching, or the error of a
// source whose watch broke down. Run it in its own goroutine.
func (l *Loader[T]) Watch(ctx context.Context) error {
	if l.Current() == nil {
		if _, err := l.Load(ctx); err != nil {
			return err
		}
	}
	sources, err := l.resolveSources()
	if err != nil {
		return err
	}
	var watchers []WatchableSource
	for _, s := range sources {
		if w, ok := s.(WatchableSource); ok {
			watchers = append(watchers, w)
		}
	}
	if len(watchers) == 0 {
		return ErrNotWatchable
	}

	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()

	trigger := make(chan struct{}, 1)
	notify := func() {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
	errc := make(chan error, len(watchers))
	for _, w := range watchers {
		wg.Go(func() {
			if err := w.Watch(ctx, notify); err != nil && ctx.Err() == nil {
				errc <- fmt.Errorf("config: watch %s: %w", w, err)
			}
		})
	}

	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errc:
			return err
		case <-trigger:
			timer.Reset(l.opts.debounce)
		case <-timer.C:
			l.reload(ctx)
		}
	}
}

func (l *Loader[T]) reload(ctx context.Context) {
	next, err := l.load(ctx)
	l.mu.Lock()
	onChange, onError := l.onChange, l.onError
	l.mu.Unlock()

	if err != nil {
		l.opts.log().Error("config reload failed, keeping the current config", "error", err)
		for _, fn := range onError {
			fn(err)
		}
		return
	}
	old := l.current.Load()
	if reflect.DeepEqual(old, next) {
		return
	}
	l.current.Store(next)
	l.opts.log().Info("config reloaded")
	for _, fn := range onChange {
		fn(old, next)
	}
}

func (l *Loader[T]) resolveSources() ([]Source, error) {
	var out []Source
	for _, s := range l.opts.sources {
		e, ok := s.(expander)
		if !ok {
			out = append(out, s)
			continue
		}
		parts, err := e.expand()
		if err != nil {
			return nil, fmt.Errorf("config: %s: %w", s, err)
		}
		out = append(out, parts...)
	}
	return out, nil
}

func (l *Loader[T]) load(ctx context.Context) (*T, error) {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("config: target type %s is not a struct", t)
	}
	sources, err := l.resolveSources()
	if err != nil {
		return nil, err
	}

	// Shape problems are collected rather than returned right away so that one
	// load reports every mistake.
	var problems []error
	merged := map[string]any{}
	loaded := make([]string, 0, len(sources))
	for _, src := range sources {
		raw, err := src.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("config: load %s: %w", src, err)
		}
		norm, unknown, err := normalize(raw, t, l.opts.tagName, isLenient(src))
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %w", src, err))
		}
		if len(unknown) > 0 {
			if l.opts.strict {
				problems = append(problems, fmt.Errorf("%s: unknown keys: %s", src, strings.Join(unknown, ", ")))
			} else {
				l.opts.log().Warn("config keys match no field and are ignored", "source", src.String(), "keys", unknown)
			}
		}
		merge(merged, norm)
		loaded = append(loaded, src.String())
	}
	merge(merged, envTagValues(t, l.opts.tagName))

	cfg := new(T)
	v := reflect.ValueOf(cfg).Elem()
	d := &decoder{tagName: l.opts.tagName}
	d.applyDefaults("", v)
	d.decode("", merged, v)
	if err := errors.Join(append(problems, d.errs...)...); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	if err := validateValue(l.opts.validator, v, l.opts.tagName); err != nil {
		return nil, fmt.Errorf("config: invalid: %w", err)
	}

	l.opts.log().Info("config loaded", "sources", loaded)
	return cfg, nil
}
