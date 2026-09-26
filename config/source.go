package config

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Source provides one layer of raw configuration.
type Source interface {
	// Load returns the raw tree: map[string]any whose values are
	// map[string]any, []any or scalars. A nil map means "nothing to add".
	Load(ctx context.Context) (map[string]any, error)
	// String describes the source in logs and errors, e.g. "file(app.yaml)".
	String() string
}

// WatchableSource is a Source that can report changes for hot reload.
type WatchableSource interface {
	Source
	// Watch blocks until ctx is done, calling changed whenever the source may
	// have changed. Spurious calls are fine: the loader compares results.
	// It returns nil when ctx is done and an error if watching broke down.
	Watch(ctx context.Context, changed func()) error
}

// expander is implemented by composite sources that resolve into several
// sources at load time. Each part is normalized separately before merging.
type expander interface {
	expand() ([]Source, error)
}

// ---------------------------------------------------------------------------
// File

// FileOption configures [File].
type FileOption func(*fileSource)

// Optional makes a missing file load as empty instead of failing.
func Optional() FileOption { return func(s *fileSource) { s.optional = true } }

// FileFormat sets the format instead of inferring it from the extension.
func FileFormat(format string) FileOption { return func(s *fileSource) { s.format = format } }

// File loads a JSON, YAML or TOML file; the format is inferred from the
// extension unless [FileFormat] is given. File sources are watchable.
func File(path string, opts ...FileOption) Source {
	s := &fileSource{path: path}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type fileSource struct {
	path     string
	format   string
	optional bool
}

func (s *fileSource) String() string { return "file(" + s.path + ")" }

func (s *fileSource) Load(context.Context) (map[string]any, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if s.optional && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	format := s.format
	if format == "" {
		if format, err = FormatOf(s.path); err != nil {
			return nil, err
		}
	}
	return Parse(format, data)
}

func (s *fileSource) Watch(ctx context.Context, changed func()) error {
	return watchFiles(ctx, []string{s.path}, changed)
}

// ---------------------------------------------------------------------------
// Bytes / Map

// Bytes loads an in-memory document in the given format.
func Bytes(format string, data []byte) Source {
	return &bytesSource{format: format, data: data}
}

type bytesSource struct {
	format string
	data   []byte
}

func (s *bytesSource) String() string { return "bytes(" + s.format + ")" }

func (s *bytesSource) Load(context.Context) (map[string]any, error) {
	return Parse(s.format, s.data)
}

// Map loads an in-memory tree, e.g. programmatic defaults or test fixtures.
func Map(m map[string]any) Source { return mapSource(m) }

type mapSource map[string]any

func (s mapSource) String() string { return "map" }

func (s mapSource) Load(context.Context) (map[string]any, error) {
	out, err := sanitize(cloneTree(map[string]any(s)))
	if err != nil {
		return nil, err
	}
	return out.(map[string]any), nil
}

// ---------------------------------------------------------------------------
// Env

// EnvOption configures [Env].
type EnvOption func(*envSource)

// EnvSeparator sets the separator between key levels (default "__").
func EnvSeparator(sep string) EnvOption { return func(s *envSource) { s.sep = sep } }

// Env maps environment variables named PREFIX_KEY__SUBKEY onto the key path
// key.subkey, e.g. with prefix "APP":
//
//	APP_PORT=8080                  -> port
//	APP_MYSQL__ORDERS__DSN=...   -> mysql.orders.dsn
//	APP_REDIS__CACHE__ADDRS=a,b    -> redis.cache.addrs (comma separated list)
//
// Variables that match no field of the target struct are ignored. Fields can
// also name their variable explicitly with an `env` tag, which works without
// this source.
func Env(prefix string, opts ...EnvOption) Source {
	s := &envSource{prefix: strings.TrimSuffix(prefix, "_"), sep: "__", environ: os.Environ}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type envSource struct {
	prefix  string
	sep     string
	environ func() []string
}

func (s *envSource) String() string { return "env(" + s.prefix + "_*)" }

func (s *envSource) lenient() bool { return true }

func (s *envSource) Load(context.Context) (map[string]any, error) {
	if s.prefix == "" {
		return nil, errors.New("env source needs a prefix")
	}
	vars := func(yield func(string, string) bool) {
		for _, kv := range s.environ() {
			name, value, _ := strings.Cut(kv, "=")
			if !yield(name, value) {
				return
			}
		}
	}
	return envTree(s.prefix, s.sep, vars), nil
}

// ---------------------------------------------------------------------------
// Profile

// ProfileOption configures [Profile].
type ProfileOption func(*profileSource)

// ProfileDir sets the directory holding the files (default "./configs").
func ProfileDir(dir string) ProfileOption { return func(s *profileSource) { s.dir = dir } }

// ProfileEnvVar sets the variable selecting the environment (default "ENV").
func ProfileEnvVar(name string) ProfileOption { return func(s *profileSource) { s.envVar = name } }

// ProfileDefaultEnv sets the environment used when the variable is unset
// (default "dev").
func ProfileDefaultEnv(env string) ProfileOption {
	return func(s *profileSource) { s.defaultEnv = env }
}

// ProfilePathEnvVar sets the variable that, when set, points at the single
// file to load instead (default "CONFIG_PATH").
func ProfilePathEnvVar(name string) ProfileOption {
	return func(s *profileSource) { s.pathEnvVar = name }
}

// ProfileEnvs restricts the accepted environments, e.g. "dev", "stage", "prod".
func ProfileEnvs(envs ...string) ProfileOption {
	return func(s *profileSource) { s.allowed = envs }
}

// Profile loads per-environment files for the named process:
//
//	{dir}/{name}.{ext}        optional base shared by all environments
//	{dir}/{name}.{env}.{ext}  required overlay for the current environment
//
// where env comes from $ENV (default "dev") and ext is the first existing of
// .yaml, .yml, .json, .toml. When $CONFIG_PATH is set, only that file is
// loaded. Both files are watched for hot reload.
func Profile(name string, opts ...ProfileOption) Source {
	s := &profileSource{
		name:       name,
		dir:        "./configs",
		envVar:     "ENV",
		defaultEnv: "dev",
		pathEnvVar: "CONFIG_PATH",
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type profileSource struct {
	name       string
	dir        string
	envVar     string
	defaultEnv string
	pathEnvVar string
	allowed    []string
}

func (s *profileSource) String() string { return "profile(" + s.name + ")" }

func (s *profileSource) env() string {
	if env := strings.TrimSpace(os.Getenv(s.envVar)); env != "" {
		return env
	}
	return s.defaultEnv
}

func (s *profileSource) expand() ([]Source, error) {
	if path := strings.TrimSpace(os.Getenv(s.pathEnvVar)); path != "" {
		return []Source{File(path)}, nil
	}

	env := s.env()
	if len(s.allowed) > 0 && !slices.Contains(s.allowed, env) {
		return nil, fmt.Errorf("unsupported %s=%q (want one of %s)", s.envVar, env, strings.Join(s.allowed, ", "))
	}

	base := s.find(s.name)
	if base == "" {
		// Keep watching the conventional path so the base file can be added later.
		base = filepath.Join(s.dir, s.name+".yaml")
	}
	overlay := s.find(s.name + "." + env)
	if overlay == "" {
		return nil, fmt.Errorf("no config file %s.%s{%s} in %s", s.name, env, strings.Join(knownExts(), ","), s.dir)
	}
	return []Source{File(base, Optional()), File(overlay)}, nil
}

func (s *profileSource) find(stem string) string {
	for _, ext := range knownExts() {
		path := filepath.Join(s.dir, stem+ext)
		if st, err := os.Stat(path); err == nil && !st.IsDir() {
			return path
		}
	}
	return ""
}

// Load merges the resolved files. The loader does not call it: it expands the
// profile and normalizes each file on its own, which merges Named lists by
// instance name.
func (s *profileSource) Load(ctx context.Context) (map[string]any, error) {
	parts, err := s.expand()
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, part := range parts {
		m, err := part.Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", part, err)
		}
		merge(out, m)
	}
	return out, nil
}
