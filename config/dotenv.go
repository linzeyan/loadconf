package config

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"os"
	"strings"
)

// lenientSource is implemented by sources whose keys are not all meant for
// the config, such as the environment. Their unknown keys and values of the
// wrong shape are dropped instead of reported.
type lenientSource interface {
	lenient() bool
}

func isLenient(s Source) bool {
	l, ok := s.(lenientSource)
	return ok && l.lenient()
}

// envTree maps variables named PREFIX_KEY__SUBKEY onto key paths. An empty
// prefix maps every variable.
func envTree(prefix, sep string, vars iter.Seq2[string, string]) map[string]any {
	if prefix != "" {
		prefix += "_"
	}
	out := map[string]any{}
	for name, value := range vars {
		rest, ok := strings.CutPrefix(name, prefix)
		if !ok || rest == "" {
			continue
		}
		if path := envKeyPath(rest, sep); path != nil {
			setPath(out, path, value)
		}
	}
	return out
}

// DotEnvOption configures [DotEnv].
type DotEnvOption func(*dotenvSource)

// DotEnvOptional makes a missing file load as empty instead of failing.
func DotEnvOptional() DotEnvOption { return func(s *dotenvSource) { s.optional = true } }

// DotEnvSeparator sets the separator between key levels (default "__").
func DotEnvSeparator(sep string) DotEnvOption { return func(s *dotenvSource) { s.sep = sep } }

// DotEnv loads a .env file and maps its variables like [Env]: with prefix
// "APP", APP_MYSQL__MAIN__HOST sets mysql.main.host. An empty prefix maps
// every variable, so MYSQL__MAIN__HOST works too. Variables that match no
// field are ignored.
//
// The file is a config source like any other: its precedence is its position
// in [From], and it neither changes the process environment nor feeds `env`
// tags. The syntax is described at [ParseDotEnv]. DotEnv sources are
// watchable.
//
//	config.From(
//		config.Profile("app"),
//		config.DotEnv(".env", "APP", config.DotEnvOptional()),
//		config.DotEnv(".env.local", "APP", config.DotEnvOptional()),
//		config.Env("APP"),
//	)
func DotEnv(path, prefix string, opts ...DotEnvOption) Source {
	s := &dotenvSource{path: path, prefix: strings.TrimSuffix(prefix, "_"), sep: "__", lookup: os.LookupEnv}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type dotenvSource struct {
	path     string
	prefix   string
	sep      string
	optional bool
	lookup   func(string) (string, bool)
}

func (s *dotenvSource) String() string { return "dotenv(" + s.path + ")" }

func (s *dotenvSource) lenient() bool { return true }

func (s *dotenvSource) Load(context.Context) (map[string]any, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if s.optional && errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	vars, err := parseDotEnv(string(data), s.lookup)
	if err != nil {
		return nil, err
	}
	return envTree(s.prefix, s.sep, maps.All(vars)), nil
}

func (s *dotenvSource) Watch(ctx context.Context, changed func()) error {
	return watchFiles(ctx, []string{s.path}, changed)
}

// ParseDotEnv parses .env content:
//
//	# comment
//	export APP_PORT=8080                 # "export" is optional
//	APP_NAME = order service          # unquoted: trimmed, " #" starts a comment
//	APP_GREETING="hello\nworld"          # double quotes: \n \r \t \" \\ \$ escapes
//	APP_PATTERN='^\d+$'                  # single quotes: literal
//	APP_CERT="-----BEGIN CERTIFICATE-----
//	...
//	-----END CERTIFICATE-----"           # quoted values may span lines
//	APP_DSN=mysql://${DB_USER}@db/app    # $VAR, ${VAR} and ${VAR:-default}
//
// Variables are expanded in unquoted and double-quoted values from those
// defined earlier in the content, then from the process environment; unset
// variables expand to "". Write \$ for a literal dollar sign.
func ParseDotEnv(data []byte) (map[string]string, error) {
	return parseDotEnv(string(data), os.LookupEnv)
}

func parseDotEnv(src string, lookup func(string) (string, bool)) (map[string]string, error) {
	p := &dotenvParser{
		s:      strings.TrimPrefix(strings.ReplaceAll(src, "\r\n", "\n"), "\ufeff"),
		line:   1,
		vars:   map[string]string{},
		lookup: lookup,
	}
	if err := p.parse(); err != nil {
		return nil, err
	}
	return p.vars, nil
}

type dotenvParser struct {
	s      string
	pos    int
	line   int
	vars   map[string]string
	lookup func(string) (string, bool)
}

func (p *dotenvParser) errorf(format string, args ...any) error {
	return fmt.Errorf("line %d: %s", p.line, fmt.Sprintf(format, args...))
}

func (p *dotenvParser) peek() byte {
	if p.pos < len(p.s) {
		return p.s[p.pos]
	}
	return 0
}

func (p *dotenvParser) skipBlanks() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t') {
		p.pos++
	}
}

func (p *dotenvParser) skipLine() {
	if i := strings.IndexByte(p.s[p.pos:], '\n'); i >= 0 {
		p.pos += i
	} else {
		p.pos = len(p.s)
	}
}

func isEnvKeyChar(c byte) bool {
	return c == '_' || c == '.' || c == '-' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func (p *dotenvParser) parse() error {
	for p.pos < len(p.s) {
		p.skipBlanks()
		// peek also returns 0 for a NUL byte, which must fail as a bad key
		// instead of silently ending the file.
		if p.pos == len(p.s) {
			return nil
		}
		switch p.peek() {
		case '\n':
			p.pos++
			p.line++
			continue
		case '#':
			p.skipLine()
			continue
		}

		if rest := p.s[p.pos:]; strings.HasPrefix(rest, "export ") || strings.HasPrefix(rest, "export\t") {
			p.pos += len("export")
			p.skipBlanks()
		}
		start := p.pos
		for p.pos < len(p.s) && isEnvKeyChar(p.s[p.pos]) {
			p.pos++
		}
		key := p.s[start:p.pos]
		if key == "" {
			return p.errorf("expected KEY=VALUE")
		}
		p.skipBlanks()
		if p.peek() != '=' {
			return p.errorf("expected '=' after %s", key)
		}
		p.pos++
		eq := p.pos
		p.skipBlanks()

		value, err := p.value(p.pos > eq)
		if err != nil {
			return err
		}
		p.vars[key] = value
	}
	return nil
}

// value reads the value after '='; spaced reports blanks after the '=', which
// make a following '#' start a comment.
func (p *dotenvParser) value(spaced bool) (string, error) {
	switch q := p.peek(); q {
	case '"', '\'':
		startLine := p.line
		p.pos++
		start := p.pos
		for {
			if p.pos >= len(p.s) {
				p.line = startLine
				return "", p.errorf("unterminated %c quote", q)
			}
			c := p.s[p.pos]
			if c == '\\' && q == '"' && p.pos+1 < len(p.s) {
				if p.s[p.pos+1] == '\n' {
					p.line++
				}
				p.pos += 2
				continue
			}
			if c == '\n' {
				p.line++
			}
			if c == q {
				break
			}
			p.pos++
		}
		raw := p.s[start:p.pos]
		p.pos++
		p.skipBlanks()
		switch {
		case p.pos == len(p.s), p.peek() == '\n':
		case p.peek() == '#':
			p.skipLine()
		default:
			return "", p.errorf("unexpected text after closing quote")
		}
		if q == '\'' {
			return raw, nil
		}
		return p.interpolate(raw, true)
	default:
		start := p.pos
		p.skipLine()
		raw := p.s[start:p.pos]
		// An inline comment starts at a '#' preceded by a blank.
		for i := 1; i < len(raw); i++ {
			if raw[i] == '#' && (raw[i-1] == ' ' || raw[i-1] == '\t') {
				raw = raw[:i]
				break
			}
		}
		if spaced && strings.HasPrefix(raw, "#") {
			raw = ""
		}
		return p.interpolate(strings.TrimSpace(raw), false)
	}
}

// interpolate expands variables and, in double-quoted values, escapes.
func (p *dotenvParser) interpolate(raw string, quoted bool) (string, error) {
	if !strings.ContainsAny(raw, `$\`) {
		return raw, nil
	}
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		switch {
		case c == '\\' && i+1 < len(raw):
			next := raw[i+1]
			switch {
			case next == '$':
				b.WriteByte('$')
			case !quoted:
				b.WriteByte(c)
				continue
			case next == 'n':
				b.WriteByte('\n')
			case next == 'r':
				b.WriteByte('\r')
			case next == 't':
				b.WriteByte('\t')
			case next == '"' || next == '\\':
				b.WriteByte(next)
			default:
				b.WriteByte(c)
				continue
			}
			i++
		case c == '$' && i+1 < len(raw) && raw[i+1] == '{':
			end := strings.IndexByte(raw[i:], '}')
			if end < 0 {
				return "", p.errorf("unterminated ${ in %q", raw)
			}
			expr := raw[i+2 : i+end]
			name, def, hasDef := strings.Cut(expr, ":-")
			if name == "" || strings.IndexFunc(name, func(r rune) bool { return r > 0x7f || !isEnvKeyChar(byte(r)) }) >= 0 {
				return "", p.errorf("invalid variable ${%s}", expr)
			}
			v, ok := p.get(name)
			if hasDef && (!ok || v == "") {
				v = def
			}
			b.WriteString(v)
			i += end
		case c == '$' && i+1 < len(raw) && (raw[i+1] == '_' || raw[i+1] >= 'a' && raw[i+1] <= 'z' || raw[i+1] >= 'A' && raw[i+1] <= 'Z'):
			j := i + 1
			for j < len(raw) && (raw[j] == '_' || raw[j] >= '0' && raw[j] <= '9' || raw[j] >= 'a' && raw[j] <= 'z' || raw[j] >= 'A' && raw[j] <= 'Z') {
				j++
			}
			v, _ := p.get(raw[i+1 : j])
			b.WriteString(v)
			i = j - 1
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}

func (p *dotenvParser) get(name string) (string, bool) {
	if v, ok := p.vars[name]; ok {
		return v, true
	}
	if p.lookup != nil {
		return p.lookup(name)
	}
	return "", false
}
