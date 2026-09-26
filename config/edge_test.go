package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goccy/go-yaml"

	"github.com/linzeyan/loadconf/config"
)

func loadMap[T any](t *testing.T, m map[string]any, opts ...config.Option) (*T, error) {
	t.Helper()
	opts = append([]config.Option{quiet(), config.From(config.Map(m))}, opts...)
	return config.Load[T](t.Context(), opts...)
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

// clip keeps failure output readable when a whole config is printed.
func clip(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

// ---------------------------------------------------------------------------
// ByteSize

func TestByteSizeEdges(t *testing.T) {
	for in, want := range map[string]config.ByteSize{
		"1.5GiB":     3 << 29,
		".5KiB":      512,
		"0":          0,
		"0B":         0,
		"1 b":        1,
		"\t2k\n":     2 << 10,
		"8388607TiB": 8388607 << 40, // the largest whole TiB count that fits int64
	} {
		got, err := config.ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	// Signs, exponents, hex and look-alike digits are rejected instead of being
	// half-parsed: a size silently read as 0 or 1 would starve a buffer.
	for _, in := range []string{"+1", "1 2", "1..5KiB", ".", "\uff11KiB", "1KiB2", "NaN", "Inf", "0x10", "1iB", "KiB1", "1e3KiB"} {
		if got, err := config.ParseByteSize(in); err == nil {
			t.Errorf("ParseByteSize(%q) = %d, want an error", in, got)
		}
	}

	// Numbers from YAML/JSON: integral floats are fine, anything that is not a
	// whole non-negative byte count is not.
	var b config.ByteSize
	if err := b.UnmarshalConfig(2048.0); err != nil || b != 2048 {
		t.Errorf("UnmarshalConfig(2048.0) = %d, %v", b, err)
	}
	for _, raw := range []any{1.5, uint64(math.MaxUint64), true, []any{1}, map[string]any{}, int64(-1), "-1"} {
		if err := b.UnmarshalConfig(raw); err == nil {
			t.Errorf("UnmarshalConfig(%#v) = %d, want an error", raw, b)
		}
	}

	// Sizes written back by MarshalText (config dumps) must read back as the
	// same value.
	for _, want := range []config.ByteSize{0, 1, 1023, 1024, 1536, 64 * config.MiB, 3 * config.TiB, 1<<53 - 1} {
		text, _ := want.MarshalText()
		var got config.ByteSize
		if err := got.UnmarshalText(text); err != nil || got != want {
			t.Errorf("round trip of %d via %q = %d, %v", int64(want), text, got, err)
		}
	}
}

// A size written by String/MarshalText reads back unchanged, also above 2^53,
// where parsing through float64 would drop the low bits (2^53+1 would read
// back as 2^53).
func TestByteSizeRoundTripAbove2To53(t *testing.T) {
	for _, want := range []config.ByteSize{1<<53 + 1, 1<<62 + 1} {
		got, err := config.ParseByteSize(want.String())
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", want.String(), int64(got), err, int64(want))
		}
	}
}

// Sizes of 2^63 bytes or more fail as "too large". float64(MaxInt64) rounds up
// to 2^63, so a bound compared as a float must exclude it: converting 2^63 to
// int64 is implementation-defined (MaxInt64 on arm64, MinInt64 on amd64).
func TestByteSizeRejects2To63(t *testing.T) {
	for _, in := range []string{"8388608TiB", "9223372036854775808"} {
		if got, err := config.ParseByteSize(in); err == nil {
			t.Errorf("ParseByteSize(%q) = %d, want a too-large error", in, int64(got))
		}
	}
}

// ---------------------------------------------------------------------------
// Numbers

func TestIntegerOverflowIsRejected(t *testing.T) {
	type ints struct {
		I8  int8    `config:"i8"`
		U8  uint8   `config:"u8"`
		U   uint    `config:"u"`
		I64 int64   `config:"i64"`
		U64 uint64  `config:"u64"`
		F32 float32 `config:"f32"`
	}
	// A value that does not fit must fail with its path rather than wrap
	// around: a pool size of 300 stored in an int8 would become 44.
	for _, tc := range []struct {
		key string
		raw any
		ok  bool
	}{
		{"i8", 127, true}, {"i8", 128, false}, {"i8", -128, true}, {"i8", -129, false},
		{"i8", " 127 ", true}, {"i8", "128", false},
		{"u8", 255, true}, {"u8", 256, false}, {"u8", -1, false}, {"u8", "-1", false},
		{"u", -1, false}, {"u", 1.5, false},
		{"u64", "18446744073709551615", true}, {"u64", "18446744073709551616", false}, {"u64", uint64(math.MaxUint64), true},
		{"i64", uint64(1 << 63), false}, {"i64", "9223372036854775808", false}, {"i64", 2.0, true}, {"i64", 1.5, false},
		{"f32", 3.4e38, true}, {"f32", 1e39, false}, {"f32", "1e39", false},
	} {
		_, err := loadMap[ints](t, map[string]any{tc.key: tc.raw})
		if (err == nil) != tc.ok {
			t.Errorf("%s = %#v: err = %v, want ok=%v", tc.key, tc.raw, err, tc.ok)
		}
		if err != nil && !strings.Contains(err.Error(), tc.key+": ") {
			t.Errorf("%s = %#v: error does not name the key: %v", tc.key, tc.raw, err)
		}
	}
}

// A float at or beyond 2^63 (2^64) is rejected for int64 (uint64) fields like
// any other out-of-range number. float64(MaxInt64/MaxUint64) rounds up to
// exactly 2^63/2^64, whose conversion to an integer is implementation-defined.
func TestFloatAtIntBoundaryIsRejected(t *testing.T) {
	type conf struct {
		I64 int64  `config:"i64"`
		U64 uint64 `config:"u64"`
	}
	for key, raw := range map[string]float64{"i64": 1 << 63, "u64": 1 << 64} {
		if cfg, err := loadMap[conf](t, map[string]any{key: raw}); err == nil {
			t.Errorf("%s = %g decoded as %+v, want an overflow error", key, raw, *cfg)
		}
	}
}

// ---------------------------------------------------------------------------
// Keys

func TestKeyMatchingEdges(t *testing.T) {
	cfg := mustYAML[appConf](t, "NAME: svc\nPort: 1\nMySQL: {Main: {HOST: db1, Max_Open_Conns: 3}}")
	if m := cfg.MySQL.MustGet("main"); cfg.Name != "svc" || cfg.Port != 1 || m.Host != "db1" || m.MaxOpenConns != 3 {
		t.Errorf("mixed-case keys: %+v / %+v", cfg, m)
	}

	// Two spellings of one key in one mapping would otherwise resolve by sort
	// order; failing makes the conflict visible.
	for _, doc := range []string{"name: a\nNAME: b", "name: x\nmysql: {a: {host: h, HOST: h2}}"} {
		if _, err := loadYAML[appConf](t, doc); err == nil || !strings.Contains(err.Error(), "given more than once") {
			t.Errorf("%q: got %v", doc, err)
		}
	}

	// Keys are matched by case only: dropping or swapping the underscore is a
	// typo that strict mode must report, not a spelling to guess.
	_, err := loadYAML[appConf](t, "name: x\nmysql: {a: {host: h, maxopenconns: 1, max-open-conns: 2}}\n\uff4e\uff41\uff4d\uff45: y", config.WithStrict())
	for _, want := range []string{"mysql.a.max-open-conns", "mysql.a.maxopenconns", "\uff4e\uff41\uff4d\uff45"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("strict error missing %q: %v", want, err)
		}
	}

	// Sources written by different people spell keys and instance names
	// differently, and mix the list and mapping forms; they must still merge
	// into one instance.
	cfg, err = config.Load[appConf](t.Context(), quiet(), config.From(
		config.Bytes("yaml", []byte("name: x\nMYSQL: {Main: {host: db1}}\nPORT: 1")),
		config.Bytes("json", []byte(`{"mysql": [{"NAME": "MAIN", "database": "d"}], "port": 2}`)),
	))
	if err != nil {
		t.Fatal(err)
	}
	if m := cfg.MySQL.MustGet("main"); cfg.MySQL.Len() != 1 || m.Host != "db1" || m.Database != "d" || cfg.Port != 2 {
		t.Errorf("merged = %+v / %v", cfg, cfg.MySQL.Names())
	}
}

// ---------------------------------------------------------------------------
// Named

func TestNamedShapeErrors(t *testing.T) {
	for _, tc := range []struct{ doc, want string }{
		{"mysql: [{name: A, host: h}, {name: a, host: h}]", `mysql: duplicate instance name "a"`},
		{"mysql: {A: {host: h}, ' a ': {host: h}}", `mysql: duplicate instance name "a"`},
		{"mysql: {'  ': {host: h}}", "mysql: instance name must not be empty"},
		{"mysql: [{host: h}]", "mysql: item 0: name is required"},
		{"mysql: [{name: '', host: h}]", "mysql: item 0: name is required"},
		{"mysql: [{name: 7, host: h}]", "mysql: item 0: name must be a string"},
		{"mysql: [main]", "mysql: item 0: expected a mapping with a name"},
		{"mysql: main", "mysql: expected a mapping or a list"},
	} {
		if _, err := loadYAML[appConf](t, "name: x\n"+tc.doc); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want %q", tc.doc, err, tc.want)
		}
	}
}

func TestNamedNameKeyAndEmptyInstance(t *testing.T) {
	// The name key is matched like any key and the Name field gets the
	// normalized instance name, so logs and metrics label an instance the same
	// way however a file spells it.
	cfg := mustYAML[tagged](t, "hosts: [{NAME: ' Edge ', addr: 'h:1'}]\ninner: {min: 1}")
	if h, ok := cfg.Hosts.Get("EDGE"); !ok || h.Name != "edge" || h.Addr != "h:1" {
		t.Errorf("hosts = %v, edge = %+v", cfg.Hosts.Names(), h)
	}

	// An instance declared without a body still exists and is validated, so a
	// half-written entry fails loudly instead of disappearing.
	_, err := loadYAML[tagged](t, "hosts: {a: }\ninner: {min: 1}")
	if err == nil || !strings.Contains(err.Error(), `hosts.a.addr: does not satisfy "required"`) {
		t.Errorf("empty instance: %v", err)
	}
}

// A list item carrying both "name" and "Name" fails, like any other key given
// twice with different case: picking one would depend on map iteration order,
// so the instance would be named "a" or "b" from one load to the next.
func TestNamedListRejectsConflictingNameKeys(t *testing.T) {
	seen := map[string]bool{}
	for range 64 {
		cfg, err := loadYAML[tagged](t, "hosts: [{name: a, Name: b, addr: 'h:1'}]\ninner: {min: 1}")
		if err == nil {
			seen[strings.Join(cfg.Hosts.Names(), ",")] = true
		}
	}
	if len(seen) > 0 {
		t.Errorf("conflicting name keys were accepted; instance names seen across 64 loads: %v", slices.Sorted(maps.Keys(seen)))
	}
}

func TestNamedDiffEdges(t *testing.T) {
	type named = config.Named[config.Redis]
	build := func(kv ...any) named {
		var n named
		for i := 0; i < len(kv); i += 2 {
			n.Set(kv[i].(string), kv[i+1].(config.Redis))
		}
		return n
	}
	r := func(db int) config.Redis { return config.Redis{Addrs: []string{"a:1"}, DB: db} }
	for _, tc := range []struct {
		name      string
		old, next named
		want      string // added removed changed
	}{
		{"both empty", named{}, named{}, "[] [] []"},
		{"from empty", named{}, build("B", r(0), "a", r(0)), "[a b] [] []"},
		{"to empty", build("a", r(0)), named{}, "[] [a] []"},
		{"case and blanks only", build(" Cache ", r(1)), build("CACHE", r(1)), "[] [] []"},
		// A rotated password prints as ****** both times, yet the connection
		// must be rebuilt.
		{"secret rotated", build("a", config.Redis{Password: config.NewSecret("old")}), build("a", config.Redis{Password: config.NewSecret("new")}), "[] [] [a]"},
		{"mixed", build("keep", r(0), "gone", r(0), "edit", r(1)), build("keep", r(0), "edit", r(2), "new", r(0)), "[new] [gone] [edit]"},
	} {
		added, removed, changed := tc.old.Diff(tc.next)
		if got := fmt.Sprint(added, removed, changed); got != tc.want {
			t.Errorf("%s: diff = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Struct shapes

type leafConf struct {
	V int           `config:"v" default:"7"`
	D time.Duration `config:"d" default:"1s"`
}

type midConf struct {
	Leaf   leafConf             `config:"leaf"`
	PLeaf  *leafConf            `config:"pleaf"`
	Leaves []leafConf           `config:"leaves"`
	ByName map[string]*leafConf `config:"by_name"`
}

type deepConf struct {
	Mid  midConf    `config:"mid"`
	PMid *midConf   `config:"pmid"`
	PP   **leafConf `config:"pp"`
}

func TestDeepNestingAndPointers(t *testing.T) {
	// Defaults reach nested value structs, while absent pointers stay nil so
	// that "not configured" remains observable.
	cfg := mustYAML[deepConf](t, "")
	if cfg.Mid.Leaf.V != 7 || cfg.Mid.Leaf.D != time.Second || cfg.Mid.PLeaf != nil || cfg.PMid != nil || cfg.PP != nil {
		t.Errorf("empty = %+v", cfg)
	}

	cfg = mustYAML[deepConf](t, `
pmid: {pleaf: {}}
pp: {v: 3}
mid:
  pleaf: null
  leaves: [{}, {v: 2}]
  by_name: {x: {}, y: {d: 1m}}
`)
	// Every value created while decoding gets its defaults, at any depth.
	if cfg.PMid == nil || cfg.PMid.PLeaf == nil || cfg.PMid.PLeaf.V != 7 || cfg.PMid.Leaf.V != 7 {
		t.Errorf("pmid = %+v", cfg.PMid)
	}
	if cfg.PP == nil || *cfg.PP == nil || (**cfg.PP).V != 3 || (**cfg.PP).D != time.Second {
		t.Errorf("pp = %v", cfg.PP)
	}
	if cfg.Mid.PLeaf != nil {
		t.Errorf("explicit null allocated %+v", cfg.Mid.PLeaf)
	}
	if l := cfg.Mid.Leaves; len(l) != 2 || l[0].V != 7 || l[1].V != 2 || l[1].D != time.Second {
		t.Errorf("leaves = %+v", l)
	}
	if x, y := cfg.Mid.ByName["x"], cfg.Mid.ByName["y"]; x == nil || x.V != 7 || y == nil || y.V != 7 || y.D != time.Minute {
		t.Errorf("by_name = %v / %v", x, y)
	}
}

type SharedBase struct {
	Port int    `config:"port" default:"1"`
	Host string `config:"host"`
}

type shadowConf struct {
	SharedBase
	Port int `config:"port"`
}

type hiddenBase struct {
	Addr string `config:"addr"`
}

type hiddenConf struct {
	*hiddenBase
	Name string `config:"name"`
}

func TestEmbeddedConflictsAndPointers(t *testing.T) {
	// Like encoding/json, the shallower field owns the key; the embedded one
	// must not receive the same value behind the user's back.
	s := mustYAML[shadowConf](t, "port: 5\nhost: h")
	if s.Port != 5 || s.SharedBase.Port == 5 || s.Host != "h" {
		t.Errorf("shadow = %+v", s)
	}

	// Fields promoted through a nil unexported embedded pointer cannot be set
	// by reflection; that must be an error naming the key, not a panic.
	if _, err := loadYAML[hiddenConf](t, "addr: x"); err == nil || !strings.Contains(err.Error(), "addr: cannot set field Addr through a nil unexported embedded pointer") {
		t.Errorf("hidden embedded: %v", err)
	}
	if h, err := loadYAML[hiddenConf](t, "name: n"); err != nil || h.Name != "n" {
		t.Errorf("sibling of hidden embedded: %+v, %v", h, err)
	}
}

type selfEmbed struct {
	*selfEmbed
	Value int `config:"value"`
}

// Loading a struct that embeds a pointer to itself (legal Go, and handled by
// encoding/json) either works or fails with an error. Flattening embedded
// structs without tracking the enclosing ones would recurse until the stack
// overflows, which kills the whole process and cannot be recovered; hence the
// child process.
func TestRecursiveEmbeddedStructDoesNotCrash(t *testing.T) {
	if os.Getenv("CONFIG_EDGE_CHILD") == "1" {
		debug.SetMaxStack(16 << 20) // fail fast instead of growing to 1 GB
		_, err := loadMap[selfEmbed](t, map[string]any{"value": 1})
		t.Logf("load returned %v", err)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecursiveEmbeddedStructDoesNotCrash$", "-test.count=1", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "CONFIG_EDGE_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		lines := strings.SplitN(string(out), "\n", 4)
		t.Errorf("Load of a self-embedding struct crashed the process (%v):\n%s", err, strings.Join(lines[:min(3, len(lines))], "\n"))
	}
}

// ---------------------------------------------------------------------------
// Env

func TestEnvSourceSkipsMalformedNames(t *testing.T) {
	t.Setenv("EDGE_PORT__", "1")        // trailing separator
	t.Setenv("EDGE___PORT", "2")        // leading separator
	t.Setenv("EDGE_MYSQL____HOST", "3") // empty segment
	t.Setenv("EDGE_", "4")              // prefix only
	t.Setenv("EDGE_NAME", " a=b c ")    // '=' and blanks belong to the value
	cfg, err := config.Load[appConf](t.Context(), quiet(), config.WithStrict(), config.From(config.Env("EDGE_")))
	if err != nil {
		t.Fatal(err)
	}
	// Malformed names must never land on a neighbouring key or create phantom
	// instances.
	if cfg.Port != 8080 || cfg.MySQL.Len() != 0 || cfg.Name != " a=b c " {
		t.Errorf("cfg = %+v", cfg)
	}
}

// In env sources "values whose shape does not fit the target are dropped"
// (normalize.go), as APP_NAME__NESTED is for a string field. That includes the
// mapping a deeper variable produces for a list field or a duration: kept, one
// stray variable would fail the whole load or silently replace a list from the
// file with a single default element.
func TestLenientEnvDropsShapeMismatch(t *testing.T) {
	type conf struct {
		Tags    []string      `config:"tags"`
		Items   []item        `config:"items"`
		Timeout time.Duration `config:"timeout"`
	}
	load := func(t *testing.T) *conf {
		t.Helper()
		cfg, err := config.Load[conf](t.Context(), quiet(), config.From(
			config.Bytes("yaml", []byte("tags: [a, b]\nitems: [{weight: 5}]\ntimeout: 2s")),
			config.Env("SHAPE"),
		))
		if err != nil {
			t.Fatalf("a stray env var failed the load: %v", err)
		}
		return cfg
	}
	t.Run("list of scalars", func(t *testing.T) {
		t.Setenv("SHAPE_TAGS__FIRST", "x")
		if cfg := load(t); strings.Join(cfg.Tags, ",") != "a,b" {
			t.Errorf("tags = %v", cfg.Tags)
		}
	})
	t.Run("list of structs", func(t *testing.T) {
		t.Setenv("SHAPE_ITEMS__0__WEIGHT", "7")
		if cfg := load(t); len(cfg.Items) != 1 || cfg.Items[0].Weight != 5 {
			t.Errorf("items from the file were replaced: %+v", cfg.Items)
		}
	})
	t.Run("duration", func(t *testing.T) {
		t.Setenv("SHAPE_TIMEOUT__READ", "5s")
		if cfg := load(t); cfg.Timeout != 2*time.Second {
			t.Errorf("timeout = %v", cfg.Timeout)
		}
	})
}

// ---------------------------------------------------------------------------
// DotEnv

func TestParseDotEnvEdges(t *testing.T) {
	t.Setenv("CFG_EDGE_INDIRECT", "$CFG_EDGE_SECRET")
	t.Setenv("CFG_EDGE_SECRET", "leaked")
	for _, tc := range []struct {
		name, src string
		want      map[string]string
		err       string
	}{
		{"empty", "", map[string]string{}, ""},
		{"only comments and blanks", "\n# a\n   \n\t# b\n", map[string]string{}, ""},
		{"export as key", "export=1", map[string]string{"export": "1"}, ""},
		{"export with tab", "export\tA=1", map[string]string{"A": "1"}, ""},
		{"bare export", "export", nil, "line 1: expected '=' after export"},
		{"export without value", "export A", nil, "line 1: expected '=' after A"},
		{"crlf in quoted value", "A=1\r\nB=\"x\r\ny\"\r\n", map[string]string{"A": "1", "B": "x\ny"}, ""},
		{"bom only at start", "A=1\n\ufeffB=2", nil, "line 2: expected KEY=VALUE"},
		{"unterminated single quote", "A=1\nB='open\n\n", nil, "line 2: unterminated ' quote"},
		{"comment right after quote", `A="x"#c`, map[string]string{"A": "x"}, ""},
		{"escaped quote", `A="a\"b"`, map[string]string{"A": `a"b`}, ""},
		{"blanks around", "A \t=\t x \t", map[string]string{"A": "x"}, ""},
		{"self reference", "CFG_EDGE_SELF=${CFG_EDGE_SELF}", map[string]string{"CFG_EDGE_SELF": ""}, ""},
		{"redefinition sees earlier", "A=x\nA=${A}y", map[string]string{"A": "xy"}, ""},
		{"later definitions are not visible", "A=$CFG_EDGE_LATER\nCFG_EDGE_LATER=v", map[string]string{"A": "", "CFG_EDGE_LATER": "v"}, ""},
		// Values are data, not templates: re-expanding a value taken from the
		// environment would let it pull in other variables, e.g. secrets.
		{"no re-expansion", "A=$CFG_EDGE_INDIRECT", map[string]string{"A": "$CFG_EDGE_SECRET"}, ""},
		{"empty default", "A=${CFG_EDGE_UNSET:-}", map[string]string{"A": ""}, ""},
		{"empty braces", "A=${}", nil, "line 1: invalid variable ${}"},
		{"lone dollars", "A=$\nB=cost $5 $", map[string]string{"A": "$", "B": "cost $5 $"}, ""},
		{"line after crlf lines", "A=1\r\nB=2\r\nC\r\n", nil, "line 3: expected '=' after C"},
	} {
		got, err := config.ParseDotEnv([]byte(tc.src))
		if tc.err != "" {
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s: err = %v, want %q", tc.name, err, tc.err)
			}
			continue
		}
		if err != nil || !maps.Equal(got, tc.want) {
			t.Errorf("%s: got %q, %v; want %q", tc.name, got, err, tc.want)
		}
	}
}

// A NUL byte (e.g. from a botched copy of a binary file) is a syntax error, or
// the lines after it are still parsed. peek returns 0 both at the end of input
// and for a NUL byte; mistaking one for the other would stop parsing at a NUL
// at the start of a line or after a closing quote and drop every later
// variable without an error.
func TestDotEnvNULDoesNotEndParsing(t *testing.T) {
	for _, src := range []string{"A=1\n\x00\nB=2", "A=\"1\"\x00\nB=2"} {
		got, err := config.ParseDotEnv([]byte(src))
		if err == nil && got["B"] != "2" {
			t.Errorf("ParseDotEnv(%q) = %q, nil: B was dropped silently", src, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Secret

func TestSecretStaysRedacted(t *testing.T) {
	s := config.NewSecret("hunter2")
	type holder struct {
		Pw   config.Secret
		PPw  *config.Secret
		List []config.Secret
		ByID map[string]config.Secret
		Conn config.MySQL
	}
	h := holder{Pw: s, PPw: &s, List: []config.Secret{s}, ByID: map[string]config.Secret{"k": s},
		Conn: config.MySQL{DSN: config.NewSecret("root:hunter2@/db"), Password: s}}
	leaks := func(out string) bool {
		return strings.Contains(out, "hunter2") || strings.Contains(strings.ToLower(out), "68756e74657232")
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X"} {
		for _, arg := range []any{s, &s, h, &h} {
			if out := fmt.Sprintf(verb, arg); leaks(out) {
				t.Errorf("Sprintf(%s, %T) leaks: %s", verb, arg, clip(out))
			}
		}
	}

	var buf bytes.Buffer
	for _, hnd := range []slog.Handler{slog.NewTextHandler(&buf, nil), slog.NewJSONHandler(&buf, nil)} {
		slog.New(hnd).Info("cfg", "holder", h, "secret", s, "ptr", &s)
	}
	if leaks(buf.String()) {
		t.Errorf("slog leaks: %s", clip(buf.String()))
	}

	j1, _ := json.Marshal(h)
	j2, _ := json.Marshal(map[config.Secret]int{s: 1})
	y, _ := yaml.Marshal(h)
	for _, out := range [][]byte{j1, j2, y} {
		if leaks(string(out)) {
			t.Errorf("marshal leaks: %s", clip(string(out)))
		}
	}
}

// Secret is redacted whatever the verb, as its doc promises for printing
// (secret.go:7). For verbs invalid for strings (%d, %t, %e) fmt reports a bad
// verb and prints the operand without calling String, also for Secrets inside
// a struct; vet flags constant formats, not formats built at run time.
func TestSecretRedactedWithNonStringVerbs(t *testing.T) {
	s := config.NewSecret("hunter2")
	m := config.MySQL{Password: s, Port: 3306}
	for _, verb := range []string{"%d", "%t", "%e"} {
		for _, arg := range []any{s, m} {
			if out := fmt.Sprintf(verb, arg); strings.Contains(out, "hunter2") {
				t.Errorf("Sprintf(%s, %T) leaks: %s", verb, arg, clip(out))
			}
		}
	}
}

// %p is redacted like every other verb. fmt handles %p before looking for any
// method (fmt/print.go printArg) and prints a non-pointer operand with %v
// through badVerb, skipping the methods of nested values too, so no method on
// Secret can intercept it; only keeping the value behind a pointer does. A
// Secret holding a plain string field would print as
// "%!p(config.Secret={hunter2})", also inside structs, maps and Named. vet
// flags constant formats, not formats built at run time.
func TestSecretRedactedWithPointerVerb(t *testing.T) {
	s := config.NewSecret("hunter2")
	m := config.MySQL{DSN: config.NewSecret("root:hunter2@tcp(db)/app"), Password: s, Port: 3306}
	var n config.Named[config.MySQL]
	n.Set("main", m)
	nested := struct{ Conns []config.MySQL }{[]config.MySQL{m}}
	for _, verb := range []string{"%p"} {
		for _, arg := range []any{s, m, nested, n, appConf{Name: "svc", MySQL: n}} {
			if out := fmt.Sprintf(verb, arg); strings.Contains(out, "hunter2") {
				t.Errorf("Sprintf(%s, %T) leaks: %s", verb, arg, clip(out))
			}
		}
	}
}

// Printing a config holding Named connections redacts their secrets, as
// printing the connection types directly does. Named keeps instances in an
// unexported map, and fmt does not call String/GoString/Format on values
// reached through unexported fields, so Named formats its map as an operand of
// its own; slog's TextHandler goes through the same %+v.
func TestNamedRedactsSecretsViaFmt(t *testing.T) {
	var n config.Named[config.MySQL]
	n.Set("main", config.MySQL{DSN: config.NewSecret("root:hunter2@tcp(db)/app"), Password: config.NewSecret("hunter2")})
	cfg := appConf{Name: "svc", MySQL: n}
	for _, verb := range []string{"%v", "%+v", "%#v"} {
		for _, arg := range []any{n, cfg, &cfg} {
			if out := fmt.Sprintf(verb, arg); strings.Contains(out, "hunter2") {
				t.Errorf("Sprintf(%s, %T) leaks: %s", verb, arg, clip(out))
			}
		}
	}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("config loaded", "config", cfg)
	if strings.Contains(buf.String(), "hunter2") {
		t.Errorf("slog TextHandler leaks: %s", clip(buf.String()))
	}
}

// A misspelled key is most often a misspelled secret: the warning must name
// the key without echoing its value into the logs.
func TestUnknownKeysAreReportedWithoutValues(t *testing.T) {
	doc := []byte("name: x\npasswrod: hunter2\nmysql: [{name: Main, host: h, pasword: hunter2}]")
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	if _, err := config.Load[appConf](t.Context(), config.WithLogger(logger), config.From(config.Bytes("yaml", doc))); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{`"level":"WARN"`, "passwrod", "mysql.main.pasword"} {
		if !strings.Contains(out, want) {
			t.Errorf("log missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "hunter2") {
		t.Errorf("value of an unknown key reached the log:\n%s", out)
	}

	_, err := config.Load[appConf](t.Context(), quiet(), config.WithStrict(), config.From(config.Bytes("yaml", doc)))
	if err == nil || !strings.Contains(err.Error(), "unknown keys: mysql.main.pasword, passwrod") || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("strict: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Loader and sources

func TestLoadRejectsNonStructTargets(t *testing.T) {
	if _, err := config.Load[int](t.Context(), quiet()); err == nil || !strings.Contains(err.Error(), "not a struct") {
		t.Errorf("int: %v", err)
	}
	if _, err := config.Load[*appConf](t.Context(), quiet()); err == nil || !strings.Contains(err.Error(), "not a struct") {
		t.Errorf("pointer: %v", err)
	}
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustLoad did not panic")
		}
	}()
	config.MustLoad[map[string]any](t.Context(), quiet())
}

func TestMapSourceWithGoTypes(t *testing.T) {
	// Programmatic defaults and fixtures are written with ordinary Go types.
	fixture := map[string]any{
		"name":  "svc",
		"tags":  []string{"a", "b"},
		"extra": map[string]string{"k": "v"},
		"mysql": map[string]map[string]any{"main": {"host": "db1", "port": int32(3307)}},
		"redis": []map[string]any{{"name": "cache", "addrs": []string{"r:1"}}},
	}
	cfg, err := loadMap[appConf](t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	if m := cfg.MySQL.MustGet("main"); strings.Join(cfg.Tags, ",") != "a,b" || cfg.Extra["k"] != "v" || m.Port != 3307 || len(cfg.Redis.MustGet("cache").Addrs) != 1 {
		t.Errorf("cfg = %+v", cfg)
	}
	// Load works on a copy: fixtures shared between tests must not be
	// rewritten into the canonical tree form.
	if _, ok := fixture["tags"].([]string); !ok {
		t.Errorf("tags rewritten to %T", fixture["tags"])
	}
	if r := fixture["redis"].([]map[string]any); r[0]["name"] != "cache" {
		t.Errorf("redis fixture rewritten: %v", r)
	}

	if _, err := loadMap[appConf](t, map[string]any{"extra": map[[2]int]string{{1, 2}: "x"}}); err == nil || !strings.Contains(err.Error(), "unsupported mapping key") {
		t.Errorf("array key: %v", err)
	}
}

func TestParseEdges(t *testing.T) {
	for _, tc := range []struct{ format, doc, err string }{
		{"json", `{"a": 1} {"b": 2}`, "unexpected data after the top-level object"},
		{"json", `[1, 2]`, "parse json"},
		{"yaml", "- a\n- b", "parse yaml"},
		{"yaml", "a: 1\na: 2", `mapping key "a" already defined`},
		{"toml", "a = 1\na = 2", "parse toml"},
		{"ini", "a=1", `unknown config format "ini"`},
	} {
		if _, err := config.Parse(tc.format, []byte(tc.doc)); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("Parse(%s, %q) = %v, want %q", tc.format, tc.doc, err, tc.err)
		}
	}
	// Empty documents are an empty layer, not an error: an overlay file may be
	// all comments.
	for _, tc := range []struct{ format, doc string }{{"json", "  \n"}, {"JSON", "null"}, {"yaml", ""}, {"yaml", "# nothing"}, {"yaml", "---\n...\n"}, {"toml", ""}} {
		if m, err := config.Parse(tc.format, []byte(tc.doc)); err != nil || m == nil || len(m) != 0 {
			t.Errorf("Parse(%s, %q) = %v, %v", tc.format, tc.doc, m, err)
		}
	}
}

// The TOML parser's message alone names no line, which leaves a typo in a
// long file to be hunted down; the error must point at it as YAML errors do.
func TestTOMLErrorsHavePosition(t *testing.T) {
	_, err := config.Parse("toml", []byte("[mysql]\nport = 3306\nhost = db\n"))
	if err == nil || !strings.Contains(err.Error(), "[3:8] ") || !strings.Contains(err.Error(), "3| host = db") {
		t.Fatalf("err = %v", err)
	}
}

// YAML merge keys are the idiomatic way to share settings between instances.
func TestYAMLAnchorsAndMergeKeys(t *testing.T) {
	cfg := mustYAML[appConf](t, `
name: svc
mysql:
  a: &db {host: h1, port: 3307, user: app, params: {charset: utf8mb4}}
  b: {<<: *db, host: h2}
`)
	a, b := cfg.MySQL.MustGet("a"), cfg.MySQL.MustGet("b")
	if a.Host != "h1" || b.Host != "h2" || b.Port != 3307 || b.User != "app" || b.Params["charset"] != "utf8mb4" {
		t.Fatalf("a = %+v\nb = %+v", a, b)
	}
	// The alias shares one parsed map; decoding must copy it, or tuning one
	// instance at run time would retune the other.
	a.Params["charset"] = "latin1"
	if cfg.MySQL.MustGet("b").Params["charset"] != "utf8mb4" {
		t.Error("instances share the params map")
	}
}

func TestDecodeOptionsEdges(t *testing.T) {
	var o driverOptions
	// Case-insensitive matching must not become a way around owned keys.
	err := config.DecodeOptions(map[string]any{"ADDRS": "a:1", "Retry": map[string]any{"MAX": 1}}, &o, "addrs", "retry.max")
	for _, want := range []string{"options.addrs: set by its own config key", "options.retry.max: set by its own config key"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("owned: missing %q in %v", want, err)
		}
	}
	if err := config.DecodeOptions(map[string]any{"pool_size": 1, "POOL_SIZE": 2}, &o); err == nil || !strings.Contains(err.Error(), "given more than once") {
		t.Errorf("duplicate: %v", err)
	}
	// No options must not even look at the target.
	if err := config.DecodeOptions(map[string]any{}, 42); err != nil {
		t.Errorf("empty options: %v", err)
	}
	x := 1
	if err := config.DecodeOptions(map[string]any{"a": 1}, &x); err == nil || !strings.Contains(err.Error(), "non-nil struct pointer") {
		t.Errorf("pointer to int: %v", err)
	}
	// Connectors pass typed values from Go code; they are taken as they are.
	o = driverOptions{}
	if err := config.DecodeOptions(map[string]any{"dial_timeout": 2 * time.Second, "addrs": []string{"a", "b"}}, &o); err != nil || o.DialTimeout != 2*time.Second || len(o.Addrs) != 2 {
		t.Errorf("typed values: %+v, %v", o, err)
	}
}

func TestTLSConfigEdges(t *testing.T) {
	garbage := filepath.Join(t.TempDir(), "garbage.pem")
	writeFile(t, garbage, "not a certificate")
	for _, tc := range []struct {
		tls  config.TLS
		want string
	}{
		{config.TLS{Enabled: true, MinVersion: "1.1"}, `unsupported min_version "1.1"`},
		{config.TLS{Enabled: true, MinVersion: "1.3 "}, "unsupported min_version"},
		{config.TLS{Enabled: true, CAFile: garbage}, "contains no PEM certificates"},
		{config.TLS{Enabled: true, CertFile: garbage, KeyFile: garbage}, "load client certificate"},
		{config.TLS{Enabled: true, KeyFile: garbage}, "must be set together"},
	} {
		if c, err := tc.tls.Config(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: got %v, %v; want %q", tc.tls, c, err, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Watch

// memSource is an in-memory watchable source: tests change its value and
// fire notifications by hand, so debounce and cancellation are observable
// without file system timing.
type memSource struct {
	mu       sync.Mutex
	port     int
	fail     error
	loads    atomic.Int32
	notify   chan func()
	watchErr error
	stopped  atomic.Bool
}

func newMemSource(port int) *memSource { return &memSource{port: port, notify: make(chan func(), 1)} }

func (s *memSource) String() string { return "mem" }

func (s *memSource) set(port int, fail error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.port, s.fail = port, fail
}

func (s *memSource) Load(context.Context) (map[string]any, error) {
	s.loads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	// The password puts a Secret in the reload comparison: each load holds it
	// behind a fresh pointer, yet an unchanged value must compare equal.
	return map[string]any{
		"name":  "mem",
		"port":  s.port,
		"mysql": map[string]any{"main": map[string]any{"host": fmt.Sprint("db", s.port), "password": "hunter2"}},
	}, nil
}

func (s *memSource) Watch(ctx context.Context, changed func()) error {
	s.notify <- changed
	if s.watchErr != nil {
		return s.watchErr
	}
	<-ctx.Done()
	s.stopped.Store(true)
	return nil
}

func TestWatchDebounceAndCancel(t *testing.T) {
	src := newMemSource(1)
	l := config.New[appConf](quiet(), config.WithDebounce(100*time.Millisecond), config.From(src))
	changes := make(chan int, 8)
	l.OnChange(func(_, next *appConf) { changes <- next.Port })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Watch(ctx) }()
	changed := <-src.notify
	base := src.loads.Load()

	// Editors and etcd transactions produce bursts of events; each burst must
	// cost one reload, not one per event.
	src.set(2, nil)
	for range 100 {
		changed()
	}
	select {
	case p := <-changes:
		if p != 2 {
			t.Errorf("port = %d", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload")
	}
	time.Sleep(300 * time.Millisecond)
	if n := src.loads.Load() - base; n != 1 {
		t.Errorf("a burst of 100 notifications caused %d reloads, want 1", n)
	}

	// A reload that yields an equal config must not wake subscribers: they
	// would rebuild connections for nothing.
	changed()
	waitUntil(t, func() bool { return src.loads.Load()-base == 2 })
	select {
	case p := <-changes:
		t.Errorf("OnChange fired for an unchanged config (port %d)", p)
	case <-time.After(200 * time.Millisecond):
	}

	// Watch waits for its source watchers, so a caller may close clients as
	// soon as it returns.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Watch = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Watch did not return after cancel")
	}
	if !src.stopped.Load() {
		t.Error("Watch returned before the source watcher stopped")
	}
}

func TestWatchErrors(t *testing.T) {
	boom := errors.New("boom")
	src := newMemSource(1)
	src.watchErr = boom
	if err := config.New[appConf](quiet(), config.From(src)).Watch(t.Context()); !errors.Is(err, boom) || !strings.Contains(err.Error(), "watch mem") {
		t.Errorf("broken source watch: %v", err)
	}

	// Without a first config there is nothing to keep serving: Watch fails
	// before starting any watcher.
	src = newMemSource(1)
	src.set(1, boom)
	if err := config.New[appConf](quiet(), config.From(src)).Watch(t.Context()); !errors.Is(err, boom) {
		t.Errorf("failing first load: %v", err)
	}
	if len(src.notify) != 0 {
		t.Error("a watcher was started although the first load failed")
	}
}

// Readers call Current from request goroutines while reloads publish new
// configs; run with -race.
func TestCurrentIsSafeDuringReloads(t *testing.T) {
	src := newMemSource(0)
	l := config.New[appConf](quiet(), config.WithDebounce(time.Millisecond), config.From(src))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- l.Watch(ctx) }()
	changed := <-src.notify

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			last := -1
			for {
				select {
				case <-stop:
					return
				default:
				}
				c := l.Current()
				// A published config is never modified: its fields belong to one
				// generation, and generations never go backwards.
				if m, ok := c.MySQL.Get("main"); !ok || m.Host != fmt.Sprint("db", c.Port) || c.Port < last {
					t.Errorf("torn or stale config: port %d (last %d), mysql %+v", c.Port, last, m)
					return
				}
				last = c.Port
			}
		})
	}
	wg.Go(func() {
		for range 50 {
			l.OnChange(func(_, _ *appConf) {})
			l.OnError(func(error) {})
			time.Sleep(time.Millisecond)
		}
	})

	for i := 1; i <= 30; i++ {
		src.set(i, nil)
		changed()
		waitUntil(t, func() bool { return l.Current().Port == i })
	}
	close(stop)
	wg.Wait()
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Watch = %v", err)
	}
}

// Like the etcd sources, which resume from the revision of the last load, a
// file edited between Load and Watch is picked up once Watch starts: Watch
// only loads when nothing was loaded yet, so an edit made while the service was
// starting up (Load, open connections, then go Watch) raised no event the
// watcher could see.
func TestFileWatchAppliesChangeBeforeWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "name: v1")
	l := config.New[appConf](quiet(), config.WithDebounce(10*time.Millisecond), config.From(config.File(path)))
	if _, err := l.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, "name: v2")

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.Watch(ctx) }()
	deadline := time.Now().Add(time.Second)
	for l.Current().Name != "v2" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if got := l.Current().Name; got != "v2" {
		t.Errorf("config after Watch started = %q, want v2 (edited between Load and Watch)", got)
	}
}

// An Optional file that does not exist, directory included, loads as empty and
// does not keep the other sources from being watched: fsnotify cannot watch a
// missing directory, and failing on it would turn hot reload off for every
// source.
func TestWatchToleratesOptionalFileInMissingDir(t *testing.T) {
	dir := t.TempDir()
	main := filepath.Join(dir, "app.yaml")
	writeFile(t, main, "name: v1")
	l := config.New[appConf](quiet(), config.From(
		config.File(main),
		config.File(filepath.Join(dir, "missing", "override.yaml"), config.Optional()),
	))
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := l.Watch(ctx); err != nil {
		t.Errorf("Watch = %v, want it to keep watching until ctx is done", err)
	}
}

// Each GELF UDP chunk starts with a 12-byte header, so the logger rejects a
// chunk_size of 1-12, but only when it opens the output, without saying
// where in the config it is. The loader reports it with the key path.
func TestGELFChunkSizeFailsAtLoad(t *testing.T) {
	type conf struct {
		Log config.Log `config:"log"`
	}
	gelf := func(size int) map[string]any {
		return map[string]any{"log": map[string]any{"outputs": map[string]any{
			"graylog": map[string]any{"type": "gelf", "addr": "graylog:12201", "chunk_size": size},
		}}}
	}
	for _, size := range []int{1, 12} {
		if _, err := loadMap[conf](t, gelf(size)); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("log.outputs.graylog: chunk_size %d", size)) {
			t.Errorf("chunk_size %d: err = %v, want it reported at log.outputs.graylog", size, err)
		}
	}
	if _, err := loadMap[conf](t, gelf(13)); err != nil {
		t.Errorf("chunk_size 13 leaves a byte of data per chunk: %v", err)
	}
}

// connector/conn_streamload rejects these addrs (feURL) when the client is
// built: an empty host would dial localhost, a stray bracket cannot be split
// into host and port again, and port 0 or one above 65535 cannot be dialed.
// Validate rejects them at load, without the password some addrs carry.
func TestStreamLoadAddrsFailAtLoad(t *testing.T) {
	base := config.StreamLoad{Flavor: config.StreamLoadDoris, Database: "d", Format: "json"}
	for _, addr := range []string{":8030", "u:hunter2@:8030", "http://:8030", "]", "http://fe]:8030", "fe:0", "http://u:hunter2@fe:65536", "fe:99999999999999999999"} {
		s := base
		s.Addrs = []string{addr}
		if err := s.Validate(); err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("addr %q: Validate = %v, want an error without the password", addr, err)
		}
	}
	for _, addr := range []string{"fe", "fe:8030", "u:hunter2@fe:8030", "[::1]:8030", "https://fe:8443", "http://10.0.0.1:65535"} {
		s := base
		s.Addrs = []string{addr}
		if err := s.Validate(); err != nil {
			t.Errorf("addr %q: %v", addr, err)
		}
	}
}

// Addresses may embed credentials; a rejected one must be reported without
// them, since Validate errors end up in logs.
func TestAddrErrorsRedactPassword(t *testing.T) {
	for name, err := range map[string]error{
		"stream_load addr":               config.StreamLoad{Flavor: config.StreamLoadDoris, Addrs: []string{"ftp://u:hunter2@fe"}, Database: "d", Format: "json"}.Validate(),
		"stream_load unparseable addr":   config.StreamLoad{Flavor: config.StreamLoadDoris, Addrs: []string{"http://u:hunter2@fe:%zz"}, Database: "d", Format: "json"}.Validate(),
		"mqtt_rest base_url":             config.MQTTREST{Provider: config.MQTTRESTEMQX, BaseURL: "ftp://u:hunter2@emqx"}.Validate(),
		"mqtt_rest unparseable base_url": config.MQTTREST{Provider: config.MQTTRESTEMQX, BaseURL: "http://u:hunter2@emqx:%zz"}.Validate(),
	} {
		if err == nil || strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: Validate = %v, want an error without the password", name, err)
		}
	}
}
