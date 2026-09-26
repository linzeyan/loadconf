package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

type appConf struct {
	Name  string                        `config:"name" validate:"required"`
	Port  int                           `config:"port" default:"8080"`
	Debug bool                          `config:"debug"`
	MySQL config.Named[config.MySQL]    `config:"mysql"`
	Redis config.Named[config.Redis]    `config:"redis"`
	Kafka config.Named[config.Kafka]    `config:"kafka"`
	Tags  []string                      `config:"tags"`
	Extra map[string]string             `config:"extra"`
	MQTT  config.Named[config.MQTT]     `config:"mqtt"`
	HTTP  config.Named[config.MQTTREST] `config:"mqtt_rest"`
}

func quiet() config.Option { return config.WithLogger(nil) }

func loadYAML[T any](t *testing.T, doc string, opts ...config.Option) (*T, error) {
	t.Helper()
	opts = append([]config.Option{quiet(), config.From(config.Bytes("yaml", []byte(doc)))}, opts...)
	return config.Load[T](context.Background(), opts...)
}

func mustYAML[T any](t *testing.T, doc string, opts ...config.Option) *T {
	t.Helper()
	cfg, err := loadYAML[T](t, doc, opts...)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return cfg
}

func TestLoadNamedListAndMapForms(t *testing.T) {
	cfg := mustYAML[appConf](t, `
name: svc
mysql:
  - name: Orders
    host: db1
    database: orders
    max_open_conns: 50
  - name: core
    dsn: "user:pw@tcp(db2:3306)/core"
redis:
  cache:
    addrs: [127.0.0.1:6379]
    db: 2
  Session:
    addrs: 10.0.0.1:6379   # a single scalar becomes a one-item list
`)

	if cfg.Port != 8080 {
		t.Errorf("port default = %d", cfg.Port)
	}
	if got := cfg.MySQL.Names(); strings.Join(got, ",") != "core,orders" {
		t.Fatalf("mysql names = %v", got)
	}
	fc := cfg.MySQL.MustGet("ORDERS")
	// Unset pool fields stay zero so that database/sql keeps its defaults.
	if fc.Host != "db1" || fc.Port != 3306 || fc.MaxOpenConns != 50 || fc.MaxIdleConns != 0 || fc.ConnMaxLifetime != 0 {
		t.Errorf("orders = %+v", fc)
	}
	if fc.PingTimeout != 3*time.Second {
		t.Errorf("ping timeout default = %v", fc.PingTimeout)
	}
	core, _ := cfg.MySQL.Get("core")
	if core.DSN.Value() != "user:pw@tcp(db2:3306)/core" {
		t.Errorf("core dsn = %q", core.DSN.Value())
	}
	if s := cfg.Redis.MustGet("session"); len(s.Addrs) != 1 || s.Addrs[0] != "10.0.0.1:6379" {
		t.Errorf("session addrs = %v", s.Addrs)
	}
	if c := cfg.Redis.MustGet("cache"); c.DB != 2 || c.ResolvedMode() != config.RedisSingle {
		t.Errorf("cache = %+v", c)
	}
}

func TestMergeNamedAcrossSourcesAndShapes(t *testing.T) {
	base := `
name: svc
mysql:
  - name: orders
    host: db1
    database: orders
  - name: core
    host: db2
    database: core
`
	overlay := `
mysql:
  orders:
    host: db1-prod
    password: s3cret
`
	cfg, err := config.Load[appConf](context.Background(), quiet(), config.From(
		config.Bytes("yaml", []byte(base)),
		config.Bytes("yaml", []byte(overlay)),
	))
	if err != nil {
		t.Fatal(err)
	}
	fc := cfg.MySQL.MustGet("orders")
	if fc.Host != "db1-prod" || fc.Database != "orders" || fc.Password.Value() != "s3cret" {
		t.Errorf("orders = %+v", fc)
	}
	if core := cfg.MySQL.MustGet("core"); core.Host != "db2" {
		t.Errorf("core lost after merge: %+v", core)
	}
}

func TestEnvSource(t *testing.T) {
	t.Setenv("APP_PORT", "9090")
	t.Setenv("APP_DEBUG", "true")
	t.Setenv("APP_TAGS", "a, b,c")
	t.Setenv("APP_MYSQL__ORDERS__PASSWORD", "from-env")
	t.Setenv("APP_MYSQL__ORDERS__CONN_MAX_LIFETIME", "10m")
	t.Setenv("APP_REDIS__NEW__ADDRS", "r1:6379,r2:6379")
	t.Setenv("APP_UNRELATED", "ignored")
	t.Setenv("APP_NAME__NESTED", "shape mismatch is dropped")

	cfg, err := config.Load[appConf](context.Background(), quiet(), config.WithStrict(), config.From(
		config.Bytes("yaml", []byte("name: svc\nmysql: [{name: orders, host: db1}]")),
		config.Env("APP"),
	))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9090 || !cfg.Debug || strings.Join(cfg.Tags, "|") != "a|b|c" {
		t.Errorf("port=%d debug=%v tags=%v", cfg.Port, cfg.Debug, cfg.Tags)
	}
	fc := cfg.MySQL.MustGet("orders")
	if fc.Password.Value() != "from-env" || fc.Host != "db1" || fc.ConnMaxLifetime != 10*time.Minute {
		t.Errorf("orders = %+v", fc)
	}
	if r := cfg.Redis.MustGet("new"); len(r.Addrs) != 2 || r.ResolvedMode() != config.RedisCluster {
		t.Errorf("redis new = %+v", r)
	}
}

type envTagConf struct {
	Port    int    `config:"port" env:"PORT,APP_PORT"`
	Name    string `config:"name"`
	Backend struct {
		Addr string `config:"addr" env:"ADDR"`
	} `config:"backend" env-prefix:"BACKEND_"`
	Embedded `env-prefix:"EMB_"`
}

type Embedded struct {
	Token config.Secret `config:"token" env:"TOKEN"`
}

func TestEnvTags(t *testing.T) {
	t.Setenv("APP_PORT", "7000")
	t.Setenv("BACKEND_ADDR", "backend:80")
	t.Setenv("EMB_TOKEN", "tok")
	t.Setenv("PORT", "") // empty counts as unset, so APP_PORT is used

	cfg := mustYAML[envTagConf](t, "port: 1\nbackend: {addr: file}\ntoken: file")
	if cfg.Port != 7000 || cfg.Backend.Addr != "backend:80" || cfg.Token.Value() != "tok" {
		t.Errorf("cfg = %+v", cfg)
	}
}

type hooked struct {
	Level string `config:"level"`
	Items []item `config:"items"`
}

type item struct {
	Enabled bool          `config:"enabled" default:"true"`
	Timeout time.Duration `config:"timeout" default:"2s"`
	Weight  int           `config:"weight"`
}

func (i *item) SetDefaults() { i.Weight = 1 }

func (h *hooked) SetDefaults() { h.Level = "info" }

func (h hooked) Validate() error {
	if h.Level == "panic" {
		return errors.New("level panic is not allowed")
	}
	return nil
}

func TestDefaultsAndHooks(t *testing.T) {
	cfg := mustYAML[hooked](t, `
items:
  - {}
  - enabled: false
    timeout: 1m
    weight: 5
`)
	if cfg.Level != "info" {
		t.Errorf("level = %q", cfg.Level)
	}
	if got := cfg.Items[0]; !got.Enabled || got.Timeout != 2*time.Second || got.Weight != 1 {
		t.Errorf("item 0 = %+v", got)
	}
	// Explicit false/zero values must win over defaults.
	if got := cfg.Items[1]; got.Enabled || got.Timeout != time.Minute || got.Weight != 5 {
		t.Errorf("item 1 = %+v", got)
	}

	_, err := loadYAML[hooked](t, "level: panic")
	if err == nil || !strings.Contains(err.Error(), "level panic is not allowed") {
		t.Errorf("want hook error, got %v", err)
	}
}

func TestValidation(t *testing.T) {
	_, err := loadYAML[appConf](t, `
mysql:
  orders: {database: x}
  core: {host: h, max_open_conns: 1, max_idle_conns: 5}
redis:
  cache: {addrs: ['a:1'], mode: weird}
kafka:
  main: {brokers: ['k:9092'], consumer: {initial_offset: middle}}
`)
	if err == nil {
		t.Fatal("expected validation errors")
	}
	for _, want := range []string{
		`name: does not satisfy "required"`,
		"mysql.orders: either dsn or host is required",
		"mysql.core: max_idle_conns (5) exceeds max_open_conns (1)",
		`redis.cache: unsupported mode "weird"`,
		`kafka.main: unsupported consumer.initial_offset "middle"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

type tagged struct {
	Hosts config.Named[host] `config:"hosts"`
	Inner struct {
		Min int `config:"min" validate:"gte=1"`
	} `config:"inner"`
}

type host struct {
	Name string `config:"name"`
	Addr string `config:"addr" validate:"required,hostname_port"`
}

func TestValidateTagsInsideNamed(t *testing.T) {
	cfg := mustYAML[tagged](t, "hosts: {a: {addr: 'h:1'}}\ninner: {min: 1}")
	if got := cfg.Hosts.MustGet("a"); got.Name != "a" {
		t.Errorf("name field not filled: %+v", got)
	}

	_, err := loadYAML[tagged](t, "hosts: [{name: b, addr: nope}]\ninner: {min: 0}")
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{`hosts.b.addr: does not satisfy "hostname_port"`, `inner.min: does not satisfy "gte=1"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

// validate tags on a Secret apply to the string it holds, as when Secret was
// a string; seen as a struct, a rule such as min panics with "Bad field type".
func TestValidateTagsOnSecret(t *testing.T) {
	type conf struct {
		Token config.Secret `config:"token" validate:"required,min=4"`
	}
	for doc, want := range map[string]string{
		"token: ''":   `token: does not satisfy "required"`,
		"token: abc":  `token: does not satisfy "min=4"`,
		"token: abcd": "",
	} {
		_, err := loadYAML[conf](t, doc)
		if (err == nil) != (want == "") || (err != nil && !strings.Contains(err.Error(), want)) {
			t.Errorf("%s: err = %v, want %q", doc, err, want)
		}
	}
}

func TestStrictUnknownKeys(t *testing.T) {
	_, err := loadYAML[appConf](t, "name: x\nprot: 1\nmysql: {a: {host: h, databse: d}}", config.WithStrict())
	if err == nil || !strings.Contains(err.Error(), "unknown keys: mysql.a.databse, prot") {
		t.Errorf("got %v", err)
	}
}

// Without strict, a typo must not stop the service, but it must not vanish
// either: it reaches the application's logger as a warning, even though the
// loader was created before that logger became the default.
func TestUnknownKeysWarnOnCurrentDefaultLogger(t *testing.T) {
	l := config.New[appConf](config.From(config.Bytes("yaml", []byte("name: x\nmysql: {a: {host: h, max_open_conn: 5}}"))))
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(old)

	if _, err := l.Load(t.Context()); err != nil {
		t.Fatalf("non-strict load failed: %v", err)
	}
	if out := buf.String(); !strings.Contains(out, `"level":"WARN"`) || !strings.Contains(out, "mysql.a.max_open_conn") {
		t.Errorf("want a WARN record naming mysql.a.max_open_conn, got:\n%s", out)
	}
}

// Field names without a config tag must match the keys people write:
// mysql, not the literal snake_case my_sql.
func TestUntaggedFieldsMatchProperNouns(t *testing.T) {
	type conf struct {
		MySQL      config.Named[config.MySQL]
		ClickHouse config.ClickHouse
	}
	t.Setenv("UNTAGGED_MYSQL__MAIN__USER", "app")
	cfg, err := config.Load[conf](context.Background(), quiet(), config.WithStrict(), config.From(
		config.Bytes("yaml", []byte("mysql: {main: {host: db1}}\nclickhouse: {addrs: [\"ch:9000\"]}")),
		config.Env("UNTAGGED"),
	))
	if err != nil {
		t.Fatal(err)
	}
	if m := cfg.MySQL.MustGet("main"); m.Host != "db1" || m.User != "app" {
		t.Errorf("mysql.main = %+v", m)
	}
	if len(cfg.ClickHouse.Addrs) != 1 {
		t.Errorf("clickhouse.addrs = %v", cfg.ClickHouse.Addrs)
	}
}

func TestDecodeErrorsHavePaths(t *testing.T) {
	_, err := loadYAML[appConf](t, `
name: x
port: abc
mysql:
  a: {host: h, conn_max_lifetime: 300}
redis:
  - addrs: [x]
`)
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		`port: invalid integer "abc"`,
		`mysql.a.conn_max_lifetime: invalid duration 300: add a unit`,
		"redis: item 0: name is required",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

func TestJSONAndTOML(t *testing.T) {
	type conf struct {
		Name    string                     `config:"name"`
		Big     int64                      `config:"big"`
		Ratio   float64                    `config:"ratio"`
		At      time.Time                  `config:"at"`
		Mongo   config.Named[config.Mongo] `config:"mongo"`
		Timeout time.Duration              `config:"timeout"`
	}
	j, err := config.Load[conf](context.Background(), quiet(), config.From(config.Bytes("json", []byte(
		`{"name":"j","big":9007199254740993,"ratio":0.5,"at":"2026-01-02T03:04:05Z","mongo":{"main":{"uri":"mongodb://h"}},"timeout":"1s"}`,
	))))
	if err != nil {
		t.Fatal(err)
	}
	if j.Big != 9007199254740993 || j.Ratio != 0.5 || j.At.Year() != 2026 || j.Timeout != time.Second {
		t.Errorf("json = %+v", j)
	}

	tm, err := config.Load[conf](context.Background(), quiet(), config.From(config.Bytes("toml", []byte(`
name = "t"
big = 42
at = 2026-01-02T03:04:05Z
timeout = "250ms"

[mongo.main]
hosts = ["m1:27017", "m2:27017"]

[mongo.main.params]
maxPoolSize = "20"
`))))
	if err != nil {
		t.Fatal(err)
	}
	m := tm.Mongo.MustGet("main")
	if tm.Big != 42 || tm.At.Month() != time.January || len(m.Hosts) != 2 || m.Params["maxPoolSize"] != "20" || tm.Timeout != 250*time.Millisecond {
		t.Errorf("toml = %+v / %+v", tm, m)
	}
}

type yamlTagged struct {
	Base    `yaml:",inline"`
	Listen  string `yaml:"listen_addr"`
	Skipped string `yaml:"-"`
}

type Base struct {
	Env string `yaml:"env" default:"dev"`
}

func TestWithTagName(t *testing.T) {
	cfg := mustYAML[yamlTagged](t, "listen_addr: ':80'\nskipped: x", config.WithTagName("yaml"))
	if cfg.Listen != ":80" || cfg.Env != "dev" || cfg.Skipped != "" {
		t.Errorf("cfg = %+v", cfg)
	}
}

type custom struct {
	Addr  netip.Addr `config:"addr"`
	Level level      `config:"level"`
	Will  *will
	Nope  *will
	Any   any `config:"any"`
}

type will struct {
	Topic string `config:"topic"`
}

type level int

func (l *level) UnmarshalConfig(v any) error {
	switch v {
	case "debug":
		*l = 1
	case "info":
		*l = 2
	default:
		return fmt.Errorf("unknown level %v", v)
	}
	return nil
}

func TestCustomDecoding(t *testing.T) {
	cfg := mustYAML[custom](t, "addr: 10.0.0.1\nlevel: info\nwill: {topic: t}\nany: {a: [1, 2]}")
	if cfg.Addr.String() != "10.0.0.1" || cfg.Level != 2 {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.Will == nil || cfg.Will.Topic != "t" || cfg.Nope != nil {
		t.Errorf("will = %+v nope = %+v", cfg.Will, cfg.Nope)
	}
	if m, ok := cfg.Any.(map[string]any); !ok || len(m["a"].([]any)) != 2 {
		t.Errorf("any = %#v", cfg.Any)
	}

	if _, err := loadYAML[custom](t, "level: loud"); err == nil || !strings.Contains(err.Error(), "level: unknown level loud") {
		t.Errorf("got %v", err)
	}
}

func TestSecretRedaction(t *testing.T) {
	m := config.MySQL{DSN: config.NewSecret("root:pw@/db"), Password: config.NewSecret("pw")}
	for _, s := range []string{fmt.Sprintf("%v", m), fmt.Sprintf("%+v", m), fmt.Sprintf("%#v", m.Password)} {
		if strings.Contains(s, "pw") {
			t.Errorf("secret leaked: %s", s)
		}
	}
	b, _ := json.Marshal(m)
	if strings.Contains(string(b), "pw") {
		t.Errorf("secret leaked in json: %s", b)
	}
	if m.Password.Value() != "pw" {
		t.Error("Value must return the secret")
	}
}

// Secret is a struct, yet every source writes it as a plain string, decoded
// through UnmarshalText rather than field by field. Like other text scalars, a
// mapping given for it fails from a file and is dropped from env, where
// APP_FILE__X is a deeper variable rather than a value for file.
func TestSecretDecodesFromEverySource(t *testing.T) {
	type conf struct {
		File    config.Secret              `config:"file"`
		Num     config.Secret              `config:"num"`
		DotEnv  config.Secret              `config:"dotenv"`
		Env     config.Secret              `config:"env"`
		Tag     config.Secret              `config:"tag" env:"SECRET_TEST_TAG"`
		Default config.Secret              `config:"default" default:"dflt"`
		MySQL   config.Named[config.MySQL] `config:"mysql"`
	}
	dotenv := filepath.Join(t.TempDir(), ".env")
	writeFile(t, dotenv, "APP_DOTENV=d\n")
	t.Setenv("APP_ENV", "e")
	t.Setenv("APP_FILE__X", "1")
	t.Setenv("SECRET_TEST_TAG", "t")
	cfg, err := config.Load[conf](t.Context(), quiet(), config.WithStrict(), config.From(
		config.Bytes("yaml", []byte("file: f\nmysql: [{name: main, host: h, password: p}]")),
		config.Bytes("json", []byte(`{"num": 1234}`)),
		config.DotEnv(dotenv, "APP"),
		config.Env("APP"),
	))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join([]string{cfg.File.Value(), cfg.Num.Value(), cfg.DotEnv.Value(), cfg.Env.Value(),
		cfg.Tag.Value(), cfg.Default.Value(), cfg.MySQL.MustGet("main").Password.Value()}, ",")
	if want := "f,1234,d,e,t,dflt,p"; got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	var opts struct{ Token config.Secret }
	if err := config.DecodeOptions(map[string]any{"token": "o"}, &opts); err != nil || opts.Token.Value() != "o" {
		t.Errorf("DecodeOptions: %q, %v", opts.Token.Value(), err)
	}

	_, err = loadYAML[conf](t, "file: {x: 1}", config.WithStrict())
	if err == nil || !strings.Contains(err.Error(), "file: expected a string") || strings.Contains(err.Error(), "file.x") {
		t.Errorf("mapping for a Secret: %v", err)
	}
}

func TestNamedDiff(t *testing.T) {
	var a, b config.Named[config.Redis]
	a.Set("keep", config.Redis{Addrs: []string{"x"}})
	a.Set("gone", config.Redis{})
	a.Set("edit", config.Redis{DB: 1})
	b.Set("keep", config.Redis{Addrs: []string{"x"}})
	b.Set("edit", config.Redis{DB: 2})
	b.Set("NEW", config.Redis{})
	added, removed, changed := a.Diff(b)
	if fmt.Sprint(added, removed, changed) != "[new] [gone] [edit]" {
		t.Errorf("diff = %v %v %v", added, removed, changed)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestProfile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "app.yaml"), "name: base\nport: 1\nmysql: [{name: a, host: base}]")
	writeFile(t, filepath.Join(dir, "app.dev.yaml"), "port: 2\nmysql: {a: {database: dev}}")
	writeFile(t, filepath.Join(dir, "app.prod.toml"), "name = 'prod'\nport = 3")
	writeFile(t, filepath.Join(dir, "other.yaml"), "name: other")

	load := func(opts ...config.ProfileOption) (*appConf, error) {
		opts = append([]config.ProfileOption{config.ProfileDir(dir)}, opts...)
		return config.Load[appConf](context.Background(), quiet(), config.From(config.Profile("app", opts...)))
	}

	t.Setenv("ENV", "")
	cfg, err := load()
	if err != nil {
		t.Fatal(err)
	}
	if a := cfg.MySQL.MustGet("a"); cfg.Name != "base" || cfg.Port != 2 || a.Host != "base" || a.Database != "dev" {
		t.Errorf("dev = %+v", cfg)
	}

	t.Setenv("ENV", "prod")
	if cfg, err = load(); err != nil || cfg.Name != "prod" || cfg.Port != 3 {
		t.Errorf("prod = %+v, %v", cfg, err)
	}

	t.Setenv("ENV", "stage")
	if _, err = load(); err == nil || !strings.Contains(err.Error(), "no config file app.stage") {
		t.Errorf("stage: %v", err)
	}
	if _, err = load(config.ProfileEnvs("dev", "prod")); err == nil || !strings.Contains(err.Error(), `unsupported ENV="stage"`) {
		t.Errorf("allowed envs: %v", err)
	}

	t.Setenv("CONFIG_PATH", filepath.Join(dir, "other.yaml"))
	if cfg, err = load(); err != nil || cfg.Name != "other" {
		t.Errorf("config path = %+v, %v", cfg, err)
	}
}

func TestOptionalFile(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	if _, err := config.Load[appConf](context.Background(), quiet(), config.From(config.File(missing))); err == nil {
		t.Error("missing required file should fail")
	}
	cfg, err := config.Load[appConf](context.Background(), quiet(), config.From(
		config.File(missing, config.Optional()),
		config.Map(map[string]any{"name": "from-map"}),
	))
	if err != nil || cfg.Name != "from-map" {
		t.Errorf("cfg = %+v, %v", cfg, err)
	}
}

func TestWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.yaml")
	writeFile(t, path, "name: v1\nport: 1")

	l := config.New[appConf](quiet(), config.WithDebounce(20*time.Millisecond), config.From(config.File(path)))
	changes := make(chan [2]*appConf, 4)
	errs := make(chan error, 4)
	l.OnChange(func(old, new *appConf) { changes <- [2]*appConf{old, new} })
	l.OnError(func(err error) { errs <- err })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Watch(ctx) }()

	waitFor := func(cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatal("timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitFor(func() bool { return l.Current() != nil })
	time.Sleep(100 * time.Millisecond) // let the watcher start

	writeFile(t, path, "name: v2\nport: 2")
	select {
	case c := <-changes:
		if c[0].Name != "v1" || c[1].Name != "v2" || l.Current().Port != 2 {
			t.Errorf("change = %+v -> %+v", c[0], c[1])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no change notification")
	}

	// An invalid file keeps the current config and reports the error.
	writeFile(t, path, "name: ''\nport: 3")
	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "required") {
			t.Errorf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no error notification")
	}
	if l.Current().Name != "v2" {
		t.Errorf("current replaced by invalid config: %+v", l.Current())
	}

	// Atomic replace via rename, as editors and ConfigMaps do.
	tmp := path + ".tmp"
	writeFile(t, tmp, "name: v3")
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	waitFor(func() bool { return l.Current().Name == "v3" })

	cancel()
	if err := <-done; err != nil {
		t.Errorf("watch returned %v", err)
	}
}

func TestWatchWithoutWatchableSource(t *testing.T) {
	l := config.New[appConf](quiet(), config.From(config.Map(map[string]any{"name": "x"})))
	if err := l.Watch(context.Background()); !errors.Is(err, config.ErrNotWatchable) {
		t.Errorf("got %v", err)
	}
}
