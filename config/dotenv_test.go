package config_test

import (
	"context"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

func TestParseDotEnv(t *testing.T) {
	src := "\ufeff# comment\r\n" + `
export APP_PORT=8080
APP_NAME = order service   # trailing comment
APP_HASH=#not-a-comment
APP_EMPTY=
APP_BLANK= # only a comment
APP_SINGLE='literal $APP_PORT \n # kept'
APP_DOUBLE="a\tb\n\"c\" \$APP_PORT \\ ${APP_PORT}"
APP_MULTI="line1
line2"
APP_REF=http://${APP_HOST:-localhost}:$APP_PORT/x
APP_UNSET=[${NOPE}]
APP_OS=${DOTENV_TEST_OS}
APP_DOLLAR=cost\$5
APP_WIN=C:\dir\file
dotted.key-name=ok
`
	t.Setenv("DOTENV_TEST_OS", "from-os")
	got, err := config.ParseDotEnv([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"APP_PORT":        "8080",
		"APP_NAME":        "order service",
		"APP_HASH":        "#not-a-comment",
		"APP_EMPTY":       "",
		"APP_BLANK":       "",
		"APP_SINGLE":      `literal $APP_PORT \n # kept`,
		"APP_DOUBLE":      "a\tb\n\"c\" $APP_PORT \\ 8080",
		"APP_MULTI":       "line1\nline2",
		"APP_REF":         "http://localhost:8080/x",
		"APP_UNSET":       "[]",
		"APP_OS":          "from-os",
		"APP_DOLLAR":      "cost$5",
		"APP_WIN":         `C:\dir\file`,
		"dotted.key-name": "ok",
	}
	if !maps.Equal(got, want) {
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
		for k := range got {
			if _, ok := want[k]; !ok {
				t.Errorf("unexpected key %s", k)
			}
		}
	}
}

func TestParseDotEnvErrors(t *testing.T) {
	for _, tc := range []struct{ src, want string }{
		{"A=1\nnot a pair", "line 2: expected '=' after not"},
		{"A=1\n=2", "line 2: expected KEY=VALUE"},
		{"A=1\nB=\"open\nstill open", `line 2: unterminated " quote`},
		{"A='x' y", "line 1: unexpected text after closing quote"},
		{"A=${B", "line 1: unterminated ${"},
		{"A=${B C}", "line 1: invalid variable ${B C}"},
		{"A=\"x\"\nB=\"a\nb\nc\"\nC", "line 5: expected '=' after C"},
	} {
		_, err := config.ParseDotEnv([]byte(tc.src))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("parse %q: got %v, want %q", tc.src, err, tc.want)
		}
	}
}

func TestDotEnvSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	writeFile(t, path, `
APP_PORT=9090
APP_MYSQL__MAIN__HOST=db1
APP_MYSQL__MAIN__MAX_OPEN_CONNS=not-a-number-but-lenient
OTHER_TOOL_SETTING=ignored
COMPOSE_PROJECT_NAME=x
`)
	type conf struct {
		Port  int                        `config:"port"`
		Name  string                     `config:"name" env:"DOTENV_TEST_NAME"`
		MySQL config.Named[config.MySQL] `config:"mysql"`
		Extra map[string]string          `config:"extra"`
	}
	t.Setenv("APP_PORT", "7070")
	// Env comes before DotEnv here, so the file wins.
	cfg, err := config.Load[conf](context.Background(), config.WithLogger(nil), config.WithStrict(), config.From(
		config.Map(map[string]any{"name": "base"}),
		config.Env("APP"),
		config.DotEnv(path, "APP"),
		config.DotEnv(filepath.Join(dir, "missing.env"), "APP", config.DotEnvOptional()),
	))
	if err == nil {
		t.Fatalf("want decode error for max_open_conns, got %+v", cfg)
	}
	if !strings.Contains(err.Error(), "mysql.main.max_open_conns") {
		t.Fatalf("error should name the key: %v", err)
	}

	writeFile(t, path, "APP_PORT=9090\nAPP_MYSQL__MAIN__HOST=db1\nOTHER=1\n")
	cfg, err = config.Load[conf](context.Background(), config.WithLogger(nil), config.WithStrict(), config.From(
		config.Map(map[string]any{"name": "base"}),
		config.Env("APP"),
		config.DotEnv(path, "APP"),
	))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9090 || cfg.MySQL.MustGet("main").Host != "db1" || cfg.Name != "base" {
		t.Fatalf("unexpected config %+v", cfg)
	}
	if _, set := os.LookupEnv("APP_MYSQL__MAIN__HOST"); set {
		t.Fatal("DotEnv must not change the process environment")
	}

	// Without a prefix every variable maps; env tags ignore the file.
	writeFile(t, path, "PORT=1\nDOTENV_TEST_NAME=from-file\n")
	cfg, err = config.Load[conf](context.Background(), config.WithLogger(nil), config.From(config.DotEnv(path, "")))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 1 || cfg.Name != "" {
		t.Fatalf("unexpected config %+v", cfg)
	}

	if _, err := config.Load[conf](context.Background(), config.WithLogger(nil), config.From(config.DotEnv(filepath.Join(dir, "nope"), "APP"))); err == nil {
		t.Fatal("missing required file should fail")
	}
	writeFile(t, path, "APP_PORT='x\n")
	if _, err := config.Load[conf](context.Background(), config.WithLogger(nil), config.From(config.DotEnv(path, "APP"))); err == nil || !strings.Contains(err.Error(), "dotenv("+path+"): line 1") {
		t.Fatalf("parse error should name the file and line: %v", err)
	}
}

func TestDotEnvWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	writeFile(t, path, "APP_PORT=1\n")
	type conf struct {
		Port int `config:"port"`
	}
	l := config.New[conf](config.WithLogger(nil), config.WithDebounce(20*time.Millisecond), config.From(config.DotEnv(path, "APP")))
	if _, err := l.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed := make(chan int, 1)
	l.OnChange(func(_, next *conf) { changed <- next.Port })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = l.Watch(ctx) }()
	time.Sleep(100 * time.Millisecond)
	writeFile(t, path, "APP_PORT=2\n")
	select {
	case port := <-changed:
		if port != 2 {
			t.Fatalf("port = %d, want 2", port)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload after the .env file changed")
	}
}
