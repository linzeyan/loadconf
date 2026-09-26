package logger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// memSink collects records in memory; withMem makes it the output type "mem".
type memSink struct {
	mu      sync.Mutex
	records [][]byte
	closed  bool
}

var mems sync.Map // output name -> *memSink

var withMem = logger.WithSink("mem", func(_ context.Context, out config.LogOutput, _ logger.SinkEnv) (logger.Sink, error) {
	s := &memSink{}
	mems.Store(out.Options["id"], s)
	return s, nil
})

func (m *memSink) Write(b []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, bytes.Clone(b))
	return len(b), nil
}

func (m *memSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *memSink) decoded(t *testing.T) []map[string]any {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]map[string]any, len(m.records))
	for i, r := range m.records {
		if err := json.Unmarshal(r, &out[i]); err != nil {
			t.Fatalf("record %d is not JSON: %v\n%s", i, err, r)
		}
	}
	return out
}

func memOutput(t *testing.T, name string, extra config.LogOutput) (config.LogOutput, func() *memSink) {
	id := t.Name() + "/" + name
	extra.Name, extra.Type, extra.Options = name, "mem", map[string]any{"id": id}
	return extra, func() *memSink {
		v, ok := mems.Load(id)
		if !ok {
			t.Fatalf("sink %s not opened", id)
		}
		return v.(*memSink)
	}
}

func logConfig(level string, outs ...config.LogOutput) config.Log {
	cfg := config.Log{Level: level, Format: "json", StackLevel: "error"}
	for _, o := range outs {
		cfg.Outputs.Set(o.Name, o)
	}
	return cfg
}

func TestSlogLevelsFieldsAndUpdate(t *testing.T) {
	all, allSink := memOutput(t, "all", config.LogOutput{})
	errs, errSink := memOutput(t, "errors", config.LogOutput{Level: "error"})
	cfg := logConfig("debug", all, errs)
	cfg.Service = "svc"
	cfg.Fields = map[string]string{"env": "test", "version": "1.2"}
	cfg.AddSource = true

	l, err := logger.New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	l.Log(context.Background(), logger.LevelTrace, "trace dropped")
	l.Debug("debug kept", "n", 1)
	l.Error("failed", "err", errors.New("boom"))

	got := allSink().decoded(t)
	if len(got) != 2 {
		t.Fatalf("all output got %d records: %v", len(got), got)
	}
	d := got[0]
	if d["msg"] != "debug kept" || d["level"] != "DEBUG" || d["service"] != "svc" || d["env"] != "test" || d["version"] != "1.2" {
		t.Errorf("debug record = %v", d)
	}
	if src, ok := d["source"].(map[string]any); !ok || !strings.HasSuffix(src["file"].(string), "logger_test.go") {
		t.Errorf("source = %v", d["source"])
	}
	if _, has := d["stack"]; has {
		t.Error("debug record should not carry a stack")
	}
	if _, err := time.Parse(logger.RFC3339Milli, d["time"].(string)); err != nil {
		t.Errorf("time %v: %v", d["time"], err)
	}
	e := got[1]
	if !strings.Contains(e["stack"].(string), "TestSlogLevelsFieldsAndUpdate") {
		t.Errorf("stack should start at the test:\n%v", e["stack"])
	}
	if strings.Contains(e["stack"].(string), "log/slog") {
		t.Errorf("stack should skip slog internals:\n%v", e["stack"])
	}
	if n := len(errSink().decoded(t)); n != 1 {
		t.Errorf("errors output got %d records, want 1", n)
	}

	// Raise the logger level and lower the errors output to warn.
	errs.Level = "warn"
	cfg.Outputs.Set("errors", errs)
	cfg.Level = "warn"
	cfg.StackLevel = "none"
	if err := l.Update(cfg); err != nil {
		t.Fatal(err)
	}
	l.Info("info dropped")
	l.Warn("warn kept")
	l.Error("no stack")
	got = allSink().decoded(t)
	if len(got) != 4 || got[2]["msg"] != "warn kept" {
		t.Fatalf("after update: %v", got)
	}
	if _, has := got[3]["stack"]; has {
		t.Error("stack_level none should disable stacks")
	}
	if n := len(errSink().decoded(t)); n != 3 {
		t.Errorf("errors output got %d records, want 3", n)
	}

	cfg.Level = "loud"
	if err := l.Update(cfg); err == nil {
		t.Error("invalid level should fail")
	}
	if err := l.Close(); err != nil || !allSink().closed {
		t.Errorf("close: %v", err)
	}
}

func TestSlogTraceAndTimeFormats(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	cfg := logConfig("trace", out)
	cfg.TimeFormat = "unixmilli"
	l, err := logger.New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now().UnixMilli()
	l.Log(context.Background(), logger.LevelTrace, "t")
	rec := sink().decoded(t)[0]
	if rec["level"] != "TRACE" {
		t.Errorf("level = %v", rec["level"])
	}
	if ms, ok := rec["time"].(float64); !ok || int64(ms) < before {
		t.Errorf("time = %v", rec["time"])
	}

	for format, check := range map[string]func(string) bool{
		"rfc3339": func(s string) bool {
			_, err := time.Parse(time.RFC3339, s)
			return err == nil && !strings.Contains(s, ".")
		},
		"rfc3339nano":         func(s string) bool { _, err := time.Parse(time.RFC3339Nano, s); return err == nil },
		"2006-01-02 15:04:05": func(s string) bool { _, err := time.Parse("2006-01-02 15:04:05", s); return err == nil },
	} {
		tf := logger.NewTimeFormat(config.Log{TimeFormat: format, UTC: true})
		if s := tf.String(time.Now()); !check(s) {
			t.Errorf("%s formatted as %q", format, s)
		}
	}
}

func TestFileOutputTextAndPretty(t *testing.T) {
	dir := t.TempDir()
	cfg := logConfig("info",
		config.LogOutput{Name: "text", Type: "file", Path: filepath.Join(dir, "text.log"), Format: "text", MaxSize: config.MiB},
		config.LogOutput{Name: "json", Type: "file", Path: filepath.Join(dir, "sub", "json.log"), MaxSize: config.MiB},
	)
	cfg.PrettyJSON = true
	cfg.StackLevel = "none"
	l, err := logger.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	l.Info("hello", "user", "ann")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	text, _ := os.ReadFile(filepath.Join(dir, "text.log"))
	if !strings.Contains(string(text), "level=INFO msg=hello user=ann") {
		t.Errorf("text log = %q", text)
	}
	pretty, _ := os.ReadFile(filepath.Join(dir, "sub", "json.log"))
	if !strings.Contains(string(pretty), "\n  \"msg\": \"hello\"") {
		t.Errorf("pretty log = %q", pretty)
	}

	cfg = logConfig("info", config.LogOutput{Name: "file", Path: filepath.Join(dir, "x.log", "\x00bad")})
	if _, err := logger.New(context.Background(), cfg); err == nil {
		t.Error("bad path should fail at startup")
	}
}

func TestOutputsErrors(t *testing.T) {
	_, err := logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "kafka"}))
	if err == nil || !strings.Contains(err.Error(), `unknown type "kafka"`) || !strings.Contains(err.Error(), "WithSink") {
		t.Errorf("unknown type: %v", err)
	}
	_, err = logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "otel", Type: "otlp"}))
	if err == nil || !strings.Contains(err.Error(), "logger/sink_otlp") {
		t.Errorf("otlp hint: %v", err)
	}
	_, err = logger.New(context.Background(), logConfig("loud"))
	if err == nil {
		t.Error("invalid config should fail")
	}

	// Custom types belong to the logger they are passed to: another logger,
	// even in the same process, does not see them.
	mem, _ := memOutput(t, "m", config.LogOutput{})
	if _, err := logger.New(context.Background(), logConfig("info", mem), withMem); err != nil {
		t.Fatal(err)
	}
	if _, err := logger.New(context.Background(), logConfig("info", mem)); err == nil || !strings.Contains(err.Error(), `unknown type "mem"`) {
		t.Errorf("a type passed to another logger leaked: %v", err)
	}
	// Built-in types are fixed, so a config means the same everywhere.
	_, err = logger.New(context.Background(), logConfig("info"), logger.WithSink("File", nil))
	if err == nil || !strings.Contains(err.Error(), `"file" is built in`) {
		t.Errorf("replacing a built-in type: %v", err)
	}

	outs, err := logger.OpenOutputs(context.Background(), logConfig("info",
		config.LogOutput{Name: "off", Type: "file", Disabled: true}), nil, nil)
	if err != nil || len(outs.List()) != 0 {
		t.Errorf("disabled output: %v %v", outs, err)
	}
	outs, err = logger.OpenOutputs(context.Background(), logConfig("info"), nil, nil)
	if err != nil || len(outs.List()) != 1 || outs.List()[0].Type != "stdout" {
		t.Errorf("default output: %v", err)
	}
}

type failingSink struct{}

func (failingSink) Write([]byte) (int, error) { return 0, errors.New("disk full") }
func (failingSink) Close() error              { return nil }

var withFailing = logger.WithSink("failing", func(context.Context, config.LogOutput, logger.SinkEnv) (logger.Sink, error) {
	return failingSink{}, nil
})

func TestWriteErrorsAreReported(t *testing.T) {
	var mu sync.Mutex
	var reported []string
	l, err := logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "f", Type: "failing"}), withFailing,
		logger.WithErrorHandler(func(output string, err error) {
			mu.Lock()
			defer mu.Unlock()
			reported = append(reported, output+": "+err.Error())
		}))
	if err != nil {
		t.Fatal(err)
	}
	l.Info("x")
	if len(reported) != 1 || reported[0] != "f: disk full" {
		t.Errorf("reported = %v", reported)
	}
}

type ctxKey struct{}

func TestContextAttrsAndReplace(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	l, err := logger.New(context.Background(), logConfig("info", out), withMem,
		logger.WithContextAttrs(func(ctx context.Context) []slog.Attr {
			if id, ok := ctx.Value(ctxKey{}).(string); ok {
				return []slog.Attr{slog.String("request_id", id)}
			}
			return nil
		}),
		logger.WithReplaceAttr(func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == "password" {
				a.Value = slog.StringValue("***")
			}
			return a
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), ctxKey{}, "r-1")
	l.InfoContext(ctx, "login", "password", "hunter2")
	l.With("a", 1).WithGroup("g").Info("grouped", "b", 2)
	recs := sink().decoded(t)
	if recs[0]["request_id"] != "r-1" || recs[0]["password"] != "***" {
		t.Errorf("record = %v", recs[0])
	}
	if g, ok := recs[1]["g"].(map[string]any); !ok || g["b"] != float64(2) || recs[1]["a"] != float64(1) {
		t.Errorf("grouped record = %v", recs[1])
	}
}

func TestParseRecord(t *testing.T) {
	for name, tc := range map[string]struct {
		in    string
		level slog.Level
		msg   string
		stack string
		time  bool
		field string
	}{
		"slog":    {`{"time":"2026-01-02T03:04:05.123Z","level":"WARN","msg":"m","source":{"file":"a.go"},"k":1}`, slog.LevelWarn, "m", "", true, "k"},
		"slog+":   {`{"level":"DEBUG-4","msg":"m"}`, logger.LevelTrace, "m", "", false, ""},
		"zap":     {`{"level":"error","time":1767323045.5,"msg":"m","stack":"s","caller":"a.go:1"}`, slog.LevelError, "m", "s", true, "caller"},
		"zerolog": {`{"level":"panic","message":"m","time":1767323045123,"stacktrace":"s"}`, logger.LevelFatal, "m", "s", true, ""},
	} {
		r, err := logger.ParseRecord([]byte(tc.in))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if r.Level != tc.level || r.Message != tc.msg || r.Stack != tc.stack || r.Time.IsZero() == tc.time {
			t.Errorf("%s: %+v", name, r)
		}
		if tc.field != "" && r.Fields[tc.field] == nil {
			t.Errorf("%s: field %s missing: %v", name, tc.field, r.Fields)
		}
		for _, k := range []string{"level", "msg", "message", "time", "stack", "stacktrace"} {
			if _, has := r.Fields[k]; has {
				t.Errorf("%s: %s should be removed from fields", name, k)
			}
		}
	}
	if _, err := logger.ParseRecord([]byte("level=INFO")); err == nil {
		t.Error("text should not parse")
	}
}

func TestLevelNames(t *testing.T) {
	for l, want := range map[slog.Level]string{
		logger.LevelTrace: "TRACE", logger.LevelTrace + 1: "TRACE+1", slog.LevelDebug: "DEBUG",
		slog.LevelError + 2: "ERROR+2", logger.LevelFatal: "FATAL",
	} {
		if got := logger.LevelName(l); got != want {
			t.Errorf("LevelName(%d) = %s, want %s", l, got, want)
		}
	}
	for s, want := range map[string]slog.Level{"TRACE": logger.LevelTrace, "warning": slog.LevelWarn, "": slog.LevelInfo, "Fatal": logger.LevelFatal} {
		if got, err := logger.ParseLevel(s); err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v", s, got, err)
		}
	}
}

func TestStackSkipping(t *testing.T) {
	s := logger.StackSkipping("github.com/linzeyan/loadconf/logger_test.helperFrame")
	if !strings.HasPrefix(s, "github.com/linzeyan/loadconf/logger_test.TestStackSkipping") {
		t.Errorf("stack = %s", s)
	}
	if s := helperFrame(); !strings.HasPrefix(s, "github.com/linzeyan/loadconf/logger_test.TestStackSkipping") {
		t.Errorf("helper frames should be skipped:\n%s", s)
	}
}

func helperFrame() string {
	return logger.StackSkipping("github.com/linzeyan/loadconf/logger_test.helperFrame")
}

func TestZeroConfigDefaults(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	var cfg config.Log // no defaults applied, as when built in code
	cfg.Outputs.Set(out.Name, out)
	l, err := logger.New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Debug("dropped")
	l.Info("json")
	l.Error("with stack")
	got := sink().decoded(t)
	if len(got) != 2 || got[0]["msg"] != "json" {
		t.Fatalf("records = %v", got)
	}
	if _, has := got[0]["stack"]; has {
		t.Error("an empty stack_level must mean error, not info")
	}
	if _, has := got[1]["stack"]; !has {
		t.Error("error records carry a stack by default")
	}
}
