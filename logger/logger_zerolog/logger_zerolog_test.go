package logger_zerolog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

type memSink struct {
	mu      sync.Mutex
	records [][]byte
	closed  bool
}

var mems sync.Map

// markSink creates the file at its path when closed.
type markSink struct{ path string }

func (markSink) Write(b []byte) (int, error) { return len(b), nil }
func (m markSink) Close() error              { return os.WriteFile(m.path, nil, 0o600) }

// withMark and withMem pass the test sinks like any non-built-in output type.
var (
	withMark = WithSink("zerologmark", func(_ context.Context, out config.LogOutput, _ logger.SinkEnv) (logger.Sink, error) {
		return markSink{out.Path}, nil
	})
	withMem = WithSink("zerologmem", func(_ context.Context, out config.LogOutput, _ logger.SinkEnv) (logger.Sink, error) {
		s := &memSink{}
		mems.Store(out.Options["id"], s)
		return s, nil
	})
)

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
	var out []map[string]any
	for _, b := range m.records {
		var r map[string]any
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatalf("decode %s: %v", b, err)
		}
		out = append(out, r)
	}
	return out
}

func mem(t *testing.T, id string) *memSink {
	t.Helper()
	s, ok := mems.Load(id)
	if !ok {
		t.Fatalf("sink %s not opened", id)
	}
	return s.(*memSink)
}

func memOut(name, id, level string) config.LogOutput {
	return config.LogOutput{Name: name, Type: "zerologmem", Level: level, Options: map[string]any{"id": id}}
}

func outputs(outs ...config.LogOutput) config.Named[config.LogOutput] {
	n := config.Named[config.LogOutput]{}
	for _, o := range outs {
		n.Set(o.Name, o)
	}
	return n
}

func TestRecords(t *testing.T) {
	cfg := config.Log{
		Level: "trace", AddSource: true, StackLevel: "error", Service: "api", Fields: map[string]string{"env": "prod"},
		Outputs: outputs(memOut("all", t.Name()+"all", ""), memOut("warn", t.Name()+"warn", "warn")),
	}
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	l.Trace().Msg("tracing")
	l.Info().Str("user", "u1").Msg("hello")
	l.Error().Err(errors.New("boom")).Msg("failed")

	all := mem(t, t.Name()+"all").decoded(t)
	if len(all) != 3 {
		t.Fatalf("all = %v", all)
	}
	r := all[1]
	if r["level"] != "info" || r["message"] != "hello" || r["user"] != "u1" || r["service"] != "api" || r["env"] != "prod" {
		t.Errorf("info = %v", r)
	}
	if _, err := time.Parse(logger.RFC3339Milli, r["time"].(string)); err != nil {
		t.Errorf("time %v: %v", r["time"], err)
	}
	if src, _ := r["source"].(string); !strings.Contains(src, "logger_zerolog_test.go:") {
		t.Errorf("source = %v", r["source"])
	}
	if _, ok := r["stack"]; ok {
		t.Errorf("info must not carry a stack: %v", r)
	}
	e := all[2]
	stack, _ := e["stack"].(string)
	if e["error"] != "boom" || !strings.HasPrefix(stack, "github.com/linzeyan/loadconf/logger/logger_zerolog.TestRecords") || strings.Contains(stack, "rs/zerolog") {
		t.Errorf("error = %v\nstack:\n%s", e, stack)
	}
	if all[0]["level"] != "trace" {
		t.Errorf("trace = %v", all[0])
	}

	warn := mem(t, t.Name()+"warn").decoded(t)
	if len(warn) != 1 || warn[0]["message"] != "failed" {
		t.Errorf("warn output = %v", warn)
	}

	// The records parse like the slog ones, which the network sinks rely on.
	rec, err := logger.ParseRecord(mem(t, t.Name()+"all").records[2])
	if err != nil || rec.Level != 8 || rec.Message != "failed" || rec.Stack == "" || rec.Time.IsZero() {
		t.Errorf("ParseRecord = %+v %v", rec, err)
	}
	if rec, _ := logger.ParseRecord(mem(t, t.Name()+"all").records[0]); rec.Level != logger.LevelTrace {
		t.Errorf("trace level = %v", rec.Level)
	}
}

func TestUpdateSamplingAndTime(t *testing.T) {
	id := t.Name()
	cfg := config.Log{Level: "warn", TimeFormat: "unix", StackLevel: "none", Outputs: outputs(memOut("m", id, ""))}
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	built := false
	l.Info().Func(func(*zerolog.Event) { built = true }).Msg("hidden")
	if built {
		t.Error("a disabled record should be dropped before it is built")
	}
	cfg.Level = "info"
	if err := l.Update(cfg); err != nil {
		t.Fatal(err)
	}
	l.Info().Msg("shown")
	l.Error().Msg("no stack")
	recs := mem(t, id).decoded(t)
	if len(recs) != 2 || recs[0]["message"] != "shown" {
		t.Fatalf("records = %v", recs)
	}
	sec, ok := recs[0]["time"].(float64)
	if !ok || time.Since(time.Unix(int64(sec), 0)) > time.Minute {
		t.Errorf("time = %v", recs[0]["time"])
	}
	if _, ok := recs[1]["stack"]; ok || recs[1]["source"] != nil {
		t.Errorf("stack/source should be off: %v", recs[1])
	}
}

func TestTextFileAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	l, err := New(context.Background(), config.Log{Format: "text", Outputs: outputs(
		config.LogOutput{Name: "file", Path: path},
		memOut("m", t.Name(), ""),
	)}, withMem)
	if err != nil {
		t.Fatal(err)
	}
	l.Info().Int("port", 80).Msg("plain text")
	// zerolog's Fatal closes the writer before exiting; that closes the outputs.
	if c, ok := NewWriter(l.Outputs()).(interface{ Close() error }); !ok || c.Close() != nil {
		t.Fatal("the writer must close the outputs")
	}
	if !mem(t, t.Name()).closed {
		t.Error("outputs not closed")
	}
	b, _ := os.ReadFile(path)
	line := string(b)
	if !strings.Contains(line, "INF plain text port=80") || strings.Contains(line, "{") {
		t.Errorf("line = %q", line)
	}
	if _, err := time.Parse(logger.RFC3339Milli, strings.Fields(line)[0]); err != nil {
		t.Errorf("time in %q: %v", line, err)
	}
}

func TestLevelMapping(t *testing.T) {
	for zl, want := range map[zerolog.Level]string{
		zerolog.TraceLevel: "TRACE", zerolog.DebugLevel: "DEBUG", zerolog.InfoLevel: "INFO", zerolog.NoLevel: "INFO",
		zerolog.WarnLevel: "WARN", zerolog.ErrorLevel: "ERROR", zerolog.FatalLevel: "FATAL", zerolog.PanicLevel: "FATAL",
	} {
		if got := logger.LevelName(SlogLevel(zl)); got != want {
			t.Errorf("%v -> %s, want %s", zl, got, want)
		}
	}
	if _, err := New(context.Background(), config.Log{Level: "loud"}); err == nil {
		t.Error("an invalid level should fail")
	}
}

func TestFatalClosesOutputs(t *testing.T) {
	if path := os.Getenv("ZEROLOGGER_FATAL"); path != "" {
		l, err := New(context.Background(), config.Log{Outputs: outputs(config.LogOutput{Name: "m", Type: "zerologmark", Path: path})}, withMark)
		if err != nil {
			t.Fatal(err)
		}
		l.Fatal().Msg("bye")
		return
	}
	mark := filepath.Join(t.TempDir(), "closed")
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalClosesOutputs$")
	cmd.Env = append(os.Environ(), "ZEROLOGGER_FATAL="+mark)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("exit = %v\n%s", err, out)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Errorf("outputs not closed before exit: %v", err)
	}
}
