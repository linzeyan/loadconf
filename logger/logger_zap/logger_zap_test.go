package logger_zap

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

	"go.uber.org/zap"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

type memSink struct {
	mu      sync.Mutex
	records [][]byte
}

var mems sync.Map

// markSink creates the file at its path when closed.
type markSink struct{ path string }

func (markSink) Write(b []byte) (int, error) { return len(b), nil }
func (m markSink) Close() error              { return os.WriteFile(m.path, nil, 0o600) }

// withMark and withMem pass the test sinks like any non-built-in output type.
var (
	withMark = WithSink("zapmark", func(_ context.Context, out config.LogOutput, _ logger.SinkEnv) (logger.Sink, error) {
		return markSink{out.Path}, nil
	})
	withMem = WithSink("zapmem", func(_ context.Context, out config.LogOutput, _ logger.SinkEnv) (logger.Sink, error) {
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

func (m *memSink) Close() error { return nil }

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

func outputs(outs ...config.LogOutput) config.Named[config.LogOutput] {
	n := config.Named[config.LogOutput]{}
	for _, o := range outs {
		n.Set(o.Name, o)
	}
	return n
}

func TestRecords(t *testing.T) {
	cfg := config.Log{
		Level: "debug", AddSource: true, StackLevel: "error", Service: "api", Fields: map[string]string{"env": "prod"},
		Outputs: outputs(
			config.LogOutput{Name: "all", Type: "zapmem", Options: map[string]any{"id": t.Name() + "all"}},
			config.LogOutput{Name: "warn", Type: "zapmem", Level: "warn", Options: map[string]any{"id": t.Name() + "warn"}},
		),
	}
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	l.Debug("debugging", zap.Int("n", 1))
	l.Info("hello", zap.String("user", "u1"), zap.Duration("took", 1500*time.Millisecond))
	l.Named("db").Error("failed", zap.Error(os.ErrNotExist))
	l.DPanic("odd")
	_ = l.Sync()

	all := mem(t, t.Name()+"all").decoded(t)
	if len(all) != 4 {
		t.Fatalf("all = %v", all)
	}
	r := all[1]
	if r["level"] != "INFO" || r["msg"] != "hello" || r["user"] != "u1" || r["took"] != float64(1500*time.Millisecond) ||
		r["service"] != "api" || r["env"] != "prod" {
		t.Errorf("info = %v", r)
	}
	if _, err := time.Parse(logger.RFC3339Milli, r["time"].(string)); err != nil {
		t.Errorf("time %v: %v", r["time"], err)
	}
	if src, _ := r["source"].(string); !strings.Contains(src, "logger_zap_test.go:") {
		t.Errorf("source = %v", r["source"])
	}
	if _, ok := r["stack"]; ok {
		t.Errorf("info must not carry a stack: %v", r)
	}
	e := all[2]
	if e["level"] != "ERROR" || e["logger"] != "db" || e["error"] != "file does not exist" ||
		!strings.Contains(e["stack"].(string), "logger_zap.TestRecords") {
		t.Errorf("error = %v", e)
	}
	if all[3]["level"] != "DPANIC" || all[0]["level"] != "DEBUG" {
		t.Errorf("levels = %v %v", all[0]["level"], all[3]["level"])
	}

	warn := mem(t, t.Name()+"warn").decoded(t)
	if len(warn) != 2 || warn[0]["msg"] != "failed" {
		t.Errorf("warn output = %v", warn)
	}

	// The records parse like the slog ones, which the network sinks rely on.
	m := mem(t, t.Name()+"all")
	rec, err := logger.ParseRecord(m.records[2])
	if err != nil || rec.Level != 8 || rec.Message != "failed" || rec.Stack == "" || rec.Time.IsZero() {
		t.Errorf("ParseRecord = %+v %v", rec, err)
	}
	if rec, _ := logger.ParseRecord(m.records[3]); rec.Level != logger.LevelFatal {
		t.Errorf("dpanic level = %v", rec.Level)
	}
}

func TestUpdateAndTimeFormat(t *testing.T) {
	id := t.Name()
	cfg := config.Log{Level: "warn", TimeFormat: "unixmilli", StackLevel: "none",
		Outputs: outputs(config.LogOutput{Name: "m", Type: "zapmem", Options: map[string]any{"id": id}})}
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Info("hidden")
	cfg.Level = "info"
	if err := l.Update(cfg); err != nil {
		t.Fatal(err)
	}
	l.Info("shown")
	l.Error("no stack")
	recs := mem(t, id).decoded(t)
	if len(recs) != 2 || recs[0]["msg"] != "shown" {
		t.Fatalf("records = %v", recs)
	}
	ms, ok := recs[0]["time"].(float64)
	if !ok || time.Since(time.UnixMilli(int64(ms))) > time.Minute {
		t.Errorf("time = %v", recs[0]["time"])
	}
	if _, ok := recs[1]["stack"]; ok || recs[1]["source"] != nil {
		t.Errorf("stack/source should be off: %v", recs[1])
	}
}

func TestTextFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	l, err := New(context.Background(), config.Log{Level: "info", Format: "text", Outputs: outputs(
		config.LogOutput{Name: "file", Path: path},
	)})
	if err != nil {
		t.Fatal(err)
	}
	l.Info("plain text", zap.Duration("took", 1500*time.Millisecond))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	line := string(b)
	if !strings.Contains(line, "\tINFO\tplain text\t") || !strings.Contains(line, `"took": "1.5s"`) {
		t.Errorf("line = %q", line)
	}
}

func TestErrors(t *testing.T) {
	if _, err := New(context.Background(), config.Log{Level: "loud"}); err == nil {
		t.Error("an invalid level should fail")
	}
	l, err := New(context.Background(), config.Log{Level: "info", Outputs: outputs(config.LogOutput{Name: "m", Type: "zapmem", Options: map[string]any{"id": t.Name()}})},
		withMem,
		WithErrorHandler(func(string, error) {}),
		WithZapOptions(zap.Fields(zap.String("extra", "1"))))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Info("x")
	if r := mem(t, t.Name()).decoded(t); r[0]["extra"] != "1" {
		t.Errorf("zap options not applied: %v", r)
	}
	if l.Outputs() == nil {
		t.Error("no outputs")
	}
}

func TestFatalClosesOutputs(t *testing.T) {
	if path := os.Getenv("ZAPLOGGER_FATAL"); path != "" {
		l, err := New(context.Background(), config.Log{Outputs: outputs(config.LogOutput{Name: "m", Type: "zapmark", Path: path})}, withMark)
		if err != nil {
			t.Fatal(err)
		}
		l.Fatal("bye")
		return
	}
	mark := filepath.Join(t.TempDir(), "closed")
	cmd := exec.Command(os.Args[0], "-test.run=^TestFatalClosesOutputs$")
	cmd.Env = append(os.Environ(), "ZAPLOGGER_FATAL="+mark)
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("exit = %v\n%s", err, out)
	}
	if _, err := os.Stat(mark); err != nil {
		t.Errorf("outputs not closed before exit: %v", err)
	}
}
