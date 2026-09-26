package logger_zap

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

func memConfig(id, level string) config.Log {
	return config.Log{Level: level, StackLevel: "error", Outputs: outputs(
		config.LogOutput{Name: "m", Type: "zapmem", Options: map[string]any{"id": id}},
	)}
}

// Every zap level, including the ones that panic or exit, must parse back
// to the slog level the outputs filter on, or the network sinks get the
// severity wrong. Writing to the core directly skips the panic and exit.
func TestEveryZapLevelParsesBack(t *testing.T) {
	l, err := New(context.Background(), memConfig(t.Name(), "debug"), withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	levels := []zapcore.Level{zapcore.DebugLevel, zapcore.InfoLevel, zapcore.WarnLevel, zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel}
	for _, lvl := range levels {
		if err := l.Core().Write(zapcore.Entry{Level: lvl, Time: time.Now(), Message: lvl.String()}, nil); err != nil {
			t.Fatal(err)
		}
	}
	recs := mem(t, t.Name()).records
	if len(recs) != len(levels) {
		t.Fatalf("%d records", len(recs))
	}
	for i, lvl := range levels {
		r, err := logger.ParseRecord(recs[i])
		if err != nil || r.Level != SlogLevel(lvl) || r.Message != lvl.String() {
			t.Errorf("%v: %s parsed as %+v (%v)", lvl, recs[i], r, err)
		}
	}
}

// zap closes open namespaces before it writes the stack, so the stack stays
// at the top level where ParseRecord looks for it (as after slog's
// WithGroup: TestWithGroupKeepsStackAndContextAttrsAtTopLevel in package
// logger).
func TestNamespaceKeepsStackAtTopLevel(t *testing.T) {
	l, err := New(context.Background(), memConfig(t.Name(), "info"), withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.With(zap.Namespace("req")).Error("failed", zap.String("path", "/x"))
	rec := mem(t, t.Name()).decoded(t)[0]
	if req, _ := rec["req"].(map[string]any); req["path"] != "/x" || rec["stack"] == nil {
		t.Errorf("record = %v", rec)
	}
}

// Hot reload calls Update while other goroutines log (run with -race).
func TestConcurrentLoggingDuringUpdate(t *testing.T) {
	cfg := memConfig(t.Name(), "info")
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	done := make(chan struct{})
	var updates, loggers sync.WaitGroup
	updates.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			c := cfg
			c.Level, c.StackLevel = "debug", "none"
			if i%2 == 1 {
				c.Level, c.StackLevel = "error", "error"
			}
			if err := l.Update(c); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for g := range 4 {
		loggers.Go(func() {
			for i := range 100 {
				l.Error("e", zap.Int("g", g), zap.Int("i", i))
				l.Info("i")
				l.Debug("d")
			}
		})
	}
	loggers.Wait()
	close(done)
	updates.Wait()

	cfg.Level = "error"
	if err := l.Update(cfg); err != nil {
		t.Fatal(err)
	}
	before := len(mem(t, t.Name()).decoded(t))
	l.Info("dropped")
	if after := len(mem(t, t.Name()).decoded(t)); after != before {
		t.Error("the last Update did not take effect")
	}
}

func TestCloseTwiceAndLogAfterClose(t *testing.T) {
	l, err := New(context.Background(), memConfig(t.Name(), "info"), withMem)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if err := l.Close(); err != nil {
			t.Errorf("Close #%d: %v", i+1, err)
		}
	}
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("logging after Close panicked: %v", p)
		}
	}()
	l.Info("after close")
	_ = l.Sync()
}
