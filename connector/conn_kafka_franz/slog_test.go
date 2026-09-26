package conn_kafka_franz

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func jsonLogger(buf *bytes.Buffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level, AddSource: true}))
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(buf.String()) {
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

func sourceFile(r map[string]any) string {
	src, _ := r["source"].(map[string]any)
	file, _ := src["file"].(string)
	return file
}

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	var lv slog.LevelVar
	lv.Set(slog.LevelWarn)
	l := SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: &lv, AddSource: true})))
	if l.Level() != kgo.LogLevelWarn {
		t.Errorf("level = %v", l.Level())
	}
	lv.Set(slog.LevelDebug)
	if l.Level() != kgo.LogLevelDebug {
		t.Errorf("level after change = %v", l.Level())
	}
	lv.Set(slog.LevelError + 1)
	if l.Level() != kgo.LogLevelNone {
		t.Errorf("level above error = %v", l.Level())
	}

	lv.Set(slog.LevelInfo)
	l.Log(kgo.LogLevelInfo, "metadata update", "broker", 1, "topic", "t")
	l.Log(kgo.LogLevelDebug, "dropped")
	l.Log(kgo.LogLevelError, "fatal", "err", "boom")
	recs := records(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("records = %v", recs)
	}
	if r := recs[0]; r["level"] != "INFO" || r["msg"] != "metadata update" || r["broker"] != float64(1) || r["topic"] != "t" {
		t.Errorf("info = %v", r)
	}
	if recs[1]["level"] != "ERROR" || !strings.HasSuffix(sourceFile(recs[1]), "slog_test.go") {
		t.Errorf("error = %v", recs[1])
	}
	if WithLogger(nil) == nil {
		t.Error("WithLogger returned nil")
	}
}
