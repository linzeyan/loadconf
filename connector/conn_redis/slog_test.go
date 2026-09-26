package conn_redis

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
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
	p := SlogLogger(jsonLogger(&buf, slog.LevelInfo), slog.LevelWarn)
	var _ = func() { redis.SetLogger(p) } // assignable to the go-redis logger
	p.Printf(context.Background(), "redis: pool %s: %d conns", "main", 3)
	p.Printf(nil, "no ctx") //nolint:staticcheck // go-redis may pass a nil context

	recs := records(t, &buf)
	if len(recs) != 2 || recs[0]["msg"] != "pool main: 3 conns" || recs[0]["level"] != "WARN" {
		t.Fatalf("records = %v", recs)
	}
	if !strings.HasSuffix(sourceFile(recs[0]), "slog_test.go") {
		t.Errorf("source = %v", recs[0]["source"])
	}

	buf.Reset()
	SlogLogger(jsonLogger(&buf, slog.LevelError), slog.LevelWarn).Printf(context.Background(), "dropped")
	if buf.Len() != 0 {
		t.Errorf("disabled level logged: %s", buf.String())
	}
}
