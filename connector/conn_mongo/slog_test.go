package conn_mongo

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/linzeyan/loadconf/config"
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

func TestSlogSink(t *testing.T) {
	var buf bytes.Buffer
	s := SlogSink(jsonLogger(&buf, slog.LevelDebug))
	s.Info(1, "Command started", "commandName", "find", "requestId", 7)
	s.Info(2, "Connection checked out", "serverHost", "db1")
	s.Error(errors.New("boom"), "Server heartbeat failed", "serverHost", "db1")

	recs := records(t, &buf)
	if len(recs) != 3 {
		t.Fatalf("records = %v", recs)
	}
	if r := recs[0]; r["level"] != "INFO" || r["msg"] != "Command started" || r["commandName"] != "find" || r["requestId"] != float64(7) {
		t.Errorf("info = %v", r)
	}
	if recs[1]["level"] != "DEBUG" || recs[2]["level"] != "ERROR" || recs[2]["error"] != "boom" {
		t.Errorf("records = %v", recs)
	}
	if !strings.HasSuffix(sourceFile(recs[0]), "slog_test.go") {
		t.Errorf("source = %v", recs[0]["source"])
	}
}

func TestWithLogger(t *testing.T) {
	var buf bytes.Buffer
	opts, err := ClientOptions(config.Mongo{Hosts: []string{"db1:27017"}})
	if err != nil {
		t.Fatal(err)
	}
	WithLogger(jsonLogger(&buf, slog.LevelInfo), options.LogLevelInfo)(opts)
	lo := opts.LoggerOptions
	if lo == nil || lo.Sink == nil || lo.ComponentLevels[options.LogComponentAll] != options.LogLevelInfo {
		t.Fatalf("logger options = %+v", lo)
	}
}
