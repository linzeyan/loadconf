package conn_kafka_sarama

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/IBM/sarama"
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
	var l sarama.StdLogger = SlogLogger(jsonLogger(&buf, slog.LevelDebug), slog.LevelInfo)
	l.Print("client/metadata ", "fetching")
	l.Printf("producer/broker/%d starting up\n", 1)
	l.Println("consumer", "closed")

	recs := records(t, &buf)
	if len(recs) != 3 {
		t.Fatalf("records = %v", recs)
	}
	for i, want := range []string{"client/metadata fetching", "producer/broker/1 starting up", "consumer closed"} {
		if recs[i]["msg"] != want || recs[i]["level"] != "INFO" {
			t.Errorf("record %d = %v", i, recs[i])
		}
	}
	if !strings.HasSuffix(sourceFile(recs[1]), "slog_test.go") {
		t.Errorf("source = %v", recs[1]["source"])
	}

	buf.Reset()
	SlogLogger(jsonLogger(&buf, slog.LevelInfo), slog.LevelDebug).Print("dropped")
	if buf.Len() != 0 {
		t.Errorf("disabled level logged: %s", buf.String())
	}
}
