package conn_mqtt

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

func TestSetLogger(t *testing.T) {
	if os.Getenv("CONN_MQTT_SETLOGGER_CHILD") == "" {
		// paho offers no way to wait for its goroutines: after Open gives up
		// on ctx or Disconnect returns they keep reading the global loggers,
		// which SetLogger writes. Run in a child process so that no client
		// of another test can race with this one.
		cmd := exec.Command(os.Args[0], "-test.run=^TestSetLogger$", "-test.count=1")
		cmd.Env = append(os.Environ(), "CONN_MQTT_SETLOGGER_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v\n%s", err, out)
		}
		return
	}
	var buf bytes.Buffer
	restore := SetLogger(jsonLogger(&buf, slog.LevelInfo))
	if _, noop := mqtt.DEBUG.(mqtt.NOOPLogger); !noop {
		t.Error("DEBUG must stay off when debug is disabled")
	}
	mqtt.ERROR.Println("[client]", "connection lost")
	mqtt.CRITICAL.Printf("[net] %s", "fatal")
	mqtt.WARN.Println("[store] memorystore wiped")
	restore()
	if _, noop := mqtt.ERROR.(mqtt.NOOPLogger); !noop {
		t.Error("restore did not reset ERROR")
	}

	recs := records(t, &buf)
	if len(recs) != 3 {
		t.Fatalf("records = %v", recs)
	}
	for i, want := range []struct{ level, msg string }{
		{"ERROR", "[client] connection lost"}, {"ERROR", "[net] fatal"}, {"WARN", "[store] memorystore wiped"},
	} {
		if recs[i]["level"] != want.level || recs[i]["msg"] != want.msg {
			t.Errorf("record %d = %v", i, recs[i])
		}
	}
	if !strings.HasSuffix(sourceFile(recs[0]), "slog_test.go") {
		t.Errorf("source = %v", recs[0]["source"])
	}

	restore = SetLogger(jsonLogger(&buf, slog.LevelDebug))
	defer restore()
	if _, noop := mqtt.DEBUG.(mqtt.NOOPLogger); noop {
		t.Error("DEBUG should be installed when debug is enabled")
	}
}
