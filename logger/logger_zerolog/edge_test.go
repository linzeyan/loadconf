package logger_zerolog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// zerolog's RawJSON writes its argument verbatim, and pretty-printed JSON
// (an upstream response, a config dump) contains newlines. The
// Elasticsearch sink copies the record's members into the NDJSON bulk body,
// and RFC 3164 over TCP frames by newline, so such a record would split into
// several lines: Elasticsearch rejects the whole bulk request (every record
// of the batch is lost) and the syslog receiver sees fragments. The writer
// compacts the record to one line first.
func TestRawJSONWithNewlinesStaysOneLine(t *testing.T) {
	raw := []byte("{\n  \"status\": \"ok\"\n}")

	t.Run("elasticsearch bulk", func(t *testing.T) {
		var (
			mu   sync.Mutex
			body []byte
		)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			mu.Lock()
			body = append(body, b...)
			mu.Unlock()
			_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
		}))
		defer srv.Close()
		l, err := New(context.Background(), config.Log{Outputs: outputs(config.LogOutput{
			Name: "es", Type: "elasticsearch", Addresses: []string{srv.URL}, FlushInterval: time.Hour, Timeout: 5 * time.Second,
		})})
		if err != nil {
			t.Fatal(err)
		}
		l.Info().RawJSON("response", raw).Msg("upstream answered")
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		lines := strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
		if len(lines) != 2 {
			t.Errorf("one record became %d NDJSON lines:\n%s", len(lines), body)
		}
		for i, line := range lines {
			if !json.Valid([]byte(line)) {
				t.Errorf("line %d is not JSON: %s", i+1, line)
			}
		}
	})

	t.Run("syslog rfc3164 tcp", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		received := make(chan []byte, 1)
		go func() {
			c, err := ln.Accept()
			if err != nil {
				received <- nil
				return
			}
			defer c.Close()
			_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
			b, _ := io.ReadAll(c)
			received <- b
		}()
		l, err := New(context.Background(), config.Log{Outputs: outputs(config.LogOutput{
			Name: "syslog", Network: "tcp", Addr: ln.Addr().String(), SyslogFormat: "rfc3164", Timeout: time.Second,
		})})
		if err != nil {
			t.Fatal(err)
		}
		l.Info().RawJSON("response", raw).Msg("upstream answered")
		_ = l.Close()
		data := <-received
		if n := bytes.Count(data, []byte("\n")); n != 1 {
			t.Errorf("one record became %d newline-framed messages: %q", n, data)
		}
	})
}

// Log() records have no level; they go where info records go, and parse as
// info.
func TestNoLevelRecordsFollowInfo(t *testing.T) {
	id := t.Name()
	cfg := config.Log{Level: "warn", Outputs: outputs(memOut("m", id, ""))}
	l, err := New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Log().Msg("hidden")
	cfg.Level = "info"
	if err := l.Update(cfg); err != nil {
		t.Fatal(err)
	}
	l.Log().Msg("shown")
	recs := mem(t, id).records
	if len(recs) != 1 {
		t.Fatalf("records = %q", recs)
	}
	if r, err := logger.ParseRecord(recs[0]); err != nil || r.Level != slog.LevelInfo || r.Message != "shown" {
		t.Errorf("%s parsed as %+v (%v)", recs[0], r, err)
	}
}

// zerolog writes its own message as "message", after the fields, so a field
// named "msg" (say, an upstream's error message) comes first. Syslog, GELF
// and OTLP take the message from ParseRecord: they must send the record's
// own, not the field's, and keep the field.
func TestMsgFieldIsNotTheMessage(t *testing.T) {
	id := t.Name()
	l, err := New(context.Background(), config.Log{Outputs: outputs(memOut("m", id, ""))}, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Warn().Str("msg", "upstream said no").Msg("request failed")
	l.Log().Str("msg", "upstream said no").Msg("request failed")
	recs := mem(t, id).records
	if len(recs) != 2 {
		t.Fatalf("records = %q", recs)
	}
	for _, rec := range recs {
		r, err := logger.ParseRecord(rec)
		if err != nil || r.Message != "request failed" || r.Fields["msg"] != "upstream said no" {
			t.Errorf("%s parsed as message %q, fields %v (%v)", rec, r.Message, r.Fields, err)
		}
	}
}

// Hot reload calls Update while other goroutines log (run with -race).
func TestConcurrentLoggingDuringUpdate(t *testing.T) {
	id := t.Name()
	cfg := config.Log{Level: "info", StackLevel: "error", AddSource: true, Outputs: outputs(memOut("m", id, ""))}
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
			c.Level, c.StackLevel = "trace", "none"
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
				l.Error().Int("g", g).Int("i", i).Msg("e")
				l.Info().Msg("i")
				l.Trace().Msg("t")
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
	before := len(mem(t, id).decoded(t))
	l.Info().Msg("dropped")
	if after := len(mem(t, id).decoded(t)); after != before {
		t.Error("the last Update did not take effect")
	}
}

func TestCloseTwiceAndLogAfterClose(t *testing.T) {
	l, err := New(context.Background(), config.Log{Outputs: outputs(memOut("m", t.Name(), ""))}, withMem)
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
	l.Info().Msg("after close")
}
