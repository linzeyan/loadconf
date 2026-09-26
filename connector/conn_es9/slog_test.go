package conn_es9

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

func decodeRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
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

func TestSlogLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	lg := SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	req := httptest.NewRequest(http.MethodGet, "https://user:secret@es:9200/_search?q=x", nil)

	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 200}, nil, time.Now(), 5*time.Millisecond)
	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 503}, nil, time.Now(), time.Millisecond)
	_ = lg.LogRoundTrip(req, &http.Response{}, errors.New("connection refused"), time.Now(), time.Millisecond)
	_ = lg.LogRoundTrip(nil, nil, errors.New("no connection"), time.Time{}, 0)
	if lg.RequestBodyEnabled() || lg.ResponseBodyEnabled() {
		t.Error("bodies must not be logged")
	}

	if strings.Contains(buf.String(), "secret") {
		t.Errorf("log leaks credentials: %s", buf.String())
	}
	recs := decodeRecords(t, &buf)
	if len(recs) != 4 {
		t.Fatalf("got %d records", len(recs))
	}
	for i, want := range []string{"DEBUG", "WARN", "ERROR", "ERROR"} {
		if recs[i]["level"] != want {
			t.Errorf("record %d level = %v, want %s", i, recs[i]["level"], want)
		}
	}
	if r := recs[0]; r["method"] != "GET" || r["url"] != "https://es:9200/_search?q=x" || r["status"] != float64(200) || r["duration"] == nil {
		t.Errorf("record = %v", r)
	}
	if _, ok := recs[2]["status"]; ok || recs[2]["error"] != "connection refused" {
		t.Errorf("error record = %v", recs[2])
	}

	// Disabled levels are skipped.
	buf.Reset()
	lg = SlogLogger(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	_ = lg.LogRoundTrip(req, &http.Response{StatusCode: 200}, nil, time.Now(), 0)
	if buf.Len() != 0 {
		t.Errorf("debug record written: %s", buf.String())
	}
}

func TestWithLogger(t *testing.T) {
	srv := httptest.NewServer(fakeES(nil, http.StatusOK, infoBody, true))
	defer srv.Close()
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	es, err := Open(context.Background(), config.Elasticsearch{Addresses: []string{srv.URL}}, WithLogger(l))
	if err != nil {
		t.Fatal(err)
	}
	defer es.Close(context.Background())
	recs := decodeRecords(t, &buf)
	if len(recs) != 1 || recs[0]["msg"] != "elasticsearch request" || recs[0]["url"] != srv.URL+"/" || recs[0]["status"] != float64(200) {
		t.Errorf("records = %v", recs)
	}
}
