package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeConn keeps each Write as one datagram or segment. Only the methods
// connWriter uses are implemented.
type fakeConn struct {
	net.Conn
	mu     sync.Mutex
	writes [][]byte
}

func (c *fakeConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes = append(c.writes, bytes.Clone(b))
	return len(b), nil
}

func (c *fakeConn) Close() error                     { return nil }
func (c *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func fakeWriter(c *fakeConn) *connWriter {
	return &connWriter{dial: func() (net.Conn, error) { return c, nil }}
}

// reassembleGELF checks the chunk headers of the datagrams of one chunked
// message and returns the payload they carry.
func reassembleGELF(datagrams [][]byte, chunkSize int) ([]byte, error) {
	if len(datagrams) < 2 || len(datagrams) > gelfMaxChunks {
		return nil, fmt.Errorf("%d chunks", len(datagrams))
	}
	var out []byte
	for i, d := range datagrams {
		switch {
		case len(d) > chunkSize:
			return nil, fmt.Errorf("chunk %d has %d bytes, more than %d", i, len(d), chunkSize)
		case len(d) < 12 || d[0] != 0x1e || d[1] != 0x0f:
			return nil, fmt.Errorf("chunk %d has no GELF chunk header: %x", i, d[:min(len(d), 12)])
		case !bytes.Equal(d[2:10], datagrams[0][2:10]):
			return nil, fmt.Errorf("chunk %d has another message id", i)
		case int(d[10]) != i || int(d[11]) != len(datagrams):
			return nil, fmt.Errorf("chunk %d is numbered %d of %d, sent %d", i, d[10], d[11], len(datagrams))
		}
		out = append(out, d[12:]...)
	}
	return out, nil
}

// A datagram at exactly the chunk size is sent whole, one byte more is
// chunked, and 128 chunks is the protocol limit: beyond it the message is
// refused before anything is sent (a receiver would wait for the rest).
func TestGELFChunkBoundaries(t *testing.T) {
	const size = 100 // 88 bytes of payload per chunk
	for _, tc := range []struct {
		n      int
		chunks int // 0: sent unchunked
		fail   bool
	}{
		{1, 0, false},
		{size, 0, false},
		{size + 1, 2, false},
		{88 * 2, 2, false},
		{88*2 + 1, 3, false},
		{88 * gelfMaxChunks, gelfMaxChunks, false},
		{88*gelfMaxChunks + 1, 0, true},
	} {
		msg := make([]byte, tc.n)
		for i := range msg {
			msg[i] = byte(i * 7)
		}
		c := &fakeConn{}
		err := (&gelfSink{w: fakeWriter(c), udp: true, chunkSize: size}).writeUDP(msg)
		switch {
		case tc.fail:
			if err == nil || !strings.Contains(err.Error(), "129 chunks (max 128)") || len(c.writes) != 0 {
				t.Errorf("%d bytes: err = %v, %d datagrams sent", tc.n, err, len(c.writes))
			}
			continue
		case err != nil:
			t.Errorf("%d bytes: %v", tc.n, err)
			continue
		case tc.chunks == 0:
			if len(c.writes) != 1 || !bytes.Equal(c.writes[0], msg) {
				t.Errorf("%d bytes: want one plain datagram, got %d", tc.n, len(c.writes))
			}
			continue
		case len(c.writes) != tc.chunks:
			t.Errorf("%d bytes: %d chunks, want %d", tc.n, len(c.writes), tc.chunks)
		}
		if got, err := reassembleGELF(c.writes, size); err != nil || !bytes.Equal(got, msg) {
			t.Errorf("%d bytes: reassembled %d bytes (%v)", tc.n, len(got), err)
		}
	}
}

func TestParseIndexTemplates(t *testing.T) {
	at := time.Date(2026, 1, 2, 23, 30, 0, 0, time.FixedZone("TPE", 8*3600)) // 2026-01-02 15:30 UTC
	for tmpl, want := range map[string]string{
		"logs-{service}-default":         "logs-my-api-default",
		"App-{date:2006.01.02}":          "app-2026.01.02", // the UTC date, lower case
		"{date:2006}{date:01}":           "202601",
		"{service}{service}":             "my-apimy-api",
		"a}b":                            "a}b",
		"{date:Jan}-{date:Monday}":       "jan-friday",
		"{date:{}":                       "{", // odd, but a valid layout of literal text
		"logs-{service}-{date:15:04:05}": "logs-my-api-15:30:00",
	} {
		parts, err := parseIndex(tmpl, "My-API")
		if err != nil {
			t.Errorf("%q: %v", tmpl, err)
			continue
		}
		if got := formatIndex(parts, at); got != want {
			t.Errorf("%q = %q, want %q", tmpl, got, want)
		}
	}
	for tmpl, msg := range map[string]string{
		"{":           "unterminated",
		"logs-{date:": "unterminated",
		"{}":          "unknown placeholder {}",
		"{date:}":     "unknown placeholder {date:}",
		"{Service}":   "unknown placeholder {Service}",
		"{{service}}": "unknown placeholder {{service}",
		"{ service }": "unknown placeholder { service }",
	} {
		if _, err := parseIndex(tmpl, "svc"); err == nil || !strings.Contains(err.Error(), msg) {
			t.Errorf("%q: err = %v, want %q", tmpl, err, msg)
		}
	}
}

// Each record becomes one NDJSON document line with a top-level @timestamp,
// indexed by the record's own date.
func TestElasticsearchWriteDocShapes(t *testing.T) {
	index, _ := parseIndex("logs-{date:2006.01.02}", "svc")
	for _, tc := range []struct{ in, index, check string }{
		{`{}`, "", `{"@timestamp":`},
		{"{ }\n", "", `{"@timestamp":`},
		{`{"a":1}`, "", `,"a":1}`},
		{`{"time":"2026-01-02T01:00:00+08:00","msg":"m"}`, "logs-2026.01.01", `"msg":"m"`},
		{`{"@timestamp":"2020-05-06T07:08:09Z","a":1}`, "logs-2020.05.06", `{"@timestamp":"2020-05-06T07:08:09Z","a":1}`},
		{`{"host":"web-1","a":1}`, "", `"host":{"name":"web-1"}`},
		{`{"a":12345678901234567890123}`, "", `"a":12345678901234567890123`},
	} {
		s := &esSink{index: index, queue: make(chan esDoc, 1)}
		if n, err := s.Write([]byte(tc.in)); err != nil || n != len(tc.in) {
			t.Errorf("%s: %d, %v", tc.in, n, err)
			continue
		}
		d := <-s.queue
		var doc map[string]any
		if err := json.Unmarshal(d.doc, &doc); err != nil || doc["@timestamp"] == nil || bytes.ContainsAny(d.doc, "\r\n") {
			t.Errorf("%s: document %s (%v)", tc.in, d.doc, err)
		}
		if !bytes.Contains(d.doc, []byte(tc.check)) || tc.index != "" && d.index != tc.index {
			t.Errorf("%s: document %s in %s", tc.in, d.doc, d.index)
		}
	}
	for _, in := range []string{``, `[1]`, `null`, `x`} {
		s := &esSink{index: index, queue: make(chan esDoc, 1)}
		if _, err := s.Write([]byte(in)); err == nil || len(s.queue) != 0 {
			t.Errorf("%q: err = %v, %d queued", in, err, len(s.queue))
		}
	}
	s := &esSink{index: index, queue: make(chan esDoc, 1), closed: true}
	if _, err := s.Write([]byte(`{}`)); err == nil {
		t.Error("a closed sink should refuse records")
	}
}
