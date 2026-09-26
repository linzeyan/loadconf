package logger_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// errorLog collects what outputs report to the error handler.
type errorLog struct {
	mu   sync.Mutex
	errs []string
}

func (e *errorLog) handler() logger.Option {
	return logger.WithErrorHandler(func(out string, err error) {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.errs = append(e.errs, out+": "+err.Error())
	})
}

func (e *errorLog) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.errs)
}

// rawBulkServer is a fake Elasticsearch bulk endpoint that keeps each request
// body as sent, so tests can check the NDJSON itself rather than what a
// lenient decoder makes of it. A non-nil gate holds every response until it
// is closed.
func rawBulkServer(t *testing.T, gate <-chan struct{}) (string, func() [][]byte) {
	t.Helper()
	var (
		mu     sync.Mutex
		bodies [][]byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			body = zr
		}
		b, _ := io.ReadAll(body)
		if gate != nil {
			<-gate
		}
		mu.Lock()
		bodies = append(bodies, b)
		mu.Unlock()
		_, _ = io.WriteString(w, `{"errors":false,"items":[]}`)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, func() [][]byte {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(bodies)
	}
}

// bulkDocs returns the document lines (every second line) of bulk bodies.
func bulkDocs(bodies [][]byte) [][]byte {
	var docs [][]byte
	for _, b := range bodies {
		lines := bytes.Split(bytes.TrimSuffix(b, []byte("\n")), []byte("\n"))
		for i := 1; i < len(lines); i += 2 {
			docs = append(docs, lines[i])
		}
	}
	return docs
}

// topLevelDuplicates returns the keys that occur more than once at the top
// level of a JSON object.
func topLevelDuplicates(t *testing.T, doc []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(doc))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		t.Fatalf("not an object: %s", doc)
	}
	seen := map[string]bool{}
	var dups []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatalf("%s: %v", doc, err)
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatalf("%s: %v", doc, err)
		}
		key := tok.(string)
		if seen[key] {
			dups = append(dups, key)
		}
		seen[key] = true
	}
	return dups
}

// ---------------------------------------------------------------------------
// Fixed bugs.

// Structured outputs (syslog, GELF, Elasticsearch, OTLP) take a record's
// level, message and time from ParseRecord. slog writes these keys before
// the attrs and does not deduplicate, so an ordinary attr of the same name
// follows them; were it to win, an ERROR carrying a numeric "level" attr
// would reach Graylog and syslog as INFO, and a "time" duration would move
// the record to 2017 (and into that day's ES index).
func TestRecordMetadataWinsOverSameNamedAttrs(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	l, err := logger.New(context.Background(), logConfig("debug", out), withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Error("disk failed", "level", 3)
	l.Info("login", "msg", "user supplied")
	l.Info("request", "time", 1500*time.Millisecond)

	recs := sink().records
	for i, want := range []struct {
		level slog.Level
		msg   string
	}{{slog.LevelError, "disk failed"}, {slog.LevelInfo, "login"}, {slog.LevelInfo, "request"}} {
		r, err := logger.ParseRecord(recs[i])
		if err != nil {
			t.Fatal(err)
		}
		if r.Level != want.level || r.Message != want.msg || time.Since(r.Time).Abs() > time.Minute {
			t.Errorf("record %s parsed as level=%v msg=%q time=%v", recs[i], r.Level, r.Message, r.Time)
		}
	}
}

// rootHandler adds the context attrs (e.g. sink_otlp.TraceAttrs) and the
// stack to the record. After WithGroup they must stay at the top level,
// where ParseRecord finds the stack (GELF full_message, OTLP
// code.stacktrace) and sink_otlp the trace_id and span_id; otherwise every
// grouped logger loses its trace correlation. Only the call's own attrs
// belong in the group.
func TestWithGroupKeepsStackAndContextAttrsAtTopLevel(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	l, err := logger.New(context.Background(), logConfig("info", out), withMem,
		logger.WithContextAttrs(func(context.Context) []slog.Attr {
			return []slog.Attr{slog.String("request_id", "r-1")}
		}))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.WithGroup("req").ErrorContext(context.Background(), "failed", "path", "/x")
	rec := sink().decoded(t)[0]
	if g, _ := rec["req"].(map[string]any); g["path"] != "/x" {
		t.Errorf("the call's own attrs belong in the group: %v", rec)
	}
	if rec["request_id"] != "r-1" {
		t.Errorf("context attr not at the top level: %v", rec)
	}
	if _, ok := rec["stack"]; !ok {
		t.Errorf("stack not at the top level: %v", rec)
	}
	// Attrs added after a group stay in it, as slog nests them.
	l.WithGroup("req").With("id", 7).WithGroup("q").Info("nested", "k", "v")
	rec = sink().decoded(t)[1]
	req, _ := rec["req"].(map[string]any)
	if q, _ := req["q"].(map[string]any); req["id"] != float64(7) || q["k"] != "v" || rec["request_id"] != "r-1" {
		t.Errorf("nested groups: %v", rec)
	}
}

// Data streams reject a document without a top-level @timestamp. A nested
// "@timestamp" key, as in a logged Beats event or ES document, is not one, so
// the sink still adds its own.
func TestElasticsearchTopLevelTimestampDespiteNestedOne(t *testing.T) {
	url, bodies := rawBulkServer(t, nil)
	l := netLogger(t, config.LogOutput{Type: "elasticsearch", Addresses: []string{url}, FlushInterval: time.Hour, Timeout: 5 * time.Second})
	l.Info("event received", "event", map[string]any{"@timestamp": "2020-01-01T00:00:00Z", "action": "login"})
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	docs := bulkDocs(bodies())
	if len(docs) != 1 {
		t.Fatalf("docs = %q", docs)
	}
	var doc map[string]any
	if err := json.Unmarshal(docs[0], &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["@timestamp"]; !ok {
		t.Errorf("document without a top-level @timestamp: %s", docs[0])
	}
}

// Elasticsearch rejects a document with a duplicate key ("Duplicate field",
// strict duplicate detection is on since 6.0), and the record is lost as "1
// of N records rejected". slog does not deduplicate, so the static service
// field plus an attr of the same name, or l.With("user", a).Info(..., "user",
// b), yields such a record; the sink keeps one member per key.
func TestElasticsearchDocHasNoDuplicateKeys(t *testing.T) {
	url, bodies := rawBulkServer(t, nil)
	l := netLogger(t, config.LogOutput{Type: "elasticsearch", Addresses: []string{url}, FlushInterval: time.Hour, Timeout: 5 * time.Second})
	l.Info("charged", "service", "billing")
	l.With("user", "a").Info("switched", "user", "b")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, doc := range bulkDocs(bodies()) {
		if dups := topLevelDuplicates(t, doc); len(dups) > 0 {
			t.Errorf("duplicate keys %v in %s", dups, doc)
		}
	}
}

// config.LogOutput.Validate only rejects a negative chunk_size, but
// gelfSink.writeUDP subtracts the 12-byte chunk header from it: chunk_size 12
// would divide by zero and panic inside the caller's log statement, a
// smaller one yield a negative chunk count and drop every record without an
// error. New rejects such sizes (or the record is sent or reported).
func TestGELFRejectsChunkSizeAtMost12(t *testing.T) {
	for _, size := range []int{12, 5} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			c := udpListener(t)
			out := config.LogOutput{Name: "gelf", Addr: c.LocalAddr().String(), ChunkSize: size, Timeout: time.Second}
			var errs errorLog
			l, err := logger.New(context.Background(), logConfig("info", out), errs.handler())
			if err != nil {
				return // rejected up front
			}
			defer l.Close()
			func() {
				defer func() {
					if p := recover(); p != nil {
						t.Errorf("logging panicked: %v", p)
					}
				}()
				l.Info("hello")
			}()
			_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
			if _, _, err := c.ReadFromUDP(make([]byte, 65536)); err != nil && len(errs.all()) == 0 {
				t.Error("the record was neither sent nor reported")
			}
		})
	}
}

// New is meant for configs built in code too (see TestZeroConfigDefaults),
// which the config loader never validated, so OpenOutputs validates each
// output itself. An output that config.LogOutput.Validate rejects would
// otherwise open: a file output without a path writes to
// $TMPDIR/<exe>-lumberjack.log, a negative queue_size panics in New, and
// Elasticsearch without addresses opens a sink that crashes later
// (TestElasticsearchWithoutAddressesFailsInNew).
func TestOpenOutputsValidatesOutputs(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir()) // where lumberjack's fallback file lands
	for name, out := range map[string]config.LogOutput{
		"file without path":               {Type: "file"},
		"gelf without addr":               {Type: "gelf"},
		"negative gelf chunk size":        {Type: "gelf", Addr: "127.0.0.1:1", ChunkSize: -1},
		"elasticsearch without addresses": {Type: "elasticsearch"},
		"negative queue size":             {Type: "elasticsearch", Addresses: []string{"http://127.0.0.1:1"}, QueueSize: -1},
	} {
		t.Run(name, func(t *testing.T) {
			out.Name = "out"
			if out.Validate() == nil {
				t.Fatal("premise: config.LogOutput.Validate rejects this output")
			}
			var l *logger.Logger
			err := func() (err error) {
				defer func() {
					if p := recover(); p != nil {
						err = fmt.Errorf("panic: %v", p)
					}
				}()
				l, err = logger.New(context.Background(), logConfig("info", out))
				return err
			}()
			switch {
			case err == nil:
				_ = l.Close() // nothing logged, so nothing flushes
				t.Error("New accepted an output that config.LogOutput.Validate rejects")
			case strings.HasPrefix(err.Error(), "panic"):
				t.Errorf("New %v", err)
			}
		})
	}
}

// Fatal is a level of this package (zap and zerolog log fatal and panic
// records at it) and ParseLevel accepts it, so an output that only takes
// fatal records, such as one that pages someone, is configured with level
// fatal. The config validation that New runs rejected it.
func TestFatalLevelIsConfigurable(t *testing.T) {
	out, sink := memOutput(t, "page", config.LogOutput{Level: "fatal"})
	cfg := logConfig("info", out)
	cfg.StackLevel = "fatal"
	l, err := logger.New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.Error("disk failed")
	l.Log(context.Background(), logger.LevelFatal, "out of disk")
	recs := sink().records
	if len(recs) != 1 || !bytes.Contains(recs[0], []byte(`"msg":"out of disk"`)) || !bytes.Contains(recs[0], []byte(`"stack":`)) {
		t.Errorf("the fatal output received %q, want only the fatal record, with a stack", recs)
	}
}

// The worst case of the above: esSink.send picks the node with
// next % len(addrs), which divides by zero without addresses. It runs on the
// sink's goroutine, so the first flush would kill the process and no caller
// could recover. New fails instead (or logging and Close work).
func TestElasticsearchWithoutAddressesFailsInNew(t *testing.T) {
	if os.Getenv("LOGGER_ES_NO_ADDRESSES") == "1" {
		l, err := logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "es", Type: "elasticsearch"}))
		if err != nil {
			return
		}
		l.Info("first record")
		_ = l.Close()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestElasticsearchWithoutAddressesFailsInNew$")
	cmd.Env = append(os.Environ(), "LOGGER_ES_NO_ADDRESSES=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		var why []string
		for line := range strings.Lines(string(out)) {
			if strings.HasPrefix(line, "panic:") || strings.Contains(line, "sink_es.go:") {
				why = append(why, strings.TrimSpace(line))
			}
		}
		t.Errorf("process died (%v):\n%s", err, strings.Join(why, "\n"))
	}
}

// openFile opens the file at startup so that a bad path fails at startup.
// When the path is an existing directory, lumberjack cannot open it and
// falls back to rotating it away: the directory, with whatever it holds,
// would be renamed to <path>-<timestamp> and a log file take its place. New
// fails and the directory stays.
func TestFileOutputRejectsDirectory(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "logs")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "file", Path: dir}))
	if err == nil {
		_ = l.Close()
		t.Error("New accepted a directory as the log file")
	}
	if _, err := os.Stat(filepath.Join(dir, "keep.txt")); err != nil {
		entries, _ := os.ReadDir(parent)
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("the directory was moved away (%v); %s now holds %v", err, parent, names)
	}
}

// RFC 3164 over TCP frames messages with a newline, and its TAG ends at the
// first non-alphanumeric character. A tag written verbatim would split at a
// space in the service name, and a trailing newline (a YAML block scalar, a
// secret file) would split the frame in two; like RFC 5424's APP-NAME, the
// tag is one printable token.
func TestSyslog3164TagIsOneToken(t *testing.T) {
	addr, received := tcpListener(t)
	l := netLogger(t, config.LogOutput{Type: "syslog", Network: "tcp", Addr: addr, SyslogFormat: "rfc3164", Tag: "billing api\n", Timeout: time.Second})
	l.Info("hello")
	_ = l.Close()
	data := received()
	if n := bytes.Count(data, []byte("\n")); n != 1 {
		t.Errorf("one record became %d newline-framed messages: %q", n, data)
	}
	if !regexp.MustCompile(`^<\d+>\w{3} [ \d]\d \d\d:\d\d:\d\d \S+ [!-~]+\[\d+\]: `).Match(data) {
		t.Errorf("tag is not one printable token: %q", data)
	}
}

// RFC 5424 §6 limits APP-NAME to 48 printable characters and TIME-SECFRAC to
// 6 digits, and strict receivers (rsyslog's pmrfc5424) reject a longer
// header. The record time has nanoseconds whenever time_format is
// rfc3339nano or unixnano, or from the time.Now() fallback on Linux.
func TestSyslog5424HeaderWithinRFCLimits(t *testing.T) {
	c := udpListener(t)
	cfg := logConfig("info", config.LogOutput{Name: "s", Type: "syslog", Network: "udp", Addr: c.LocalAddr().String(), Timeout: time.Second})
	cfg.Service = strings.Repeat("a", 60)
	cfg.TimeFormat = "rfc3339nano"
	cfg.UTC = true
	l, err := logger.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	r := slog.NewRecord(time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC), slog.LevelInfo, "x", 0)
	if err := l.Handler().Handle(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	// <PRI>1 TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG
	f := strings.SplitN(string(readUDP(t, c)), " ", 8)
	if len(f) < 8 {
		t.Fatalf("header = %q", f)
	}
	if len(f[3]) > 48 {
		t.Errorf("APP-NAME of %d characters: %s", len(f[3]), f[3])
	}
	if m := regexp.MustCompile(`\.(\d+)`).FindStringSubmatch(f[1]); m != nil && len(m[1]) > 6 {
		t.Errorf("TIME-SECFRAC of %d digits: %s", len(m[1]), f[1])
	}
}

// ---------------------------------------------------------------------------
// Edge cases that hold.

func TestParseLevelOddInputs(t *testing.T) {
	for s, want := range map[string]slog.Level{
		" warn\t": slog.LevelWarn, "WARNING": slog.LevelWarn, "Fatal\n": logger.LevelFatal, "\u00a0trace": logger.LevelTrace,
	} {
		if got, err := logger.ParseLevel(s); err != nil || got != want {
			t.Errorf("ParseLevel(%q) = %v, %v", s, got, err)
		}
	}
	// Offsets are for records (ParseRecord), not for configured levels, and
	// no Unicode case folding may turn a lookalike into a level.
	for _, s := range []string{"info+1", "DEBUG-4", "in fo", "\x00info", "infoo", "tr\u200bace", "none", "panic"} {
		_, err := logger.ParseLevel(s)
		if err == nil {
			t.Errorf("ParseLevel(%q) should fail", s)
		} else if strings.ContainsAny(err.Error(), "\n\x00") {
			t.Errorf("error for %q carries raw control characters: %q", s, err)
		}
	}
}

// Every level, including offsets beyond the named ones, must survive the
// trip through a record: structured sinks derive severities from it.
func TestLevelNamesParseBackAtExtremes(t *testing.T) {
	for _, l := range []slog.Level{math.MinInt, logger.LevelTrace - 1, logger.LevelTrace, -5, slog.LevelDebug, -1, slog.LevelInfo, 3,
		slog.LevelWarn, 7, slog.LevelError, 11, logger.LevelFatal, logger.LevelFatal + 1, math.MaxInt32, math.MaxInt} {
		name := logger.LevelName(l)
		r, err := logger.ParseRecord([]byte(`{"level":"` + name + `"}`))
		if err != nil || r.Level != l {
			t.Errorf("LevelName(%d) = %s parses back as %d (%v)", int(l), name, int(r.Level), err)
		}
	}
}

func TestParseRecordEdgeCases(t *testing.T) {
	for _, in := range []string{"", " ", "null", "[]", "1", `"s"`, "{", `{"a":}`, "\xff", "\ufeff{}"} {
		if _, err := logger.ParseRecord([]byte(in)); err == nil {
			t.Errorf("ParseRecord(%q) should fail", in)
		}
	}
	// Built-in keys that do not parse stay in Fields, so sinks forward them
	// rather than losing data.
	r, err := logger.ParseRecord([]byte(`{"level":"LOUD","msg":5,"time":"yesterday","ts":1767323045,"stack":["a"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if r.Level != slog.LevelInfo || r.Message != "" || r.Stack != "" || !r.Time.Equal(time.Unix(1767323045, 0)) {
		t.Errorf("record = %+v", r)
	}
	for _, k := range []string{"level", "msg", "time", "stack"} {
		if _, ok := r.Fields[k]; !ok {
			t.Errorf("%s dropped from fields: %v", k, r.Fields)
		}
	}
	if _, ok := r.Fields["ts"]; ok {
		t.Errorf("the parsed time key should be consumed: %v", r.Fields)
	}
	// Deeper than encoding/json allows: a clean error, no stack overflow.
	deep := strings.Repeat(`{"a":`, 20000) + "1" + strings.Repeat("}", 20000)
	if _, err := logger.ParseRecord([]byte(deep)); err == nil {
		t.Error("a record nested 20000 deep should fail to parse")
	}
	if _, err := logger.GELFMessage([]byte(deep), "h", ""); err == nil {
		t.Error("GELFMessage of a record nested 20000 deep should fail")
	}
}

func TestTimeFormatBoundariesAndZones(t *testing.T) {
	zones := []*time.Location{time.UTC, time.FixedZone("TPE", 8*3600), time.FixedZone("NST", -(3*3600 + 30*60)), time.FixedZone("NPT", 5*3600+45*60)}
	instants := []time.Time{
		time.Unix(0, 0), time.Unix(0, 1), time.Unix(-1, 999_999_999), // around the epoch
		time.Unix(math.MaxInt32, 999_999_999), time.Unix(math.MaxInt32+1, 0), // 32-bit rollover
		time.Date(9999, 12, 30, 23, 59, 59, 999_999_999, time.UTC), // 4-digit year in every zone
	}
	layouts := map[string]time.Duration{"rfc3339milli": time.Millisecond, "RFC3339": time.Second, "rfc3339nano": time.Nanosecond}
	for _, zone := range zones {
		for _, inst := range instants {
			ts := inst.In(zone)
			for format, prec := range layouts {
				for _, utc := range []bool{false, true} {
					s := logger.NewTimeFormat(config.Log{TimeFormat: format, UTC: utc}).String(ts)
					got, err := time.Parse(time.RFC3339Nano, s)
					if err != nil || !got.Equal(ts.Truncate(prec)) {
						t.Errorf("%s utc=%v: %v formatted as %s, parses as %v (%v)", format, utc, ts, s, got, err)
					}
					_, want := ts.Zone()
					if utc {
						want = 0
					}
					if _, off := got.Zone(); off != want {
						t.Errorf("%s utc=%v: %s has offset %d, want %d", format, utc, s, off, want)
					}
				}
			}
			for format, want := range map[string]int64{"unix": ts.Unix(), "unixmilli": ts.UnixMilli(), "unixnano": ts.UnixNano()} {
				if format == "unixnano" && ts.Year() > 2262 {
					continue // beyond int64 nanoseconds, undefined by time.Time.UnixNano
				}
				tf := logger.NewTimeFormat(config.Log{TimeFormat: format})
				if !tf.Epoch() || tf.Int(ts) != want || tf.String(ts) != strconv.FormatInt(want, 10) {
					t.Errorf("%s of %v = %d / %s, want %d", format, ts, tf.Int(ts), tf.String(ts), want)
				}
			}
		}
	}
	if tf := logger.NewTimeFormat(config.Log{TimeFormat: "2006-01-02 15:04"}); tf.Epoch() || tf.Int(time.Now()) != 0 {
		t.Error("a layout is not an epoch format")
	}
}

type shortWriter struct{ bytes.Buffer }

// Write accepts less than it is given without an error, as a broken
// io.Writer might.
func (w *shortWriter) Write(b []byte) (int, error) {
	w.Buffer.Write(b)
	return len(b) / 2, nil
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestPrettyJSONEdgeCases(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"a":1,"b":[true,null]}` + "\n", "{\n  \"a\": 1,\n  \"b\": [\n    true,\n    null\n  ]\n}\n"},
		{`{"a":1}` + "\r\n", "{\n  \"a\": 1\n}\r\n"},   // json.Indent keeps trailing space
		{"level=INFO msg=hi\n", "level=INFO msg=hi\n"}, // text passes through
		{`{"a":`, `{"a":`}, // a partial record is not buffered or mangled
		{`{"a":1}` + "\n" + `{"b":2}` + "\n", `{"a":1}` + "\n" + `{"b":2}` + "\n"}, // two records in one write are not merged
		{"", ""},
		{"\n", "\n"},
	} {
		var buf bytes.Buffer
		n, err := logger.PrettyJSON(&buf).Write([]byte(tc.in))
		if err != nil || n != len(tc.in) || buf.String() != tc.want {
			t.Errorf("Write(%q) = %d, %v, wrote %q, want %q", tc.in, n, err, buf.String(), tc.want)
		}
	}
	// A record split over two writes comes out unchanged rather than half
	// indented.
	var buf bytes.Buffer
	w := logger.PrettyJSON(&buf)
	_, _ = w.Write([]byte(`{"msg":"he`))
	_, _ = w.Write([]byte(`llo"}`))
	if buf.String() != `{"msg":"hello"}` {
		t.Errorf("split record = %q", buf.String())
	}
	if n, err := logger.PrettyJSON(brokenWriter{}).Write([]byte(`{"a":1}`)); err == nil || n != 0 {
		t.Errorf("write error = %d, %v", n, err)
	}
	var sw shortWriter
	if n, err := logger.PrettyJSON(&sw).Write([]byte(`{"a":1}`)); n != len(`{"a":1}`) || err != nil {
		t.Errorf("short write = %d, %v", n, err)
	}
}

func TestGELFMessageEdgeCases(t *testing.T) {
	gelf := func(t *testing.T, rec, host string) map[string]any {
		t.Helper()
		b, err := logger.GELFMessage([]byte(rec), host, "svc")
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if err := dec.Decode(&m); err != nil {
			t.Fatalf("%s: %v", b, err)
		}
		valid := regexp.MustCompile(`^(version|host|short_message|full_message|timestamp|level|_[\w.\-]*)$`)
		for k := range m {
			if !valid.MatchString(k) || k == "_id" {
				t.Errorf("invalid GELF field %q", k)
			}
		}
		return m
	}

	t.Run("reserved names", func(t *testing.T) {
		m := gelf(t, `{"level":"ERROR","msg":"m","id":1,"version":"x","host":"h2","short_message":"s","timestamp":"t","_x":1,"a b":2,"用户":"u"}`, "h")
		for k, want := range map[string]any{
			"version": "1.1", "host": "h", "short_message": "m", "level": json.Number("3"), "_service": "svc",
			"_id_": json.Number("1"), "_version": "x", "_host": "h2", "_short_message": "s", "_timestamp": "t", "__x": json.Number("1"), "_a_b": json.Number("2"),
		} {
			if m[k] != want {
				t.Errorf("%s = %#v, want %#v", k, m[k], want)
			}
		}
	})
	t.Run("empty record", func(t *testing.T) {
		m := gelf(t, `{}`, "")
		if m["host"] != "unknown" || m["short_message"] != "-" || m["level"] != json.Number("6") {
			t.Errorf("gelf = %v", m)
		}
	})
	t.Run("control characters and invalid UTF-8", func(t *testing.T) {
		m := gelf(t, "{\"msg\":\"a\\u0000b\\nc\",\"k\":\"\xff\"}", "h")
		if m["short_message"] != "a\x00b\nc" || m["_k"] != "\ufffd" {
			t.Errorf("gelf = %q", m)
		}
	})
	t.Run("large integers stay exact", func(t *testing.T) {
		b, _ := logger.GELFMessage([]byte(`{"n":12345678901234567890123,"f":0.1000000000000000055511151231257827}`), "h", "")
		if !bytes.Contains(b, []byte(`"_n":12345678901234567890123`)) || !bytes.Contains(b, []byte(`"_f":0.1000000000000000055511151231257827`)) {
			t.Errorf("gelf = %s", b)
		}
	})
	t.Run("deep nesting flattens", func(t *testing.T) {
		rec := strings.Repeat(`{"a":`, 500) + `{"k":"v"}` + strings.Repeat("}", 500)
		m := gelf(t, rec, "h")
		if m["_"+strings.Repeat("a_", 500)+"k"] != "v" {
			t.Errorf("deep key missing: %d fields", len(m))
		}
	})
	t.Run("levels map to syslog severities", func(t *testing.T) {
		for level, want := range map[string]string{"TRACE-100": "7", "DEBUG": "7", "INFO+3": "6", "WARN": "4", "ERROR+3": "3", "FATAL": "2", "FATAL+100": "2"} {
			if m := gelf(t, `{"level":"`+level+`"}`, "h"); m["level"] != json.Number(want) || m["_level_name"] != level {
				t.Errorf("%s: level = %v, name = %v", level, m["level"], m["_level_name"])
			}
		}
	})
}

// Over UDP a GELF message may not exceed 128 chunks. A bigger record must be
// reported, not sent in part (a receiver waits for the missing chunks).
func TestGELFOversizedRecord(t *testing.T) {
	payload := strings.Repeat("x", 200_000) // ~143 chunks of 1408 bytes
	t.Run("reported", func(t *testing.T) {
		c := udpListener(t)
		var errs errorLog
		l := netLogger(t, config.LogOutput{Type: "gelf", Addr: c.LocalAddr().String(), Timeout: time.Second}, errs.handler())
		defer l.Close()
		l.Info("big", "payload", payload)
		if e := errs.all(); len(e) != 1 || !strings.Contains(e[0], "(max 128)") {
			t.Errorf("errors = %v", e)
		}
		_ = c.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		if n, _, err := c.ReadFromUDP(make([]byte, 65536)); err == nil {
			t.Errorf("part of the message was sent (%d bytes)", n)
		}
	})
	t.Run("compressed fits", func(t *testing.T) {
		c := udpListener(t)
		l := netLogger(t, config.LogOutput{Type: "gelf", Addr: c.LocalAddr().String(), Compress: true, Timeout: time.Second})
		defer l.Close()
		l.Info("big", "payload", payload)
		if m := gelfDecode(t, readUDP(t, c)); m["_payload"] != payload {
			t.Errorf("payload of %d bytes", len(fmt.Sprint(m["_payload"])))
		}
	})
}

// Control characters and invalid UTF-8 in messages and attrs must not break
// the framing of stream transports: newline (RFC 3164), NUL (GELF over TCP)
// and octet counting in bytes, not characters (RFC 5424).
func TestStreamFramingWithControlChars(t *testing.T) {
	const msg = "line1\nline2\x00end\xff\r"
	const want = "line1\nline2\x00end\ufffd\r"

	t.Run("syslog rfc3164", func(t *testing.T) {
		addr, received := tcpListener(t)
		l := netLogger(t, config.LogOutput{Type: "syslog", Network: "tcp", Addr: addr, SyslogFormat: "rfc3164", Timeout: time.Second})
		l.Info(msg, "k", "v\n")
		_ = l.Close()
		data := received()
		if bytes.Count(data, []byte("\n")) != 1 || !bytes.HasSuffix(data, []byte("\n")) {
			t.Fatalf("frame = %q", data)
		}
		var rec map[string]any
		if err := json.Unmarshal(data[bytes.IndexByte(data, '{'):], &rec); err != nil || rec["msg"] != want || rec["k"] != "v\n" {
			t.Errorf("body = %v (%v)", rec, err)
		}
	})
	t.Run("gelf tcp", func(t *testing.T) {
		addr, received := tcpListener(t)
		l := netLogger(t, config.LogOutput{Type: "gelf", Network: "tcp", Addr: addr, Timeout: time.Second})
		l.Info(msg)
		_ = l.Close()
		data := received()
		if bytes.Count(data, []byte{0}) != 1 || data[len(data)-1] != 0 {
			t.Fatalf("frame = %q", data)
		}
		if m := gelfDecode(t, data[:len(data)-1]); m["short_message"] != want {
			t.Errorf("short_message = %q", m["short_message"])
		}
	})
	t.Run("syslog rfc5424 octet counting", func(t *testing.T) {
		addr, received := tcpListener(t)
		l := netLogger(t, config.LogOutput{Type: "syslog", Network: "tcp", Addr: addr, Timeout: time.Second})
		l.Info("héllo 世界")
		_ = l.Close()
		data := received()
		n, rest, ok := bytes.Cut(data, []byte(" "))
		if size, err := strconv.Atoi(string(n)); !ok || err != nil || size != len(rest) {
			t.Errorf("frame %q: length prefix %s for %d bytes", data, n, len(rest))
		}
	})
}

// Every record written to Elasticsearch is either delivered or counted as
// dropped; the drop count is how operators notice back pressure.
func TestElasticsearchDropAccounting(t *testing.T) {
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	url, bodies := rawBulkServer(t, gate)
	t.Cleanup(release) // before the server closes: cleanups run last-in first-out
	var errs errorLog
	l := netLogger(t, config.LogOutput{
		Type: "elasticsearch", Addresses: []string{url}, BatchSize: 1, QueueSize: 2, FlushInterval: 10 * time.Millisecond, Timeout: 5 * time.Second,
	}, errs.handler())
	const n = 50
	for i := range n {
		l.Info("burst", "i", i)
	}
	release()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	delivered, dropped := len(bulkDocs(bodies())), 0
	re := regexp.MustCompile(`dropped (\d+) records: queue full`)
	for _, e := range errs.all() {
		m := re.FindStringSubmatch(e)
		if m == nil {
			t.Errorf("unexpected error: %s", e)
			continue
		}
		k, _ := strconv.Atoi(m[1])
		dropped += k
	}
	if dropped == 0 || delivered+dropped != n {
		t.Errorf("delivered %d + dropped %d != %d written", delivered, dropped, n)
	}
}

func TestCloseTwiceAndLogAfterClose(t *testing.T) {
	url, _ := rawBulkServer(t, nil)
	udp := udpListener(t)
	mem, _ := memOutput(t, "mem", config.LogOutput{})
	cfg := logConfig("info",
		config.LogOutput{Name: "es", Type: "elasticsearch", Addresses: []string{url}, FlushInterval: time.Hour, Timeout: time.Second},
		config.LogOutput{Name: "gelf", Addr: udp.LocalAddr().String(), Timeout: time.Second},
		config.LogOutput{Name: "syslog", Network: "udp", Addr: udp.LocalAddr().String(), Timeout: time.Second},
		config.LogOutput{Name: "file", Path: filepath.Join(t.TempDir(), "app.log")},
		mem)
	var errs errorLog
	l, err := logger.New(context.Background(), cfg, withMem, errs.handler())
	if err != nil {
		t.Fatal(err)
	}
	l.Info("before")
	for i := range 2 {
		if err := l.Close(); err != nil {
			t.Errorf("Close #%d: %v", i+1, err)
		}
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("logging after Close panicked: %v", p)
			}
		}()
		l.Info("after close")
	}()
	if e := strings.Join(errs.all(), "\n"); !strings.Contains(e, "es: elasticsearch: sink closed") {
		t.Errorf("a record after Close should be reported, errors = %q", e)
	}
}

// Hot reload calls Update while other goroutines log; levels, output levels
// and the stack level must switch without data races (run with -race).
func TestConcurrentLoggingDuringUpdate(t *testing.T) {
	all, allSink := memOutput(t, "all", config.LogOutput{})
	errs, _ := memOutput(t, "errors", config.LogOutput{Level: "error"})
	cfg := logConfig("info", all, errs)
	cfg.AddSource = true
	l, err := logger.New(context.Background(), cfg, withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	done := make(chan struct{})
	var updates sync.WaitGroup
	updates.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				return
			default:
			}
			c := cfg
			o := errs
			if i%2 == 0 {
				c.Level, c.StackLevel, o.Level = "debug", "none", "warn"
			} else {
				c.Level, c.StackLevel, o.Level = "error", "error", ""
			}
			c.Outputs = config.Named[config.LogOutput]{}
			c.Outputs.Set(all.Name, all)
			c.Outputs.Set(o.Name, o)
			if err := l.Update(c); err != nil {
				t.Error(err)
				return
			}
		}
	})
	var loggers sync.WaitGroup
	for g := range 4 {
		loggers.Go(func() {
			for i := range 100 {
				l.Error("e", "g", g, "i", i)
				l.With("w", 1).Info("i")
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
	before := len(allSink().decoded(t)) // also checks every record is JSON
	l.Info("dropped")
	if after := len(allSink().decoded(t)); after != before {
		t.Error("the last Update did not take effect")
	}
}

// The default error handler writes to stderr at most once per output every
// 10 seconds, however many goroutines hit the error at once.
func TestDefaultErrorHandlerRateLimitsPerOutput(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	l, err := logger.New(context.Background(), logConfig("info",
		config.LogOutput{Name: "a", Type: "failing"}, config.LogOutput{Name: "b", Type: "failing"}), withFailing)
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = f
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 25 {
				l.Info("x")
			}
		})
	}
	wg.Wait()
	os.Stderr = orig

	b, _ := os.ReadFile(f.Name())
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	slices.Sort(lines)
	if want := []string{"logger: output a: disk full", "logger: output b: disk full"}; !slices.Equal(lines, want) {
		t.Errorf("stderr = %q, want %q", lines, want)
	}
}

func TestFileOutputUnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	for _, path := range []string{filepath.Join(dir, "app.log"), filepath.Join(dir, "sub", "app.log")} {
		if l, err := logger.New(context.Background(), logConfig("info", config.LogOutput{Name: "file", Path: path})); err == nil {
			_ = l.Close()
			t.Errorf("%s: want an error at startup", path)
		}
	}
}

// Deeply nested groups flatten into one GELF field name.
func TestDeepGroupsFlattenInGELF(t *testing.T) {
	out, sink := memOutput(t, "m", config.LogOutput{})
	l, err := logger.New(context.Background(), logConfig("info", out), withMem)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	g := l.Logger
	for range 100 {
		g = g.WithGroup("g")
	}
	g.Info("deep", "k", "v")
	b, err := logger.GELFMessage(sink().records[0], "h", "")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil || m["_"+strings.Repeat("g_", 100)+"k"] != "v" || m["short_message"] != "deep" {
		t.Errorf("gelf = %s (%v)", b, err)
	}
}
