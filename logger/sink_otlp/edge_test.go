package sink_otlp

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// memExporter keeps exported records in memory.
type memExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *memExporter) Export(_ context.Context, recs []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range recs {
		e.records = append(e.records, recs[i].Clone())
	}
	return nil
}

func (e *memExporter) Shutdown(context.Context) error   { return nil }
func (e *memExporter) ForceFlush(context.Context) error { return nil }

func (e *memExporter) all() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

// newMemSink returns the otlp sink exporting synchronously to memory.
func newMemSink() (*sink, *memExporter) {
	exp := &memExporter{}
	p := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	return &sink{provider: p, logger: p.Logger("test"), wait: time.Second}, exp
}

func recordAttrs(r sdklog.Record) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	r.WalkAttributes(func(kv attribute.KeyValue) bool {
		m[string(kv.Key)] = kv.Value
		return true
	})
	return m
}

func TestWriteOddRecords(t *testing.T) {
	const tid, sid = "0102030405060708090a0b0c0d0e0f10", "0102030405060708"
	for _, tc := range []struct {
		name, in string
		check    func(t *testing.T, r sdklog.Record, a map[string]attribute.Value)
	}{
		{"all-zero trace ids stay data", `{"msg":"m","trace_id":"00000000000000000000000000000000","span_id":"0000000000000000"}`,
			func(t *testing.T, r sdklog.Record, a map[string]attribute.Value) {
				if r.TraceID().IsValid() || a["trace_id"].AsString() == "" || a["span_id"].AsString() == "" {
					t.Errorf("trace %v, attrs %v", r.TraceID(), a)
				}
			}},
		{"odd trace flags stay data", `{"msg":"m","trace_id":"` + tid + `","span_id":"` + sid + `","trace_flags":"1"}`,
			func(t *testing.T, r sdklog.Record, a map[string]attribute.Value) {
				if r.TraceID().String() != tid || r.SpanID().String() != sid || a["trace_flags"].AsString() != "1" {
					t.Errorf("trace %v/%v, attrs %v", r.TraceID(), r.SpanID(), a)
				}
				if _, ok := a["trace_id"]; ok {
					t.Error("trace_id should become the record's trace context")
				}
			}},
		{"windows source", `{"msg":"m","source":"C:\\app\\main.go:42"}`,
			func(t *testing.T, _ sdklog.Record, a map[string]attribute.Value) {
				if a["code.file.path"].AsString() != `C:\app\main.go` || a["code.line.number"].AsInt64() != 42 {
					t.Errorf("attrs %v", a)
				}
			}},
		{"source without line", `{"msg":"m","source":"main.go:"}`,
			func(t *testing.T, _ sdklog.Record, a map[string]attribute.Value) {
				if _, ok := a["code.line.number"]; ok || a["code.file.path"].AsString() != "main.go" {
					t.Errorf("attrs %v", a)
				}
			}},
		{"values", `{"msg":"m","n":null,"arr":[1,"a",null,{"k":true}],"big":12345678901234567890,"f":1.5,"obj":{"b":2,"a":1}}`,
			func(t *testing.T, _ sdklog.Record, a map[string]attribute.Value) {
				if a["big"].AsFloat64() != 12345678901234567890 || a["f"].AsFloat64() != 1.5 || len(a["arr"].AsSlice()) != 4 {
					t.Errorf("attrs %v", a)
				}
				if kv := a["obj"].AsMap(); len(kv) != 2 || kv[0].Key != "a" {
					t.Errorf("map attr %v should be sorted by key", kv)
				}
			}},
		{"levels beyond fatal and trace", `{"msg":"m","level":"FATAL+100"}`,
			func(t *testing.T, r sdklog.Record, _ map[string]attribute.Value) {
				if r.Severity() != 24 || r.SeverityText() != "FATAL+100" {
					t.Errorf("severity %v %s", r.Severity(), r.SeverityText())
				}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, exp := newMemSink()
			defer s.Close()
			if n, err := s.Write([]byte(tc.in)); err != nil || n != len(tc.in) {
				t.Fatalf("Write = %d, %v", n, err)
			}
			recs := exp.all()
			if len(recs) != 1 {
				t.Fatalf("%d records", len(recs))
			}
			tc.check(t, recs[0], recordAttrs(recs[0]))
		})
	}
	s, _ := newMemSink()
	defer s.Close()
	if _, err := s.Write([]byte("level=INFO")); err == nil {
		t.Error("text records should be refused")
	}
}

// Close is idempotent, and a record after Close is dropped quietly rather
// than panicking in the caller's log statement.
func TestCloseTwiceAndWriteAfterClose(t *testing.T) {
	s, exp := newMemSink()
	for i := range 2 {
		if err := s.Close(); err != nil {
			t.Errorf("Close #%d: %v", i+1, err)
		}
	}
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("Write after Close panicked: %v", p)
			}
		}()
		_, _ = s.Write([]byte(`{"msg":"late"}`))
	}()
	if n := len(exp.all()); n != 0 {
		t.Errorf("%d records exported after Close", n)
	}
	if err := s.Sync(); err != nil {
		t.Logf("Sync after Close: %v", err)
	}
}
