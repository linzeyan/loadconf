package sink_otlp

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// received collects exported requests.
type received struct {
	mu     sync.Mutex
	reqs   []*collogspb.ExportLogsServiceRequest
	header map[string]string
}

func (r *received) add(req *collogspb.ExportLogsServiceRequest, header map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reqs = append(r.reqs, req)
	r.header = header
}

func (r *received) records() (map[string]*logspb.LogRecord, map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	recs := map[string]*logspb.LogRecord{}
	res := map[string]string{}
	for _, req := range r.reqs {
		for _, rl := range req.ResourceLogs {
			for _, kv := range rl.GetResource().GetAttributes() {
				res[kv.Key] = kv.Value.GetStringValue()
			}
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					recs[lr.Body.GetStringValue()] = lr
				}
			}
		}
	}
	return recs, res
}

func attrs(lr *logspb.LogRecord) map[string]*commonpb.AnyValue {
	m := map[string]*commonpb.AnyValue{}
	for _, kv := range lr.Attributes {
		m[kv.Key] = kv.Value
	}
	return m
}

func outputs(outs ...config.LogOutput) config.Named[config.LogOutput] {
	n := config.Named[config.LogOutput]{}
	for _, o := range outs {
		n.Set(o.Name, o)
	}
	return n
}

// withOTLP is how applications enable the otlp output type.
var withOTLP = logger.WithSink("otlp", Open)

var (
	traceID = trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID  = trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
)

func TestHTTPExport(t *testing.T) {
	var got received
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
		var req collogspb.ExportLogsServiceRequest
		if err := proto.Unmarshal(b, &req); err != nil {
			t.Errorf("%s: %v", r.URL.Path, err)
		}
		got.add(&req, map[string]string{"path": r.URL.Path, "tenant": r.Header.Get("X-Tenant"), "encoding": r.Header.Get("Content-Encoding")})
		resp, _ := proto.Marshal(&collogspb.ExportLogsServiceResponse{})
		w.Header().Set("Content-Type", "application/x-protobuf")
		_, _ = w.Write(resp)
	}))
	defer srv.Close()

	cfg := config.Log{Service: "api", Level: "debug", AddSource: true, Outputs: outputs(config.LogOutput{
		Name: "otlp", Endpoint: srv.URL, Protocol: "http", Insecure: true, Compress: true,
		Headers: map[string]string{"X-Tenant": "t1"}, Timeout: 5 * time.Second, BatchSize: 10, FlushInterval: time.Second, QueueSize: 100,
	})}
	l, err := logger.New(context.Background(), cfg, withOTLP, logger.WithContextAttrs(TraceAttrs))
	if err != nil {
		t.Fatal(err)
	}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
	}))
	l.InfoContext(ctx, "hello", "user", "u1", "n", 3, "ratio", 0.5, slog.Group("req", "method", "GET", "ok", true))
	l.Error("failed")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	recs, res := got.records()
	if res["service.name"] != "api" || res["host.name"] == "" {
		t.Errorf("resource = %v", res)
	}
	if got.header["path"] != "/v1/logs" || got.header["tenant"] != "t1" || got.header["encoding"] != "gzip" {
		t.Errorf("request = %v", got.header)
	}
	hello := recs["hello"]
	if hello == nil {
		t.Fatalf("records = %v", recs)
	}
	if hello.SeverityNumber != 9 || hello.SeverityText != "INFO" || hello.TimeUnixNano == 0 {
		t.Errorf("hello = %v", hello)
	}
	if trace.TraceID(hello.TraceId) != traceID || trace.SpanID(hello.SpanId) != spanID || hello.Flags != 1 {
		t.Errorf("trace = %x %x %d", hello.TraceId, hello.SpanId, hello.Flags)
	}
	a := attrs(hello)
	if a["user"].GetStringValue() != "u1" || a["n"].GetIntValue() != 3 || a["ratio"].GetDoubleValue() != 0.5 || a["service"].GetStringValue() != "api" {
		t.Errorf("attributes = %v", a)
	}
	if req := a["req"].GetKvlistValue(); req == nil || len(req.Values) != 2 || req.Values[1].Value.GetBoolValue() != true {
		t.Errorf("group = %v", a["req"])
	}
	if !strings.HasSuffix(a["code.file.path"].GetStringValue(), "sink_otlp_test.go") || a["code.line.number"].GetIntValue() == 0 ||
		!strings.Contains(a["code.function.name"].GetStringValue(), "TestHTTPExport") {
		t.Errorf("source = %v %v %v", a["code.file.path"], a["code.line.number"], a["code.function.name"])
	}
	for _, k := range []string{"trace_id", "span_id", "trace_flags", "source", "time", "level", "msg"} {
		if _, ok := a[k]; ok {
			t.Errorf("attribute %s should have been consumed", k)
		}
	}
	failed := recs["failed"]
	if failed == nil || failed.SeverityNumber != 17 || !strings.Contains(attrs(failed)["code.stacktrace"].GetStringValue(), "TestHTTPExport") {
		t.Errorf("failed = %v", failed)
	}
}

type collector struct {
	collogspb.UnimplementedLogsServiceServer
	got *received
}

func (c collector) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	c.got.add(req, map[string]string{"tenant": strings.Join(md.Get("x-tenant"), ",")})
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func TestGRPCExport(t *testing.T) {
	var got received
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(s, collector{got: &got})
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	l, err := logger.New(context.Background(), config.Log{Level: "info", Outputs: outputs(config.LogOutput{
		Name: "otlp", Endpoint: lis.Addr().String(), Insecure: true, Headers: map[string]string{"x-tenant": "t2"}, Timeout: 5 * time.Second,
	})}, withOTLP)
	if err != nil {
		t.Fatal(err)
	}
	l.Warn("over grpc")
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	recs, _ := got.records()
	if r := recs["over grpc"]; r == nil || r.SeverityNumber != 13 || got.header["tenant"] != "t2" {
		t.Errorf("records = %v header = %v", recs, got.header)
	}
}

func TestExportErrorsGoToTheErrorHandler(t *testing.T) {
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := lis.Addr().String()
	_ = lis.Close()
	var (
		mu     sync.Mutex
		errs   []string
		output string
	)
	l, err := logger.New(context.Background(), config.Log{Level: "info", Outputs: outputs(config.LogOutput{
		Name: "otlp", Endpoint: "http://" + addr, Protocol: "http", Timeout: 300 * time.Millisecond, FlushInterval: 50 * time.Millisecond,
	})}, withOTLP, logger.WithErrorHandler(func(out string, err error) {
		mu.Lock()
		defer mu.Unlock()
		output = out
		errs = append(errs, err.Error())
	}))
	if err != nil {
		t.Fatal(err)
	}
	l.Info("lost")
	_ = l.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(errs) == 0 || output != "otlp" || !strings.Contains(errs[0], "otlp export of 1 records") {
		t.Errorf("errors = %v (output %q)", errs, output)
	}
}

func TestSeverityAndValues(t *testing.T) {
	for l, want := range map[slog.Level]int{logger.LevelTrace: 1, -20: 1, slog.LevelDebug: 5, slog.LevelInfo: 9, slog.LevelInfo + 2: 11,
		slog.LevelWarn: 13, slog.LevelError: 17, logger.LevelFatal: 21, 40: 24} {
		if got := int(Severity(l)); got != want {
			t.Errorf("Severity(%v) = %d, want %d", l, got, want)
		}
	}
	if attrs := sourceAttrs("/app/main.go:42"); len(attrs) != 2 || attrs[0].Value.AsString() != "/app/main.go" || attrs[1].Value.AsInt64() != 42 {
		t.Errorf("zap source = %v", attrs)
	}
	if TraceAttrs(context.Background()) != nil {
		t.Error("no span, no attributes")
	}
}
