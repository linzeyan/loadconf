// Package sink_otlp provides the otlp output type of github.com/linzeyan/loadconf/logger,
// exporting records to an OpenTelemetry collector over gRPC or HTTP. Pass it
// to the logger, which opens it only when the config has an otlp output:
//
//	log, err := logger.New(ctx, cfg.Log, logger.WithSink("otlp", sink_otlp.Open))
//
//	log:
//	  outputs:
//	    otlp:
//	      endpoint: otel-collector:4317   # empty: OTEL_EXPORTER_OTLP_* variables
//	      insecure: true
//
// The endpoint is host:port or a URL; over http a URL without a path gets
// /v1/logs.
//
// Records keep their fields as attributes; the source becomes
// code.file.path, code.line.number and code.function.name, the stack
// code.stacktrace. slog levels map to OpenTelemetry severities (level + 9,
// so DEBUG is 5, INFO 9, WARN 13, ERROR 17). The resource carries
// service.name and host.name from the logger config, merged over the
// OTEL_RESOURCE_ATTRIBUTES defaults.
//
// To correlate logs with traces, add the trace and span IDs of the context:
//
//	log, err := logger.New(ctx, cfg.Log, logger.WithContextAttrs(sink_otlp.TraceAttrs))
//	log.InfoContext(ctx, "handled")
//
// Export errors go to the logger's error handler, not to otel's global one.
package sink_otlp

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/credentials"

	"github.com/linzeyan/loadconf/config"
	"github.com/linzeyan/loadconf/logger"
)

// Open opens an otlp sink; pass it as the otlp output type with
// logger.WithSink.
func Open(ctx context.Context, out config.LogOutput, env logger.SinkEnv) (logger.Sink, error) {
	exp, err := newExporter(ctx, out)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", env.Service),
		attribute.String("host.name", env.Hostname),
	))
	if err != nil {
		return nil, err
	}
	var bopts []sdklog.BatchProcessorOption
	if out.QueueSize > 0 {
		bopts = append(bopts, sdklog.WithMaxQueueSize(out.QueueSize))
	}
	if out.BatchSize > 0 {
		bopts = append(bopts, sdklog.WithExportMaxBatchSize(out.BatchSize))
	}
	if out.FlushInterval > 0 {
		bopts = append(bopts, sdklog.WithExportInterval(out.FlushInterval))
	}
	if out.Timeout > 0 {
		bopts = append(bopts, sdklog.WithExportTimeout(out.Timeout))
	}
	onError := env.OnError
	if onError == nil {
		onError = func(error) {}
	}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(reportingExporter{exp, onError}, bopts...)),
	)
	return &sink{
		provider: provider,
		logger:   provider.Logger("github.com/linzeyan/loadconf/logger"),
		wait:     cmp.Or(out.Timeout, 5*time.Second) + cmp.Or(out.FlushInterval, time.Second),
	}, nil
}

func newExporter(ctx context.Context, out config.LogOutput) (sdklog.Exporter, error) {
	tlsCfg, err := out.TLS.Config()
	if err != nil {
		return nil, fmt.Errorf("otlp tls: %w", err)
	}
	if strings.EqualFold(out.Protocol, "http") {
		var opts []otlploghttp.Option
		switch {
		case strings.Contains(out.Endpoint, "://"):
			// Like OTEL_EXPORTER_OTLP_ENDPOINT: a URL without a path gets
			// the logs path.
			u, err := url.Parse(out.Endpoint)
			if err != nil {
				return nil, fmt.Errorf("otlp endpoint: %w", err)
			}
			if u.Path == "" || u.Path == "/" {
				u.Path = "/v1/logs"
			}
			opts = append(opts, otlploghttp.WithEndpointURL(u.String()))
		case out.Endpoint != "":
			opts = append(opts, otlploghttp.WithEndpoint(out.Endpoint))
		}
		if out.Insecure {
			opts = append(opts, otlploghttp.WithInsecure())
		} else if tlsCfg != nil {
			opts = append(opts, otlploghttp.WithTLSClientConfig(tlsCfg))
		}
		if len(out.Headers) > 0 {
			opts = append(opts, otlploghttp.WithHeaders(out.Headers))
		}
		if out.Timeout > 0 {
			opts = append(opts, otlploghttp.WithTimeout(out.Timeout))
		}
		if out.Compress {
			opts = append(opts, otlploghttp.WithCompression(otlploghttp.GzipCompression))
		}
		return otlploghttp.New(ctx, opts...)
	}

	var opts []otlploggrpc.Option
	switch {
	case strings.Contains(out.Endpoint, "://"):
		opts = append(opts, otlploggrpc.WithEndpointURL(out.Endpoint))
	case out.Endpoint != "":
		opts = append(opts, otlploggrpc.WithEndpoint(out.Endpoint))
	}
	if out.Insecure {
		opts = append(opts, otlploggrpc.WithInsecure())
	} else if tlsCfg != nil {
		opts = append(opts, otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
	}
	if len(out.Headers) > 0 {
		opts = append(opts, otlploggrpc.WithHeaders(out.Headers))
	}
	if out.Timeout > 0 {
		opts = append(opts, otlploggrpc.WithTimeout(out.Timeout))
	}
	if out.Compress {
		opts = append(opts, otlploggrpc.WithCompressor("gzip"))
	}
	return otlploggrpc.New(ctx, opts...)
}

// reportingExporter sends export errors to the logger's error handler
// instead of otel's global one.
type reportingExporter struct {
	sdklog.Exporter
	onError func(error)
}

func (e reportingExporter) Export(ctx context.Context, records []sdklog.Record) error {
	if err := e.Exporter.Export(ctx, records); err != nil {
		e.onError(fmt.Errorf("otlp export of %d records: %w", len(records), err))
	}
	return nil
}

type sink struct {
	provider *sdklog.LoggerProvider
	logger   otellog.Logger
	wait     time.Duration
}

// Write converts one JSON record and queues it for export.
func (s *sink) Write(b []byte) (int, error) {
	rec, err := logger.ParseRecord(b)
	if err != nil {
		return 0, fmt.Errorf("otlp: %w", err)
	}
	var r otellog.Record
	now := time.Now()
	r.SetTimestamp(cmp.Or(rec.Time, now))
	r.SetObservedTimestamp(now)
	r.SetSeverity(Severity(rec.Level))
	r.SetSeverityText(logger.LevelName(rec.Level))
	r.SetBody(attribute.StringValue(rec.Message))

	ctx := context.Background()
	if sc, ok := spanContext(rec.Fields); ok {
		ctx = trace.ContextWithSpanContext(ctx, sc)
	}
	if src, ok := rec.Fields[logger.SourceKey]; ok {
		delete(rec.Fields, logger.SourceKey)
		r.AddAttributes(sourceAttrs(src)...)
	}
	if rec.Stack != "" {
		r.AddAttributes(attribute.String("code.stacktrace", rec.Stack))
	}
	for _, k := range slices.Sorted(maps.Keys(rec.Fields)) {
		r.AddAttributes(attribute.KeyValue{Key: attribute.Key(k), Value: value(rec.Fields[k])})
	}
	s.logger.Emit(ctx, r)
	return len(b), nil
}

// Sync exports the queued records.
func (s *sink) Sync() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.wait)
	defer cancel()
	return s.provider.ForceFlush(ctx)
}

// Close exports the queued records and stops the exporter.
func (s *sink) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.wait)
	defer cancel()
	return s.provider.Shutdown(ctx)
}

// Severity maps a slog level to an OpenTelemetry severity: slog's levels
// are 4 apart like the severity ranges, so it is level + 9, within 1..24.
func Severity(l slog.Level) otellog.Severity {
	return otellog.Severity(min(max(int(l)+9, 1), 24))
}

// spanContext reads trace_id and span_id (hex) as added by TraceAttrs and
// removes them from fields.
func spanContext(fields map[string]any) (trace.SpanContext, bool) {
	tid, _ := fields["trace_id"].(string)
	sid, _ := fields["span_id"].(string)
	traceID, err1 := trace.TraceIDFromHex(tid)
	spanID, err2 := trace.SpanIDFromHex(sid)
	if err1 != nil || err2 != nil {
		return trace.SpanContext{}, false
	}
	delete(fields, "trace_id")
	delete(fields, "span_id")
	cfg := trace.SpanContextConfig{TraceID: traceID, SpanID: spanID}
	if flags, ok := fields["trace_flags"].(string); ok {
		if b, err := hex.DecodeString(flags); err == nil && len(b) == 1 {
			cfg.TraceFlags = trace.TraceFlags(b[0])
			delete(fields, "trace_flags")
		}
	}
	return trace.NewSpanContext(cfg), true
}

// sourceAttrs converts slog's {"function","file","line"} or zap and
// zerolog's "file:line".
func sourceAttrs(v any) []attribute.KeyValue {
	var fn, file, line string
	switch src := v.(type) {
	case map[string]any:
		fn, _ = src["function"].(string)
		file, _ = src["file"].(string)
		line = fmt.Sprint(src["line"])
	case string:
		file = src
		if i := strings.LastIndexByte(src, ':'); i > 0 {
			file, line = src[:i], src[i+1:]
		}
	default:
		return nil
	}
	var attrs []attribute.KeyValue
	if file != "" {
		attrs = append(attrs, attribute.String("code.file.path", file))
	}
	if n, err := strconv.ParseInt(line, 10, 64); err == nil {
		attrs = append(attrs, attribute.Int64("code.line.number", n))
	}
	if fn != "" {
		attrs = append(attrs, attribute.String("code.function.name", fn))
	}
	return attrs
}

// value converts a decoded JSON value.
func value(v any) attribute.Value {
	switch v := v.(type) {
	case string:
		return attribute.StringValue(v)
	case bool:
		return attribute.BoolValue(v)
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return attribute.Int64Value(n)
		}
		f, _ := v.Float64()
		return attribute.Float64Value(f)
	case []any:
		vals := make([]attribute.Value, len(v))
		for i, e := range v {
			vals[i] = value(e)
		}
		return attribute.SliceValue(vals...)
	case map[string]any:
		kvs := make([]attribute.KeyValue, 0, len(v))
		for _, k := range slices.Sorted(maps.Keys(v)) {
			kvs = append(kvs, attribute.KeyValue{Key: attribute.Key(k), Value: value(v[k])})
		}
		return attribute.MapValue(kvs...)
	default:
		return attribute.Value{}
	}
}

// TraceAttrs returns the trace_id and span_id of the span in ctx, for
// logger.WithContextAttrs; the otlp output turns them back into the
// record's trace context.
func TraceAttrs(ctx context.Context) []slog.Attr {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []slog.Attr{
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
		slog.String("trace_flags", sc.TraceFlags().String()),
	}
}
