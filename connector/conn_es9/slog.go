package conn_es9

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v9"
)

// SlogLogger returns a transport logger that writes one record per request
// attempt to l: Debug for successful responses, Warn for 4xx and 5xx
// statuses and Error for transport errors. Records carry the method, the URL
// without user info, the status, the duration and the error. Request and
// response bodies are not logged. A nil l uses slog.Default().
func SlogLogger(l *slog.Logger) elastictransport.Logger {
	if l == nil {
		l = slog.Default()
	}
	return slogLogger{l}
}

// WithLogger sets [SlogLogger](l) as the transport logger instead of
// SlogLogger(slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(c *elasticsearch.Config) { c.Logger = SlogLogger(l) }
}

type slogLogger struct{ l *slog.Logger }

func (s slogLogger) LogRoundTrip(req *http.Request, res *http.Response, err error, _ time.Time, dur time.Duration) error {
	ctx := context.Background()
	if req != nil {
		ctx = req.Context()
	}
	status := 0
	if res != nil {
		status = res.StatusCode
	}
	level := slog.LevelDebug
	switch {
	case err != nil:
		level = slog.LevelError
	case status >= 400:
		level = slog.LevelWarn
	}
	if !s.l.Enabled(ctx, level) {
		return nil
	}

	attrs := make([]slog.Attr, 0, 5)
	if req != nil {
		attrs = append(attrs, slog.String("method", req.Method))
		if req.URL != nil {
			attrs = append(attrs, slog.String("url", redactURL(req.URL)))
		}
	}
	if status != 0 {
		attrs = append(attrs, slog.Int("status", status))
	}
	attrs = append(attrs, slog.Duration("duration", dur))
	if err != nil {
		attrs = append(attrs, slog.Any("error", err))
	}
	s.l.LogAttrs(ctx, level, "elasticsearch request", attrs...)
	return nil
}

func (slogLogger) RequestBodyEnabled() bool  { return false }
func (slogLogger) ResponseBodyEnabled() bool { return false }
