package conn_mssql

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
)

func TestSlogLogger(t *testing.T) {
	var buf bytes.Buffer
	l := SlogLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx := context.Background()
	l.Log(ctx, msdsn.LogErrors, "login failed")
	l.Log(ctx, msdsn.LogRetries, "retrying")
	l.Log(ctx, msdsn.LogMessages, "Changed database context")
	l.Log(ctx, msdsn.LogSQL, "select 1") // Debug: filtered out
	l.Log(ctx, msdsn.Log(4096), "odd")   // unknown category: Debug

	out := buf.String()
	for _, want := range []string{
		`level=ERROR msg="login failed" category=errors`,
		`level=WARN msg=retrying category=retries`,
		`level=INFO msg="Changed database context" category=messages`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("%s missing %q", out, want)
		}
	}
	if strings.Contains(out, "select 1") || strings.Contains(out, "odd") {
		t.Errorf("debug records not filtered: %s", out)
	}

	buf.Reset()
	SlogLogger(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))).Log(ctx, msdsn.LogParams, "@p1=1")
	if !strings.Contains(buf.String(), `level=DEBUG msg="@p1=1" category=params`) {
		t.Errorf("got %s", buf.String())
	}
	if SlogLogger(nil) == nil {
		t.Error("nil logger should fall back to slog.Default")
	}
}
