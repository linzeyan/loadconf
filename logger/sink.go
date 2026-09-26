package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/linzeyan/loadconf/config"
)

// Sink is an opened log destination. Each Write receives exactly one
// encoded record; sinks must be safe for concurrent use.
type Sink interface {
	io.Writer
	// Close flushes buffered records and releases the destination.
	Close() error
}

// Syncer is implemented by sinks that can flush without closing.
type Syncer interface {
	Sync() error
}

// SinkEnv carries process-wide settings to sinks.
type SinkEnv struct {
	// Service is config.Log.Service, or the executable name.
	Service  string
	Hostname string
	// OnError reports delivery problems of asynchronous sinks, such as
	// dropped records; it must not block.
	OnError func(error)
}

// SinkOpener opens a sink for an output. Custom output types pass one to the
// logger with WithSink; custom sinks receive JSON records, see [ParseRecord].
type SinkOpener func(ctx context.Context, out config.LogOutput, env SinkEnv) (Sink, error)

// builtinSink returns the opener of a built-in output type. There is no
// process-wide registry: a logger opens exactly the outputs its config lists,
// and types outside this set come from the caller.
func builtinSink(typ string) (SinkOpener, bool) {
	switch typ {
	case config.LogStdout:
		return func(context.Context, config.LogOutput, SinkEnv) (Sink, error) {
			return stdSink{stdoutMu, os.Stdout}, nil
		}, true
	case config.LogStderr:
		return func(context.Context, config.LogOutput, SinkEnv) (Sink, error) {
			return stdSink{stderrMu, os.Stderr}, nil
		}, true
	case config.LogFile:
		return openFile, true
	case config.LogSyslog:
		return openSyslog, true
	case config.LogGELF:
		return openGELF, true
	case config.LogElasticsearch:
		return openElasticsearch, true
	}
	return nil, false
}

// textCapable reports whether an output type honors the text format.
func textCapable(typ string) bool {
	return typ == config.LogStdout || typ == config.LogStderr || typ == config.LogFile
}

// The standard streams are shared by every output writing to them.
var (
	stdoutMu = &sync.Mutex{}
	stderrMu = &sync.Mutex{}
)

type stdSink struct {
	mu *sync.Mutex
	f  *os.File
}

func (s stdSink) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Write(b)
}

func (stdSink) Close() error { return nil }

func (s stdSink) Sync() error {
	// Syncing a terminal or pipe fails on some systems; there is nothing to
	// flush anyway.
	_ = s.f.Sync()
	return nil
}

func openFile(_ context.Context, out config.LogOutput, _ SinkEnv) (Sink, error) {
	// lumberjack cannot open a directory and rotates it away instead,
	// renaming it with all it holds.
	if fi, err := os.Stat(out.Path); err == nil && fi.IsDir() {
		return nil, fmt.Errorf("%s is a directory", out.Path)
	}
	const mb = 1 << 20
	l := &lumberjack.Logger{
		Filename:   out.Path,
		MaxSize:    int((out.MaxSize + mb - 1) / mb),
		MaxBackups: out.MaxBackups,
		MaxAge:     int((out.MaxAge + 24*time.Hour - 1) / (24 * time.Hour)),
		LocalTime:  out.LocalTime,
		Compress:   out.Compress,
	}
	// Open now so that a bad path fails at startup rather than at the first
	// record.
	if _, err := l.Write(nil); err != nil {
		return nil, err
	}
	return l, nil
}
