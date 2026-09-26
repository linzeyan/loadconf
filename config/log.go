package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Log configures a logger built by github.com/linzeyan/loadconf/logger (slog) or its zap
// (logger/logger_zap) and zerolog (logger/logger_zerolog) variants.
//
//	log:
//	  level: info
//	  service: orders
//	  outputs:
//	    console: {type: stdout, format: text}
//	    file:    {path: logs/app.log, max_size: 100MiB, max_backups: 7}
//	    graylog: {type: gelf, addr: graylog:12201}
type Log struct {
	// Level is trace, debug, info (also when empty), warn, error or fatal. It
	// can be changed on hot reload.
	Level string `config:"level" default:"info"`
	// Format is json (also when empty) or text, the default of stdout,
	// stderr and file outputs.
	Format string `config:"format" default:"json"`
	// AddSource adds the file and line of the call site.
	AddSource bool `config:"add_source"`
	// StackLevel adds a stack trace to records at or above this level
	// (error when empty); none disables stack traces.
	StackLevel string `config:"stack_level" default:"error"`
	// TimeFormat is rfc3339, rfc3339milli (the default), rfc3339nano, unix,
	// unixmilli, unixnano or a Go layout such as "2006-01-02 15:04:05.000".
	TimeFormat string `config:"time_format"`
	// UTC formats times in UTC instead of the local zone.
	UTC bool `config:"utc"`
	// PrettyJSON indents JSON records of stdout, stderr and file outputs.
	// Meant for local development.
	PrettyJSON bool `config:"pretty_json"`

	// Service adds a "service" field and names the process towards syslog,
	// GELF and OTLP (default: the executable name).
	Service string `config:"service"`
	// Hostname adds a "host" field.
	Hostname bool `config:"hostname"`
	// Fields are static fields added to every record, e.g. env or version.
	Fields map[string]string `config:"fields"`

	// Outputs are the destinations; stdout when empty.
	Outputs Named[LogOutput] `config:"outputs"`
}

// Log output types.
const (
	LogStdout        = "stdout"
	LogStderr        = "stderr"
	LogFile          = "file"
	LogSyslog        = "syslog"
	LogGELF          = "gelf"
	LogElasticsearch = "elasticsearch"
	LogOTLP          = "otlp"
)

// LogOutput is one log destination. Fields apply to the types noted in their
// comments; custom types passed with logger.WithSink read Options.
type LogOutput struct {
	Name string `config:"name"`
	// Type is stdout, stderr, file, syslog, gelf, elasticsearch, otlp or a
	// custom type passed with logger.WithSink. It defaults to the output
	// name, so an output named "file" needs no type.
	Type string `config:"type"`
	// Level is the minimum level of this output; the higher of it and the
	// logger level applies.
	Level string `config:"level"`
	// Format overrides Log.Format for stdout, stderr and file. The other
	// outputs always receive JSON.
	Format string `config:"format"`
	// Disabled turns the output off, e.g. in one environment.
	Disabled bool `config:"disabled"`

	// Path is the log file (file).
	Path string `config:"path"`
	// MaxSize rotates the file when it grows beyond this size (file).
	MaxSize ByteSize `config:"max_size" default:"100MiB"`
	// MaxBackups and MaxAge bound the rotated files kept; zero keeps all
	// (file).
	MaxBackups int           `config:"max_backups"`
	MaxAge     time.Duration `config:"max_age"`
	// LocalTime names rotated files in local time instead of UTC (file).
	LocalTime bool `config:"local_time"`
	// Compress gzips rotated files (file), UDP datagrams (gelf) and request
	// bodies (elasticsearch, otlp).
	Compress bool `config:"compress"`

	// Network is udp, tcp or unix (syslog; empty for the local syslog
	// daemon) or udp and tcp (gelf, default udp).
	Network string `config:"network"`
	// Addr is host:port (syslog, gelf) or a socket path (syslog over unix).
	Addr string `config:"addr"`
	// Facility is the syslog facility, e.g. local0 (the default), user or
	// daemon (syslog).
	Facility string `config:"facility"`
	// Tag is the syslog app name (default Log.Service) (syslog).
	Tag string `config:"tag"`
	// SyslogFormat is rfc5424 (the default) or rfc3164 (syslog).
	SyslogFormat string `config:"syslog_format"`
	// ChunkSize is the largest UDP datagram before chunking (gelf, default
	// 1420).
	ChunkSize int `config:"chunk_size"`

	// Addresses are node URLs (elasticsearch).
	Addresses []string `config:"addresses"`
	// Username/Password or APIKey authenticate (elasticsearch).
	Username string `config:"username"`
	Password Secret `config:"password"`
	APIKey   Secret `config:"api_key"`
	// Index is the target index or data stream. {service} is replaced by
	// Log.Service and {date:LAYOUT} by the record date, e.g.
	// "app-{date:2006.01.02}" (elasticsearch, default "logs-{service}-default").
	Index string `config:"index"`

	// Endpoint is host:port or a URL of the collector; empty uses the
	// OTEL_EXPORTER_OTLP_* variables (otlp).
	Endpoint string `config:"endpoint"`
	// Protocol is grpc (the default) or http (otlp).
	Protocol string `config:"protocol"`
	// Insecure disables TLS towards the collector (otlp).
	Insecure bool `config:"insecure"`

	// Headers are sent with each request (elasticsearch, otlp).
	Headers map[string]string `config:"headers"`
	// Timeout bounds one write or export (syslog, gelf, elasticsearch, otlp).
	Timeout time.Duration `config:"timeout" default:"5s"`
	// BatchSize, FlushInterval and QueueSize tune asynchronous delivery.
	// Records beyond a full queue are dropped (elasticsearch, otlp).
	BatchSize     int           `config:"batch_size"     default:"500"`
	FlushInterval time.Duration `config:"flush_interval" default:"1s"`
	QueueSize     int           `config:"queue_size"     default:"10000"`
	// TLS secures syslog over tcp, gelf over tcp, elasticsearch and otlp.
	TLS TLS `config:"tls"`

	// Options holds the settings of custom output types.
	Options map[string]any `config:"options"`
}

// ResolvedType returns Type, or the output name when Type is empty.
func (o LogOutput) ResolvedType() string {
	if o.Type != "" {
		return strings.ToLower(o.Type)
	}
	return strings.ToLower(o.Name)
}

var (
	// The levels of logger.ParseLevel. Fatal is a level of its own there
	// (zap and zerolog log fatal and panic records at it), so an output that
	// takes only those, such as an alerting one, is configured with it.
	logLevels  = []string{"trace", "debug", "info", "warn", "warning", "error", "fatal"}
	logFormats = []string{"json", "text"}
)

func validLogLevel(s string) bool { return slices.Contains(logLevels, strings.ToLower(s)) }

func (l Log) Validate() error {
	var errs []error
	if l.Level != "" && !validLogLevel(l.Level) {
		errs = append(errs, fmt.Errorf("unsupported level %q (want trace, debug, info, warn, error or fatal)", l.Level))
	}
	if l.Format != "" && !slices.Contains(logFormats, strings.ToLower(l.Format)) {
		errs = append(errs, fmt.Errorf("unsupported format %q (want json or text)", l.Format))
	}
	if s := strings.ToLower(l.StackLevel); s != "" && s != "none" && !validLogLevel(s) {
		errs = append(errs, fmt.Errorf("unsupported stack_level %q (want a level or none)", l.StackLevel))
	}
	return errors.Join(errs...)
}

func (o LogOutput) Validate() error {
	var errs []error
	if o.Level != "" && !validLogLevel(o.Level) {
		errs = append(errs, fmt.Errorf("unsupported level %q", o.Level))
	}
	if o.Format != "" && !slices.Contains(logFormats, strings.ToLower(o.Format)) {
		errs = append(errs, fmt.Errorf("unsupported format %q (want json or text)", o.Format))
	}
	if o.MaxBackups < 0 || o.MaxAge < 0 || o.ChunkSize < 0 || o.BatchSize < 0 || o.QueueSize < 0 {
		errs = append(errs, errors.New("sizes and counts must not be negative"))
	}
	if o.Disabled {
		return errors.Join(errs...)
	}
	network := strings.ToLower(o.Network)
	switch o.ResolvedType() {
	case LogStdout, LogStderr:
	case LogFile:
		if o.Path == "" {
			errs = append(errs, errors.New("path is required for a file output"))
		}
	case LogSyslog:
		if !slices.Contains([]string{"", "udp", "tcp", "unix"}, network) {
			errs = append(errs, fmt.Errorf("unsupported network %q for syslog (want udp, tcp or unix)", o.Network))
		}
		if network != "" && o.Addr == "" {
			errs = append(errs, errors.New("addr is required when network is set"))
		}
		if !slices.Contains([]string{"", "rfc5424", "rfc3164"}, strings.ToLower(o.SyslogFormat)) {
			errs = append(errs, fmt.Errorf("unsupported syslog_format %q (want rfc5424 or rfc3164)", o.SyslogFormat))
		}
	case LogGELF:
		if !slices.Contains([]string{"", "udp", "tcp"}, network) {
			errs = append(errs, fmt.Errorf("unsupported network %q for gelf (want udp or tcp)", o.Network))
		}
		if o.Addr == "" {
			errs = append(errs, errors.New("addr is required for a gelf output"))
		}
		// The logger rejects these too, but only when it opens the output;
		// here the loader reports them with the key path.
		if o.ChunkSize > 0 && o.ChunkSize <= 12 {
			errs = append(errs, fmt.Errorf("chunk_size %d leaves no room after the 12-byte chunk header", o.ChunkSize))
		}
	case LogElasticsearch:
		if len(o.Addresses) == 0 {
			errs = append(errs, errors.New("addresses is required for an elasticsearch output"))
		}
		if o.Username != "" && o.APIKey.Value() != "" {
			errs = append(errs, errors.New("username and api_key are mutually exclusive"))
		}
	case LogOTLP:
		if !slices.Contains([]string{"", "grpc", "http"}, strings.ToLower(o.Protocol)) {
			errs = append(errs, fmt.Errorf("unsupported protocol %q for otlp (want grpc or http)", o.Protocol))
		}
	}
	return errors.Join(errs...)
}
