package logger

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// connWriter writes framed messages over a lazily (re)dialed connection. A
// failed write is retried once on a fresh connection.
type connWriter struct {
	mu      sync.Mutex
	dial    func() (net.Conn, error)
	conn    net.Conn
	timeout time.Duration
}

func (c *connWriter) write(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		if c.conn == nil {
			if c.conn, err = c.dial(); err != nil {
				c.conn = nil
				continue
			}
		}
		if c.timeout > 0 {
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
		}
		if _, err = c.conn.Write(msg); err == nil {
			return nil
		}
		_ = c.conn.Close()
		c.conn = nil
	}
	return err
}

func (c *connWriter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func dialer(network, addr string, timeout time.Duration, tlsCfg *tls.Config) func() (net.Conn, error) {
	d := &net.Dialer{Timeout: timeout}
	if tlsCfg != nil {
		return func() (net.Conn, error) {
			return tls.DialWithDialer(d, network, addr, tlsCfg)
		}
	}
	return func() (net.Conn, error) { return d.Dial(network, addr) }
}

func tlsFor(out config.LogOutput, addr string) (*tls.Config, error) {
	cfg, err := out.TLS.Config()
	if err != nil || cfg == nil {
		return nil, err
	}
	if cfg.ServerName == "" {
		host, _, _ := net.SplitHostPort(addr)
		cfg.ServerName = host
	}
	return cfg, nil
}

// ---------------------------------------------------------------------------
// Syslog

var syslogFacilities = map[string]int{
	"kern": 0, "user": 1, "mail": 2, "daemon": 3, "auth": 4, "syslog": 5, "lpr": 6, "news": 7,
	"uucp": 8, "cron": 9, "authpriv": 10, "ftp": 11,
	"local0": 16, "local1": 17, "local2": 18, "local3": 19, "local4": 20, "local5": 21, "local6": 22, "local7": 23,
}

// severity maps a level to a syslog severity, which GELF uses too.
func severity(l slog.Level) int {
	switch {
	case l >= LevelFatal:
		return 2 // critical
	case l >= slog.LevelError:
		return 3
	case l >= slog.LevelWarn:
		return 4
	case l >= slog.LevelInfo:
		return 6
	default:
		return 7
	}
}

type syslogSink struct {
	w        *connWriter
	facility int
	rfc3164  bool
	local    bool
	octet    bool // octet-counting framing (RFC 6587) for stream transports
	lf       bool // newline framing for rfc3164 over streams
	host     string
	tag      string
	pid      string
}

func openSyslog(_ context.Context, out config.LogOutput, env SinkEnv) (Sink, error) {
	facility := 16
	if out.Facility != "" {
		f, ok := syslogFacilities[strings.ToLower(out.Facility)]
		if !ok {
			return nil, fmt.Errorf("unknown syslog facility %q", out.Facility)
		}
		facility = f
	}
	s := &syslogSink{
		facility: facility,
		rfc3164:  strings.EqualFold(out.SyslogFormat, "rfc3164"),
		host:     env.Hostname,
		tag:      nilValue(cmp.Or(out.Tag, env.Service)),
		pid:      strconv.Itoa(os.Getpid()),
	}
	// The tag is one printable token in both formats: RFC 3164 over TCP
	// frames by newline and ends the TAG at a space. RFC 5424 §6 also limits
	// APP-NAME to 48 characters (nilValue leaves ASCII).
	if !s.rfc3164 {
		s.tag = s.tag[:min(len(s.tag), 48)]
	}
	network := strings.ToLower(out.Network)
	switch network {
	case "":
		s.local = true
		s.w = &connWriter{dial: dialLocalSyslog, timeout: out.Timeout}
	case "unix":
		s.w = &connWriter{dial: func() (net.Conn, error) {
			if c, err := net.DialTimeout("unixgram", out.Addr, out.Timeout); err == nil {
				return c, nil
			}
			return net.DialTimeout("unix", out.Addr, out.Timeout)
		}, timeout: out.Timeout}
	case "udp", "tcp":
		tlsCfg, err := tlsFor(out, out.Addr)
		if err != nil {
			return nil, err
		}
		if network == "udp" && tlsCfg != nil {
			return nil, errors.New("syslog over udp does not support tls")
		}
		s.w = &connWriter{dial: dialer(network, out.Addr, out.Timeout, tlsCfg), timeout: out.Timeout}
		if network == "tcp" {
			s.octet, s.lf = !s.rfc3164, s.rfc3164
		}
	default:
		return nil, fmt.Errorf("unsupported syslog network %q", out.Network)
	}
	return s, nil
}

func dialLocalSyslog() (net.Conn, error) {
	var err error
	for _, path := range []string{"/dev/log", "/var/run/syslog", "/var/run/log"} {
		for _, network := range []string{"unixgram", "unix"} {
			var c net.Conn
			if c, err = net.Dial(network, path); err == nil {
				return c, nil
			}
		}
	}
	return nil, fmt.Errorf("no local syslog daemon: %w", err)
}

func (s *syslogSink) Write(b []byte) (int, error) {
	rec, err := ParseRecord(b)
	if err != nil {
		return 0, fmt.Errorf("syslog: %w", err)
	}
	t := rec.Time
	if t.IsZero() {
		t = time.Now()
	}
	pri := s.facility*8 + severity(rec.Level)
	body := bytes.TrimRight(b, "\n")
	var msg []byte
	switch {
	case s.rfc3164 && s.local:
		msg = fmt.Appendf(nil, "<%d>%s %s[%s]: %s", pri, t.Format(time.Stamp), s.tag, s.pid, body)
	case s.rfc3164:
		msg = fmt.Appendf(nil, "<%d>%s %s %s[%s]: %s", pri, t.Format(time.Stamp), s.host, s.tag, s.pid, body)
	default:
		msg = fmt.Appendf(nil, "<%d>1 %s %s %s %s - - %s", pri, t.Format(rfc5424Time),
			nilValue(s.host), s.tag, s.pid, body)
	}
	switch {
	case s.octet:
		msg = append(strconv.AppendInt(nil, int64(len(msg)), 10), append([]byte{' '}, msg...)...)
	case s.lf:
		msg = append(msg, '\n')
	}
	if err := s.w.write(msg); err != nil {
		return 0, fmt.Errorf("syslog: %w", err)
	}
	return len(b), nil
}

func (s *syslogSink) Close() error { return s.w.Close() }

// rfc5424Time is RFC 3339 with at most the 6 fraction digits that RFC 5424
// §6.2.3 allows; strict receivers reject RFC3339Nano's 9.
const rfc5424Time = "2006-01-02T15:04:05.999999Z07:00"

// nilValue returns "-" for an empty RFC 5424 header field and strips
// characters it does not allow.
func nilValue(s string) string {
	s = strings.Map(func(r rune) rune {
		if r <= ' ' || r > '~' {
			return -1
		}
		return r
	}, s)
	if s == "" {
		return "-"
	}
	return s
}

// ---------------------------------------------------------------------------
// GELF

const (
	gelfMaxChunks        = 128
	gelfDefaultChunkSize = 1420
)

type gelfSink struct {
	w         *connWriter
	udp       bool
	compress  bool
	chunkSize int
	host      string
	service   string
}

func openGELF(_ context.Context, out config.LogOutput, env SinkEnv) (Sink, error) {
	network := cmp.Or(strings.ToLower(out.Network), "udp")
	tlsCfg, err := tlsFor(out, out.Addr)
	if err != nil {
		return nil, err
	}
	if network == "udp" && tlsCfg != nil {
		return nil, errors.New("gelf over udp does not support tls")
	}
	// Each chunk starts with a 12-byte header, so a smaller size leaves no
	// room for data (writeUDP would divide by zero or send nothing).
	if out.ChunkSize != 0 && out.ChunkSize <= 12 {
		return nil, fmt.Errorf("chunk_size %d leaves no room after the 12-byte chunk header", out.ChunkSize)
	}
	return &gelfSink{
		w:         &connWriter{dial: dialer(network, out.Addr, out.Timeout, tlsCfg), timeout: out.Timeout},
		udp:       network == "udp",
		compress:  out.Compress && network == "udp",
		chunkSize: cmp.Or(out.ChunkSize, gelfDefaultChunkSize),
		host:      env.Hostname,
		service:   env.Service,
	}, nil
}

// gelfName replaces each character that GELF does not allow in a field name
// (other than [\w.-]) with "_". Unlike a regexp, it allocates only when it
// replaces something.
func gelfName(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '.' || r == '-' || r == '_' || '0' <= r && r <= '9' || 'a' <= r && r <= 'z' || 'A' <= r && r <= 'Z' {
			return r
		}
		return '_'
	}, s)
}

// GELFMessage converts a JSON record into a GELF 1.1 message. Nested
// objects are flattened with "_" and other fields are prefixed with "_".
func GELFMessage(b []byte, host, service string) ([]byte, error) {
	rec, err := ParseRecord(b)
	if err != nil {
		return nil, err
	}
	t := rec.Time
	if t.IsZero() {
		t = time.Now()
	}
	msg := map[string]any{
		"version":       "1.1",
		"host":          cmp.Or(host, "unknown"),
		"short_message": cmp.Or(rec.Message, "-"),
		"timestamp":     json.Number(strconv.FormatFloat(float64(t.UnixMicro())/1e6, 'f', 6, 64)),
		"level":         severity(rec.Level),
		"_level_name":   LevelName(rec.Level),
	}
	if rec.Stack != "" {
		msg["full_message"] = rec.Message + "\n" + rec.Stack
	}
	if service != "" {
		msg["_service"] = service
	}
	var add func(prefix string, v any)
	add = func(prefix string, v any) {
		switch x := v.(type) {
		case map[string]any:
			for k, vv := range x {
				add(prefix+"_"+gelfName(k), vv)
			}
			return
		case nil:
			return
		case json.Number, string:
		case bool:
			v = strconv.FormatBool(x)
		default:
			enc, _ := json.Marshal(x)
			v = string(enc)
		}
		if prefix == "_id" {
			prefix = "_id_" // reserved by GELF
		}
		msg[prefix] = v
	}
	for k, v := range rec.Fields {
		add("_"+gelfName(k), v)
	}
	// Written like json.Marshal(msg), which allocates for every entry of a
	// map. The keys need no escaping.
	keys := slices.AppendSeq(make([]string, 0, len(msg)), maps.Keys(msg))
	slices.Sort(keys)
	var buf bytes.Buffer
	buf.Grow(len(b) + 256)
	enc := json.NewEncoder(&buf)
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(k)
		buf.WriteString(`":`)
		if err := enc.Encode(msg[k]); err != nil {
			return nil, err
		}
		buf.Truncate(buf.Len() - 1) // Encode ends each value with a newline
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (g *gelfSink) Write(b []byte) (int, error) {
	msg, err := GELFMessage(b, g.host, g.service)
	if err != nil {
		return 0, fmt.Errorf("gelf: %w", err)
	}
	if !g.udp {
		err = g.w.write(append(msg, 0))
	} else {
		err = g.writeUDP(msg)
	}
	if err != nil {
		return 0, fmt.Errorf("gelf: %w", err)
	}
	return len(b), nil
}

func (g *gelfSink) writeUDP(msg []byte) error {
	if g.compress {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(msg)
		_ = zw.Close()
		msg = buf.Bytes()
	}
	if len(msg) <= g.chunkSize {
		return g.w.write(msg)
	}
	payload := g.chunkSize - 12
	count := (len(msg) + payload - 1) / payload
	if count > gelfMaxChunks {
		return fmt.Errorf("message of %d bytes needs %d chunks (max %d)", len(msg), count, gelfMaxChunks)
	}
	var id [8]byte
	_, _ = rand.Read(id[:])
	for i := range count {
		chunk := msg[i*payload : min((i+1)*payload, len(msg))]
		buf := make([]byte, 0, 12+len(chunk))
		buf = append(buf, 0x1e, 0x0f)
		buf = append(buf, id[:]...)
		buf = append(buf, byte(i), byte(count))
		buf = append(buf, chunk...)
		if err := g.w.write(buf); err != nil {
			return err
		}
	}
	return nil
}

func (g *gelfSink) Close() error { return g.w.Close() }
