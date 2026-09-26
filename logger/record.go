package logger

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Record is a JSON log record as seen by structured sinks such as syslog,
// GELF, Elasticsearch and OTLP.
type Record struct {
	// Time is zero when the record has no parsable time.
	Time    time.Time
	Level   slog.Level
	Message string
	Stack   string
	// Fields holds the other top-level fields, including the source.
	// Numbers are json.Number.
	Fields map[string]any
}

var (
	timeKeys    = []string{TimeKey, "ts", "timestamp", "@timestamp"}
	messageKeys = []string{MessageKey, "message"}
	// zerolog writes its message as "message", after the attrs, and never
	// writes "msg", so beside a "message" a "msg" is an attr. Without one,
	// "msg" is still read, as from records of other origin.
	zerologMessageKeys = []string{"message", MessageKey}
	stackKeys          = []string{StackKey, "stacktrace"}
	// firstKeys are written by slog and zap ahead of the attrs, so the first
	// of duplicate keys is the record's own and the others are attrs of the
	// same name. For other keys the last is the record's own (every backend
	// writes the stack after the attrs, zerolog also its message).
	firstKeys = []string{TimeKey, LevelKey, MessageKey}
)

// ParseRecord parses one JSON record written by the slog, zap or zerolog
// logger of this package, accepting the key names of each (msg or message,
// stack or stacktrace, time or ts).
func ParseRecord(b []byte) (Record, error) {
	r, _, _, err := parseRecord(b, make([]member, 0, 32))
	return r, err
}

// parseRecord is ParseRecord that also returns the record's top-level
// members as written, appended to ms, and whether a key repeats, so that
// sinks needing both scan the record once and skip deduplication when no
// key repeats, as in almost every record.
func parseRecord(b []byte, ms []member) (Record, []member, bool, error) {
	var m map[string]any
	if err := decode(b, &m); err != nil {
		return Record{}, nil, false, err
	}
	if m == nil {
		return Record{}, nil, false, errors.New("log record is not a JSON object")
	}
	// Decoding keeps the last of duplicate keys, which for firstKeys is an
	// attr. Parse the first instead and put the attr back into Fields after.
	var attrs map[string]any
	ms = members(ms, b)
	dup := len(ms) > len(m)
	if dup {
		for _, k := range firstKeys {
			is := func(mb member) bool { return isKey(mb.key, k) }
			var v any
			if i := slices.IndexFunc(ms, is); i >= 0 && slices.ContainsFunc(ms[i+1:], is) && decode(ms[i].val, &v) == nil {
				if attrs == nil {
					attrs = map[string]any{}
				}
				attrs[k], m[k] = m[k], v
			}
		}
	}
	r := Record{Level: slog.LevelInfo, Fields: m}
	for _, k := range timeKeys {
		if v, ok := m[k]; ok {
			if t, ok := parseTime(v); ok {
				r.Time = t
				delete(m, k)
				break
			}
		}
	}
	msgKeys := zerologMessageKeys
	if s, ok := m[LevelKey].(string); ok {
		// slog and zap always write an upper-case level, zerolog a lower-case
		// one or, for Log(), none: that tells whose message key comes first.
		if s != "" && 'A' <= s[0] && s[0] <= 'Z' {
			msgKeys = messageKeys
		}
		if l, ok := parseLevelName(s); ok {
			r.Level = l
			delete(m, LevelKey)
		}
	}
	r.Message = takeString(m, msgKeys)
	r.Stack = takeString(m, stackKeys)
	maps.Copy(m, attrs)
	return r, ms, dup, nil
}

// decode decodes the first JSON value of b into v, numbers as json.Number.
func decode(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(v)
}

// member is a top-level member of a JSON object, undecoded: the key with
// its quotes and the value.
type member struct{ key, val []byte }

// members appends the members of the JSON object at the start of b to ms,
// in order and with duplicate keys. b must start with a valid object; what
// follows the object is ignored. Unlike decoding, it only slices b.
func members(ms []member, b []byte) []member {
	i := skipSpace(b, 0) + 1 // past '{'
	for {
		i = skipSpace(b, i)
		if i < len(b) && b[i] == ',' {
			i = skipSpace(b, i+1)
		}
		if i >= len(b) || b[i] == '}' {
			return ms
		}
		k := i
		i = skipValue(b, i)
		key := b[k:i]
		i = skipSpace(b, skipSpace(b, i)+1) // past ':'
		v := i
		i = skipValue(b, i)
		ms = append(ms, member{key, b[v:i]})
	}
}

func skipSpace(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// skipValue returns the end of the valid JSON value starting at b[i].
func skipValue(b []byte, i int) int {
	depth := 0
	for ; i < len(b); i++ {
		switch b[i] {
		case '"':
			for i++; i < len(b) && b[i] != '"'; i++ {
				if b[i] == '\\' {
					i++
				}
			}
		case '{', '[':
			depth++
			continue
		case '}', ']':
			if depth == 0 {
				return i // a number or literal ends the enclosing value
			}
			depth--
		case ',', ':', ' ', '\t', '\n', '\r':
			if depth == 0 {
				return i
			}
			continue
		default:
			continue
		}
		if depth == 0 {
			return i + 1
		}
	}
	return i
}

// isKey reports whether the JSON string s decodes to k.
func isKey(s []byte, k string) bool {
	if bytes.IndexByte(s, '\\') >= 0 {
		return unquote(s) == k
	}
	return string(s[1:len(s)-1]) == k
}

// sameKey reports whether the JSON strings s and t decode to the same key.
// Decoding also replaces invalid UTF-8, so different bytes can be one key.
func sameKey(s, t []byte) bool {
	if bytes.IndexByte(s, '\\') < 0 && bytes.IndexByte(t, '\\') < 0 && utf8.Valid(s) && utf8.Valid(t) {
		return bytes.Equal(s, t)
	}
	return unquote(s) == unquote(t)
}

// unquote decodes a JSON string. Encoders escape only unusual characters in
// keys, so it is rarely needed.
func unquote(s []byte) string {
	var d string
	_ = json.Unmarshal(s, &d)
	return d
}

// kept reports whether ms[i] is the occurrence of its key that ParseRecord
// reads: the first for firstKeys, otherwise the last. It is the one to keep
// where duplicate keys are not allowed.
//
// ponytail: compares with every other member, quadratic but allocation-free
// for the few dozen keys of a record; index keys in a map if records grow to
// hundreds.
func kept(ms []member, i int) bool {
	others := ms[i+1:]
	if slices.ContainsFunc(firstKeys, func(k string) bool { return isKey(ms[i].key, k) }) {
		others = ms[:i]
	}
	return !slices.ContainsFunc(others, func(o member) bool { return sameKey(o.key, ms[i].key) })
}

func takeString(m map[string]any, keys []string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok {
			delete(m, k)
			return s
		}
	}
	return ""
}

// parseLevelName accepts level names with an offset, such as "DEBUG-4" as
// written by slog for trace.
func parseLevelName(s string) (slog.Level, bool) {
	name, off := s, 0
	if i := strings.IndexAny(s, "+-"); i > 0 {
		n, err := strconv.Atoi(s[i:])
		if err != nil {
			return 0, false
		}
		name, off = s[:i], n
	}
	l, err := ParseLevel(name)
	if err != nil {
		switch strings.ToLower(name) {
		case "panic", "dpanic":
			l = LevelFatal
		default:
			return 0, false
		}
	}
	return l + slog.Level(off), true
}

func parseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, x); err == nil {
			return t, true
		}
	case json.Number:
		if n, err := x.Int64(); err == nil {
			switch {
			case n > 1e17:
				return time.Unix(0, n), true
			case n > 1e14:
				return time.UnixMicro(n), true
			case n > 1e11:
				return time.UnixMilli(n), true
			default:
				return time.Unix(n, 0), true
			}
		}
		if f, err := x.Float64(); err == nil {
			sec := int64(f)
			return time.Unix(sec, int64((f-float64(sec))*1e9)), true
		}
	}
	return time.Time{}, false
}
