package config

import (
	"encoding"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
)

// DefaultTagName is the struct tag used for key names unless changed with
// [WithTagName].
const DefaultTagName = "config"

// fieldInfo describes one decodable field of a struct, after squashed
// (embedded) structs have been flattened into their parent.
type fieldInfo struct {
	index     []int // index path for reflect.Value.FieldByIndex
	key       string
	name      string // Go field name, for diagnostics
	typ       reflect.Type
	env       []string // explicit env var names, already prefixed by squashed parents
	envPrefix string   // prefix for env tags of nested fields
	def       string
	hasDef    bool
}

type structInfo struct {
	fields  []fieldInfo
	byKey   map[string]int
	byLower map[string]int
}

// lookup finds a field by exact key first, then case-insensitively.
func (s *structInfo) lookup(key string) (*fieldInfo, bool) {
	if i, ok := s.byKey[key]; ok {
		return &s.fields[i], true
	}
	if i, ok := s.byLower[strings.ToLower(key)]; ok {
		return &s.fields[i], true
	}
	return nil, false
}

type structInfoKey struct {
	typ     reflect.Type
	tagName string
}

var structInfoCache sync.Map // structInfoKey -> *structInfo

func getStructInfo(t reflect.Type, tagName string) *structInfo {
	k := structInfoKey{typ: t, tagName: tagName}
	if v, ok := structInfoCache.Load(k); ok {
		return v.(*structInfo)
	}

	si := &structInfo{byKey: map[string]int{}}
	collectFields(t, tagName, nil, "", nil, si)
	si.byLower = make(map[string]int, len(si.fields))
	for i, f := range si.fields {
		si.byKey[f.key] = i
		si.byLower[strings.ToLower(f.key)] = i
	}

	v, _ := structInfoCache.LoadOrStore(k, si)
	return v.(*structInfo)
}

// stack lists the enclosing structs whose fields are being collected.
func collectFields(t reflect.Type, tagName string, index []int, envPrefix string, stack []reflect.Type, si *structInfo) {
	stack = append(stack, t)
	for i := range t.NumField() {
		sf := t.Field(i)
		key, squash, skip := parseKeyTag(sf.Tag.Get(tagName))
		if skip {
			continue
		}
		idx := append(slices.Clone(index), i)

		if (sf.Anonymous && key == "") || squash {
			bt := sf.Type
			if bt.Kind() == reflect.Pointer {
				bt = bt.Elem()
			}
			if bt.Kind() == reflect.Struct && !isOpaque(bt) {
				// A struct embedding itself (through a pointer) would recurse
				// forever. Its fields are already collected at a shallower depth,
				// which wins every key, so skipping it loses nothing.
				if !slices.Contains(stack, bt) {
					collectFields(bt, tagName, idx, envPrefix+sf.Tag.Get("env-prefix"), stack, si)
				}
				continue
			}
		}
		if !sf.IsExported() {
			continue
		}
		if key == "" {
			key = snakeCase(sf.Name)
		}

		fi := fieldInfo{
			index:     idx,
			key:       key,
			name:      sf.Name,
			typ:       sf.Type,
			envPrefix: envPrefix + sf.Tag.Get("env-prefix"),
		}
		fi.def, fi.hasDef = sf.Tag.Lookup("default")
		for name := range strings.SplitSeq(sf.Tag.Get("env"), ",") {
			if name = strings.TrimSpace(name); name != "" {
				fi.env = append(fi.env, envPrefix+name)
			}
		}

		// Like encoding/json, the shallowest field wins a key conflict.
		if j := slices.IndexFunc(si.fields, func(f fieldInfo) bool { return f.key == key }); j >= 0 {
			if len(si.fields[j].index) > len(idx) {
				si.fields[j] = fi
			}
			continue
		}
		si.fields = append(si.fields, fi)
	}
}

// parseKeyTag parses `config:"key,opt"`. The squash option is spelled either
// "squash" or "inline" so that yaml tags work with [WithTagName].
func parseKeyTag(tag string) (key string, squash, skip bool) {
	if tag == "-" {
		return "", false, true
	}
	key, opts, _ := strings.Cut(tag, ",")
	for opt := range strings.SplitSeq(opts, ",") {
		if opt == "squash" || opt == "inline" {
			squash = true
		}
	}
	return key, squash, false
}

// properNouns rewrites product and protocol names so that their inner
// capitals do not start a word: config files say mysql, never my_sql.
var properNouns = func() *strings.Replacer {
	var pairs []string
	for _, n := range []string{
		"MySQL", "MariaDB", "PostgreSQL", "SQLite", "SQLServer", "TiDB", "OceanBase",
		"MongoDB", "ClickHouse", "StarRocks", "ScyllaDB", "CockroachDB", "YugabyteDB",
		"TimescaleDB", "InfluxDB", "DuckDB", "CouchDB", "DynamoDB", "BigQuery",
		"ElasticSearch", "OpenSearch", "RabbitMQ", "RocketMQ", "ActiveMQ", "ZooKeeper",
		"MinIO", "GraphQL", "OAuth", "WebSocket", "GitHub", "GitLab", "IPv4", "IPv6",
	} {
		pairs = append(pairs, n, n[:1]+strings.ToLower(n[1:]))
	}
	return strings.NewReplacer(pairs...)
}()

// snakeCase converts a Go identifier to snake_case: MaxOpenConns ->
// max_open_conns, ClientID -> client_id, HTTPServer -> http_server,
// OrdersMySQL -> orders_mysql.
func snakeCase(s string) string {
	runes := []rune(properNouns.Replace(s))
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i, r := range runes {
		if unicode.IsUpper(r) {
			if i > 0 {
				prev := runes[i-1]
				nextLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
				if unicode.IsLower(prev) || unicode.IsDigit(prev) || (unicode.IsUpper(prev) && nextLower) {
					b.WriteByte('_')
				}
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

var (
	durationType        = reflect.TypeFor[time.Duration]()
	headerType          = reflect.TypeFor[http.Header]()
	textUnmarshalerType = reflect.TypeFor[encoding.TextUnmarshaler]()
	unmarshalerType     = reflect.TypeFor[Unmarshaler]()
	namedIfaceType      = reflect.TypeFor[namedContainer]()
)

// isOpaque reports whether t decodes itself as a whole, so the decoder must
// not look into its fields.
func isOpaque(t reflect.Type) bool {
	pt := reflect.PointerTo(t)
	return t == durationType ||
		pt.Implements(unmarshalerType) ||
		pt.Implements(textUnmarshalerType) ||
		pt.Implements(namedIfaceType)
}

func isNamed(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && reflect.PointerTo(t).Implements(namedIfaceType)
}

// namedElemType returns T for a Named[T] type.
func namedElemType(t reflect.Type) reflect.Type {
	return reflect.New(t).Interface().(namedContainer).elemType()
}

// fieldByIndex walks an index path, allocating nil embedded pointers when
// alloc is set. It returns false when a nil pointer blocks the path.
func fieldByIndex(v reflect.Value, index []int, alloc bool) (reflect.Value, bool) {
	for i, x := range index {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				if !alloc || !v.CanSet() {
					return reflect.Value{}, false
				}
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v, true
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
