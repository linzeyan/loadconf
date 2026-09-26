package config

import (
	"encoding"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// decoder decodes a raw source tree (maps, slices and scalars as produced by
// the format parsers) into Go values with weak typing: strings from env vars
// convert to numbers, bools, durations and comma separated slices.
type decoder struct {
	tagName string
	errs    []error
}

func (d *decoder) fail(path string, err error) {
	d.errs = append(d.errs, &FieldError{Path: path, Err: err})
}

func (d *decoder) failf(path, format string, args ...any) {
	d.fail(path, fmt.Errorf(format, args...))
}

// decode stores raw into v, which must be settable. A nil raw leaves v as is.
func (d *decoder) decode(path string, raw any, v reflect.Value) {
	if raw == nil {
		return
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			nv := reflect.New(v.Type().Elem())
			d.applyDefaults(path, nv.Elem())
			v.Set(nv)
		}
		d.decode(path, raw, v.Elem())
		return
	}

	// Values the parsers already produced in the right type, e.g. time.Time
	// from TOML, are stored as is.
	if reflect.TypeOf(raw) == v.Type() {
		v.Set(reflect.ValueOf(raw))
		return
	}

	if v.CanAddr() {
		switch u := v.Addr().Interface().(type) {
		case namedContainer:
			u.decodeNamed(d, path, raw)
			return
		case Unmarshaler:
			if err := u.UnmarshalConfig(raw); err != nil {
				d.fail(path, err)
			}
			return
		case encoding.TextUnmarshaler:
			s, ok := scalarString(raw)
			if !ok {
				d.failf(path, "expected a string for %s, got %T", v.Type(), raw)
				return
			}
			if err := u.UnmarshalText([]byte(s)); err != nil {
				d.fail(path, err)
			}
			return
		}
	}

	if v.Type() == durationType {
		dur, err := toDuration(raw)
		if err != nil {
			d.fail(path, err)
			return
		}
		v.SetInt(int64(dur))
		return
	}
	if v.Type() == headerType {
		d.decodeHeader(path, raw, v)
		return
	}

	switch v.Kind() {
	case reflect.Struct:
		d.decodeStruct(path, raw, v)
	case reflect.Map:
		d.decodeMap(path, raw, v)
	case reflect.Slice:
		d.decodeSlice(path, raw, v)
	case reflect.Array:
		d.decodeArray(path, raw, v)
	case reflect.Interface:
		if v.NumMethod() != 0 {
			d.failf(path, "cannot decode into non-empty interface %s", v.Type())
			return
		}
		v.Set(reflect.ValueOf(raw))
	default:
		d.decodeScalar(path, raw, v)
	}
}

func (d *decoder) decodeStruct(path string, raw any, v reflect.Value) {
	m, ok := raw.(map[string]any)
	if !ok {
		d.failf(path, "expected a mapping, got %T", raw)
		return
	}

	si := getStructInfo(v.Type(), d.tagName)
	for _, key := range slices.Sorted(maps.Keys(m)) {
		f, ok := si.lookup(key)
		if !ok {
			continue // unknown keys are reported by the normalizer in strict mode
		}
		fv, ok := fieldByIndex(v, f.index, true)
		if !ok {
			d.failf(joinPath(path, f.key), "cannot set field %s through a nil unexported embedded pointer", f.name)
			continue
		}
		d.decode(joinPath(path, f.key), m[key], fv)
	}
}

func (d *decoder) decodeMap(path string, raw any, v reflect.Value) {
	m, ok := raw.(map[string]any)
	if !ok {
		d.failf(path, "expected a mapping, got %T", raw)
		return
	}
	t := v.Type()
	if v.IsNil() {
		v.Set(reflect.MakeMapWithSize(t, len(m)))
	}

	for _, k := range slices.Sorted(maps.Keys(m)) {
		p := joinPath(path, k)
		key := reflect.New(t.Key()).Elem()
		d.decode(p, k, key)

		elem := reflect.New(t.Elem()).Elem()
		if existing := v.MapIndex(key); existing.IsValid() {
			elem.Set(existing)
		} else {
			d.applyDefaults(p, elem)
		}
		d.decode(p, m[k], elem)
		v.SetMapIndex(key, elem)
	}
}

// decodeHeader stores header names in canonical form, as http.Header.Add
// does: clients read headers with Get, which misses keys such as the
// lower-case ones of environment variables.
func (d *decoder) decodeHeader(path string, raw any, v reflect.Value) {
	var m map[string][]string
	d.decodeMap(path, raw, reflect.ValueOf(&m).Elem())
	if v.IsNil() {
		v.Set(reflect.ValueOf(http.Header{}))
	}
	h := v.Interface().(http.Header)
	for k, values := range m {
		h.Del(k)
		for _, s := range values {
			h.Add(k, s)
		}
	}
}

func (d *decoder) decodeSlice(path string, raw any, v reflect.Value) {
	if s, ok := raw.(string); ok && v.Type().Elem().Kind() == reflect.Uint8 {
		v.SetBytes([]byte(s))
		return
	}
	items := toList(raw)
	out := reflect.MakeSlice(v.Type(), len(items), len(items))
	for i, item := range items {
		p := fmt.Sprintf("%s[%d]", path, i)
		d.applyDefaults(p, out.Index(i))
		d.decode(p, item, out.Index(i))
	}
	v.Set(out)
}

func (d *decoder) decodeArray(path string, raw any, v reflect.Value) {
	items := toList(raw)
	if len(items) > v.Len() {
		d.failf(path, "expected at most %d items, got %d", v.Len(), len(items))
		return
	}
	for i, item := range items {
		d.decode(fmt.Sprintf("%s[%d]", path, i), item, v.Index(i))
	}
}

// toList accepts a list, a comma separated string (env vars) or a single
// scalar that becomes a one-item list.
func toList(raw any) []any {
	switch x := raw.(type) {
	case []any:
		return x
	case string:
		var out []any
		for part := range strings.SplitSeq(x, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return []any{raw}
	}
}

func (d *decoder) decodeScalar(path string, raw any, v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		s, ok := scalarString(raw)
		if !ok {
			d.failf(path, "expected a string, got %T", raw)
			return
		}
		v.SetString(s)
	case reflect.Bool:
		switch x := raw.(type) {
		case bool:
			v.SetBool(x)
		case string:
			b, err := strconv.ParseBool(strings.TrimSpace(x))
			if err != nil {
				d.failf(path, "invalid bool %q", x)
				return
			}
			v.SetBool(b)
		default:
			d.failf(path, "expected a bool, got %T", raw)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := toInt64(raw)
		if err == nil && v.OverflowInt(n) {
			err = fmt.Errorf("%d overflows %s", n, v.Type())
		}
		if err != nil {
			d.fail(path, err)
			return
		}
		v.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		n, err := toUint64(raw)
		if err == nil && v.OverflowUint(n) {
			err = fmt.Errorf("%d overflows %s", n, v.Type())
		}
		if err != nil {
			d.fail(path, err)
			return
		}
		v.SetUint(n)
	case reflect.Float32, reflect.Float64:
		f, err := toFloat64(raw)
		if err == nil && v.OverflowFloat(f) {
			err = fmt.Errorf("%v overflows %s", f, v.Type())
		}
		if err != nil {
			d.fail(path, err)
			return
		}
		v.SetFloat(f)
	default:
		d.failf(path, "unsupported field type %s", v.Type())
	}
}

// scalarString formats a scalar as a string; maps and lists are rejected.
func scalarString(raw any) (string, bool) {
	switch x := raw.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(x), true
	case fmt.Stringer:
		return x.String(), true
	default:
		return "", false
	}
}

func toInt64(raw any) (int64, error) {
	switch x := raw.(type) {
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 0, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid integer %q", x)
		}
		return n, nil
	case json.Number:
		return toInt64(x.String())
	}
	rv := reflect.ValueOf(raw)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if rv.Uint() > math.MaxInt64 {
			return 0, fmt.Errorf("%d overflows int64", rv.Uint())
		}
		return int64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		// float64(math.MaxInt64) rounds up to 2^63, which does not fit an int64;
		// math.MinInt64 is exact.
		if f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
			return 0, fmt.Errorf("%v is not an integer", f)
		}
		return int64(f), nil
	}
	return 0, fmt.Errorf("expected an integer, got %T", raw)
}

func toUint64(raw any) (uint64, error) {
	switch x := raw.(type) {
	case string:
		n, err := strconv.ParseUint(strings.TrimSpace(x), 0, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid unsigned integer %q", x)
		}
		return n, nil
	case json.Number:
		return toUint64(x.String())
	}
	rv := reflect.ValueOf(raw)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.Int() < 0 {
			return 0, fmt.Errorf("%d is negative", rv.Int())
		}
		return uint64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		// float64(math.MaxUint64) rounds up to 2^64, which does not fit a uint64.
		if f != math.Trunc(f) || f < 0 || f >= math.MaxUint64 {
			return 0, fmt.Errorf("%v is not an unsigned integer", f)
		}
		return uint64(f), nil
	}
	return 0, fmt.Errorf("expected an unsigned integer, got %T", raw)
}

func toFloat64(raw any) (float64, error) {
	switch x := raw.(type) {
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid number %q", x)
		}
		return f, nil
	case json.Number:
		return toFloat64(x.String())
	}
	rv := reflect.ValueOf(raw)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	}
	return 0, fmt.Errorf("expected a number, got %T", raw)
}

// toDuration parses duration strings such as "1m30s". Bare numbers are
// rejected (except 0) because their unit would be ambiguous.
func toDuration(raw any) (time.Duration, error) {
	if s, ok := raw.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" || s == "0" {
			return 0, nil
		}
		dur, err := time.ParseDuration(s)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q (use a value such as \"500ms\", \"5s\" or \"1m\")", s)
		}
		return dur, nil
	}
	if f, err := toFloat64(raw); err == nil {
		if f == 0 {
			return 0, nil
		}
		return 0, fmt.Errorf("invalid duration %v: add a unit, e.g. \"%vs\"", raw, raw)
	}
	return 0, fmt.Errorf("expected a duration string, got %T", raw)
}

// applyDefaults fills a freshly created value from `default` tags and
// Defaulter implementations, recursing into nested structs.
func (d *decoder) applyDefaults(path string, v reflect.Value) {
	t := v.Type()
	if t.Kind() == reflect.Struct && !isOpaque(t) {
		for _, f := range getStructInfo(t, d.tagName).fields {
			if !f.hasDef && f.typ.Kind() != reflect.Struct {
				continue
			}
			fv, ok := fieldByIndex(v, f.index, true)
			if !ok {
				continue
			}
			p := joinPath(path, f.key)
			if f.hasDef {
				d.decode(p, f.def, fv)
			}
			if f.typ.Kind() == reflect.Struct {
				d.applyDefaults(p, fv)
			}
		}
	}
	if v.CanAddr() {
		if df, ok := v.Addr().Interface().(Defaulter); ok {
			df.SetDefaults()
		}
	}
}
