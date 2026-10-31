package config

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

// validateValue runs `validate` tags and Validator hooks on root, which must
// be an addressable struct.
func validateValue(root reflect.Value, tagName string) error {
	var errs []error
	walk("", root, tagName, "", func(path string, v reflect.Value, rules string) {
		if rules != "" {
			errs = append(errs, checkRules(path, v, rules)...)
		}
		if v.Kind() == reflect.Pointer && v.IsNil() {
			return
		}

		var hook Validator
		if v.CanAddr() {
			hook, _ = v.Addr().Interface().(Validator)
		}
		if hook == nil && v.CanInterface() {
			hook, _ = v.Interface().(Validator)
		}
		if hook != nil {
			for _, err := range flattenJoined(hook.Validate()) {
				errs = append(errs, &FieldError{Path: path, Err: err})
			}
		}
	})
	return errors.Join(errs...)
}

// flattenJoined splits errors.Join results so each line gets its own path.
func flattenJoined(err error) []error {
	if err == nil {
		return nil
	}
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		return []error{err}
	}
	var out []error
	for _, e := range joined.Unwrap() {
		out = append(out, flattenJoined(e)...)
	}
	return out
}

// walk visits every value reachable from v through decodable fields, Named
// instances, slices and maps, children first, passing the `validate` tag of
// the field holding each value. Visited values are addressable; map values
// and Named instances are copies. A nil pointer is visited but not entered,
// so that required can report it.
func walk(path string, v reflect.Value, tagName, rules string, fn func(path string, v reflect.Value, rules string)) {
	orig := v
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			fn(path, orig, rules)
			return
		}
		v = v.Elem()
	}
	t := v.Type()

	switch {
	case isNamed(t):
		v.Addr().Interface().(namedContainer).rangeNamed(func(name string, ev reflect.Value) {
			walk(joinPath(path, name), ev, tagName, "", fn)
		})
	case t.Kind() == reflect.Struct && !isOpaque(t):
		for _, f := range getStructInfo(t, tagName).fields {
			if fv, ok := fieldByIndex(v, f.index, false); ok {
				walk(joinPath(path, f.key), fv, tagName, f.rules, fn)
			}
		}
	case (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Uint8:
		for i := range v.Len() {
			walk(fmt.Sprintf("%s[%d]", path, i), v.Index(i), tagName, "", fn)
		}
	case t.Kind() == reflect.Map:
		for _, k := range sortedKeys(v) {
			ev := reflect.New(t.Elem()).Elem()
			ev.Set(v.MapIndex(k))
			walk(joinPath(path, fmt.Sprint(k)), ev, tagName, "", fn)
		}
	}
	fn(path, orig, rules)
}

func sortedKeys(m reflect.Value) []reflect.Value {
	keys := m.MapKeys()
	slices.SortFunc(keys, func(a, b reflect.Value) int { return cmp.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return keys
}

// ---------------------------------------------------------------------------
// validate tags
//
// A small, fixed rule set rather than go-playground/validator: config fields
// need presence, bounds, enums and address formats, and the full validator
// pulled six modules into every program that loads a config. Rules that need
// code go in a Validate() error method instead.

// checkRules applies a `validate` tag to v. A field reports only the first
// rule it fails; a malformed tag is reported whatever the value, so a typo in
// a rule cannot pass unnoticed.
func checkRules(path string, v reflect.Value, tag string) []error {
	rs, err := compileRules(tag, v.Type())
	if err != nil {
		return []error{&FieldError{Path: path, Err: fmt.Errorf("validate tag %q: %w", tag, err)}}
	}
	return rs.check(path, v)
}

type ruleSet struct {
	omitEmpty bool
	rules     []rule
	dive      *ruleSet // applied to each element when the tag has "dive"
}

type rule struct {
	name string
	text string                     // as written in the tag, for error messages
	ok   func(v reflect.Value) bool // v is never a non-nil pointer
}

func (rs *ruleSet) check(path string, v reflect.Value) []error {
	if rs.omitEmpty && isEmpty(v) {
		return nil
	}
	for v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	missing := v.Kind() == reflect.Pointer // nil: only required can judge it
	for _, r := range rs.rules {
		if missing && r.name != "required" {
			continue
		}
		if !r.ok(v) {
			return []error{&FieldError{Path: path, Err: fmt.Errorf("does not satisfy %q", r.text)}}
		}
	}
	if rs.dive == nil || missing {
		return nil
	}
	var errs []error
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			errs = append(errs, rs.dive.check(fmt.Sprintf("%s[%d]", path, i), v.Index(i))...)
		}
	case reflect.Map:
		for _, k := range sortedKeys(v) {
			errs = append(errs, rs.dive.check(joinPath(path, fmt.Sprint(k)), v.MapIndex(k))...)
		}
	}
	return errs
}

func compileRules(tag string, t reflect.Type) (*ruleSet, error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	rs := &ruleSet{}
	parts := strings.Split(tag, ",")
	for i, text := range parts {
		text = strings.TrimSpace(text)
		name, param, _ := strings.Cut(text, "=")
		switch name {
		case "":
		case "omitempty":
			rs.omitEmpty = true
		case "dive":
			if k := t.Kind(); k != reflect.Slice && k != reflect.Array && k != reflect.Map {
				return nil, fmt.Errorf("dive needs a slice, array or map, not %s", t)
			}
			d, err := compileRules(strings.Join(parts[i+1:], ","), t.Elem())
			if err != nil {
				return nil, err
			}
			rs.dive = d
			return rs, nil
		default:
			ok, err := compileRule(name, param, t)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", text, err)
			}
			rs.rules = append(rs.rules, rule{name: name, text: text, ok: ok})
		}
	}
	return rs, nil
}

var formatRules = map[string]func(string) bool{
	"url": func(s string) bool {
		// Opaque URLs are rejected so that host:port, which parses as scheme
		// "host", is not taken for a URL.
		u, err := url.Parse(s)
		return err == nil && u.Scheme != "" && u.Opaque == "" && (u.Host != "" || u.Path != "")
	},
	"hostname_port": func(s string) bool {
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			return false
		}
		if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
			return false
		}
		// An empty host, as in ":8080", listens on every interface.
		return host == "" || net.ParseIP(host) != nil || isHostname(host)
	},
	"ip": func(s string) bool { return net.ParseIP(s) != nil },
	"cidr": func(s string) bool {
		_, _, err := net.ParseCIDR(s)
		return err == nil
	},
}

func compileRule(name, param string, t reflect.Type) (func(reflect.Value) bool, error) {
	switch name {
	case "required":
		if param != "" {
			return nil, errors.New("takes no parameter")
		}
		return func(v reflect.Value) bool { return !isEmpty(v) }, nil
	case "min", "gte":
		return compare(t, param, func(c int) bool { return c >= 0 })
	case "max", "lte":
		return compare(t, param, func(c int) bool { return c <= 0 })
	case "gt":
		return compare(t, param, func(c int) bool { return c > 0 })
	case "lt":
		return compare(t, param, func(c int) bool { return c < 0 })
	case "len":
		return compare(t, param, func(c int) bool { return c == 0 })
	case "oneof":
		return oneOf(t, param)
	}
	test, ok := formatRules[name]
	if !ok {
		return nil, errors.New("unknown rule")
	}
	if param != "" {
		return nil, errors.New("takes no parameter")
	}
	if !isText(t) {
		return nil, fmt.Errorf("applies to strings, not %s", t)
	}
	return func(v reflect.Value) bool { return test(textOf(v)) }, nil
}

// compare builds a bound check. Strings, slices and maps compare their
// length; numbers compare their value, with the parameter decoded like a
// config value of the field's type, so min=1s and max=64MiB work.
func compare(t reflect.Type, param string, want func(int) bool) (func(reflect.Value) bool, error) {
	switch {
	case isText(t) || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map:
		n, err := strconv.Atoi(param)
		if err != nil {
			return nil, fmt.Errorf("parameter %q is not a length", param)
		}
		return func(v reflect.Value) bool { return want(cmp.Compare(lengthOf(v), n)) }, nil
	case isNumber(t):
		p, err := typedParam(t, param)
		if err != nil {
			return nil, err
		}
		return func(v reflect.Value) bool { return want(compareNumbers(v, p)) }, nil
	}
	return nil, fmt.Errorf("does not apply to %s", t)
}

func oneOf(t reflect.Type, param string) (func(reflect.Value) bool, error) {
	opts := strings.Fields(param)
	if len(opts) == 0 {
		return nil, errors.New("needs values separated by spaces")
	}
	switch {
	case isText(t):
		return func(v reflect.Value) bool { return slices.Contains(opts, textOf(v)) }, nil
	case isNumber(t):
		vals := make([]reflect.Value, len(opts))
		for i, o := range opts {
			p, err := typedParam(t, o)
			if err != nil {
				return nil, err
			}
			vals[i] = p
		}
		return func(v reflect.Value) bool {
			return slices.ContainsFunc(vals, func(p reflect.Value) bool { return compareNumbers(v, p) == 0 })
		}, nil
	}
	return nil, fmt.Errorf("does not apply to %s", t)
}

func typedParam(t reflect.Type, param string) (reflect.Value, error) {
	p := reflect.New(t).Elem()
	d := &decoder{}
	d.decode("", param, p)
	if err := errors.Join(d.errs...); err != nil {
		return reflect.Value{}, fmt.Errorf("parameter %q: %w", param, err)
	}
	return p, nil
}

func isEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		return v.IsNil()
	case reflect.String, reflect.Slice, reflect.Map:
		return v.Len() == 0
	}
	switch {
	case v.Type() == secretType:
		return textOf(v) == ""
	case isNamed(v.Type()):
		return v.Interface().(interface{ Len() int }).Len() == 0
	}
	return v.IsZero()
}

var secretType = reflect.TypeFor[Secret]()

// isText reports whether rules see t as a string. A Secret is checked by the
// string it holds.
func isText(t reflect.Type) bool { return t.Kind() == reflect.String || t == secretType }

func textOf(v reflect.Value) string {
	if v.Type() == secretType {
		return v.Interface().(Secret).Value()
	}
	return v.String()
}

func lengthOf(v reflect.Value) int {
	if isText(v.Type()) {
		return utf8.RuneCountInString(textOf(v))
	}
	return v.Len()
}

func isNumber(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func compareNumbers(a, b reflect.Value) int {
	switch a.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(a.Int(), b.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return cmp.Compare(a.Uint(), b.Uint())
	}
	return cmp.Compare(a.Float(), b.Float())
}

// isHostname checks RFC 1123 host names.
func isHostname(s string) bool {
	s = strings.TrimSuffix(s, ".")
	if s == "" || len(s) > 253 {
		return false
	}
	for label := range strings.SplitSeq(s, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, r := range label {
			if !(r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z') {
				return false
			}
		}
	}
	return true
}
