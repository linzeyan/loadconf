package config

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
)

// normalizer rewrites a raw source tree against the target type before
// sources are merged:
//   - struct keys are matched case-insensitively and renamed to their canonical
//     key, so "MySQL" in one source and "mysql" in another merge together;
//   - Named values in list form become mappings keyed by normalized name, so
//     instances merge per name instead of the whole list being replaced;
//   - unknown keys are collected (strict mode) or dropped.
//
// In lenient mode (environment variables) values whose shape does not fit the
// target are dropped instead of failing the decode later.
type normalizer struct {
	tagName string
	lenient bool
	unknown []string
	errs    []error
}

func normalize(raw map[string]any, t reflect.Type, tagName string, lenient bool) (map[string]any, []string, error) {
	n := &normalizer{tagName: tagName, lenient: lenient}
	out, _ := n.value("", raw, t)
	m, _ := out.(map[string]any)
	if m == nil {
		m = map[string]any{}
	}
	slices.Sort(n.unknown)
	return m, n.unknown, errors.Join(n.errs...)
}

func (n *normalizer) value(path string, raw any, t reflect.Type) (any, bool) {
	if raw == nil {
		return nil, true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if isNamed(t) {
		return n.named(path, raw, t)
	}
	if isOpaque(t) {
		// Durations and text types never decode from a mapping; an Unmarshaler
		// may, so it gets whatever the source holds.
		if _, isMap := raw.(map[string]any); isMap && n.lenient && !reflect.PointerTo(t).Implements(unmarshalerType) {
			return nil, false
		}
		return raw, true
	}

	switch t.Kind() {
	case reflect.Struct:
		m, ok := raw.(map[string]any)
		if !ok {
			return raw, !n.lenient
		}
		return n.structMap(path, m, t), true
	case reflect.Map:
		m, ok := raw.(map[string]any)
		if !ok {
			return raw, !n.lenient
		}
		out := make(map[string]any, len(m))
		for k, v := range m {
			if nv, keep := n.value(joinPath(path, k), v, t.Elem()); keep {
				out[k] = nv
			}
		}
		return out, true
	case reflect.Slice, reflect.Array:
		l, ok := raw.([]any)
		if !ok {
			// Strings are split into lists by the decoder. A mapping would become
			// a one-item list, which from env only means a deeper variable such
			// as APP_TAGS__0 that would replace the whole list.
			_, isMap := raw.(map[string]any)
			return raw, !(isMap && n.lenient)
		}
		out := make([]any, 0, len(l))
		for i, v := range l {
			if nv, keep := n.value(fmt.Sprintf("%s[%d]", path, i), v, t.Elem()); keep {
				out = append(out, nv)
			}
		}
		return out, true
	case reflect.Interface:
		return raw, true
	default:
		if _, isMap := raw.(map[string]any); isMap && n.lenient {
			return nil, false
		}
		return raw, true
	}
}

func (n *normalizer) structMap(path string, m map[string]any, t reflect.Type) map[string]any {
	si := getStructInfo(t, n.tagName)
	out := make(map[string]any, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		f, ok := si.lookup(k)
		if !ok {
			if !n.lenient {
				n.unknown = append(n.unknown, joinPath(path, k))
			}
			continue
		}
		if _, dup := out[f.key]; dup {
			n.errs = append(n.errs, &FieldError{Path: joinPath(path, f.key), Err: fmt.Errorf("key given more than once with different case")})
			continue
		}
		if nv, keep := n.value(joinPath(path, f.key), m[k], f.typ); keep {
			out[f.key] = nv
		}
	}
	return out
}

func (n *normalizer) named(path string, raw any, t reflect.Type) (any, bool) {
	entries, err := namedEntries(raw)
	if err != nil {
		if n.lenient {
			return nil, false
		}
		n.errs = append(n.errs, &FieldError{Path: path, Err: err})
		return nil, false
	}
	elem := namedElemType(t)
	out := make(map[string]any, len(entries))
	for name, item := range entries {
		if nv, keep := n.value(joinPath(path, name), item, elem); keep {
			out[name] = nv
		}
	}
	return out, true
}

// merge deep-merges src into dst. Mappings merge key by key; any other value
// in src replaces the one in dst.
func merge(dst, src map[string]any) {
	for k, sv := range src {
		if sm, ok := sv.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				merge(dm, sm)
				continue
			}
			sv = cloneTree(sm)
		}
		dst[k] = sv
	}
}

func cloneTree(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			out[k] = cloneTree(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = cloneTree(v)
		}
		return out
	default:
		return v
	}
}

// setPath stores value at the nested key path, creating mappings as needed.
// A scalar in the way is replaced by a mapping.
func setPath(m map[string]any, path []string, value any) {
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[k] = next
		}
		m = next
	}
	last := path[len(path)-1]
	if _, isMap := m[last].(map[string]any); isMap {
		return // a deeper key already claimed this path
	}
	m[last] = value
}

// sanitize converts parser output into the canonical tree types:
// map[string]any and []any all the way down.
func sanitize(v any) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			s, err := sanitize(e)
			if err != nil {
				return nil, err
			}
			x[k] = s
		}
		return x, nil
	case map[any]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			ks, ok := scalarString(k)
			if !ok {
				return nil, fmt.Errorf("unsupported mapping key %v of type %T", k, k)
			}
			s, err := sanitize(e)
			if err != nil {
				return nil, err
			}
			out[ks] = s
		}
		return out, nil
	case []any:
		for i, e := range x {
			s, err := sanitize(e)
			if err != nil {
				return nil, err
			}
			x[i] = s
		}
		return x, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			ks, ok := scalarString(iter.Key().Interface())
			if !ok {
				return nil, fmt.Errorf("unsupported mapping key of type %s", iter.Key().Type())
			}
			s, err := sanitize(iter.Value().Interface())
			if err != nil {
				return nil, err
			}
			out[ks] = s
		}
		return out, nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return v, nil
		}
		out := make([]any, rv.Len())
		for i := range rv.Len() {
			s, err := sanitize(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			out[i] = s
		}
		return out, nil
	}
	return v, nil
}

// envKeyPath splits an env var name (without prefix) into a key path.
func envKeyPath(name, sep string) []string {
	segs := strings.Split(strings.ToLower(name), sep)
	if slices.Contains(segs, "") {
		return nil
	}
	return segs
}
