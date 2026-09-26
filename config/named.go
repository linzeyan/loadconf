package config

import (
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"reflect"
	"slices"
	"strings"
)

// Named holds several named instances of T, e.g. one MySQL config per
// database. Names are case-insensitive and stored normalized by
// [NormalizeName].
//
// In a source it may be written as a mapping keyed by name:
//
//	mysql:
//	  orders: {host: db1, database: orders}
//	  core:     {host: db2, database: core}
//
// or as a list whose items carry a "name" key:
//
//	mysql:
//	  - name: orders
//	    host: db1
//
// When T has a string field keyed "name", it is filled with the instance name.
// The zero value is an empty collection ready to use.
type Named[T any] struct {
	items map[string]T
}

// NormalizeName is how [Named] normalizes instance names.
func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// Get returns the instance with the given name.
func (n Named[T]) Get(name string) (T, bool) {
	v, ok := n.items[NormalizeName(name)]
	return v, ok
}

// MustGet returns the instance with the given name or panics. It is meant for
// start-up code where a missing instance is a programming error.
func (n Named[T]) MustGet(name string) T {
	v, ok := n.Get(name)
	if !ok {
		panic(fmt.Sprintf("config: instance %q not found (have %s)", NormalizeName(name), strings.Join(n.Names(), ", ")))
	}
	return v
}

// Has reports whether an instance with the given name exists.
func (n Named[T]) Has(name string) bool {
	_, ok := n.items[NormalizeName(name)]
	return ok
}

// Len returns the number of instances.
func (n Named[T]) Len() int { return len(n.items) }

// Names returns the instance names in sorted order.
func (n Named[T]) Names() []string {
	return slices.Sorted(maps.Keys(n.items))
}

// All iterates over the instances in name order.
func (n Named[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for _, name := range n.Names() {
			if !yield(name, n.items[name]) {
				return
			}
		}
	}
}

// Set adds or replaces an instance.
func (n *Named[T]) Set(name string, v T) {
	if n.items == nil {
		n.items = make(map[string]T)
	}
	n.items[NormalizeName(name)] = v
}

// Delete removes an instance.
func (n *Named[T]) Delete(name string) {
	delete(n.items, NormalizeName(name))
}

// Diff compares n (the old state) with next and returns the names that were
// added, removed or changed. It helps deciding which connections to rebuild
// after a hot reload.
func (n Named[T]) Diff(next Named[T]) (added, removed, changed []string) {
	for _, name := range next.Names() {
		old, ok := n.items[name]
		switch {
		case !ok:
			added = append(added, name)
		case !reflect.DeepEqual(old, next.items[name]):
			changed = append(changed, name)
		}
	}
	for _, name := range n.Names() {
		if _, ok := next.items[name]; !ok {
			removed = append(removed, name)
		}
	}
	return added, removed, changed
}

// Format prints the instances as a map. fmt does not call the String or
// Format methods of values reached through unexported fields, so printing the
// struct itself would show every Secret inside T as a bare pointer instead of
// redacted, also in slog's text output, which uses %+v.
func (n Named[T]) Format(f fmt.State, verb rune) {
	fmt.Fprintf(f, fmt.FormatString(f, verb), n.items)
}

func (n Named[T]) MarshalJSON() ([]byte, error) {
	if n.items == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(n.items)
}

// MarshalYAML implements the marshaler interface of github.com/goccy/go-yaml.
func (n Named[T]) MarshalYAML() (any, error) {
	if n.items == nil {
		return map[string]T{}, nil
	}
	return n.items, nil
}

// namedContainer lets the decoder and walkers handle Named[T] without knowing T.
type namedContainer interface {
	elemType() reflect.Type
	decodeNamed(d *decoder, path string, raw any)
	rangeNamed(fn func(name string, v reflect.Value))
}

func (n *Named[T]) elemType() reflect.Type { return reflect.TypeFor[T]() }

func (n *Named[T]) decodeNamed(d *decoder, path string, raw any) {
	entries, err := namedEntries(raw)
	if err != nil {
		d.fail(path, err)
		return
	}
	if n.items == nil {
		n.items = make(map[string]T, len(entries))
	}

	nameIndex := nameFieldIndex(reflect.TypeFor[T](), d.tagName)
	for _, name := range slices.Sorted(maps.Keys(entries)) {
		p := joinPath(path, name)
		elem, exists := n.items[name]
		v := reflect.ValueOf(&elem).Elem()
		if !exists {
			d.applyDefaults(p, v)
		}
		d.decode(p, entries[name], v)
		if nameIndex != nil {
			if f, ok := fieldByIndex(v, nameIndex, false); ok && f.String() == "" {
				f.SetString(name)
			}
		}
		n.items[name] = elem
	}
}

func (n *Named[T]) rangeNamed(fn func(name string, v reflect.Value)) {
	for _, name := range n.Names() {
		elem := n.items[name]
		fn(name, reflect.ValueOf(&elem).Elem())
	}
}

// nameFieldIndex returns the index of T's string field keyed "name", if any.
func nameFieldIndex(t reflect.Type, tagName string) []int {
	if t.Kind() != reflect.Struct || isOpaque(t) {
		return nil
	}
	f, ok := getStructInfo(t, tagName).lookup("name")
	if !ok || f.typ.Kind() != reflect.String {
		return nil
	}
	return f.index
}

// namedEntries converts the mapping or list form of a Named value into a
// mapping keyed by normalized name. The "name" key is stripped from items.
func namedEntries(raw any) (map[string]any, error) {
	switch x := raw.(type) {
	case nil:
		return map[string]any{}, nil
	case map[string]any:
		out := make(map[string]any, len(x))
		for _, k := range slices.Sorted(maps.Keys(x)) {
			name := NormalizeName(k)
			if name == "" {
				return nil, fmt.Errorf("instance name must not be empty")
			}
			if _, dup := out[name]; dup {
				return nil, fmt.Errorf("duplicate instance name %q", name)
			}
			out[name] = stripNameKey(x[k])
		}
		return out, nil
	case []any:
		out := make(map[string]any, len(x))
		for i, item := range x {
			m, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("item %d: expected a mapping with a name, got %T", i, item)
			}
			var (
				rawName  any
				nameKeys int
			)
			for k, v := range m {
				if strings.EqualFold(k, "name") {
					rawName, nameKeys = v, nameKeys+1
				}
			}
			// Picking one of several spellings would depend on map order, so the
			// instance name would change from one load to the next.
			if nameKeys > 1 {
				return nil, fmt.Errorf("item %d: name given more than once with different case", i)
			}
			var name string
			if nameKeys == 1 {
				s, ok := rawName.(string)
				if !ok {
					return nil, fmt.Errorf("item %d: name must be a string, got %T", i, rawName)
				}
				name = NormalizeName(s)
			}
			if name == "" {
				return nil, fmt.Errorf("item %d: name is required", i)
			}
			if _, dup := out[name]; dup {
				return nil, fmt.Errorf("duplicate instance name %q", name)
			}
			out[name] = stripNameKey(m)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected a mapping or a list, got %T", raw)
	}
}

func stripNameKey(item any) any {
	m, ok := item.(map[string]any)
	if !ok {
		return item
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if !strings.EqualFold(k, "name") {
			out[k] = v
		}
	}
	return out
}
