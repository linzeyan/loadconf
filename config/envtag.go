package config

import (
	"os"
	"reflect"
	"slices"
)

// envTagValues collects the values of `env`-tagged fields into a raw tree with
// canonical keys. Fields inside Named, maps and slices are skipped since
// their key path is not fixed. Empty variables count as unset.
func envTagValues(t reflect.Type, tagName string) map[string]any {
	out := map[string]any{}
	var visit func(t reflect.Type, keyPath []string, prefix string, stack []reflect.Type)
	visit = func(t reflect.Type, keyPath []string, prefix string, stack []reflect.Type) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct || isOpaque(t) || slices.Contains(stack, t) {
			return
		}
		stack = append(stack, t)
		for _, f := range getStructInfo(t, tagName).fields {
			kp := append(slices.Clone(keyPath), f.key)
			for _, name := range f.env {
				if v := os.Getenv(prefix + name); v != "" {
					setPath(out, kp, v)
					break
				}
			}
			visit(f.typ, kp, prefix+f.envPrefix, stack)
		}
	}
	visit(t, nil, "", nil)
	return out
}
