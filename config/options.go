package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// DecodeOptions decodes the options map of a connection config onto dst, a
// pointer to the driver's own config struct, e.g. *redis.UniversalOptions.
// This lets a config set any driver setting without the connection types
// mirroring each one.
//
// Keys are the snake_case names of the driver's fields (PoolSize is
// pool_size), matched case-insensitively; nested structs are nested maps.
// Values convert like any config value: durations as "5s", lists from
// comma-separated strings. Fields dst already holds are kept unless set.
//
// Unknown keys fail, and so do the keys in owned (dotted for nested fields,
// e.g. net.tls): fields the connector sets from typed config fields, which
// must not be given twice.
func DecodeOptions(options map[string]any, dst any, owned ...string) error {
	if len(options) == 0 {
		return nil
	}
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("config: options target must be a non-nil struct pointer, got %T", dst)
	}
	n := &normalizer{tagName: DefaultTagName}
	norm, _ := n.value("options", options, v.Elem().Type())
	errs := n.errs
	if len(n.unknown) > 0 {
		errs = append(errs, fmt.Errorf("unknown options: %s", strings.Join(n.unknown, ", ")))
	}
	for _, k := range owned {
		if hasPath(norm, strings.Split(k, ".")) {
			errs = append(errs, &FieldError{Path: "options." + k, Err: errors.New("set by its own config key, not in options")})
		}
	}
	// Decode even after a key error so that one call reports every mistake;
	// the caller discards dst on error.
	d := &decoder{tagName: DefaultTagName}
	d.decode("options", norm, v.Elem())
	return errors.Join(append(errs, d.errs...)...)
}

// hasPath reports whether the normalized tree has a value at path.
func hasPath(tree any, path []string) bool {
	for _, k := range path {
		m, ok := tree.(map[string]any)
		if !ok {
			return false
		}
		if tree, ok = m[k]; !ok {
			return false
		}
	}
	return true
}
