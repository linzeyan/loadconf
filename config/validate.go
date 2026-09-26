package config

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
)

// squashMarker names squashed fields in validator namespaces so they can be
// dropped from error paths.
const squashMarker = "~"

func newValidator(tagName string) *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	v.RegisterTagNameFunc(func(sf reflect.StructField) string {
		key, squash, skip := parseKeyTag(sf.Tag.Get(tagName))
		switch {
		case skip:
			return ""
		case (sf.Anonymous && key == "") || squash:
			return squashMarker
		case key == "":
			return snakeCase(sf.Name)
		}
		return key
	})
	// Secret is a struct that hides its value behind a pointer; rules such as
	// required or min=8 are meant for the string it holds.
	v.RegisterCustomTypeFunc(func(f reflect.Value) any { return f.Interface().(Secret).Value() }, Secret{})
	return v
}

// validateValue runs `validate` tags and Validator hooks on root, which must
// be an addressable struct.
func validateValue(val *validator.Validate, root reflect.Value, tagName string) error {
	var errs []error
	if val != nil {
		errs = append(errs, validationErrors("", val.Struct(root.Addr().Interface()))...)
	}
	walk("", root, tagName, false, func(path string, v reflect.Value, namedElem bool) {
		// The validator cannot see into Named (unexported storage), so each
		// instance is validated on its own.
		if val != nil && namedElem {
			sv := v
			if sv.Kind() == reflect.Pointer && !sv.IsNil() {
				sv = sv.Elem()
			}
			if sv.Kind() == reflect.Struct {
				errs = append(errs, validationErrors(path, val.Struct(sv.Addr().Interface()))...)
			}
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

func validationErrors(path string, err error) []error {
	if err == nil {
		return nil
	}
	var verrs validator.ValidationErrors
	if !errors.As(err, &verrs) {
		return []error{&FieldError{Path: path, Err: err}}
	}
	out := make([]error, 0, len(verrs))
	for _, fe := range verrs {
		// The namespace starts with the struct type name: "MySQL.port".
		_, rest, _ := strings.Cut(fe.Namespace(), ".")
		segs := slices.DeleteFunc(strings.Split(rest, "."), func(s string) bool { return s == squashMarker })
		rule := fe.Tag()
		if fe.Param() != "" {
			rule += "=" + fe.Param()
		}
		out = append(out, &FieldError{
			Path: joinPath(path, strings.Join(segs, ".")),
			Err:  fmt.Errorf("does not satisfy %q", rule),
		})
	}
	return out
}

// walk visits every value reachable from v through decodable fields, Named
// instances, slices and maps, children first. Visited values are addressable;
// map values and Named instances are copies.
func walk(path string, v reflect.Value, tagName string, namedElem bool, fn func(path string, v reflect.Value, namedElem bool)) {
	orig := v
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	t := v.Type()

	switch {
	case isNamed(t):
		v.Addr().Interface().(namedContainer).rangeNamed(func(name string, ev reflect.Value) {
			walk(joinPath(path, name), ev, tagName, true, fn)
		})
	case t.Kind() == reflect.Struct && !isOpaque(t):
		for _, f := range getStructInfo(t, tagName).fields {
			if fv, ok := fieldByIndex(v, f.index, false); ok {
				walk(joinPath(path, f.key), fv, tagName, false, fn)
			}
		}
	case (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && t.Elem().Kind() != reflect.Uint8:
		for i := range v.Len() {
			walk(fmt.Sprintf("%s[%d]", path, i), v.Index(i), tagName, false, fn)
		}
	case t.Kind() == reflect.Map:
		keys := v.MapKeys()
		slices.SortFunc(keys, func(a, b reflect.Value) int { return cmp.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
		for _, k := range keys {
			ev := reflect.New(t.Elem()).Elem()
			ev.Set(v.MapIndex(k))
			walk(joinPath(path, fmt.Sprint(k)), ev, tagName, false, fn)
		}
	}
	fn(path, orig, namedElem)
}
