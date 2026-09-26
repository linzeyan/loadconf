package config

// Unmarshaler is implemented by types that decode themselves from the raw
// source tree. v is one of map[string]any, []any, string, bool, a number or
// nil.
type Unmarshaler interface {
	UnmarshalConfig(v any) error
}

// Defaulter is implemented by types that set their own defaults. SetDefaults is
// called on a zero value before decoding, after `default` tags are applied.
type Defaulter interface {
	SetDefaults()
}

// Validator is implemented by types that validate themselves after decoding.
type Validator interface {
	Validate() error
}

// FieldError reports an error at a key path such as "mysql.orders.port".
type FieldError struct {
	Path string
	Err  error
}

func (e *FieldError) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return e.Path + ": " + e.Err.Error()
}

func (e *FieldError) Unwrap() error { return e.Err }
