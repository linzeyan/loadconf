package config

import (
	"fmt"
	"io"
	"log/slog"
)

const redacted = "******"

// Secret is a string that is redacted when printed, logged or marshaled.
// The value sits behind a pointer: under %p fmt skips every method and prints
// a struct's fields with %v, which would show a string field.
// Use [Secret.Value] to get the actual value. The zero value is the empty
// secret.
type Secret struct{ v *string }

// NewSecret returns a Secret holding s.
func NewSecret(s string) Secret {
	// The empty secret is always the nil pointer, so that reflect.DeepEqual,
	// which reload and Named.Diff use to detect changes, compares values
	// rather than how an empty value was produced.
	if s == "" {
		return Secret{}
	}
	return Secret{&s}
}

// Value returns the unredacted secret; "" for the zero Secret.
func (s Secret) Value() string {
	if s.v == nil {
		return ""
	}
	return *s.v
}

func (s Secret) String() string {
	if s.v == nil {
		return ""
	}
	return redacted
}

func (s Secret) GoString() string { return `config.NewSecret("` + s.String() + `")` }

// Format redacts under every verb: for verbs that are invalid for strings,
// such as %d, fmt would otherwise print the raw value without calling String.
func (s Secret) Format(f fmt.State, verb rune) {
	if verb == 'v' && f.Flag('#') {
		io.WriteString(f, s.GoString())
		return
	}
	fmt.Fprintf(f, fmt.FormatString(f, verb), s.String())
}

func (s Secret) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText lets every source decode a Secret from a plain string.
func (s *Secret) UnmarshalText(b []byte) error {
	*s = NewSecret(string(b))
	return nil
}

func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }
