package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ByteSize is a size in bytes. It decodes from a number of bytes or a string
// with a unit: B, KB, MB, GB, TB (powers of 1000) or KiB, MiB, GiB, TiB
// (powers of 1024), e.g. "64MiB" or "1.5GB".
type ByteSize int64

// Byte size units.
const (
	KiB ByteSize = 1 << (10 * (iota + 1))
	MiB
	GiB
	TiB
)

var byteUnits = map[string]int64{
	"":    1,
	"b":   1,
	"k":   1 << 10,
	"kb":  1e3,
	"kib": 1 << 10,
	"m":   1 << 20,
	"mb":  1e6,
	"mib": 1 << 20,
	"g":   1 << 30,
	"gb":  1e9,
	"gib": 1 << 30,
	"t":   1 << 40,
	"tb":  1e12,
	"tib": 1 << 40,
}

// ParseByteSize parses a size such as "512", "64MiB" or "1.5GB".
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	i := strings.IndexFunc(s, func(r rune) bool { return (r < '0' || r > '9') && r != '.' })
	if i < 0 {
		i = len(s)
	}
	num, unit := s[:i], strings.ToLower(strings.TrimSpace(s[i:]))
	mult, ok := byteUnits[unit]
	if num == "" || !ok {
		return 0, fmt.Errorf("invalid byte size %q: want a number with an optional unit such as KiB, MB or GiB", s)
	}
	if !strings.Contains(num, ".") {
		// Whole numbers are parsed exactly: float64 drops the low bits of counts
		// above 2^53, so a size written by String would not read back. num holds
		// only digits here, so the only possible error is a value out of range.
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil || n > math.MaxInt64/mult {
			return 0, fmt.Errorf("byte size %q is too large", s)
		}
		return ByteSize(n * mult), nil
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	f *= float64(mult)
	// float64(math.MaxInt64) rounds up to 2^63, which does not fit an int64.
	if f >= math.MaxInt64 {
		return 0, fmt.Errorf("byte size %q is too large", s)
	}
	return ByteSize(f), nil
}

// UnmarshalConfig accepts a number of bytes or a string with a unit.
func (b *ByteSize) UnmarshalConfig(raw any) error {
	if s, ok := raw.(string); ok {
		v, err := ParseByteSize(s)
		if err != nil {
			return err
		}
		*b = v
		return nil
	}
	n, err := toInt64(raw)
	if err != nil {
		return fmt.Errorf("invalid byte size %v: %w", raw, err)
	}
	if n < 0 {
		return fmt.Errorf("byte size must not be negative (got %d)", n)
	}
	*b = ByteSize(n)
	return nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := ParseByteSize(string(text))
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

// String formats the size with the largest binary unit that divides it.
func (b ByteSize) String() string {
	for _, u := range []struct {
		size ByteSize
		name string
	}{{TiB, "TiB"}, {GiB, "GiB"}, {MiB, "MiB"}, {KiB, "KiB"}} {
		if b != 0 && b%u.size == 0 {
			return strconv.FormatInt(int64(b/u.size), 10) + u.name
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

// Int returns the size as an int, e.g. for APIs sized in bytes.
func (b ByteSize) Int() int { return int(b) }
