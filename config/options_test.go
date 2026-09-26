package config_test

import (
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// driverLevel stands for driver enums that parse their own names.
type driverLevel int

func (l *driverLevel) UnmarshalText(b []byte) error {
	switch string(b) {
	case "low":
		*l = 1
	case "high":
		*l = 2
	default:
		return errors.New("unknown level")
	}
	return nil
}

// driverOptions looks like a driver's config struct: no config tags, nested
// structs, pointers and fields no config can express.
type driverOptions struct {
	Addrs       []string
	PoolSize    int
	DialTimeout time.Duration
	ReadOnly    bool
	TLSConfig   *tls.Config
	Dialer      func() error
	Retry       struct {
		Max     int
		Backoff time.Duration
	}
	Limit  *int
	Level  driverLevel
	Header http.Header
}

func TestDecodeOptions(t *testing.T) {
	// Preset fields stand for the driver defaults a connector starts from.
	o := driverOptions{PoolSize: 10, ReadOnly: true}
	err := config.DecodeOptions(map[string]any{
		"Pool_Size":    "20", // an env var override arrives as a string
		"dial_timeout": "2s",
		"retry":        map[string]any{"max": 3},
		"limit":        5,
		"level":        "high",
		"header":       map[string]any{"x-tenant": "t1", "X-Tags": "a,b"},
	}, &o, "addrs")
	if err != nil {
		t.Fatal(err)
	}
	if o.PoolSize != 20 || o.DialTimeout != 2*time.Second || o.Retry.Max != 3 || o.Limit == nil || *o.Limit != 5 || o.Level != 2 {
		t.Errorf("options = %+v", o)
	}
	// Environment variables arrive in lower case; clients read with Get.
	if o.Header.Get("X-Tenant") != "t1" || len(o.Header.Values("X-Tags")) != 2 {
		t.Errorf("header = %v", o.Header)
	}
	if !o.ReadOnly {
		t.Error("a preset field that options do not set was reset")
	}
	if err := config.DecodeOptions(nil, (*driverOptions)(nil)); err != nil {
		t.Errorf("no options must not touch the target: %v", err)
	}
}

func TestDecodeOptionsErrors(t *testing.T) {
	var o driverOptions
	err := config.DecodeOptions(map[string]any{
		"pool_sise":    1,
		"retry":        map[string]any{"maxx": 1, "max": 2},
		"addrs":        "a:1",
		"dialer":       "x",
		"dial_timeout": 5,
		"level":        "medium",
	}, &o, "addrs", "retry.backoff", "retry.max")
	if err == nil {
		t.Fatal("expected errors")
	}
	// Every mistake is reported at once, with the path under options.
	for _, want := range []string{
		"unknown options: options.pool_sise, options.retry.maxx",
		"options.addrs: set by its own config key",
		"options.retry.max: set by its own config key",
		"options.dialer: unsupported field type",
		`options.dial_timeout: invalid duration 5: add a unit, e.g. "5s"`,
		"options.level: unknown level",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
	if err := config.DecodeOptions(map[string]any{"a": 1}, o); err == nil || !strings.Contains(err.Error(), "non-nil struct pointer") {
		t.Errorf("non-pointer target: %v", err)
	}
}
