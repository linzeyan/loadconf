package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The types in this file are what example.yaml loads into. The generator in
// example_gen_test.go prints their source into the file's comments, adding
// "config." to the identifiers of this package, so that they read as code of
// an application importing it.

// AppConfig holds the built-in settings under the keys schema.json
// describes, plus three sections of the application's own.
type AppConfig struct {
	Log           Log              `config:"log"`
	MySQL         Named[MySQL]     `config:"mysql"`
	Postgres      Named[Postgres]  `config:"postgres"`
	SQLServer     Named[SQLServer] `config:"sqlserver"`
	Oracle        Named[Oracle]    `config:"oracle"`
	SQLite        Named[SQLite]    `config:"sqlite"`
	Redis         Redis            `config:"redis"`
	Mongo         Mongo            `config:"mongo"`
	Cassandra     Cassandra        `config:"cassandra"`
	Scylla        Cassandra        `config:"scylla"`
	ClickHouse    ClickHouse       `config:"clickhouse"`
	Doris         StreamLoad       `config:"doris"`
	Trino         Trino            `config:"trino"`
	Elasticsearch Elasticsearch    `config:"elasticsearch"`
	OpenSearch    OpenSearch       `config:"opensearch"`
	Kafka         Kafka            `config:"kafka"`
	MQTT          MQTT             `config:"mqtt"`
	EMQX          MQTTREST         `config:"emqx"`

	Server     Server               `config:"server"`
	Tenants    Named[Tenant]        `config:"tenants"`
	RateLimits map[string]RateLimit `config:"rate_limits"`
}

// Server holds plain fields of the application, checked by validate tags.
// Rules on durations and byte sizes take values in their own units.
type Server struct {
	Listen       string        `config:"listen"        default:":8080"   validate:"hostname_port"`
	PublicURL    string        `config:"public_url"    validate:"omitempty,url"`
	Mode         string        `config:"mode"          default:"release" validate:"oneof=debug release"`
	ReadTimeout  time.Duration `config:"read_timeout"  default:"5s"      validate:"gte=1s,lte=1m"`
	MaxBodySize  ByteSize      `config:"max_body_size" default:"4MiB"    validate:"lte=32MiB"`
	TrustedCIDRs []string      `config:"trusted_cidrs" validate:"dive,cidr"`
}

// Tenant is an application type held in a Named collection. Name is filled
// with the instance name.
type Tenant struct {
	Name     string   `config:"name"`
	Database string   `config:"database" validate:"required"`
	Quota    int      `config:"quota"    default:"100" validate:"min=1"`
	Features []string `config:"features"`
	APIKey   Secret   `config:"api_key"`
}

// Validate checks a reference between sections, which no single section's
// rules can see.
func (c AppConfig) Validate() error {
	for name, t := range c.Tenants.All() {
		if !c.MySQL.Has(t.Database) {
			return fmt.Errorf("tenants.%s.database: no mysql instance %q", name, t.Database)
		}
	}
	return nil
}

// RateLimit decodes itself, so a file may write "100/s", "20/30s" or
// {requests: 100, per: 1s}.
type RateLimit struct {
	Requests int
	Per      time.Duration
}

func (r *RateLimit) UnmarshalConfig(raw any) error {
	var n, per string
	switch v := raw.(type) {
	case string:
		var ok bool
		if n, per, ok = strings.Cut(v, "/"); !ok {
			return fmt.Errorf("rate %q: want requests/period, e.g. 100/s", v)
		}
		if per != "" && (per[0] < '0' || per[0] > '9') {
			per = "1" + per
		}
	case map[string]any:
		n, per = fmt.Sprint(v["requests"]), fmt.Sprint(v["per"])
	default:
		return fmt.Errorf("rate: want a string or a mapping, got %T", raw)
	}
	requests, err := strconv.Atoi(strings.TrimSpace(n))
	if err != nil || requests < 1 {
		return fmt.Errorf("rate: requests must be a positive integer, got %q", n)
	}
	d, err := time.ParseDuration(strings.TrimSpace(per))
	if err != nil || d <= 0 {
		return fmt.Errorf("rate: invalid period %q", per)
	}
	*r = RateLimit{Requests: requests, Per: d}
	return nil
}
