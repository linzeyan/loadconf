package config

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Cassandra configures an Apache Cassandra or ScyllaDB cluster connection
// (gocql).
//
// Options sets any other gocql setting by the snake_case name of its
// gocql.ClusterConfig field, e.g. consistency (in upper case, LOCAL_QUORUM),
// timeout, connect_timeout, num_conns, page_size, proto_version or
// disable_initial_host_lookup; see [DecodeOptions]. Unset settings keep the
// driver defaults.
type Cassandra struct {
	// Hosts are contact points, host or host:port.
	Hosts    []string `config:"hosts"`
	Port     int      `config:"port" default:"9042"`
	Keyspace string   `config:"keyspace"`
	Username string   `config:"username"`
	Password Secret   `config:"password"`

	// LocalDC enables DC-aware, token-aware routing that prefers this data
	// center. Recommended for multi-DC clusters.
	LocalDC string `config:"local_dc"`
	// Compression is snappy, lz4 or empty for none.
	Compression string `config:"compression"`
	// Retries sets a simple retry policy with this many retries.
	Retries int `config:"retries"`

	TLS     TLS            `config:"tls"`
	Options map[string]any `config:"options"`
}

func (c Cassandra) Validate() error {
	var errs []error
	if len(c.Hosts) == 0 {
		errs = append(errs, errors.New("hosts is required"))
	}
	if !slices.Contains([]string{"", "snappy", "lz4"}, strings.ToLower(c.Compression)) {
		errs = append(errs, fmt.Errorf("unsupported compression %q (want snappy or lz4)", c.Compression))
	}
	return errors.Join(errs...)
}
