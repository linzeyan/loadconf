package config

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ClickHouse configures a ClickHouse connection (clickhouse-go v2) over the
// native protocol or HTTP.
//
// Either DSN (clickhouse://, http:// or https:// URL) or Addrs is required.
// Fields set next to a DSN override the corresponding DSN parts.
//
// Options sets any other clickhouse.Options field by its snake_case name,
// e.g. settings (server settings sent with every query), dial_timeout,
// read_timeout, block_buffer_size, http_headers, http_url_path or
// conn_open_strategy (a number: 0 in order, 1 round robin, 2 random); see
// [DecodeOptions]. They apply on top of the DSN.
type ClickHouse struct {
	DSN Secret `config:"dsn"`
	// Addrs are host:port pairs: 9000/9440 (TLS) for native, 8123/8443 for
	// HTTP.
	Addrs []string `config:"addrs"`
	// Protocol is native or http.
	Protocol string `config:"protocol" default:"native"`
	Database string `config:"database"`
	Username string `config:"username"`
	Password Secret `config:"password"`
	// Compression is lz4, lz4hc, zstd, gzip, deflate, br or none. gzip, deflate
	// and br are HTTP only. options.compression.level sets the level.
	Compression string        `config:"compression"`
	PingTimeout time.Duration `config:"ping_timeout" default:"3s"`

	TLS TLS `config:"tls"`
	// SQLPool sizes both the native connection pool and *sql.DB pools.
	SQLPool `config:",squash"`
	Options map[string]any `config:"options"`
}

func (c ClickHouse) Validate() error {
	var errs []error
	if c.DSN.Value() == "" && len(c.Addrs) == 0 {
		errs = append(errs, errors.New("either dsn or addrs is required"))
	}
	if !slices.Contains([]string{"", "native", "http"}, strings.ToLower(c.Protocol)) {
		errs = append(errs, fmt.Errorf("unsupported protocol %q (want native or http)", c.Protocol))
	}
	if !slices.Contains([]string{"", "none", "lz4", "lz4hc", "zstd", "gzip", "deflate", "br"}, strings.ToLower(c.Compression)) {
		errs = append(errs, fmt.Errorf("unsupported compression %q", c.Compression))
	}
	return errors.Join(append(errs, c.SQLPool.Validate())...)
}

// Stream Load flavors.
const (
	StreamLoadDoris     = "doris"
	StreamLoadStarRocks = "starrocks"
)

// StreamLoad configures HTTP Stream Load into Apache Doris or StarRocks.
// Queries go over the MySQL protocol with [MySQL] (FE query port 9030).
type StreamLoad struct {
	// Flavor is doris or starrocks.
	Flavor string `config:"flavor" default:"doris"`
	// Addrs are FE HTTP endpoints, host:port or http(s)://host:port
	// (default port 8030). Loads rotate over them.
	Addrs    []string `config:"addrs"`
	Username string   `config:"username" default:"root"`
	Password Secret   `config:"password"`
	Database string   `config:"database"`
	// Table is the default target table.
	Table string `config:"table"`
	// Format is json or csv.
	Format string `config:"format" default:"json"`
	// Headers are load properties sent with every load, e.g.
	// column_separator, strict_mode, timezone or max_filter_ratio.
	Headers map[string]string `config:"headers"`
	// LabelPrefix starts generated load labels.
	LabelPrefix string `config:"label_prefix"`

	// Timeout bounds one HTTP load request.
	Timeout time.Duration `config:"timeout" default:"5m"`
	// MaxRetries retries a failed load with the same label, which the
	// server deduplicates.
	MaxRetries   int           `config:"max_retries"   default:"2"`
	RetryBackoff time.Duration `config:"retry_backoff" default:"1s"`

	TLS   TLS             `config:"tls"`
	Batch StreamLoadBatch `config:"batch"`
}

// StreamLoadBatch configures the batching writer. A batch is loaded when any
// limit is reached.
type StreamLoadBatch struct {
	MaxRows       int           `config:"max_rows"       default:"100000"`
	MaxBytes      ByteSize      `config:"max_bytes"      default:"64MiB"`
	FlushInterval time.Duration `config:"flush_interval" default:"5s"`
}

func (s StreamLoad) Validate() error {
	var errs []error
	if s.Flavor != StreamLoadDoris && s.Flavor != StreamLoadStarRocks {
		errs = append(errs, fmt.Errorf("unsupported flavor %q (want doris or starrocks)", s.Flavor))
	}
	if len(s.Addrs) == 0 {
		errs = append(errs, errors.New("addrs is required"))
	}
	// Addrs may carry credentials, so they are printed without userinfo; one
	// that does not parse is not printed at all, as its userinfo cannot be
	// located.
	for i, a := range s.Addrs {
		// The same rules as the connector's feURL, so that a mistake fails
		// the load rather than the first load request: an empty host would
		// dial localhost, url.Parse lets stray brackets through, and port 0
		// or one above 65535 cannot be dialed.
		if !strings.Contains(a, "://") {
			a = "http://" + a
		}
		u, err := url.Parse(a)
		if err != nil {
			errs = append(errs, fmt.Errorf("addrs[%d] must be host:port or an http(s) URL", i))
			continue
		}
		u.User = nil
		port := cmp.Or(u.Port(), "8030")
		_, _, splitErr := net.SplitHostPort(net.JoinHostPort(u.Hostname(), port))
		n, portErr := strconv.Atoi(port)
		if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || splitErr != nil || portErr != nil || n < 1 || n > 65535 {
			errs = append(errs, fmt.Errorf("addr %q must be host:port or an http(s) URL, with a port of 1-65535", u.String()))
		}
	}
	if s.Database == "" {
		errs = append(errs, errors.New("database is required"))
	}
	if s.Format != "json" && s.Format != "csv" {
		errs = append(errs, fmt.Errorf("unsupported format %q (want json or csv)", s.Format))
	}
	if s.MaxRetries < 0 || s.Batch.MaxRows < 0 || s.Batch.MaxBytes < 0 || s.Batch.FlushInterval < 0 {
		errs = append(errs, errors.New("retry and batch settings must not be negative"))
	}
	return errors.Join(errs...)
}

// Trino configures a Trino connection (trino-go-client).
//
// Either DSN (http(s)://user@host:port?catalog=...) or Host is required.
// Fields set next to a DSN override the corresponding DSN parts.
type Trino struct {
	DSN  Secret `config:"dsn"`
	Host string `config:"host"`
	Port int    `config:"port" default:"8080"`
	User string `config:"user"`
	// Password and AccessToken (JWT) require TLS.
	Password    Secret `config:"password"`
	AccessToken Secret `config:"access_token"`
	Catalog     string `config:"catalog"`
	Schema      string `config:"schema"`
	// Params are DSN parameters of trino-go-client, e.g. source, clientTags
	// (comma-separated), session_properties (key:value;key:value) or
	// query_timeout.
	Params map[string]string `config:"params"`

	PingTimeout time.Duration `config:"ping_timeout" default:"5s"`

	TLS     TLS `config:"tls"`
	SQLPool `config:",squash"`
}

func (t Trino) Validate() error {
	var errs []error
	if t.DSN.Value() == "" && t.Host == "" {
		errs = append(errs, errors.New("either dsn or host is required"))
	}
	if t.DSN.Value() == "" && t.User == "" {
		errs = append(errs, errors.New("user is required"))
	}
	if (t.Password.Value() != "" || t.AccessToken.Value() != "") && t.DSN.Value() == "" && !t.TLS.Enabled {
		errs = append(errs, errors.New("password and access_token require tls.enabled"))
	}
	return errors.Join(append(errs, t.SQLPool.Validate())...)
}
