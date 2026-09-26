// Package conn_mssql opens database/sql pools for Microsoft SQL Server and
// Azure SQL (go-mssqldb) from [config.SQLServer].
//
//	db, err := conn_mssql.Open(ctx, cfg.SQLServer.MustGet("erp"))
//	defer db.Close()
//
// Driver diagnostics go to a process-wide logger; route them to slog with
//
//	mssql.SetContextLogger(conn_mssql.SlogLogger(logger))
//
// and choose the categories with the "log" param, e.g. Params{"log": "1"}
// for errors only (see [msdsn.Log]).
package conn_mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/linzeyan/loadconf/config"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	config    []func(*msdsn.Config)
	connector []func(*mssql.Connector)
}

// WithConfig adjusts the driver config after it is built from the config,
// e.g. to set Protocols or Workstation.
func WithConfig(fn func(*msdsn.Config)) Option {
	return func(o *options) { o.config = append(o.config, fn) }
}

// WithConnector adjusts the connector before the pool is opened, e.g. to set
// SessionInitSQL or a Dialer.
func WithConnector(fn func(*mssql.Connector)) Option {
	return func(o *options) { o.connector = append(o.connector, fn) }
}

func buildOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Config converts cfg into a go-mssqldb config.
//
// The DSN (URL, ADO or ODBC form), Params and the explicit fields are merged,
// in that order of precedence, into one connection string that is parsed by
// the driver, so every driver parameter keeps its meaning. Port only applies
// together with Host. When Instance is set, every port is dropped, also one
// written as "host,port", so that the SQL Server Browser resolves the
// instance port.
//
// The "dial timeout" param bounds dialing; the driver's "connection timeout"
// is a deadline on every read and write of the connection instead.
//
// When TLS is enabled, cfg.TLS replaces the TLS settings derived from the
// connection string (certificate, trustservercertificate, ...) and encrypt
// defaults to true.
func Config(cfg config.SQLServer) (msdsn.Config, error) {
	if err := cfg.Validate(); err != nil {
		return msdsn.Config{}, err
	}

	var base map[string]string
	if dsn := cfg.DSN.Value(); dsn != "" {
		p, err := msdsn.Parse(dsn)
		if err != nil {
			return msdsn.Config{}, fmt.Errorf("sqlserver dsn: %w", err)
		}
		base = p.Parameters
	}
	params := make(map[string]string, len(cfg.Params))
	for k, v := range cfg.Params {
		params[strings.ToLower(strings.TrimSpace(k))] = v
	}

	explicit := map[string]string{}
	if cfg.Host != "" || cfg.Instance != "" {
		// The server the merged string would name: the last of its ADO
		// synonyms (msdsn's adoSynonyms) in the order writeADO emits them.
		var server string
		for _, m := range []map[string]string{base, params} {
			for _, k := range []string{"addr", "address", "data source", "network address", msdsn.Server} {
				if v, ok := m[k]; ok {
					server = v
				}
			}
		}
		host, instance, _ := strings.Cut(server, `\`)
		if cfg.Host != "" {
			host, instance = cfg.Host, ""
		}
		if cfg.Instance != "" {
			// The port of a "host,port" server is dropped with the others
			// below; kept, msdsn would read "h,1500\INST" as port "1500\INST".
			host, _, _ = strings.Cut(host, ",")
			instance = cfg.Instance
		}
		explicit[msdsn.Server] = host
		if instance != "" {
			explicit[msdsn.Server] = host + `\` + instance
		}
	}
	if cfg.Host != "" && cfg.Instance == "" {
		explicit[msdsn.Port] = strconv.Itoa(portOr(cfg.Port, 1433))
	}
	for k, v := range map[string]string{
		msdsn.UserID:   cfg.User,
		msdsn.Password: cfg.Password.Value(),
		msdsn.Database: cfg.Database,
	} {
		if v != "" {
			explicit[k] = v
		}
	}

	encrypt := strings.ToLower(lastOf(msdsn.Encrypt, base, params))
	if encrypt == "" && cfg.TLS.Enabled {
		explicit[msdsn.Encrypt] = "true"
	}
	if cfg.TLS.Enabled && encrypt == "disable" {
		return msdsn.Config{}, errors.New("sqlserver: encrypt=disable contradicts tls.enabled")
	}

	// msdsn picks the grammar from the prefix; a leading ';' keeps a first
	// key such as "odbc:server" (from a malformed DSN) from switching the
	// merged ADO string to ODBC.
	var b strings.Builder
	b.WriteByte(';')
	for _, m := range []map[string]string{base, params, explicit} {
		if err := writeADO(&b, m); err != nil {
			return msdsn.Config{}, err
		}
	}
	mc, err := msdsn.Parse(b.String())
	if err != nil {
		return msdsn.Config{}, fmt.Errorf("sqlserver config: %w", err)
	}
	if cfg.Instance != "" {
		// With any port the driver skips the SQL Server Browser and ignores
		// the instance. Deleting port keys before the merge is not enough:
		// every "host,port" server value of the DSN or Params sets the port
		// while msdsn parses it, even when a later server key replaces it.
		mc.Port = 0
	}

	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return msdsn.Config{}, err
		}
		switch {
		case tlsCfg.ServerName != "":
			// Keep the name when the server routes the login elsewhere.
			mc.HostInCertificateProvided = true
		case mc.TLSConfig != nil && mc.TLSConfig.ServerName != "":
			tlsCfg.ServerName = mc.TLSConfig.ServerName // hostnameincertificate or host
		default:
			tlsCfg.ServerName = mc.Host
		}
		// SQL Server expects one TDS packet per TLS record, see
		// https://github.com/microsoft/go-mssqldb/issues/166.
		tlsCfg.DynamicRecordSizingDisabled = true
		mc.TLSConfig = tlsCfg
		mc.TrustServerCertificate = tlsCfg.InsecureSkipVerify
	}
	return mc, nil
}

// writeADO appends m in ADO form (key="value";) with sorted keys. The driver
// keeps the last value of a key, so later calls override earlier ones.
func writeADO(b *strings.Builder, m map[string]string) error {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if k == "" || strings.ContainsAny(k, `=;"`) {
			return fmt.Errorf("sqlserver: invalid parameter name %q", k)
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(strings.ReplaceAll(m[k], `"`, `""`))
		b.WriteString(`";`)
	}
	return nil
}

// lastOf returns the value of key in the last map that has it.
func lastOf(key string, ms ...map[string]string) string {
	var v string
	for _, m := range ms {
		if s, ok := m[key]; ok {
			v = s
		}
	}
	return v
}

// Connector builds a go-mssqldb connector that uses the "sqlserver" driver
// semantics (@name / @p1 placeholders).
func Connector(cfg config.SQLServer, opts ...Option) (*mssql.Connector, error) {
	o := buildOptions(opts)
	mc, err := Config(cfg)
	if err != nil {
		return nil, err
	}
	return newConnector(mc, o), nil
}

func newConnector(mc msdsn.Config, o options) *mssql.Connector {
	for _, fn := range o.config {
		fn(&mc)
	}
	c := mssql.NewConnectorConfig(mc)
	for _, fn := range o.connector {
		fn(c)
	}
	return c
}

// New builds a pool with the pool settings applied, without connecting:
// database/sql dials on first use.
func New(cfg config.SQLServer, opts ...Option) (*sql.DB, error) {
	db, _, err := newDB(cfg, buildOptions(opts))
	return db, err
}

// Open is [New] plus a ping within cfg.PingTimeout. The pool is closed if
// the ping fails.
func Open(ctx context.Context, cfg config.SQLServer, opts ...Option) (*sql.DB, error) {
	db, mc, err := newDB(cfg, buildOptions(opts))
	if err != nil {
		return nil, err
	}
	return ping(ctx, db, cfg.PingTimeout, "sqlserver "+target(mc))
}

func newDB(cfg config.SQLServer, o options) (*sql.DB, msdsn.Config, error) {
	mc, err := Config(cfg)
	if err != nil {
		return nil, mc, err
	}
	db := sql.OpenDB(newConnector(mc, o))
	cfg.SQLPool.Apply(db)
	return db, mc, nil
}

func target(mc msdsn.Config) string {
	addr := mc.Host
	switch {
	case mc.Port != 0:
		addr = net.JoinHostPort(mc.Host, strconv.FormatUint(mc.Port, 10))
	case mc.Instance != "":
		addr += `\` + mc.Instance
	}
	return addr + "/" + mc.Database
}

func ping(ctx context.Context, db *sql.DB, pingTimeout time.Duration, target string) (*sql.DB, error) {
	if pingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pingTimeout)
		defer cancel()
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping %s: %w", target, err)
	}
	return db, nil
}

func portOr(port, def int) int {
	if port == 0 {
		return def
	}
	return port
}
