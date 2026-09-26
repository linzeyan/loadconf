// Package conn_oracle opens database/sql pools for Oracle Database with the
// pure Go driver go-ora from [config.Oracle].
//
//	db, err := conn_oracle.Open(ctx, cfg.Oracle.MustGet("erp"))
//	defer db.Close()
//
// go-ora has no pluggable logger; set Params{"TRACE FILE": path} to trace
// the protocol.
package conn_oracle

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"

	"github.com/linzeyan/loadconf/config"
)

// Option customizes opening.
type Option func(*options)

type options struct {
	connector []func(*go_ora.OracleConnector)
}

// WithConnector adjusts the go-ora connector of every new connection, e.g.
// to set a Dialer or Kerberos authentication.
func WithConnector(fn func(*go_ora.OracleConnector)) Option {
	return func(o *options) { o.connector = append(o.connector, fn) }
}

// URL converts cfg into a go-ora connection URL.
//
// A DSN is taken as the base; Params, then the explicit fields override its
// parts. ConnectString is passed as go-ora's connStr option and replaces
// Host, Port and Service. TLS.Enabled turns on TCPS (SSL) with certificate verification unless
// InsecureSkipVerify is set.
func URL(cfg config.Oracle) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	u := &url.URL{Scheme: "oracle", Host: ":0"}
	if dsn := cfg.DSN.Value(); dsn != "" {
		var err error
		if u, err = url.Parse(dsn); err != nil {
			return "", fmt.Errorf("oracle dsn: %w", redactErr(err, dsn))
		}
		if u.Scheme != "oracle" {
			return "", fmt.Errorf("oracle dsn must start with oracle:// (got %q)", u.Scheme)
		}
	}
	q := u.Query()
	// go-ora options are case-insensitive, so two spellings of one key (from
	// two config layers, say) have no defined winner; refuse to guess.
	seen := make(map[string]string, len(cfg.Params))
	for _, k := range slices.Sorted(maps.Keys(cfg.Params)) {
		if prev, dup := seen[strings.ToUpper(k)]; dup {
			return "", fmt.Errorf("oracle params %q and %q name the same option", prev, k)
		}
		seen[strings.ToUpper(k)] = k
		setOption(q, k, cfg.Params[k])
	}

	if cfg.User != "" || cfg.Password.Value() != "" {
		user, pass := cfg.User, cfg.Password.Value()
		if u.User != nil {
			if user == "" {
				user = u.User.Username()
			}
			if p, ok := u.User.Password(); ok && pass == "" {
				pass = p
			}
		}
		u.User = url.UserPassword(user, pass)
	}
	switch {
	case cfg.ConnectString != "":
		setOption(q, "connStr", cfg.ConnectString)
		u.Host, u.Path = ":0", ""
	case cfg.Host != "":
		port := cfg.Port
		if port == 0 {
			port = 1521
		}
		u.Host = net.JoinHostPort(cfg.Host, strconv.Itoa(port))
	}
	if cfg.Service != "" {
		u.Path = "/" + cfg.Service
		deleteOption(q, "SID")
		deleteOption(q, "SERVICE NAME") // go-ora lets it override the path
	}
	if cfg.SID != "" {
		setOption(q, "SID", cfg.SID)
		u.Path = ""
	}
	if cfg.TLS.Enabled {
		setOption(q, "SSL", "enable")
		setOption(q, "SSL VERIFY", strconv.FormatBool(!cfg.TLS.InsecureSkipVerify))
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// setOption sets a go-ora option, replacing keys that differ only in case.
func setOption(q url.Values, key, value string) {
	deleteOption(q, key)
	q.Set(key, value)
}

func deleteOption(q url.Values, key string) {
	for k := range q {
		if strings.EqualFold(k, key) {
			delete(q, k)
		}
	}
}

// Connector returns a connector for cfg. When TLS carries a CA or client
// certificate, every connection gets its own copy of the TLS config, which
// go-ora modifies during the handshake.
func Connector(cfg config.Oracle, opts ...Option) (driver.Connector, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return newConnector(cfg, o)
}

func newConnector(cfg config.Oracle, o options) (*connector, error) {
	dsn, err := URL(cfg)
	if err != nil {
		return nil, err
	}
	c := &connector{dsn: dsn, drv: go_ora.NewDriver(), hooks: o.connector}
	if cfg.TLS.Enabled {
		if cfg.TLS.ServerName != "" {
			return nil, errors.New("tls.server_name is not supported: go-ora verifies the certificate against the server address")
		}
		// Without files, go-ora's own setup applies: wallet or system roots.
		if cfg.TLS.CAFile != "" || cfg.TLS.CertFile != "" {
			if c.tls, err = cfg.TLS.Config(); err != nil {
				return nil, err
			}
		}
	}
	return c, nil
}

// connector shares one go-ora driver, which holds registered types, across
// connections.
type connector struct {
	dsn   string
	drv   *go_ora.OracleDriver
	tls   *tls.Config
	hooks []func(*go_ora.OracleConnector)
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dc, err := c.drv.OpenConnector(c.dsn)
	if err != nil {
		return nil, err
	}
	oc := dc.(*go_ora.OracleConnector)
	if c.tls != nil {
		oc.WithTLSConfig(c.tls.Clone())
	}
	for _, fn := range c.hooks {
		fn(oc)
	}
	return oc.Connect(ctx)
}

func (c *connector) Driver() driver.Driver { return c.drv }

// New builds a pool with the pool settings applied, without connecting:
// database/sql dials on first use.
func New(cfg config.Oracle, opts ...Option) (*sql.DB, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	c, err := newConnector(cfg, o)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(c)
	cfg.SQLPool.Apply(db)
	return db, nil
}

// Open is [New] plus a ping within cfg.PingTimeout. The pool is closed if
// the ping fails.
func Open(ctx context.Context, cfg config.Oracle, opts ...Option) (*sql.DB, error) {
	db, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	pingCtx := ctx
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping oracle %s: %w", target(cfg), err)
	}
	return db, nil
}

func target(cfg config.Oracle) string {
	switch {
	case cfg.ConnectString != "":
		return "connect_string"
	case cfg.Host != "":
		return net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)) + "/" + cfg.Service + cfg.SID
	}
	if u, err := url.Parse(cfg.DSN.Value()); err == nil {
		return u.Host + u.Path
	}
	return "dsn"
}

// redactErr drops a DSN, which may hold a password, from a parse error.
func redactErr(err error, dsn string) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return errors.New(strings.ReplaceAll(err.Error(), dsn, "***"))
}
