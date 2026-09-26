// Package conn_trino opens database/sql pools for Trino with
// github.com/trinodb/trino-go-client from [config.Trino].
//
//	db, err := conn_trino.Open(ctx, cfg.Trino.MustGet("lake"))
//	defer db.Close()
//
// TLS settings beyond tls.enabled (CA, client certificate, server name,
// insecure_skip_verify, min_version) need an HTTP client of their own, which
// is registered with trino.RegisterCustomClient under a name derived from
// those settings. To use another client, register it yourself and set
// params.custom_client.
package conn_trino

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"

	"github.com/trinodb/trino-go-client/trino"

	"github.com/linzeyan/loadconf/config"
)

// DSN converts cfg into a trino-go-client DSN. The DSN is parsed first and
// the fields that are set override it; params replace DSN parameters of the
// same name. A custom TLS client is
// registered as a side effect when the TLS settings need one.
func DSN(cfg config.Trino) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	var u *url.URL
	if dsn := cfg.DSN.Value(); dsn != "" {
		var err error
		if u, err = url.Parse(dsn); err != nil {
			// url.Error repeats the DSN, password included.
			if ue := (*url.Error)(nil); errors.As(err, &ue) {
				err = ue.Err
			}
			return "", fmt.Errorf("trino dsn: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", errors.New("trino dsn: must start with http:// or https://")
		}
	} else {
		u = &url.URL{Scheme: "http", Host: net.JoinHostPort(cfg.Host, strconv.Itoa(portOr(cfg.Port, 8080)))}
	}
	if cfg.Host != "" && cfg.DSN.Value() != "" {
		u.Host = net.JoinHostPort(cfg.Host, strconv.Itoa(portOr(cfg.Port, 8080)))
	}
	if cfg.TLS.Enabled {
		u.Scheme = "https"
	}

	user, pass := u.User.Username(), ""
	if u.User != nil {
		pass, _ = u.User.Password()
	}
	if cfg.User != "" {
		user = cfg.User
	}
	if p := cfg.Password.Value(); p != "" {
		pass = p
	}
	switch {
	case pass != "":
		if u.Scheme != "https" {
			return "", errors.New("trino: a password requires https (tls.enabled)")
		}
		u.User = url.UserPassword(user, pass)
	case user != "":
		u.User = url.User(user)
	}

	// url.Query drops pairs with an unescaped ';' silently, and the driver
	// would too; session_properties must escape it as %3B.
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", fmt.Errorf("trino dsn: %w", err)
	}
	set := func(k, v string) {
		if v != "" {
			q.Set(k, v)
		}
	}
	set("catalog", cfg.Catalog)
	set("schema", cfg.Schema)
	set("accessToken", cfg.AccessToken.Value())
	for k, v := range cfg.Params {
		q.Set(k, v)
	}
	// The driver sends the token as a bearer header but only checks the
	// scheme for passwords; the token may also come from the DSN or Params.
	if q.Get("accessToken") != "" && u.Scheme != "https" {
		return "", errors.New("trino: an access token requires https (tls.enabled)")
	}
	if cfg.TLS.Enabled && q.Get("custom_client") == "" && needsClient(cfg.TLS) {
		name, err := registerTLSClient(cfg.TLS)
		if err != nil {
			return "", err
		}
		q.Set("custom_client", name)
	}
	u.RawQuery = q.Encode()

	dsn := u.String()
	if _, err := trino.ParseDSN(dsn); err != nil {
		return "", fmt.Errorf("trino dsn: %w", err)
	}
	return dsn, nil
}

func needsClient(t config.TLS) bool {
	return t.CAFile != "" || t.CertFile != "" || t.ServerName != "" || t.InsecureSkipVerify || t.MinVersion != ""
}

// registerTLSClient registers a client for t under a name derived from the
// settings, so opening the same config again reuses the name.
func registerTLSClient(t config.TLS) (string, error) {
	tlsCfg, err := t.Config()
	if err != nil {
		return "", fmt.Errorf("trino tls: %w", err)
	}
	key, _ := json.Marshal(t)
	sum := sha256.Sum256(key)
	name := "loadconf-conn_trino-" + hex.EncodeToString(sum[:8])

	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsCfg
	client := &http.Client{
		Transport: tr,
		// Like the driver's own clients: never follow redirects with
		// credentials.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	if err := trino.RegisterCustomClient(name, client); err != nil {
		return "", err
	}
	return name, nil
}

// New builds a pool with the pool settings applied, without contacting the
// server: the driver does so on the first query.
func New(cfg config.Trino) (*sql.DB, error) {
	db, _, err := newDB(cfg)
	return db, err
}

// Open is [New] plus a SELECT 1 within cfg.PingTimeout: Trino's ping does not
// reach the server. The pool is closed if the query fails.
func Open(ctx context.Context, cfg config.Trino) (*sql.DB, error) {
	db, dsn, err := newDB(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	var one int
	if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping trino %s: %w", target(dsn), err)
	}
	return db, nil
}

func newDB(cfg config.Trino) (*sql.DB, string, error) {
	dsn, err := DSN(cfg)
	if err != nil {
		return nil, "", err
	}
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, "", fmt.Errorf("open trino: %w", err)
	}
	cfg.SQLPool.Apply(db)
	return db, dsn, nil
}

func target(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return "?"
	}
	return u.Scheme + "://" + u.Host
}

func portOr(port, def int) int {
	if port > 0 {
		return port
	}
	return def
}
