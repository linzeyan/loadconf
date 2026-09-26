// Package conn_es9 opens Elasticsearch 9 clients (go-elasticsearch v9) from
// [config.Elasticsearch].
//
//	es, err := conn_es9.Open(ctx, cfg.Elasticsearch.MustGet("search"))
//	defer es.Close(context.Background())
//
// The transport logs each request to slog.Default() through [SlogLogger];
// [WithLogger] picks another logger.
package conn_es9

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/elastic/go-elasticsearch/v9"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the client config after it is built from the config, e.g.
// to set RetryBackoff, Instrumentation or EnableCompatibilityMode.
type Option func(*elasticsearch.Config)

// Config converts cfg into a go-elasticsearch config, cfg.Options first,
// then the typed fields, and applies opts.
//
// TLS settings and ResponseHeaderTimeout are applied to a clone of
// http.DefaultTransport. CertificateFingerprint is verified by the client
// with its own TLS dialer, which ignores tls settings, so the two cannot be
// combined.
func Config(cfg config.Elasticsearch, opts ...Option) (elasticsearch.Config, error) {
	if err := cfg.Validate(); err != nil {
		return elasticsearch.Config{}, err
	}
	var ec elasticsearch.Config
	// ca_cert would replace the CAs of tls.ca_file in the transport.
	if err := config.DecodeOptions(cfg.Options, &ec, "addresses", "cloud_id", "username", "password",
		"api_key", "service_token", "certificate_fingerprint", "ca_cert", "transport"); err != nil {
		return elasticsearch.Config{}, err
	}
	// The transport keeps an Authorization header it is given and then skips
	// the typed credentials, and options are not a Secret.
	for k := range ec.Header {
		if strings.EqualFold(k, "Authorization") {
			return elasticsearch.Config{}, errors.New("options.header.authorization: set by username and password, api_key or service_token, not in options")
		}
	}
	ec.Addresses, ec.CloudID = cfg.Addresses, cfg.CloudID
	ec.Username, ec.Password = cfg.Username, cfg.Password.Value()
	ec.APIKey, ec.ServiceToken = cfg.APIKey.Value(), cfg.ServiceToken.Value()
	if cfg.CertificateFingerprint != "" {
		if cfg.TLS.Enabled {
			return elasticsearch.Config{}, errors.New("certificate_fingerprint cannot be combined with tls: " +
				"the client pins the certificate with its own TLS dialer and ignores tls settings")
		}
		fp, err := fingerprint(cfg.CertificateFingerprint)
		if err != nil {
			return elasticsearch.Config{}, err
		}
		ec.CertificateFingerprint = fp
	}
	tlsCfg, err := cfg.TLS.Config()
	if err != nil {
		return elasticsearch.Config{}, err
	}
	if tlsCfg != nil || cfg.ResponseHeaderTimeout > 0 {
		tr := newTransport()
		if tlsCfg != nil {
			tr.TLSClientConfig = tlsCfg
		}
		tr.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
		ec.Transport = tr
	}
	ec.Logger = SlogLogger(nil)

	for _, opt := range opts {
		opt(&ec)
	}
	// The client installs the fingerprint check only on an *http.Transport
	// and silently skips it otherwise.
	if ec.CertificateFingerprint != "" && ec.Transport != nil {
		if _, ok := ec.Transport.(*http.Transport); !ok {
			return elasticsearch.Config{}, fmt.Errorf("certificate_fingerprint requires an *http.Transport, got %T", ec.Transport)
		}
	}
	return ec, nil
}

// New creates a client without connecting.
func New(cfg config.Elasticsearch, opts ...Option) (*elasticsearch.Client, error) {
	ec, err := Config(cfg, opts...)
	if err != nil {
		return nil, err
	}
	return elasticsearch.NewClient(ec)
}

// NewTypedClient creates a client with the typed API without connecting.
func NewTypedClient(cfg config.Elasticsearch, opts ...Option) (*elasticsearch.TypedClient, error) {
	ec, err := Config(cfg, opts...)
	if err != nil {
		return nil, err
	}
	return elasticsearch.NewTypedClient(ec)
}

// Open creates a client and verifies it with an info request (GET /),
// bounded by cfg.PingTimeout. Non-2xx responses and servers that are not
// Elasticsearch fail. The client is closed if the check fails.
func Open(ctx context.Context, cfg config.Elasticsearch, opts ...Option) (*elasticsearch.Client, error) {
	c, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	pingCtx := ctx
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		pingCtx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	if err := ping(pingCtx, c); err != nil {
		_ = c.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("elasticsearch ping %s: %w", target(cfg), err)
	}
	return c, nil
}

func ping(ctx context.Context, c *elasticsearch.Client) error {
	res, err := c.Info(c.Info.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		if body = bytes.TrimSpace(body); len(body) > 0 {
			return fmt.Errorf("status %s: %s", res.Status(), body)
		}
		return fmt.Errorf("status %s", res.Status())
	}
	_, _ = io.Copy(io.Discard, res.Body)
	return nil
}

func newTransport() *http.Transport {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
}

// fingerprint normalizes a hex SHA-256 fingerprint, accepting the
// colon-separated form printed by openssl.
func fingerprint(s string) (string, error) {
	fp := strings.ToLower(strings.ReplaceAll(s, ":", ""))
	if b, err := hex.DecodeString(fp); err != nil || len(b) != sha256.Size {
		return "", errors.New("certificate_fingerprint must be a hex encoded SHA-256 digest")
	}
	return fp, nil
}

// target describes the cluster for error messages, without credentials.
func target(cfg config.Elasticsearch) string {
	if cfg.CloudID != "" {
		name, _, _ := strings.Cut(cfg.CloudID, ":")
		return "cloud_id " + name
	}
	addrs := make([]string, len(cfg.Addresses))
	for i, a := range cfg.Addresses {
		if u, err := url.Parse(a); err == nil {
			a = redactURL(u)
		}
		addrs[i] = a
	}
	return fmt.Sprint(addrs)
}

// redactURL formats u without user info.
func redactURL(u *url.URL) string {
	if u.User == nil {
		return u.String()
	}
	c := *u
	c.User = nil
	return c.String()
}
