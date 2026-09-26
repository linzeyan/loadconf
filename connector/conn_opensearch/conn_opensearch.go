// Package conn_opensearch opens OpenSearch clients (opensearch-go v4) from
// [config.OpenSearch].
//
//	client, err := conn_opensearch.Open(ctx, cfg.OpenSearch.MustGet("logs"))
//	defer client.Close()
//
// The transport logs each request to slog.Default() through [SlogLogger];
// [WithLogger] picks another logger.
//
// OpenSearch has no Elastic Cloud IDs, API keys, service tokens or
// certificate fingerprints; configs using them are rejected. AWS SigV4
// signing (Amazon OpenSearch Service) is out of scope, but a signer from
// github.com/opensearch-project/opensearch-go/v4/signer/awsv2 can be set with
// an Option:
//
//	conn_opensearch.Open(ctx, cfg, func(c *opensearch.Config) { c.Signer = signer })
package conn_opensearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the client config after it is built from the config, e.g.
// to set a Signer, RetryBackoff or RequestTimeout.
type Option func(*opensearch.Config)

// Config converts cfg into an opensearch-go config, cfg.Options first, then
// the typed fields, and applies opts.
//
// TLS settings and ResponseHeaderTimeout are applied to a clone of
// http.DefaultTransport.
func Config(cfg config.OpenSearch, opts ...Option) (opensearch.Config, error) {
	if err := cfg.Validate(); err != nil {
		return opensearch.Config{}, err
	}
	var unsupported []error
	for name, set := range map[string]bool{
		"cloud_id":                cfg.CloudID != "",
		"api_key":                 cfg.APIKey.Value() != "",
		"service_token":           cfg.ServiceToken.Value() != "",
		"certificate_fingerprint": cfg.CertificateFingerprint != "",
	} {
		if set {
			unsupported = append(unsupported, fmt.Errorf("%s is not supported by OpenSearch", name))
		}
	}
	if err := errors.Join(unsupported...); err != nil {
		return opensearch.Config{}, err
	}

	var oc opensearch.Config
	// The client applies ca_cert and insecure_skip_verify to the transport
	// over the tls settings.
	if err := config.DecodeOptions(cfg.Options, &oc, "addresses", "username", "password", "ca_cert",
		"insecure_skip_verify", "transport"); err != nil {
		return opensearch.Config{}, err
	}
	// The transport keeps an Authorization header it is given and then skips
	// the typed credentials, and options are not a Secret.
	for k := range oc.Header {
		if strings.EqualFold(k, "Authorization") {
			return opensearch.Config{}, errors.New("options.header.authorization: set by username and password, not in options")
		}
	}
	oc.Addresses = cfg.Addresses
	oc.Username, oc.Password = cfg.Username, cfg.Password.Value()
	tlsCfg, err := cfg.TLS.Config()
	if err != nil {
		return opensearch.Config{}, err
	}
	if tlsCfg != nil || cfg.ResponseHeaderTimeout > 0 {
		tr := newTransport()
		if tlsCfg != nil {
			tr.TLSClientConfig = tlsCfg
		}
		tr.ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
		oc.Transport = tr
	}
	oc.Logger = SlogLogger(nil)
	for _, opt := range opts {
		opt(&oc)
	}
	return oc, nil
}

// New creates a client without connecting. Close it to stop its
// background node discovery and health checks.
func New(cfg config.OpenSearch, opts ...Option) (*opensearchapi.Client, error) {
	oc, err := Config(cfg, opts...)
	if err != nil {
		return nil, err
	}
	return opensearchapi.NewClient(opensearchapi.Config{Client: oc})
}

// Open creates a client and verifies it with an info request (GET /),
// bounded by cfg.PingTimeout. The client is closed if the check fails.
func Open(ctx context.Context, cfg config.OpenSearch, opts ...Option) (*opensearchapi.Client, error) {
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
	if _, err := c.Info(pingCtx, nil); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("opensearch ping %s: %w", target(cfg), err)
	}
	return c, nil
}

func newTransport() *http.Transport {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		return t.Clone()
	}
	return &http.Transport{Proxy: http.ProxyFromEnvironment, ForceAttemptHTTP2: true}
}

// target describes the cluster for error messages, without credentials.
func target(cfg config.OpenSearch) string {
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
