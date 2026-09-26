package config

import (
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Elasticsearch configures an Elasticsearch (go-elasticsearch v8 or v9) or
// OpenSearch (opensearch-go v4) client. OpenSearch does not support CloudID,
// APIKey, ServiceToken or CertificateFingerprint.
//
// Options sets any other client setting by the snake_case name of its
// elasticsearch.Config or opensearch.Config field, e.g. max_retries,
// retry_on_status, compress_request_body, discover_nodes_on_start or header
// (a map of lists); see [DecodeOptions]. Unset settings keep the client
// defaults.
type Elasticsearch struct {
	// Addresses are node URLs, e.g. https://es1:9200.
	Addresses []string `config:"addresses"`
	// CloudID connects to Elastic Cloud instead of Addresses.
	CloudID  string `config:"cloud_id"`
	Username string `config:"username"`
	Password Secret `config:"password"`
	// APIKey is the base64 encoded "id:api_key".
	APIKey       Secret `config:"api_key"`
	ServiceToken Secret `config:"service_token"`
	// CertificateFingerprint is the hex SHA-256 fingerprint of the CA
	// certificate, as printed on the first start of Elasticsearch 8.
	CertificateFingerprint string `config:"certificate_fingerprint"`

	// ResponseHeaderTimeout bounds the wait for response headers of each
	// request; zero waits as long as the request context allows. It is set on
	// the HTTP transport, which the client config does not expose.
	ResponseHeaderTimeout time.Duration `config:"response_header_timeout"`
	PingTimeout           time.Duration `config:"ping_timeout" default:"5s"`

	TLS     TLS            `config:"tls"`
	Options map[string]any `config:"options"`
}

// OpenSearch configures an OpenSearch client; see [Elasticsearch].
type OpenSearch = Elasticsearch

func (e Elasticsearch) Validate() error {
	var errs []error
	if len(e.Addresses) == 0 && e.CloudID == "" {
		errs = append(errs, errors.New("either addresses or cloud_id is required"))
	}
	if len(e.Addresses) > 0 && e.CloudID != "" {
		errs = append(errs, errors.New("addresses and cloud_id are mutually exclusive"))
	}
	// Addresses may carry credentials, so they are printed without userinfo;
	// one that does not parse is not printed at all, as its userinfo cannot
	// be located.
	for i, a := range e.Addresses {
		if u, err := url.Parse(a); err != nil {
			errs = append(errs, fmt.Errorf("addresses[%d] must be an http(s) URL", i))
		} else if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			u.User = nil
			errs = append(errs, fmt.Errorf("address %q must be an http(s) URL", u.String()))
		}
	}
	auth := 0
	for _, set := range []bool{e.Username != "", e.APIKey.Value() != "", e.ServiceToken.Value() != ""} {
		if set {
			auth++
		}
	}
	if auth > 1 {
		errs = append(errs, errors.New("username, api_key and service_token are mutually exclusive"))
	}
	return errors.Join(errs...)
}
