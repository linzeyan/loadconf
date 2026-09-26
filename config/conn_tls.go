package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
)

// TLS configures transport security of a client connection.
type TLS struct {
	Enabled bool `config:"enabled"`
	// CAFile is a PEM bundle of CAs to trust; system roots when empty.
	CAFile string `config:"ca_file"`
	// CertFile and KeyFile hold the client certificate for mutual TLS.
	CertFile           string `config:"cert_file"`
	KeyFile            string `config:"key_file"`
	ServerName         string `config:"server_name"`
	InsecureSkipVerify bool   `config:"insecure_skip_verify"`
	// MinVersion is "1.2" or "1.3" (default "1.2").
	MinVersion string `config:"min_version"`
}

var tlsVersions = map[string]uint16{
	"":    tls.VersionTLS12,
	"1.2": tls.VersionTLS12,
	"1.3": tls.VersionTLS13,
}

func (t TLS) Validate() error {
	if (t.CertFile == "") != (t.KeyFile == "") {
		return errors.New("cert_file and key_file must be set together")
	}
	if _, ok := tlsVersions[t.MinVersion]; !ok {
		return fmt.Errorf("unsupported min_version %q (want 1.2 or 1.3)", t.MinVersion)
	}
	return nil
}

// Config builds a *tls.Config, or returns nil when TLS is disabled.
func (t TLS) Config() (*tls.Config, error) {
	if !t.Enabled {
		return nil, nil
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		MinVersion:         tlsVersions[t.MinVersion],
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify, //nolint:gosec // opt-in for test environments
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file %s contains no PEM certificates", t.CAFile)
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
