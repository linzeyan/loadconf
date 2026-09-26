package conn_opensearch

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds URL delimiters and escapes; credentials from a secret store
// may contain any of them.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "p+ss", "it's", `say "hi"`,
	`back\slash`, "100%", "%zz", "a;b", "has space", " lead", "trail ", "密碼🔑",
}

func TestConfigCredentialsVerbatim(t *testing.T) {
	var s seen
	srv := httptest.NewServer(fakeOS(&s, http.StatusOK, infoBody))
	defer srv.Close()
	for _, v := range nasty {
		t.Run(v, func(t *testing.T) {
			// Secrets bypass URL syntax entirely, so nothing is escaped or
			// trimmed on the way to the Basic auth header.
			c, err := Open(context.Background(), config.OpenSearch{Addresses: []string{srv.URL}, Username: "u" + v, Password: config.NewSecret(v), Options: noRetry})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if got := s.last().Get("Authorization"); got != "Basic "+base64.StdEncoding.EncodeToString([]byte("u"+v+":"+v)) {
				t.Errorf("authorization = %q", got)
			}
		})
	}
}

func TestConfigAddressesVerbatim(t *testing.T) {
	addrs := []string{"https://[::1]:9200", "http://os2:9200/", "https://os3/prefix/", "https://u:p%40ss@os4:9200"}
	oc, err := Config(config.OpenSearch{Addresses: addrs})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(oc.Addresses, " ") != strings.Join(addrs, " ") {
		t.Errorf("addresses = %v", oc.Addresses)
	}
	c, err := New(config.OpenSearch{Addresses: addrs})
	if err != nil {
		t.Fatalf("client rejects the addresses: %v", err)
	}
	_ = c.Close()
}

func TestTargetNeverPrintsCredentials(t *testing.T) {
	for name, tc := range map[string]struct {
		addrs []string
		want  string
	}{
		"userinfo":     {[]string{"https://admin:hunter2@os1:9200", "https://os2:9200"}, "[https://os1:9200 https://os2:9200]"},
		"user only":    {[]string{"https://hunter2@os1:9200/p"}, "[https://os1:9200/p]"},
		"escaped pass": {[]string{"https://u:hunter2%3A%40@[::1]:9200"}, "[https://[::1]:9200]"},
		"at in pass":   {[]string{"https://u:hun@ter2@os1:9200"}, "[https://os1:9200]"},
	} {
		t.Run(name, func(t *testing.T) {
			got := target(config.OpenSearch{Addresses: tc.addrs})
			if got != tc.want || strings.Contains(got, "ter2") {
				t.Errorf("target = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for _, key := range []string{"ADDRESSES", "Username", "PASSWORD", "Transport"} {
		t.Run(key, func(t *testing.T) {
			_, err := Config(config.OpenSearch{Addresses: []string{"https://os:9200"}, Options: map[string]any{key: "hunter2"}})
			if err == nil || !strings.Contains(err.Error(), "options."+strings.ToLower(key)+": set by its own config key") {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsCannotInstallDriverObjects(t *testing.T) {
	for _, key := range []string{"logger", "signer", "selector", "router", "observer", "context", "health_check_request_modifier", "retry_backoff"} {
		t.Run(key, func(t *testing.T) {
			if _, err := Config(config.OpenSearch{Addresses: []string{"https://os:9200"}, Options: map[string]any{key: "x"}}); err == nil || !strings.Contains(err.Error(), "options."+key) {
				t.Errorf("err = %v", err)
			}
		})
	}
}

func TestConfigTransportPerCall(t *testing.T) {
	// Each config gets its own transport: a shared one would carry one
	// client's CA or timeout into another.
	cfg := config.OpenSearch{Addresses: []string{"https://os:9200"}, TLS: config.TLS{Enabled: true, ServerName: "os"}}
	a, err := Config(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Config(cfg)
	if a.Transport == nil || a.Transport == b.Transport || a.Transport == http.DefaultTransport {
		t.Errorf("transports %p %p", a.Transport, b.Transport)
	}
	if c, _ := Config(config.OpenSearch{Addresses: []string{"https://os:9200"}, ResponseHeaderTimeout: -time.Second}); c.Transport != nil {
		t.Errorf("negative timeout: transport = %#v", c.Transport)
	}
}

func TestConfigErrorsDoNotLeakSecrets(t *testing.T) {
	const secret = "hunter2-S3cret"
	for name, cfg := range map[string]config.OpenSearch{
		"api key":         {Addresses: []string{"https://os:9200"}, APIKey: config.NewSecret(secret)},
		"service token":   {Addresses: []string{"https://os:9200"}, ServiceToken: config.NewSecret(secret)},
		"cloud id":        {CloudID: "x:" + secret, Password: config.NewSecret(secret)},
		"fingerprint":     {Addresses: []string{"https://os:9200"}, Password: config.NewSecret(secret), CertificateFingerprint: secret},
		"bad tls":         {Addresses: []string{"https://os:9200"}, Password: config.NewSecret(secret), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
		"bad option":      {Addresses: []string{"https://os:9200"}, Password: config.NewSecret(secret), Options: map[string]any{"max_retries": "many"}},
		"unknown option":  {Addresses: []string{"https://os:9200"}, Password: config.NewSecret(secret), Options: map[string]any{"passwd": secret}},
		"two auth":        {Addresses: []string{"https://os:9200"}, Username: "u", Password: config.NewSecret(secret), APIKey: config.NewSecret(secret)},
		"no address":      {Password: config.NewSecret(secret)},
		"all unsupported": {Addresses: []string{"https://os:9200"}, APIKey: config.NewSecret(secret), ServiceToken: config.NewSecret(secret), CertificateFingerprint: secret},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Config(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaks secret: %v", err)
			}
		})
	}
}

func TestDefaultLoggerIsSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	for name, opts := range map[string][]Option{"no option": nil, "nil logger": {WithLogger(nil)}} {
		buf.Reset()
		oc, err := Config(config.OpenSearch{Addresses: []string{"https://os:9200"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "https://u:hunter2@os:9200/_bulk", nil)
		_ = oc.Logger.LogRoundTrip(req, &http.Response{StatusCode: 503}, nil, time.Now(), time.Millisecond)
		if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "url=https://os:9200/_bulk") || strings.Contains(out, "hunter2") {
			t.Errorf("%s: output = %q", name, out)
		}
	}
}

// TestInvalidAddressErrorRedactsPassword: config.OpenSearch.Validate, whose
// error Config returns as is, must redact a bad address like target() does,
// or a scheme typo or stray space puts the password into startup logs.
func TestInvalidAddressErrorRedactsPassword(t *testing.T) {
	for _, addr := range []string{"htps://admin:hunter2@os:9200", "https://admin:hunter2@os:9200 ", "https://admin:hunter2@"} {
		_, err := Config(config.OpenSearch{Addresses: []string{addr}})
		if err == nil {
			t.Fatalf("%q: want error", addr)
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("error leaks password: %v", err)
		}
	}
}

// TestAuthorizationHeaderOptionRejected: options.header is set before the
// transport adds Basic auth, which it skips when an Authorization header is
// present. A header in options (not a Secret) would silently replace
// username/password, so Config rejects it in any case. Other headers stay
// allowed.
func TestAuthorizationHeaderOptionRejected(t *testing.T) {
	for _, key := range []string{"authorization", "Authorization", "AUTHORIZATION"} {
		_, err := Config(config.OpenSearch{
			Addresses: []string{"https://os:9200"}, Username: "admin", Password: config.NewSecret("pw"),
			Options: map[string]any{"header": map[string]any{key: "Bearer from-options"}},
		})
		if err == nil || !strings.Contains(err.Error(), "options.header.authorization") || strings.Contains(err.Error(), "from-options") {
			t.Errorf("%s: err = %v", key, err)
		}
	}
	oc, err := Config(config.OpenSearch{Addresses: []string{"https://os:9200"}, Options: map[string]any{"header": map[string]any{"X-Opaque-Id": "svc"}}})
	if err != nil || oc.Header.Get("X-Opaque-Id") != "svc" {
		t.Errorf("header = %v, err = %v", oc.Header, err)
	}
}

// TestInsecureSkipVerifyOptionOwnedByTLS: opensearch.Config has its own
// InsecureSkipVerify, which would turn off certificate verification behind
// a tls section that asks for it (or with TLS disabled for https
// addresses). tls owns it, so Config rejects the option and an untrusted
// server fails.
func TestInsecureSkipVerifyOptionOwnedByTLS(t *testing.T) {
	srv := httptest.NewTLSServer(fakeOS(nil, http.StatusOK, infoBody))
	defer srv.Close()
	c, err := Open(context.Background(), config.OpenSearch{
		Addresses: []string{srv.URL}, TLS: config.TLS{Enabled: true, ServerName: "example.com"},
		Options: map[string]any{"insecure_skip_verify": true, "disable_retry": true},
	})
	if err == nil {
		c.Close()
		t.Error("an untrusted certificate was accepted with tls verification enabled")
	}
}

// TestCACertOptionOwnedByTLS: the transport replaces the RootCAs built from
// tls.ca_file with options.ca_cert, so the option would silently win over
// the typed CA. tls.ca_file owns it, and Config rejects the option.
func TestCACertOptionOwnedByTLS(t *testing.T) {
	_, err := Config(config.OpenSearch{
		Addresses: []string{"https://os:9200"}, TLS: config.TLS{Enabled: true},
		Options: map[string]any{"ca_cert": string(otherCAPEM(t))},
	})
	if err == nil || !strings.Contains(err.Error(), "options.ca_cert: set by its own config key") {
		t.Errorf("err = %v", err)
	}
}

// otherCAPEM returns a throwaway self-signed CA that signed nothing.
func otherCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "other-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
