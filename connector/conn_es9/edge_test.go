package conn_es9

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
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

func TestFingerprintForms(t *testing.T) {
	sum := sha256.Sum256([]byte("ca"))
	want := hex.EncodeToString(sum[:])
	var pairs []string
	for i := 0; i < len(want); i += 2 {
		pairs = append(pairs, want[i:i+2])
	}
	for name, tc := range map[string]struct {
		in string
		ok bool
	}{
		"openssl colon upper": {strings.ToUpper(strings.Join(pairs, ":")), true},
		"plain lower":         {want, true},
		"plain upper":         {strings.ToUpper(want), true},
		"stray colons":        {":" + want[:10] + "::" + want[10:] + ":", true},
		"sha1 length":         {want[:40], false},
		"one digit short":     {want[:63], false},
		"one byte long":       {want + "ab", false},
		"spaces":              {strings.Join(pairs, " "), false},
		"openssl prefix":      {"SHA256 Fingerprint=" + strings.Join(pairs, ":"), false},
		"non hex":             {"zz" + want[2:], false},
		"only colons":         {":::", false},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := fingerprint(tc.in)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok %v", err, tc.ok)
			}
			if tc.ok && got != want {
				t.Errorf("fingerprint = %q, want %q", got, want)
			}
		})
	}
}

func TestConfigCredentialsVerbatim(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			// Secrets bypass URL syntax entirely, so nothing is escaped or
			// trimmed on the way.
			for _, cfg := range []config.Elasticsearch{
				{Addresses: []string{"https://es:9200"}, Username: "u" + s, Password: config.NewSecret(s)},
				{Addresses: []string{"https://es:9200"}, APIKey: config.NewSecret(s)},
				{Addresses: []string{"https://es:9200"}, ServiceToken: config.NewSecret(s)},
			} {
				ec, err := Config(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if ec.Username != cfg.Username || ec.Password != cfg.Password.Value() || ec.APIKey != cfg.APIKey.Value() || ec.ServiceToken != cfg.ServiceToken.Value() {
					t.Errorf("config = %+v", ec)
				}
			}
		})
	}
}

func TestConfigAddressesVerbatim(t *testing.T) {
	addrs := []string{"https://[::1]:9200", "http://es2:9200/", "https://es3/prefix/", "https://u:p%40ss@es4:9200"}
	ec, err := Config(config.Elasticsearch{Addresses: addrs})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ec.Addresses, " ") != strings.Join(addrs, " ") {
		t.Errorf("addresses = %v", ec.Addresses)
	}
	if _, err := New(config.Elasticsearch{Addresses: addrs}); err != nil {
		t.Errorf("client rejects the addresses: %v", err)
	}
}

func TestTargetNeverPrintsCredentials(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Elasticsearch
		want string
	}{
		"userinfo":      {config.Elasticsearch{Addresses: []string{"https://elastic:hunter2@es1:9200", "https://es2:9200"}}, "[https://es1:9200 https://es2:9200]"},
		"user only":     {config.Elasticsearch{Addresses: []string{"https://hunter2@es1:9200/p"}}, "[https://es1:9200/p]"},
		"escaped pass":  {config.Elasticsearch{Addresses: []string{"https://u:hunter2%3A%40@[::1]:9200"}}, "[https://[::1]:9200]"},
		"at in pass":    {config.Elasticsearch{Addresses: []string{"https://u:hun@ter2@es1:9200"}}, "[https://es1:9200]"},
		"cloud id":      {config.Elasticsearch{CloudID: "prod:ZXhhbXBsZS5jb20kYWJjJGRlZg=="}, "cloud_id prod"},
		"cloud no name": {config.Elasticsearch{CloudID: "ZXhhbXBsZS5jb20kYWJjJGRlZg=="}, "cloud_id ZXhhbXBsZS5jb20kYWJjJGRlZg=="},
	} {
		t.Run(name, func(t *testing.T) {
			got := target(tc.cfg)
			if got != tc.want {
				t.Errorf("target = %q, want %q", got, tc.want)
			}
			if strings.Contains(got, "hunter2") || strings.Contains(got, "ter2") {
				t.Errorf("target leaks credentials: %q", got)
			}
		})
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for key, want := range map[string]string{
		"ADDRESSES":     "options.addresses: set by its own config key",
		"Cloud_ID":      "options.cloud_id: set by its own config key",
		"PASSWORD":      "options.password: set by its own config key",
		"Api_Key":       "options.api_key: set by its own config key",
		"SERVICE_TOKEN": "options.service_token: set by its own config key",
		"Transport":     "options.transport",
		// The Go field spelling is not a config key at all.
		"CertificateFingerprint": "unknown options: options.CertificateFingerprint",
	} {
		t.Run(key, func(t *testing.T) {
			_, err := Config(config.Elasticsearch{Addresses: []string{"https://es:9200"}, Options: map[string]any{key: "hunter2"}})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want %q", err, want)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsCannotInstallDriverObjects(t *testing.T) {
	for _, key := range []string{"logger", "selector", "retry_on_error", "retry_backoff", "connection_pool_func", "instrumentation", "interceptors"} {
		t.Run(key, func(t *testing.T) {
			if _, err := Config(config.Elasticsearch{Addresses: []string{"https://es:9200"}, Options: map[string]any{key: "x"}}); err == nil || !strings.Contains(err.Error(), "options."+key) {
				t.Errorf("err = %v", err)
			}
		})
	}
}

func TestConfigTransportPerCall(t *testing.T) {
	// Each config gets its own transport: a shared one would carry one
	// client's CA or timeout into another, and http.DefaultTransport must
	// never be modified.
	cfg := config.Elasticsearch{Addresses: []string{"https://es:9200"}, ResponseHeaderTimeout: time.Second}
	a, err := Config(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Config(cfg)
	if a.Transport == nil || a.Transport == b.Transport || a.Transport == http.DefaultTransport {
		t.Errorf("transports %p %p", a.Transport, b.Transport)
	}
	for _, d := range []time.Duration{0, -time.Second} {
		if c, _ := Config(config.Elasticsearch{Addresses: []string{"https://es:9200"}, ResponseHeaderTimeout: d}); c.Transport != nil {
			t.Errorf("timeout %v: transport = %#v, want the client default", d, c.Transport)
		}
	}
}

func TestConfigErrorsDoNotLeakSecrets(t *testing.T) {
	const secret = "hunter2-S3cret"
	fp := strings.Repeat("ab", sha256.Size)
	for name, cfg := range map[string]config.Elasticsearch{
		"two auth methods":     {Addresses: []string{"https://es:9200"}, Username: "u", Password: config.NewSecret(secret), APIKey: config.NewSecret(secret)},
		"token and key":        {Addresses: []string{"https://es:9200"}, ServiceToken: config.NewSecret(secret), APIKey: config.NewSecret(secret)},
		"bad fingerprint":      {Addresses: []string{"https://es:9200"}, APIKey: config.NewSecret(secret), CertificateFingerprint: "abc"},
		"fingerprint with tls": {Addresses: []string{"https://es:9200"}, Password: config.NewSecret(secret), CertificateFingerprint: fp, TLS: config.TLS{Enabled: true}},
		"bad tls":              {Addresses: []string{"https://es:9200"}, ServiceToken: config.NewSecret(secret), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
		"bad option":           {Addresses: []string{"https://es:9200"}, Password: config.NewSecret(secret), Options: map[string]any{"max_retries": "many"}},
		"unknown option":       {Addresses: []string{"https://es:9200"}, APIKey: config.NewSecret(secret), Options: map[string]any{"apikey": secret}},
		"no address":           {Password: config.NewSecret(secret), APIKey: config.NewSecret(secret)},
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
		ec, err := Config(config.Elasticsearch{Addresses: []string{"https://es:9200"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodGet, "https://u:hunter2@es:9200/_bulk", nil)
		_ = ec.Logger.LogRoundTrip(req, &http.Response{StatusCode: 503}, nil, time.Now(), time.Millisecond)
		if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "url=https://es:9200/_bulk") || strings.Contains(out, "hunter2") {
			t.Errorf("%s: output = %q", name, out)
		}
	}
}

// TestInvalidAddressErrorRedactsPassword: the client supports credentials
// in addresses, so config.Elasticsearch.Validate, whose error Config returns
// as is, must not print them for a scheme typo or stray space: that would
// put the password into startup logs.
func TestInvalidAddressErrorRedactsPassword(t *testing.T) {
	for _, addr := range []string{"htps://elastic:hunter2@es:9200", "https://elastic:hunter2@es:9200 ", "https://elastic:hunter2@"} {
		_, err := Config(config.Elasticsearch{Addresses: []string{addr}})
		if err == nil {
			t.Fatalf("%q: want error", addr)
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Errorf("error leaks password: %v", err)
		}
	}
}

// TestAuthorizationHeaderOptionRejected: options.header is sent before the
// transport adds credentials, and the transport skips its own Authorization
// when one is already present. An Authorization header in options (not a
// Secret, so also printed with the config) would silently replace api_key,
// service_token or username/password, so Config rejects it in any case.
// Other headers stay allowed.
func TestAuthorizationHeaderOptionRejected(t *testing.T) {
	for _, key := range []string{"authorization", "Authorization", "AUTHORIZATION"} {
		_, err := Config(config.Elasticsearch{
			Addresses: []string{"https://es:9200"}, APIKey: config.NewSecret("dHlwZWQ="),
			Options: map[string]any{"header": map[string]any{key: "Bearer from-options"}},
		})
		if err == nil || !strings.Contains(err.Error(), "options.header.authorization") || strings.Contains(err.Error(), "from-options") {
			t.Errorf("%s: err = %v", key, err)
		}
	}
	ec, err := Config(config.Elasticsearch{Addresses: []string{"https://es:9200"}, Options: map[string]any{"header": map[string]any{"X-Opaque-Id": "svc"}}})
	if err != nil || ec.Header.Get("X-Opaque-Id") != "svc" {
		t.Errorf("header = %v, err = %v", ec.Header, err)
	}
}

// TestCACertOptionOwnedByTLS: the transport replaces the RootCAs of the TLS
// config built from tls.ca_file with options.ca_cert, so the option would
// silently win over the typed CA. tls.ca_file owns it, and Config rejects
// the option.
func TestCACertOptionOwnedByTLS(t *testing.T) {
	_, err := Config(config.Elasticsearch{
		Addresses: []string{"https://es:9200"}, TLS: config.TLS{Enabled: true},
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
