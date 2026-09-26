package conn_trino

import (
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds URL delimiters and the session_properties separators; users,
// passwords and names may contain any of them.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "p+ss", "p;ss", "it's", `say "hi"`,
	`back\slash`, "100%", "%zz", "%3B", "has space", "密碼🔑", "x@evil:1/?catalog=evil&custom_client=evil",
}

func serverURL(t *testing.T, serverURI string) *url.URL {
	t.Helper()
	u, err := url.Parse(serverURI)
	if err != nil {
		t.Fatalf("server uri %q: %v", serverURI, err)
	}
	return u
}

func TestDSNAdversarialRoundTrip(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			c := parse(t, config.Trino{
				Host: "trino", User: "u" + s, Password: config.NewSecret(s), Catalog: "c" + s, Schema: "s" + s,
				Params: map[string]string{"source": s}, TLS: config.TLS{Enabled: true},
			})
			u := serverURL(t, c.ServerURI)
			pw, _ := u.User.Password()
			if u.Scheme != "https" || u.Host != "trino:8080" || u.User.Username() != "u"+s || pw != s {
				t.Errorf("server %q: user %q password %q", c.ServerURI, u.User.Username(), pw)
			}
			if c.Catalog != "c"+s || c.Schema != "s"+s || c.Source != s || c.CustomClientName != "" || c.AccessToken != "" {
				t.Errorf("catalog %q schema %q source %q client %q token %q", c.Catalog, c.Schema, c.Source, c.CustomClientName, c.AccessToken)
			}
		})
	}
}

func TestDSNSessionPropertyValues(t *testing.T) {
	// Values are escaped by the DSN encoding; only ';' (entry separator)
	// cannot appear in a value, and ':' splits at the first occurrence.
	props := "a:x=1&y=2;b:50%;c:#hash;d:has space;e:plus+sign;f:k:v;g:"
	c := parse(t, config.Trino{Host: "trino", User: "u", Params: map[string]string{"session_properties": props}})
	want := map[string]string{"a": "x=1&y=2", "b": "50%", "c": "#hash", "d": "has space", "e": "plus+sign", "f": "k:v", "g": ""}
	for k, v := range want {
		if c.SessionProperties[k] != v {
			t.Errorf("session %s = %q, want %q (all %v)", k, c.SessionProperties[k], v, c.SessionProperties)
		}
	}
	if len(c.SessionProperties) != len(want) {
		t.Errorf("session = %v", c.SessionProperties)
	}
}

func TestDSNHostsAndPorts(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Trino
		host string
	}{
		"default port":      {config.Trino{Host: "trino", User: "u"}, "trino:8080"},
		"negative port":     {config.Trino{Host: "trino", Port: -1, User: "u"}, "trino:8080"},
		"max port":          {config.Trino{Host: "trino", Port: 65535, User: "u"}, "trino:65535"},
		"ipv6":              {config.Trino{Host: "::1", Port: 8443, User: "u"}, "[::1]:8443"},
		"host beats dsn":    {config.Trino{DSN: config.NewSecret("https://u@old:1"), Host: "new"}, "new:8080"},
		"dsn host kept":     {config.Trino{DSN: config.NewSecret("https://u@old:1"), Port: 9}, "old:1"},
		"dsn ipv6 host":     {config.Trino{DSN: config.NewSecret("https://u@[::1]:8443")}, "[::1]:8443"},
		"ipv6 host for dsn": {config.Trino{DSN: config.NewSecret("https://u@old:1"), Host: "fe80::1", Port: 8443}, "[fe80::1]:8443"},
	} {
		t.Run(name, func(t *testing.T) {
			c := parse(t, tc.cfg)
			if u := serverURL(t, c.ServerURI); u.Host != tc.host {
				t.Errorf("host = %q, want %q", u.Host, tc.host)
			}
		})
	}
}

func TestDSNTLSClientOnlyWhenNeeded(t *testing.T) {
	for name, tc := range map[string]struct {
		tls  config.TLS
		want bool
	}{
		"system roots":       {config.TLS{Enabled: true}, false},
		"server name":        {config.TLS{Enabled: true, ServerName: "trino.internal"}, true},
		"insecure":           {config.TLS{Enabled: true, InsecureSkipVerify: true}, true},
		"min version":        {config.TLS{Enabled: true, MinVersion: "1.3"}, true},
		"disabled with name": {config.TLS{ServerName: "x"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			c := parse(t, config.Trino{Host: "trino", User: "u", TLS: tc.tls})
			if got := c.CustomClientName != ""; got != tc.want {
				t.Errorf("custom client %q, want one: %v", c.CustomClientName, tc.want)
			}
			if tc.tls.Enabled != strings.HasPrefix(c.ServerURI, "https://") {
				t.Errorf("server = %s", c.ServerURI)
			}
		})
	}

	// Equal settings share a name; different ones must not, or one pool
	// would silently use another's CA or InsecureSkipVerify.
	a, _ := DSN(config.Trino{Host: "trino", User: "u", TLS: config.TLS{Enabled: true, ServerName: "a"}})
	b, _ := DSN(config.Trino{Host: "trino", User: "u", TLS: config.TLS{Enabled: true, ServerName: "b"}})
	if query(t, a).Get("custom_client") == query(t, b).Get("custom_client") {
		t.Errorf("different TLS settings share a client: %s / %s", a, b)
	}
}

func TestDSNUserParamCustomClientWins(t *testing.T) {
	c := parse(t, config.Trino{Host: "trino", User: "u", Params: map[string]string{"custom_client": "mine"}, TLS: config.TLS{Enabled: true, ServerName: "x"}})
	if c.CustomClientName != "mine" {
		t.Errorf("custom client = %q", c.CustomClientName)
	}
}

func TestDSNConcurrentTLSRegistration(t *testing.T) {
	// Pools are often opened concurrently at startup; registering the same
	// client name must be race-free (run with -race).
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			if _, err := DSN(config.Trino{Host: "trino", User: "u", TLS: config.TLS{Enabled: true, ServerName: "srv", MinVersion: []string{"1.2", "1.3"}[i%2]}}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestDSNErrorsDoNotLeakSecrets(t *testing.T) {
	const secret = "hunter2-S3cret"
	for name, cfg := range map[string]config.Trino{
		"bad param with password":    {Host: "trino", User: "u", Password: config.NewSecret(secret), TLS: config.TLS{Enabled: true}, Params: map[string]string{"query_timeout": "soon"}},
		"bad param with token":       {Host: "trino", User: "u", AccessToken: config.NewSecret(secret), TLS: config.TLS{Enabled: true}, Params: map[string]string{"explicitPrepare": "maybe"}},
		"bad session with token":     {Host: "trino", User: "u", AccessToken: config.NewSecret(secret), TLS: config.TLS{Enabled: true}, Params: map[string]string{"session_properties": "novalue"}},
		"dsn bad escape":             {DSN: config.NewSecret("https://u:" + secret + "%zz@h:8443")},
		"dsn control char":           {DSN: config.NewSecret("https://u:" + secret + "\x7f@h:8443")},
		"dsn token over bad query":   {DSN: config.NewSecret("https://u@h:8443?accessToken=" + secret + "&a=1;b=2")},
		"dsn password over http":     {DSN: config.NewSecret("http://u:" + secret + "@h:8080")},
		"field password over http":   {DSN: config.NewSecret("http://u@h:8080"), Password: config.NewSecret(secret)},
		"field token over http":      {DSN: config.NewSecret("http://u@h:8080"), AccessToken: config.NewSecret(secret)},
		"dsn heartbeat with token":   {DSN: config.NewSecret("https://u@h:8443?accessToken=" + secret + "&heartbeat_interval=-1s")},
		"dsn wrong scheme with pass": {DSN: config.NewSecret("trino://u:" + secret + "@h")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DSN(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error leaks secret: %v", err)
			}
		})
	}
}

// TestAccessTokenOverHTTPRejected: the driver sends an access token as a
// bearer token but only checks the scheme for passwords, so a token given as
// the accessToken DSN or Params parameter must be refused over plain http
// just like the AccessToken field; otherwise the JWT goes out in cleartext.
func TestAccessTokenOverHTTPRejected(t *testing.T) {
	for name, cfg := range map[string]config.Trino{
		"dsn":    {DSN: config.NewSecret("http://u@trino:8080?accessToken=jwt")},
		"params": {Host: "trino", User: "u", Params: map[string]string{"accessToken": "jwt"}},
	} {
		if dsn, err := DSN(cfg); err == nil {
			t.Errorf("%s: token accepted over http: %s", name, dsn)
		}
	}
}

func query(t *testing.T, dsn string) url.Values {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return u.Query()
}
