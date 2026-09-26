package conn_oracle

import (
	"strings"
	"testing"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds URL delimiters and escapes; Oracle passwords from a secret
// store may contain any of them (quoted identifiers allow almost anything).
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "p+ss", "it's", `say "hi"`,
	`back\slash`, "100%", "%zz", "%41", "a;b", "has space", " lead", "trail ", "密碼🔑",
	"x@evil:1/svc?SSL=true&SERVER=evil:2", "(DESCRIPTION=(ADDRESS=(HOST=evil)))",
}

func TestURLAdversarialRoundTrip(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			c := parse(t, config.Oracle{
				Host: "ora", Port: 1522, User: "u" + s, Password: config.NewSecret(s), Service: "S" + s,
				Params: map[string]string{"PROGRAM": s},
			})
			if c.UserID != "u"+s || c.Password != s || c.ServiceName != "S"+strings.TrimRight(s, "/") || c.ProgramName != s {
				t.Errorf("user %q password %q service %q program %q", c.UserID, c.Password, c.ServiceName, c.ProgramName)
			}
			// Any extra server, SID or SSL flag would mean a value escaped
			// its URL component.
			if len(c.Servers) != 1 || c.Servers[0].Addr != "ora" || c.Servers[0].Port != 1522 || c.SID != "" || c.SSL {
				t.Errorf("servers %+v sid %q ssl %v", c.Servers, c.SID, c.SSL)
			}
		})
	}
}

func TestURLHosts(t *testing.T) {
	for name, tc := range map[string]struct {
		host string
		port int
		want int
	}{
		"default port": {"ora", 0, 1521},
		"max port":     {"ora", 65535, 65535},
		"ipv6":         {"::1", 1600, 1600},
		"ipv6 zone":    {"fe80::1%en0", 0, 1521},
	} {
		t.Run(name, func(t *testing.T) {
			c := parse(t, config.Oracle{Host: tc.host, Port: tc.port, Service: "s"})
			if len(c.Servers) != 1 || c.Servers[0].Addr != tc.host || c.Servers[0].Port != tc.want {
				t.Errorf("servers = %+v", c.Servers)
			}
		})
	}
}

func TestURLDSNEveryFieldOverridden(t *testing.T) {
	c := parse(t, config.Oracle{
		DSN:  config.NewSecret("oracle://old:oldpw@old:1600/OLD?PREFETCH_ROWS=10&SID=X"),
		Host: "new", Port: 1700, User: "nu", Password: config.NewSecret("np"), Service: "NEW",
		Params: map[string]string{"prefetch_rows": "99"},
	})
	if len(c.Servers) != 1 || c.Servers[0].Addr != "new" || c.Servers[0].Port != 1700 {
		t.Errorf("servers = %+v", c.Servers)
	}
	if c.UserID != "nu" || c.Password != "np" || c.ServiceName != "NEW" || c.SID != "" || c.PrefetchRows != 99 {
		t.Errorf("user %q password %q service %q sid %q prefetch %d", c.UserID, c.Password, c.ServiceName, c.SID, c.PrefetchRows)
	}
}

func TestURLSIDReplacesDSNService(t *testing.T) {
	c := parse(t, config.Oracle{DSN: config.NewSecret("oracle://u:p@h:1521/OLDSVC"), SID: "ORCL"})
	if c.SID != "ORCL" || c.ServiceName != "" {
		t.Errorf("sid %q service %q", c.SID, c.ServiceName)
	}
}

func TestURLConnectStringReplacesHost(t *testing.T) {
	c := parse(t, config.Oracle{
		DSN:           config.NewSecret("oracle://u:p@old:1600/OLD"),
		ConnectString: "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=rac1)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=svc)))",
	})
	if len(c.Servers) != 1 || c.Servers[0].Addr != "rac1" || c.ServiceName != "svc" {
		t.Errorf("servers %+v service %q", c.Servers, c.ServiceName)
	}
	if c.UserID != "u" || c.Password != "p" {
		t.Errorf("DSN credentials lost: user %q password %q", c.UserID, c.Password)
	}
}

func TestURLTLSReplacesDSNFlags(t *testing.T) {
	// TLS must not be weakened by a DSN or Params spelling the options in
	// another case.
	c := parse(t, config.Oracle{
		DSN: config.NewSecret("oracle://u:p@h:1521/s?ssl=false&Ssl+Verify=false"), Params: map[string]string{"SSL": "disable"},
		TLS: config.TLS{Enabled: true},
	})
	if !c.SSL || !c.SSLVerify {
		t.Errorf("ssl %v verify %v", c.SSL, c.SSLVerify)
	}
	dsn, _ := URL(config.Oracle{DSN: config.NewSecret("oracle://u:p@h:1521/s?ssl=false&Ssl+Verify=false"), TLS: config.TLS{Enabled: true}})
	if n := strings.Count(strings.ToUpper(dsn), "SSL"); n != 2 {
		t.Errorf("want exactly SSL and SSL VERIFY once each: %s", dsn)
	}
}

// TestParamsCaseVariantsRejected: go-ora options are case-insensitive, and
// config does not normalize map keys, so two layered sources (a file with
// PREFETCH_ROWS, env with prefetch_rows) yield both spellings with no
// defined winner. Picking one by map order gave a different URL from call to
// call; URL must refuse them, every time.
func TestParamsCaseVariantsRejected(t *testing.T) {
	cfg := config.Oracle{Host: "h", Params: map[string]string{"prefetch_rows": "1", "PREFETCH_ROWS": "2", "Prefetch_Rows": "3"}}
	for range 50 {
		if got, err := URL(cfg); err == nil {
			t.Fatalf("case variants accepted: %q", got)
		}
	}
}

// TestServiceFieldBeatsServiceNameOption: go-ora reads the service from the
// URL path and then lets a "SERVICE NAME" option override it, so a SERVICE
// NAME from the DSN or Params must not survive the Service field ("Params,
// then the explicit fields override").
func TestServiceFieldBeatsServiceNameOption(t *testing.T) {
	for name, cfg := range map[string]config.Oracle{
		"dsn":    {DSN: config.NewSecret("oracle://u:p@h:1521/x?SERVICE+NAME=OLD"), Service: "NEW"},
		"params": {Host: "h", Service: "NEW", Params: map[string]string{"service name": "OLD"}},
	} {
		if c := parse(t, cfg); c.ServiceName != "NEW" {
			t.Errorf("%s: service = %q, want NEW", name, c.ServiceName)
		}
	}
}

func TestURLErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Oracle{
		"bad escape":     {DSN: config.NewSecret("oracle://u:" + pw + "%zz@h:1521/s")},
		"bad port":       {DSN: config.NewSecret("oracle://u:" + pw + "@h:port/s")},
		"control char":   {DSN: config.NewSecret("oracle://u:" + pw + "\x7f@h:1521/s")},
		"missing scheme": {DSN: config.NewSecret("u:" + pw + "@h:1521/s")},
		"wrong scheme":   {DSN: config.NewSecret("oracles://u:" + pw + "@h:1521/s")},
		"bad tls":        {Host: "h", Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, CertFile: "c.pem"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := URL(cfg)
			if err == nil {
				_, err = Connector(cfg)
			}
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestTargetHasNoCredentials(t *testing.T) {
	// target() names the server in ping errors; it must never print the
	// DSN's userinfo.
	got := target(config.Oracle{DSN: config.NewSecret("oracle://u:hunter2@h:1521/s")})
	if got != "h:1521/s" {
		t.Errorf("target = %q", got)
	}
}
