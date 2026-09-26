package conn_mssql

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds delimiters of the three DSN grammars go-mssqldb accepts (ADO,
// URL, ODBC); credentials from a secret store may contain any of them.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "it's", `say "hi"`, `"`, `""`,
	`back\slash`, "100%", "a;b", "{a;b}", "has space", " lead", "trail ", "密碼🔑",
	`x";server="evil`, `x";user id="sa`, `x;encrypt=disable`, "",
}

func TestConfigAdversarialRoundTrip(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			mc, err := Config(config.SQLServer{
				Host: "db", User: "u" + s, Password: config.NewSecret("p" + s), Database: "d" + s,
				Params: map[string]string{"app name": "a" + s},
			})
			if err != nil {
				t.Fatal(err)
			}
			// A value that broke out of its quotes would change the server,
			// the user or the encryption.
			if mc.Host != "db" || mc.Port != 1433 || mc.User != "u"+s || mc.Password != "p"+s || mc.Database != "d"+s || mc.AppName != "a"+s {
				t.Errorf("host %q port %d user %q password %q db %q app %q", mc.Host, mc.Port, mc.User, mc.Password, mc.Database, mc.AppName)
			}
			if mc.Encryption != msdsn.EncryptionOff {
				t.Errorf("encryption = %v", mc.Encryption)
			}
		})
	}
}

func TestConfigHostsAndPorts(t *testing.T) {
	for name, tc := range map[string]struct {
		host     string
		port     int
		wantHost string
		wantPort uint64
		target   string
	}{
		"default port": {"db", 0, "db", 1433, "db:1433/"},
		"max port":     {"db", 65535, "db", 65535, "db:65535/"},
		"ipv6":         {"::1", 1500, "::1", 1500, "[::1]:1500/"},
		"unicode":      {"資料庫.example", 0, "資料庫.example", 1433, "資料庫.example:1433/"},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := Config(config.SQLServer{Host: tc.host, Port: tc.port, TLS: config.TLS{Enabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			if mc.Host != tc.wantHost || mc.Port != tc.wantPort || target(mc) != tc.target {
				t.Errorf("host %q port %d target %q", mc.Host, mc.Port, target(mc))
			}
			if mc.TLSConfig == nil || mc.TLSConfig.ServerName != tc.wantHost {
				t.Errorf("tls = %+v", mc.TLSConfig)
			}
		})
	}
}

func TestConfigParamsCannotOverrideFields(t *testing.T) {
	// Params are applied before the typed fields; any spelling of an owned
	// key, including the ADO synonyms, must lose against the field.
	mc, err := Config(config.SQLServer{
		Host: "db", Port: 1444, User: "app", Password: config.NewSecret("field-pw"), Database: "erp",
		Params: map[string]string{
			"server": "evil", " Data Source ": "evil2", "port": "1", "USER ID": "sa", "uid": "sa2",
			"password": "p1", "pwd": "p2", "database": "master", "initial catalog": "master2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Host != "db" || mc.Port != 1444 || mc.User != "app" || mc.Password != "field-pw" || mc.Database != "erp" {
		t.Errorf("host %q port %d user %q password %q db %q", mc.Host, mc.Port, mc.User, mc.Password, mc.Database)
	}
}

func TestConfigParamsFillUnsetFields(t *testing.T) {
	// Without the field, a synonym in Params is the only source and wins.
	mc, err := Config(config.SQLServer{Host: "db", Params: map[string]string{"pwd": "from-params", "initial catalog": "erp"}})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Password != "from-params" || mc.Database != "erp" {
		t.Errorf("password %q db %q", mc.Password, mc.Database)
	}
}

func TestConfigDSNEveryFieldOverridden(t *testing.T) {
	for name, dsn := range map[string]string{
		"ado":  `server=old\INST,1500;user id=old;password=oldpw;database=olddb;app name=keep`,
		"url":  "sqlserver://old:oldpw@old:1500/INST?database=olddb&app+name=keep",
		"odbc": "odbc:server=old,1500;uid=old;pwd={old;pw};database=olddb;app name=keep",
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := Config(config.SQLServer{DSN: config.NewSecret(dsn), Host: "new", Port: 1444, User: "nu", Password: config.NewSecret("np"), Database: "nd"})
			if err != nil {
				t.Fatal(err)
			}
			if mc.Host != "new" || mc.Instance != "" || mc.Port != 1444 || mc.User != "nu" || mc.Password != "np" || mc.Database != "nd" {
				t.Errorf("host %q instance %q port %d user %q password %q db %q", mc.Host, mc.Instance, mc.Port, mc.User, mc.Password, mc.Database)
			}
			if mc.AppName != "keep" {
				t.Errorf("DSN param lost: app name %q", mc.AppName)
			}
		})
	}
}

// TestInstanceUsesServerSynonymInParams: ADO also names the server "data
// source", "address", "addr" or "network address". Instance is added to the
// server that the merged string would name, so such a key in Params beats
// the DSN's server, as the documented precedence DSN < Params < fields says.
func TestInstanceUsesServerSynonymInParams(t *testing.T) {
	mc, err := Config(config.SQLServer{DSN: config.NewSecret("server=h0"), Instance: "NEW", Params: map[string]string{"Data Source": "h1"}})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Host != "h1" || mc.Instance != "NEW" {
		t.Errorf("host %q instance %q, want h1 NEW", mc.Host, mc.Instance)
	}
}

// TestInstanceDropsPortInServer: the driver asks the SQL Server Browser only
// when the port is 0, and dials a fixed port otherwise, whatever instance is
// named. A port written as "host,port" must therefore be dropped like the
// port key when Instance is set; kept, the connection silently goes to
// whatever listens on that port, which may be another instance.
func TestInstanceDropsPortInServer(t *testing.T) {
	for name, cfg := range map[string]config.SQLServer{
		"params server":            {DSN: config.NewSecret("server=h0"), Params: map[string]string{"server": `h\OLD,1500`}},
		"params synonym":           {DSN: config.NewSecret("server=h0"), Params: map[string]string{"Data Source": "h,1500"}},
		"odbc dsn":                 {DSN: config.NewSecret("odbc:server=h,1500")},
		"ado dsn":                  {DSN: config.NewSecret(`server=h\OLD,1500`)},
		"host field":               {Host: "h,1500"},
		"replaced params server":   {Host: "h", Params: map[string]string{"server": "h0,1500"}},
		"replaced odbc dsn server": {DSN: config.NewSecret("odbc:server=h0,1500"), Host: "h"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg.Instance = "NEW"
			mc, err := Config(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if mc.Host != "h" || mc.Instance != "NEW" || mc.Port != 0 {
				t.Errorf("host %q instance %q port %d, want h NEW 0", mc.Host, mc.Instance, mc.Port)
			}
		})
	}
}

// TestADOMergeStaysADO: msdsn picks the grammar from the string's prefix and
// its "odbc:" check is case-sensitive, so "ODBC:server=db;..." parses as ADO
// with the key "odbc:server". The merged string, which starts with the
// smallest key, must still be read as ADO; as ODBC, the ADO double quotes
// around every value would be kept literally and the login would fail.
// Found by FuzzConfigDSNOverride ("odBC:00000000000000").
func TestADOMergeStaysADO(t *testing.T) {
	mc, err := Config(config.SQLServer{DSN: config.NewSecret("ODBC:server=db;uid=sa"), Password: config.NewSecret("np")})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Password != "np" {
		t.Errorf("password = %q, want np", mc.Password)
	}
}

func TestConfigErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.SQLServer{
		"port out of range":  {Host: "db", Port: 65536, Password: config.NewSecret(pw)},
		"negative port":      {Host: "db", Port: -1, Password: config.NewSecret(pw)},
		"bad param":          {Host: "db", Password: config.NewSecret(pw), Params: map[string]string{"dial timeout": "soon"}},
		"bad encrypt":        {Host: "db", Password: config.NewSecret(pw), Params: map[string]string{"encrypt": pw}},
		"ado dsn bad value":  {DSN: config.NewSecret("server=db;password=" + pw + ";packet size=big")},
		"odbc unterminated":  {DSN: config.NewSecret("odbc:server=db;pwd={" + pw)},
		"url dsn bad port":   {DSN: config.NewSecret("sqlserver://sa:" + pw + "@db:99999")},
		"url dsn bad escape": {DSN: config.NewSecret("sqlserver://sa:" + pw + "%zz@db")},
		"bad tls":            {Host: "db", Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.1"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Config(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestConfigTLSServerNameFromDSN(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.SQLServer
		want string
	}{
		"url host":            {config.SQLServer{DSN: config.NewSecret("sqlserver://db2"), TLS: config.TLS{Enabled: true}}, "db2"},
		"ado instance":        {config.SQLServer{DSN: config.NewSecret(`server=db3\INST`), TLS: config.TLS{Enabled: true}}, "db3"},
		"host beats dsn":      {config.SQLServer{DSN: config.NewSecret("sqlserver://db2"), Host: "db4", TLS: config.TLS{Enabled: true}}, "db4"},
		"explicit beats all":  {config.SQLServer{DSN: config.NewSecret("sqlserver://db2?hostnameincertificate=x"), TLS: config.TLS{Enabled: true, ServerName: "y"}}, "y"},
		"dsn cert name kept":  {config.SQLServer{DSN: config.NewSecret("sqlserver://10.0.0.1?hostnameincertificate=db.example"), TLS: config.TLS{Enabled: true}}, "db.example"},
		"ipv6 without server": {config.SQLServer{Host: "::1", TLS: config.TLS{Enabled: true}}, "::1"},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := Config(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if mc.TLSConfig == nil || mc.TLSConfig.ServerName != tc.want {
				t.Errorf("tls = %+v, want server name %q", mc.TLSConfig, tc.want)
			}
		})
	}
}

func TestInvalidParamNames(t *testing.T) {
	// Names that would split or re-quote the ADO string must be rejected,
	// not silently turned into other keys.
	for _, k := range []string{"a=b", "a;b", `a"b`, " ", ""} {
		if _, err := Config(config.SQLServer{Host: "db", Params: map[string]string{k: "v"}}); err == nil {
			t.Errorf("param name %q accepted", k)
		}
	}
}

func TestSlogLoggerNilUsesDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	SlogLogger(nil).Log(nil, msdsn.LogErrors, "login failed") //nolint:staticcheck // the driver may pass a nil ctx
	if !strings.Contains(buf.String(), `level=ERROR msg="login failed" category=errors`) {
		t.Errorf("default logger output: %s", buf.String())
	}
}
