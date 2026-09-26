package conn_sql

import (
	"bytes"
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds strings that are delimiters in at least one of the DSN
// grammars (URL, go-sql-driver, libpq keyword/value). Credentials and names
// taken from a secret store may contain any of them.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "it's", `say "hi"`,
	`back\slash`, `trailing\`, "100%", "%zz", "a;b", "has space", " lead", "trail ",
	"密碼🔑", "x' host=evil", "x&sslmode=disable", `x\' host=evil`, "a@b:c/d?e#f",
}

func TestMySQLConfigAdversarialCredentials(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			mc, err := MySQLConfig(config.MySQL{Host: "db", User: "app", Password: config.NewSecret(s), Database: s})
			if err != nil {
				t.Fatal(err)
			}
			// Fields are assigned after parsing, so no character can be
			// reinterpreted as DSN syntax.
			if mc.User != "app" || mc.Passwd != s || mc.DBName != s || mc.Addr != "db:3306" {
				t.Fatalf("config = user %q passwd %q db %q addr %q", mc.User, mc.Passwd, mc.DBName, mc.Addr)
			}
			// The driver config must also survive its own DSN form, which is
			// what a caller logging or re-opening mc.FormatDSN() relies on.
			back, err := mysql.ParseDSN(mc.FormatDSN())
			if err != nil {
				t.Fatalf("reparse %q: %v", mc.FormatDSN(), err)
			}
			if back.Passwd != s || back.DBName != s || back.Addr != "db:3306" {
				t.Errorf("round trip = passwd %q db %q addr %q", back.Passwd, back.DBName, back.Addr)
			}
		})
	}
}

func TestMySQLConfigParamValueCannotInject(t *testing.T) {
	// A param value holding '&' and '=' must stay one value, not become a
	// second driver option that turns on parseTime or multiStatements.
	mc, err := MySQLConfig(config.MySQL{
		Host:   "db",
		Params: map[string]string{"time_zone": "'+00:00'&multiStatements=true&parseTime=true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.MultiStatements || mc.ParseTime {
		t.Errorf("param value injected options: multiStatements=%v parseTime=%v", mc.MultiStatements, mc.ParseTime)
	}
	if got := mc.Params["time_zone"]; got != "'+00:00'&multiStatements=true&parseTime=true" {
		t.Errorf("time_zone = %q", got)
	}
}

func TestMySQLConfigHosts(t *testing.T) {
	for name, tc := range map[string]struct {
		host     string
		port     int
		wantAddr string
		wantSNI  string
	}{
		"default port": {"db", 0, "db:3306", "db"},
		"max port":     {"db", 65535, "db:65535", "db"},
		"ipv6":         {"::1", 3307, "[::1]:3307", "::1"},
		"ipv6 zone":    {"fe80::1%eth0", 0, "[fe80::1%eth0]:3306", "fe80::1%eth0"},
		"unicode":      {"資料庫.example", 0, "資料庫.example:3306", "資料庫.example"},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := MySQLConfig(config.MySQL{Host: tc.host, Port: tc.port, TLS: config.TLS{Enabled: true}})
			if err != nil {
				t.Fatal(err)
			}
			if mc.Net != "tcp" || mc.Addr != tc.wantAddr {
				t.Errorf("net %q addr %q, want tcp %q", mc.Net, mc.Addr, tc.wantAddr)
			}
			// The TLS server name must be the bare host: a bracketed IPv6
			// literal or host:port never matches a certificate SAN.
			if mc.TLS == nil || mc.TLS.ServerName != tc.wantSNI {
				t.Errorf("tls = %+v, want server name %q", mc.TLS, tc.wantSNI)
			}
		})
	}
}

func TestMySQLConfigTLSServerName(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.MySQL
		want string
	}{
		"from dsn addr":      {config.MySQL{DSN: config.NewSecret("u@tcp(db2:3306)/x"), TLS: config.TLS{Enabled: true}}, "db2"},
		"from dsn ipv6 addr": {config.MySQL{DSN: config.NewSecret("u@tcp([::1]:3306)/x"), TLS: config.TLS{Enabled: true}}, "::1"},
		"host beats dsn":     {config.MySQL{DSN: config.NewSecret("u@tcp(db2:3306)/x"), Host: "db3", TLS: config.TLS{Enabled: true}}, "db3"},
		"explicit wins":      {config.MySQL{Host: "10.0.0.5", TLS: config.TLS{Enabled: true, ServerName: "db.internal"}}, "db.internal"},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := MySQLConfig(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if mc.TLS == nil || mc.TLS.ServerName != tc.want {
				t.Errorf("server name = %v, want %q", mc.TLS, tc.want)
			}
		})
	}

	// A broken TLS section must fail instead of silently connecting in
	// plaintext.
	if _, err := MySQLConfig(config.MySQL{Host: "db", TLS: config.TLS{Enabled: true, CertFile: "c.pem"}}); err == nil {
		t.Error("cert_file without key_file should fail")
	}
}

func TestMySQLConfigDSNEveryFieldOverridden(t *testing.T) {
	mc, err := MySQLConfig(config.MySQL{
		DSN:      config.NewSecret("old:oldpw@unix(/tmp/mysql.sock)/olddb?parseTime=true&timeout=1s"),
		Host:     "new",
		Port:     3310,
		User:     "newuser",
		Password: config.NewSecret("newpw"),
		Database: "newdb",
		Params:   map[string]string{"timeout": "7s", "loc": "UTC"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Net != "tcp" || mc.Addr != "new:3310" || mc.User != "newuser" || mc.Passwd != "newpw" || mc.DBName != "newdb" {
		t.Errorf("fields not overridden: %+v", mc)
	}
	// DSN params the config does not mention survive; Params appended after
	// the DSN's own win.
	if !mc.ParseTime || mc.Timeout.String() != "7s" || mc.Loc.String() != "UTC" {
		t.Errorf("params: parseTime=%v timeout=%v loc=%v", mc.ParseTime, mc.Timeout, mc.Loc)
	}
}

func TestMySQLConfigErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.MySQL{
		"bad dsn param":  {DSN: config.NewSecret("u:" + pw + "@tcp(h:3306)/db?parseTime=maybe")},
		"bad param":      {DSN: config.NewSecret("u:" + pw + "@tcp(h:3306)/db"), Params: map[string]string{"timeout": "soon"}},
		"bad addr":       {DSN: config.NewSecret("u:" + pw + "@tcp(h:3306/db")},
		"bad db escape":  {DSN: config.NewSecret("u:" + pw + "@tcp(h:3306)/%zz")},
		"no slash":       {DSN: config.NewSecret("u:" + pw + "@tcp(h:3306)")},
		"field password": {Host: "h", Password: config.NewSecret(pw), Params: map[string]string{"readTimeout": "x"}},
		"bad tls":        {Host: "h", Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := MySQLConfig(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

// TestMySQLParamsAppliedWhenDSNPasswordHasQuestionMark: go-sql-driver only
// starts the query at the first '?' after the last '/', so a '?' in the DSN's
// password must not make Params look like a continuation ("&k=v"); they
// would become part of the database name and be silently dropped.
func TestMySQLParamsAppliedWhenDSNPasswordHasQuestionMark(t *testing.T) {
	mc, err := MySQLConfig(config.MySQL{
		DSN:    config.NewSecret("app:pa?ss@tcp(db:3306)/core"),
		Params: map[string]string{"parseTime": "true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Passwd != "pa?ss" || mc.DBName != "core" || !mc.ParseTime {
		t.Errorf("passwd %q db %q parseTime %v, want pa?ss core true", mc.Passwd, mc.DBName, mc.ParseTime)
	}
}

// TestMySQLCharsetListKeepsCommas: go-sql-driver does not unescape charset
// (nor the bool, duration and collation params); it splits the raw value on
// ','. The documented fallback list "utf8mb4,utf8" must reach it with literal
// commas, or it becomes the single charset "utf8mb4%2Cutf8" and every connect
// fails on SET NAMES.
func TestMySQLCharsetListKeepsCommas(t *testing.T) {
	mc, err := MySQLConfig(config.MySQL{Host: "db", Params: map[string]string{"charset": "utf8mb4,utf8"}})
	if err != nil {
		t.Fatal(err)
	}
	if dsn := mc.FormatDSN(); !strings.Contains(dsn, "charset=utf8mb4,utf8") {
		t.Errorf("dsn = %q, want charset=utf8mb4,utf8", dsn)
	}
}

// pgForms builds the same settings once as a URL (no DSN) and once in the
// keyword/value form, so every property is checked against both grammars.
func pgForms(cfg config.Postgres) map[string]config.Postgres {
	kv := cfg
	kv.DSN = config.NewSecret("sslmode=disable")
	url := cfg
	url.DSN = config.NewSecret("")
	if url.Params == nil {
		url.Params = map[string]string{}
	} else {
		url.Params = maps.Clone(url.Params)
	}
	url.Params["sslmode"] = "disable"
	return map[string]config.Postgres{"url": url, "kv": kv}
}

func TestPostgresAdversarialRoundTrip(t *testing.T) {
	for _, s := range nasty {
		for form, cfg := range pgForms(config.Postgres{
			Host: "pg", User: s, Password: config.NewSecret(s), Database: s,
			Params: map[string]string{"application_name": s},
		}) {
			t.Run(form+"/"+s, func(t *testing.T) {
				cc, err := PostgresConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if cc.Host != "pg" || cc.Port != 5432 || cc.User != s || cc.Password != s || cc.Database != s {
					t.Errorf("host %q port %d user %q password %q db %q", cc.Host, cc.Port, cc.User, cc.Password, cc.Database)
				}
				// Nothing but the one runtime param may appear: an extra key
				// would mean a value broke out of its quoting.
				if keys := slices.Sorted(maps.Keys(cc.RuntimeParams)); !slices.Equal(keys, []string{"application_name"}) {
					t.Errorf("runtime params = %q", cc.RuntimeParams)
				}
				if got := cc.RuntimeParams["application_name"]; got != s {
					t.Errorf("application_name = %q, want %q", got, s)
				}
				if cc.TLSConfig != nil {
					t.Errorf("sslmode=disable was overridden: tls = %+v", cc.TLSConfig)
				}
			})
		}
	}
}

func TestPostgresPasswordCannotInject(t *testing.T) {
	for _, pw := range []string{
		"x' host=evil sslmode='disable",
		`x\' host=evil sslmode=disable`,
		"x&host=evil&sslmode=disable",
		"x@evil:1/db?sslmode=disable",
		"x host=evil",
	} {
		dsns := map[string]string{"url": "postgres://app@pg:5432/main?sslmode=require", "kv": "host=pg user=app dbname=main sslmode=require"}
		for form, dsn := range dsns {
			t.Run(form+"/"+pw, func(t *testing.T) {
				cc, err := PostgresConfig(config.Postgres{DSN: config.NewSecret(dsn), Password: config.NewSecret(pw)})
				if err != nil {
					t.Fatal(err)
				}
				if cc.Host != "pg" || cc.Password != pw || cc.Database != "main" || cc.TLSConfig == nil {
					t.Errorf("host %q password %q db %q tls %v", cc.Host, cc.Password, cc.Database, cc.TLSConfig != nil)
				}
			})
		}
	}
}

func TestPostgresHostsAndPorts(t *testing.T) {
	for name, tc := range map[string]struct {
		host     string
		port     int
		wantHost string
		wantPort uint16
	}{
		"default port": {"pg", 0, "pg", 5432},
		"max port":     {"pg", 65535, "pg", 65535},
		"ipv6":         {"::1", 5433, "::1", 5433},
	} {
		for form, cfg := range pgForms(config.Postgres{Host: tc.host, Port: tc.port, TLS: config.TLS{Enabled: true}}) {
			t.Run(name+"/"+form, func(t *testing.T) {
				cc, err := PostgresConfig(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if cc.Host != tc.wantHost || cc.Port != tc.wantPort {
					t.Errorf("host %q port %d, want %q %d", cc.Host, cc.Port, tc.wantHost, tc.wantPort)
				}
				// TLS replaces sslmode=disable and must verify the bare host.
				if cc.TLSConfig == nil || cc.TLSConfig.ServerName != tc.wantHost || cc.Fallbacks != nil {
					t.Errorf("tls = %+v fallbacks = %v", cc.TLSConfig, cc.Fallbacks)
				}
			})
		}
	}
}

func TestPostgresTLSExplicitServerName(t *testing.T) {
	for form, cfg := range pgForms(config.Postgres{Host: "10.0.0.5", TLS: config.TLS{Enabled: true, ServerName: "pg.internal"}}) {
		cc, err := PostgresConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if cc.TLSConfig == nil || cc.TLSConfig.ServerName != "pg.internal" || cc.Host != "10.0.0.5" {
			t.Errorf("%s: host %q tls %+v", form, cc.Host, cc.TLSConfig)
		}
	}
}

func TestPostgresErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Postgres{
		"url port out of range": {Host: "pg", Port: 65536, Password: config.NewSecret(pw)},
		"url negative port":     {Host: "pg", Port: -1, Password: config.NewSecret(pw)},
		"url bad param":         {Host: "pg", Password: config.NewSecret(pw), Params: map[string]string{"connect_timeout": "soon"}},
		"url bad sslmode":       {Host: "pg", Password: config.NewSecret(pw), Params: map[string]string{"sslmode": "sometimes"}},
		"broken url dsn":        {DSN: config.NewSecret("postgres://u:" + pw + "@pg:port/db")},
		"kv port out of range":  {DSN: config.NewSecret("sslmode=disable"), Host: "pg", Port: 65536, Password: config.NewSecret(pw)},
		"kv bad param":          {DSN: config.NewSecret("host=pg"), Password: config.NewSecret(pw), Params: map[string]string{"connect_timeout": "soon"}},
		"kv bad tls":            {DSN: config.NewSecret("host=pg"), Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.1"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := PostgresConfig(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

// TestPostgresKVParseErrorHidesQuotedPassword: pgconn's keyword/value
// redaction is the regexp password='[^']*', which stops at the first quote,
// but quoteKV escapes a quote as \'. pgx's error text would print the tail of
// such a password (" a secret'"), so no part of the password may reach the
// returned error.
func TestPostgresKVParseErrorHidesQuotedPassword(t *testing.T) {
	_, err := PostgresConfig(config.Postgres{DSN: config.NewSecret("sslmode=disable"), Host: "pg", Port: 65536, Password: config.NewSecret("it's a secret")})
	if err == nil {
		t.Fatal("port 65536 should fail")
	}
	if strings.Contains(err.Error(), "a secret") {
		t.Errorf("error leaks password tail: %v", err)
	}
}

// TestPostgresURLParamSpaceKept: pgx follows libpq's URI rules, where '+' is
// a literal plus, so a space in a URL-form param must be written as %20 and a
// %20 already in the DSN must survive; otherwise options=-c
// statement_timeout=5s reaches the server as "-c+statement_timeout=5s" and
// the connection is refused.
func TestPostgresURLParamSpaceKept(t *testing.T) {
	for name, cfg := range map[string]config.Postgres{
		"param":     {Host: "pg", Params: map[string]string{"options": "-c statement_timeout=5s"}},
		"dsn query": {DSN: config.NewSecret("postgres://u@pg/db?options=-c%20statement_timeout%3D5s")},
	} {
		cc, err := PostgresConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if got := cc.RuntimeParams["options"]; got != "-c statement_timeout=5s" {
			t.Errorf("%s: options = %q, want %q", name, got, "-c statement_timeout=5s")
		}
	}
}

// TestPostgresFieldsBeatURLDSNQueryParams: config.Postgres promises that
// fields next to a DSN override the DSN. libpq URIs may carry host, port,
// user, password and dbname as query parameters (the usual form for Unix
// sockets: postgres:///db?host=/var/run/postgresql), and pgx lets those win
// over the authority, so the fields must beat them too; otherwise the client
// connects elsewhere with the old password.
func TestPostgresFieldsBeatURLDSNQueryParams(t *testing.T) {
	cc, err := PostgresConfig(config.Postgres{
		DSN:  config.NewSecret("postgres:///olddb?host=/var/run/postgresql&port=1&user=old&password=oldpw&dbname=olddb"),
		Host: "pg", Port: 6000, User: "nu", Password: config.NewSecret("new"), Database: "newdb",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cc.Host != "pg" || cc.Port != 6000 || cc.User != "nu" || cc.Password != "new" || cc.Database != "newdb" {
		t.Errorf("host %q port %d user %q password %q db %q", cc.Host, cc.Port, cc.User, cc.Password, cc.Database)
	}
}

// TestPostgresKVDSNDanglingValueKeepsOverrides: overrides are appended to a
// keyword/value DSN as text. libpq accepts a DSN whose last value is empty
// ("password=", a natural placeholder for a password from a secret store) or
// ends in a backslash; the first override must not become that value (libpq
// allows spaces around '=', and a trailing backslash escapes the separating
// space). Found by FuzzPostgresDSNOverride (DSN "0000000=").
func TestPostgresKVDSNDanglingValueKeepsOverrides(t *testing.T) {
	cc, err := PostgresConfig(config.Postgres{DSN: config.NewSecret("host=pg user=app password="), Password: config.NewSecret("secret")})
	if err != nil {
		t.Fatal(err)
	}
	if cc.Password != "secret" {
		t.Errorf("password = %q, want secret", cc.Password)
	}

	cc, err = PostgresConfig(config.Postgres{DSN: config.NewSecret(`sslmode=disable application_name=a\`), Host: "pg"})
	if err != nil {
		t.Fatal(err)
	}
	if cc.Host != "pg" || cc.RuntimeParams["application_name"] != "a" {
		t.Errorf("host %q application_name %q, want pg a", cc.Host, cc.RuntimeParams["application_name"])
	}
}

// TestPostgresURLDSNHashIsNotAFragment: pgx (like libpq) has no URI
// fragments and reads '#...' as part of the path, so a Database override
// must replace "db#frag" whole instead of gaining the old DSN's tail (as it
// did when the URL was edited with net/url). Found by
// FuzzPostgresDSNOverride.
func TestPostgresURLDSNHashIsNotAFragment(t *testing.T) {
	cc, err := PostgresConfig(config.Postgres{DSN: config.NewSecret("postgres://h/db#frag"), Database: "nd"})
	if err != nil {
		t.Fatal(err)
	}
	if cc.Database != "nd" {
		t.Errorf("database = %q, want nd", cc.Database)
	}
}

// TestPostgresURLDSNAcceptsLibpqURI: libpq URIs are not RFC 3986 URLs. pgx
// accepts a multi-host URI with IPv6 literals and an unescaped '#' in the
// password, which net/url rejects; a DSN that works with pgx directly must
// work through the connector too. Found by FuzzPostgresDSNOverride
// ("postgres:// 0").
func TestPostgresURLDSNAcceptsLibpqURI(t *testing.T) {
	for _, dsn := range []string{"postgres://[::1]:5432,[::2]:5433/db", "postgres://u:p#w@h/db"} {
		if _, err := pgx.ParseConfig(dsn); err != nil {
			t.Fatalf("pgx rejects %q itself: %v", dsn, err)
		}
		if _, err := PostgresConfig(config.Postgres{DSN: config.NewSecret(dsn)}); err != nil {
			t.Errorf("%q: %v", dsn, err)
		}
	}
}

func TestPostgresDSNEveryFieldOverridden(t *testing.T) {
	for name, dsn := range map[string]string{
		"url": "postgres://old:oldpw@oldhost:1/olddb?sslmode=disable&application_name=old&search_path=keep",
		"kv":  "host=oldhost port=1 user=old password=oldpw dbname=olddb sslmode=disable application_name=old search_path=keep",
	} {
		t.Run(name, func(t *testing.T) {
			cc, err := PostgresConfig(config.Postgres{
				DSN: config.NewSecret(dsn), Host: "new", Port: 6000, User: "nu", Password: config.NewSecret("np"), Database: "nd",
				Params: map[string]string{"application_name": "new"},
			})
			if err != nil {
				t.Fatal(err)
			}
			if cc.Host != "new" || cc.Port != 6000 || cc.User != "nu" || cc.Password != "np" || cc.Database != "nd" {
				t.Errorf("host %q port %d user %q password %q db %q", cc.Host, cc.Port, cc.User, cc.Password, cc.Database)
			}
			if cc.RuntimeParams["application_name"] != "new" || cc.RuntimeParams["search_path"] != "keep" {
				t.Errorf("runtime params = %v", cc.RuntimeParams)
			}
		})
	}
}

func TestPostgresURLDSNKeepsDSNPasswordWhenOnlyUserSet(t *testing.T) {
	// Overriding the user of a URL DSN must not drop the DSN's password, or
	// a DSN from a secret store plus a per-service user would fail auth.
	cc, err := PostgresConfig(config.Postgres{DSN: config.NewSecret("postgres://old:p%40ss@pg/db"), User: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if cc.User != "new" || cc.Password != "p@ss" {
		t.Errorf("user %q password %q", cc.User, cc.Password)
	}
}

func TestPostgresParamsWinInBothForms(t *testing.T) {
	// The same config must mean the same connection whatever form the DSN
	// has; a param named like an owned field overrides it in both.
	for form, cfg := range pgForms(config.Postgres{Host: "pg", User: "u", Password: config.NewSecret("field"), Params: map[string]string{"password": "param", "port": "6001"}}) {
		cc, err := PostgresConfig(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if cc.Password != "param" || cc.Port != 6001 {
			t.Errorf("%s: password %q port %d", form, cc.Password, cc.Port)
		}
	}
}

func TestDefaultDriverLoggersUseSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	mc, err := MySQLConfig(config.MySQL{Host: "db"})
	if err != nil {
		t.Fatal(err)
	}
	mc.Logger.Print("mysql-default-marker")

	cc, err := PostgresConfig(config.Postgres{Host: "pg"})
	if err != nil {
		t.Fatal(err)
	}
	tl, ok := cc.Tracer.(*tracelog.TraceLog)
	if !ok {
		t.Fatalf("tracer = %T", cc.Tracer)
	}
	// Warn keeps successful queries (with their arguments) out of the logs.
	if tl.LogLevel != tracelog.LogLevelWarn {
		t.Errorf("default tracer level = %v, want warn", tl.LogLevel)
	}
	tl.Logger.Log(context.Background(), tracelog.LogLevelError, "pgx-default-marker", nil)

	for _, want := range []string{"level=WARN msg=mysql-default-marker", "level=ERROR msg=pgx-default-marker"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("default logger missing %q:\n%s", want, buf.String())
		}
	}
}

func TestWithLoggerReplacesDefaults(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	o := buildOptions([]Option{WithLogger(l)})

	mc, err := MySQLConfig(config.MySQL{Host: "db"})
	if err != nil {
		t.Fatal(err)
	}
	cc, err := PostgresConfig(config.Postgres{Host: "pg"})
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range o.mysql {
		fn(mc)
	}
	for _, fn := range o.postgres {
		fn(cc)
	}
	mc.Logger.Print("to-l")
	cc.Tracer.(*tracelog.TraceLog).Logger.Log(context.Background(), tracelog.LogLevelError, "to-l-too", nil)
	if !strings.Contains(buf.String(), "to-l") || !strings.Contains(buf.String(), "to-l-too") {
		t.Errorf("WithLogger did not replace the defaults:\n%s", buf.String())
	}
}

func TestPgxTracerSkipsDisabledLevels(t *testing.T) {
	l, buf := jsonLogger(slog.LevelWarn)
	tl := PgxTracer(l, tracelog.LogLevelInfo)
	// pgx asks at Info for every query; the slog handler at Warn must still
	// win, so a lower handler level is never bypassed by the pgx threshold.
	tl.Logger.Log(context.Background(), tracelog.LogLevelInfo, "Query", map[string]any{"sql": "SELECT 1"})
	if buf.Len() != 0 {
		t.Errorf("info logged through a warn handler: %s", buf.String())
	}
	// An out-of-range pgx level (none, or a future one) must not be lost.
	tl.Logger.Log(context.Background(), tracelog.LogLevel(42), "odd", nil)
	if m := lastRecord(t, buf); m["level"] != "ERROR" {
		t.Errorf("unknown pgx level -> %v, want ERROR", m["level"])
	}
}

func TestApplyPoolZeroKeepsDefaults(t *testing.T) {
	db, err := NewMySQL(config.MySQL{Host: "db"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Zero means "driver default" (unlimited), never "zero connections".
	if got := db.Stats().MaxOpenConnections; got != 0 {
		t.Errorf("max open = %d, want 0 (unlimited)", got)
	}
	ApplyPool(db, config.SQLPool{MaxOpenConns: 5})
	ApplyPool(db, config.SQLPool{})
	if got := db.Stats().MaxOpenConnections; got != 5 {
		t.Errorf("zero pool reset max open to %d, want 5 kept", got)
	}
}

func TestPoolValidationRejectsBeforeParsing(t *testing.T) {
	for name, pool := range map[string]config.SQLPool{
		"negative":        {MaxOpenConns: -1},
		"idle above open": {MaxOpenConns: 2, MaxIdleConns: 3},
	} {
		if _, err := MySQLConfig(config.MySQL{Host: "db", SQLPool: pool}); err == nil {
			t.Errorf("mysql %s: want error", name)
		}
		if _, err := PostgresConfig(config.Postgres{Host: "pg", SQLPool: pool}); err == nil {
			t.Errorf("postgres %s: want error", name)
		}
	}
}
