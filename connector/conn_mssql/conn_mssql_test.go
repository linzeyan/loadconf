package conn_mssql

import (
	"context"
	"strings"
	"testing"
	"time"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/msdsn"

	"github.com/linzeyan/loadconf/config"
)

func TestConfigMapping(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg   config.SQLServer
		check func(t *testing.T, mc msdsn.Config)
	}{
		"fields": {
			config.SQLServer{Host: "db1", Port: 1444, User: "app", Password: config.NewSecret(`p;w"d= x`), Database: "erp", Params: map[string]string{"app name": "svc"}},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "db1" || mc.Port != 1444 || mc.Instance != "" || mc.User != "app" || mc.Password != `p;w"d= x` ||
					mc.Database != "erp" || mc.AppName != "svc" {
					t.Errorf("config = %+v", mc)
				}
			},
		},
		"default port": {
			config.SQLServer{Host: "db1"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Port != 1433 || mc.AppName != "go-mssqldb" {
					t.Errorf("port = %d app = %q", mc.Port, mc.AppName)
				}
			},
		},
		"instance drops port": {
			config.SQLServer{Host: "db1", Port: 1433, Instance: "SQLEXPRESS"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "db1" || mc.Instance != "SQLEXPRESS" || mc.Port != 0 {
					t.Errorf("host = %q instance = %q port = %d", mc.Host, mc.Instance, mc.Port)
				}
			},
		},
		"url dsn": {
			config.SQLServer{DSN: config.NewSecret("sqlserver://u:old@h1:1500?database=a&app+name=x"), Password: config.NewSecret("new"), Database: "b"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "h1" || mc.Port != 1500 || mc.User != "u" || mc.Password != "new" || mc.Database != "b" || mc.AppName != "x" {
					t.Errorf("config = %+v", mc)
				}
			},
		},
		"url dsn instance override": {
			config.SQLServer{DSN: config.NewSecret("sqlserver://h1:1500/OLD?database=a"), Instance: "NEW"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "h1" || mc.Instance != "NEW" || mc.Port != 0 {
					t.Errorf("host = %q instance = %q port = %d", mc.Host, mc.Instance, mc.Port)
				}
			},
		},
		"ado dsn host override": {
			config.SQLServer{DSN: config.NewSecret(`server=h1\INST;user id=u;database=a`), Host: "h2", Port: 1444},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "h2" || mc.Instance != "" || mc.Port != 1444 || mc.User != "u" || mc.Database != "a" {
					t.Errorf("config = %+v", mc)
				}
			},
		},
		"odbc dsn": {
			config.SQLServer{DSN: config.NewSecret("odbc:server=h1;uid=u;pwd={a;b}")},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Host != "h1" || mc.User != "u" || mc.Password != "a;b" {
					t.Errorf("config = %+v", mc)
				}
			},
		},
		"params interpreted": {
			config.SQLServer{Host: "h", Database: "a", Params: map[string]string{"Packet Size": "4096", "log": "3", "ApplicationIntent": "ReadOnly", "TimeZone": "Asia/Taipei"}},
			func(t *testing.T, mc msdsn.Config) {
				if mc.PacketSize != 4096 || mc.LogFlags != 3 || !mc.ReadOnlyIntent || mc.Encoding.Timezone.String() != "Asia/Taipei" {
					t.Errorf("packet = %d log = %d readonly = %v tz = %v", mc.PacketSize, mc.LogFlags, mc.ReadOnlyIntent, mc.Encoding.Timezone)
				}
			},
		},
		"precedence dsn < params < fields": {
			config.SQLServer{DSN: config.NewSecret("server=h;database=a;app name=x"), Params: map[string]string{"Initial Catalog": "b", "app name": "y"}, Database: "c"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.Database != "c" || mc.AppName != "y" {
					t.Errorf("database = %q app = %q", mc.Database, mc.AppName)
				}
			},
		},
		"timeouts": {
			config.SQLServer{Host: "h", Params: map[string]string{"dial timeout": "3", "keepAlive": "10"}},
			func(t *testing.T, mc msdsn.Config) {
				if mc.DialTimeout != 3*time.Second || mc.KeepAlive != 10*time.Second || mc.ConnTimeout != 0 {
					t.Errorf("dial = %v keepalive = %v conn = %v", mc.DialTimeout, mc.KeepAlive, mc.ConnTimeout)
				}
			},
		},
		"driver timeouts by default": {
			config.SQLServer{Host: "h"},
			func(t *testing.T, mc msdsn.Config) {
				if mc.DialTimeout != 15*time.Second || mc.KeepAlive != 30*time.Second {
					t.Errorf("dial = %v keepalive = %v", mc.DialTimeout, mc.KeepAlive)
				}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := Config(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, mc)
		})
	}
}

func TestConfigEncrypt(t *testing.T) {
	for _, tc := range []struct {
		encrypt string
		want    msdsn.Encryption
		tls     bool
	}{
		{"", msdsn.EncryptionOff, true},
		{"disable", msdsn.EncryptionDisabled, false},
		{"DISABLE", msdsn.EncryptionDisabled, false},
		{"false", msdsn.EncryptionOff, true},
		{"optional", msdsn.EncryptionOff, true},
		{"true", msdsn.EncryptionRequired, true},
		{"mandatory", msdsn.EncryptionRequired, true},
		{"strict", msdsn.EncryptionStrict, true},
	} {
		t.Run(tc.encrypt, func(t *testing.T) {
			cfg := config.SQLServer{Host: "db"}
			if tc.encrypt != "" {
				cfg.Params = map[string]string{"encrypt": tc.encrypt}
			}
			mc, err := Config(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if mc.Encryption != tc.want || (mc.TLSConfig != nil) != tc.tls {
				t.Errorf("encryption = %v tls = %v", mc.Encryption, mc.TLSConfig != nil)
			}
			if mc.TLSConfig != nil && mc.TLSConfig.ServerName != "db" {
				t.Errorf("server name = %q", mc.TLSConfig.ServerName)
			}
			// Without an explicit encrypt the driver trusts the certificate.
			if want := tc.encrypt == ""; mc.TrustServerCertificate != want {
				t.Errorf("trust server certificate = %v", mc.TrustServerCertificate)
			}
		})
	}
}

func TestConfigTLS(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg         config.SQLServer
		encryption  msdsn.Encryption
		serverName  string
		hostInCert  bool
		trustServer bool
	}{
		"implies encrypt true": {
			config.SQLServer{Host: "db", TLS: config.TLS{Enabled: true, MinVersion: "1.3"}},
			msdsn.EncryptionRequired, "db", false, false,
		},
		"explicit server name": {
			config.SQLServer{Host: "10.0.0.1", TLS: config.TLS{Enabled: true, ServerName: "db.example"}},
			msdsn.EncryptionRequired, "db.example", true, false,
		},
		"hostnameincertificate param": {
			config.SQLServer{Host: "10.0.0.1", Params: map[string]string{"HostNameInCertificate": "db.example"}, TLS: config.TLS{Enabled: true}},
			msdsn.EncryptionRequired, "db.example", true, false,
		},
		"strict kept": {
			config.SQLServer{Host: "db", Params: map[string]string{"Encrypt": "strict"}, TLS: config.TLS{Enabled: true}},
			msdsn.EncryptionStrict, "db", false, false,
		},
		"dsn encrypt kept": {
			config.SQLServer{DSN: config.NewSecret("sqlserver://db?encrypt=strict"), TLS: config.TLS{Enabled: true}},
			msdsn.EncryptionStrict, "db", false, false,
		},
		"insecure replaces trust param": {
			config.SQLServer{Host: "db", Params: map[string]string{"TrustServerCertificate": "false"}, TLS: config.TLS{Enabled: true, InsecureSkipVerify: true}},
			msdsn.EncryptionRequired, "db", false, true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			mc, err := Config(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			tc2 := mc.TLSConfig
			if mc.Encryption != tc.encryption || tc2 == nil || tc2.ServerName != tc.serverName || !tc2.DynamicRecordSizingDisabled ||
				mc.HostInCertificateProvided != tc.hostInCert || tc2.InsecureSkipVerify != tc.trustServer || mc.TrustServerCertificate != tc.trustServer {
				t.Errorf("encryption = %v hostInCert = %v trust = %v tls = %+v", mc.Encryption, mc.HostInCertificateProvided, mc.TrustServerCertificate, tc2)
			}
			if tc.cfg.TLS.MinVersion == "1.3" && tc2.MinVersion != 0x0304 {
				t.Errorf("min version = %x", tc2.MinVersion)
			}
		})
	}
}

func TestConfigErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.SQLServer
		want string
	}{
		"empty":               {config.SQLServer{}, "either dsn or host is required"},
		"bad encrypt":         {config.SQLServer{Host: "h", Params: map[string]string{"encrypt": "maybe"}}, "encrypt"},
		"tls with disable":    {config.SQLServer{Host: "h", Params: map[string]string{"encrypt": "disable"}, TLS: config.TLS{Enabled: true}}, "contradicts"},
		"tls missing ca":      {config.SQLServer{Host: "h", TLS: config.TLS{Enabled: true, CAFile: "/nonexistent/ca.pem"}}, "ca_file"},
		"bad dsn":             {config.SQLServer{DSN: config.NewSecret("sqlserver://u:secret@h?encrypt=bogus")}, "sqlserver dsn"},
		"bad dsn url":         {config.SQLServer{DSN: config.NewSecret("sqlserver://u:secret@h:port")}, "sqlserver dsn"},
		"bad param value":     {config.SQLServer{Host: "h", Password: config.NewSecret("secret"), Params: map[string]string{"packet size": "big"}}, "packet size"},
		"bad param name":      {config.SQLServer{Host: "h", Params: map[string]string{"a=b": "c"}}, "invalid parameter name"},
		"readonly without db": {config.SQLServer{Host: "h", Params: map[string]string{"applicationintent": "ReadOnly"}}, "database must be specified"},
		"negative pool":       {config.SQLServer{Host: "h", SQLPool: config.SQLPool{MaxOpenConns: -1}}, "must not be negative"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Config(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "secret") {
				t.Errorf("got %v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewAppliesPoolWithoutConnecting(t *testing.T) {
	var sawConfig, sawConnector bool
	db, err := New(config.SQLServer{Host: "db", SQLPool: config.SQLPool{MaxOpenConns: 7}},
		WithConfig(func(mc *msdsn.Config) { sawConfig = mc.Host == "db" }),
		WithConnector(func(c *mssql.Connector) { sawConnector = true; c.SessionInitSQL = "SET XACT_ABORT ON" }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Stats().MaxOpenConnections != 7 || !sawConfig || !sawConnector {
		t.Errorf("max open = %d, hooks = %v %v", db.Stats().MaxOpenConnections, sawConfig, sawConnector)
	}

	c, err := Connector(config.SQLServer{Host: "db"}, WithConnector(func(c *mssql.Connector) { c.SessionInitSQL = "SET NOCOUNT ON" }))
	if err != nil || c.SessionInitSQL != "SET NOCOUNT ON" {
		t.Errorf("connector = %+v, %v", c, err)
	}
	if _, err := Open(context.Background(), config.SQLServer{}); err == nil {
		t.Error("expected validation failure")
	}
}

func TestOpenPingFailure(t *testing.T) {
	_, err := Open(context.Background(), config.SQLServer{Host: "127.0.0.1", Port: 1, Database: "erp", Password: config.NewSecret("secret"), PingTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "ping sqlserver 127.0.0.1:1/erp") || strings.Contains(err.Error(), "secret") {
		t.Errorf("got %v", err)
	}
}
