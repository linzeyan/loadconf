package conn_oracle

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/configurations"

	"github.com/linzeyan/loadconf/config"
)

func parse(t *testing.T, cfg config.Oracle) *configurations.ConnectionConfig {
	t.Helper()
	dsn, err := URL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := go_ora.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("go-ora rejects %s: %v", dsn, err)
	}
	return c
}

func TestURLFields(t *testing.T) {
	c := parse(t, config.Oracle{
		Host: "ora1", Port: 1522, Service: "ORCLPDB1", User: "app", Password: config.NewSecret("p@ss/w:rd?"),
		Params: map[string]string{"PREFETCH_ROWS": "500", "TRACE FILE": "/tmp/trace.log", "CONNECTION TIMEOUT": "2"},
	})
	if len(c.Servers) != 1 || c.Servers[0].Addr != "ora1" || c.Servers[0].Port != 1522 || c.ServiceName != "ORCLPDB1" {
		t.Errorf("server = %+v service = %q", c.Servers, c.ServiceName)
	}
	if c.UserID != "app" || c.Password != "p@ss/w:rd?" {
		t.Errorf("user = %q password = %q", c.UserID, c.Password)
	}
	if c.ConnectTimeout != 2*time.Second || c.TraceFilePath != "/tmp/trace.log" || c.SSL {
		t.Errorf("connect timeout = %v trace = %q ssl = %v", c.ConnectTimeout, c.TraceFilePath, c.SSL)
	}
}

func TestURLDefaultsAndSID(t *testing.T) {
	c := parse(t, config.Oracle{Host: "ora1", SID: "ORCL", User: "u"})
	if c.Servers[0].Port != 1521 || c.SID != "ORCL" || c.ServiceName != "" {
		t.Errorf("port = %d sid = %q service = %q", c.Servers[0].Port, c.SID, c.ServiceName)
	}
}

func TestURLOverridesDSN(t *testing.T) {
	c := parse(t, config.Oracle{
		DSN:      config.NewSecret("oracle://old:secret@h1:1600/OLD?SID=X&ssl=false&PREFETCH_ROWS=10"),
		Service:  "NEW",
		User:     "app",
		Params:   map[string]string{"prefetch_rows": "99"},
		TLS:      config.TLS{Enabled: true},
		Password: config.NewSecret(""),
	})
	if c.Servers[0].Addr != "h1" || c.Servers[0].Port != 1600 || c.ServiceName != "NEW" || c.SID != "" {
		t.Errorf("server = %+v service = %q sid = %q", c.Servers, c.ServiceName, c.SID)
	}
	if c.UserID != "app" || c.Password != "secret" {
		t.Errorf("user = %q password = %q", c.UserID, c.Password)
	}
	if !c.SSL || !c.SSLVerify {
		t.Errorf("ssl = %v verify = %v", c.SSL, c.SSLVerify)
	}
	dsn, _ := URL(config.Oracle{DSN: config.NewSecret("oracle://u:p@h:1/s?PREFETCH_ROWS=10"), Params: map[string]string{"prefetch_rows": "99"}})
	if strings.Count(strings.ToUpper(dsn), "PREFETCH_ROWS") != 1 || !strings.Contains(dsn, "99") {
		t.Errorf("params should replace dsn options case-insensitively: %s", dsn)
	}
}

func TestURLConnectString(t *testing.T) {
	desc := "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=rac1)(PORT=1521))(ADDRESS=(PROTOCOL=TCP)(HOST=rac2)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=svc)))"
	c := parse(t, config.Oracle{ConnectString: desc, User: "u", Password: config.NewSecret("p"), TLS: config.TLS{Enabled: true, InsecureSkipVerify: true}})
	if len(c.Servers) != 2 || c.Servers[1].Addr != "rac2" || c.ServiceName != "svc" {
		t.Errorf("servers = %+v service = %q", c.Servers, c.ServiceName)
	}
	if !c.SSL || c.SSLVerify {
		t.Errorf("ssl = %v verify = %v", c.SSL, c.SSLVerify)
	}
}

func TestURLErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Oracle
		want string
	}{
		"nothing":   {config.Oracle{}, "one of dsn, connect_string or host is required"},
		"scheme":    {config.Oracle{DSN: config.NewSecret("mysql://u:p@h/db")}, "must start with oracle://"},
		"bad dsn":   {config.Oracle{DSN: config.NewSecret("oracle://u:hunter2@h:bad port/s")}, "oracle dsn"},
		"sid+svc":   {config.Oracle{Host: "h", Service: "s", SID: "x"}, "mutually exclusive"},
		"pool":      {config.Oracle{Host: "h", SQLPool: config.SQLPool{MaxOpenConns: 1, MaxIdleConns: 2}}, "max_idle_conns"},
		"tls files": {config.Oracle{Host: "h", TLS: config.TLS{Enabled: true, CertFile: "c.pem"}}, "cert_file and key_file"},
	} {
		_, err := URL(tc.cfg)
		if err == nil {
			_, err = Connector(tc.cfg)
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
		if err != nil && strings.Contains(err.Error(), "hunter2") {
			t.Errorf("%s: error leaks the password: %v", name, err)
		}
	}
	if _, err := Connector(config.Oracle{Host: "h", TLS: config.TLS{Enabled: true, ServerName: "db"}}); err == nil ||
		!strings.Contains(err.Error(), "server_name is not supported") {
		t.Errorf("server_name: %v", err)
	}
}

func TestConnectorTLSAndHooks(t *testing.T) {
	c, err := newConnector(config.Oracle{Host: "h", TLS: config.TLS{Enabled: true}}, options{})
	if err != nil || c.tls != nil {
		t.Fatalf("without files go-ora's TLS setup applies: %+v %v", c, err)
	}
	var hooked int
	c, err = newConnector(config.Oracle{Host: "127.0.0.1", Port: 1}, options{connector: []func(*go_ora.OracleConnector){
		func(*go_ora.OracleConnector) { hooked++ },
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Connect(context.Background()); err == nil {
		t.Fatal("connecting to a closed port should fail")
	}
	if hooked != 1 || c.Driver() != c.drv {
		t.Errorf("hooked = %d", hooked)
	}
}

func TestOpen(t *testing.T) {
	db, err := New(config.Oracle{Host: "h", SQLPool: config.SQLPool{MaxOpenConns: 7}})
	if err != nil {
		t.Fatal(err)
	}
	if db.Stats().MaxOpenConnections != 7 {
		t.Errorf("max open = %d", db.Stats().MaxOpenConnections)
	}
	_ = db.Close()

	// A listener that accepts and immediately closes fails the handshake fast.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	_, err = Open(context.Background(), config.Oracle{Host: "127.0.0.1", Port: port, Service: "s", User: "u", Password: config.NewSecret("hunter2"), PingTimeout: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "ping oracle 127.0.0.1") || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("ping error = %v", err)
	}
}
