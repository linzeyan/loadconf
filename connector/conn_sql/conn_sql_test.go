package conn_sql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/linzeyan/loadconf/config"
)

func TestMySQLConfigFromFields(t *testing.T) {
	mc, err := MySQLConfig(config.MySQL{
		Host:     "db1",
		Port:     3307,
		User:     "app",
		Password: config.NewSecret("p@ss/word:x"),
		Database: "orders",
		Params:   map[string]string{"parseTime": "true", "loc": "Asia/Taipei", "charset": "utf8mb4", "timeout": "2s"},
		TLS:      config.TLS{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Addr != "db1:3307" || mc.User != "app" || mc.Passwd != "p@ss/word:x" || mc.DBName != "orders" || mc.Timeout != 2*time.Second {
		t.Errorf("config = %+v", mc)
	}
	if !mc.ParseTime || mc.Loc.String() != "Asia/Taipei" || !strings.Contains(mc.FormatDSN(), "charset=utf8mb4") {
		t.Errorf("params not interpreted: parseTime=%v loc=%v params=%v", mc.ParseTime, mc.Loc, mc.Params)
	}
	if mc.TLS == nil || mc.TLS.ServerName != "db1" {
		t.Errorf("tls = %+v", mc.TLS)
	}
}

func TestMySQLConfigDSNOverride(t *testing.T) {
	mc, err := MySQLConfig(config.MySQL{
		DSN:      config.NewSecret("app@tcp(db2:3306)/core?parseTime=true"),
		Password: config.NewSecret("from-secret"),
		Params:   map[string]string{"timeout": "5s"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if mc.Addr != "db2:3306" || mc.Passwd != "from-secret" || mc.DBName != "core" || !mc.ParseTime || mc.Timeout != 5*time.Second {
		t.Errorf("config = %+v", mc)
	}
	if _, err := MySQLConfig(config.MySQL{DSN: config.NewSecret("no-slash")}); err == nil {
		t.Error("bad dsn should fail")
	}
}

func TestPostgresConnString(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config.Postgres
		want []string
	}{
		"fields": {
			config.Postgres{Host: "pg", Port: 5433, User: "app", Password: config.NewSecret("p w@/"), Database: "main", Params: map[string]string{"sslmode": "require", "search_path": "app"}},
			[]string{"postgres://?host=pg&port=5433&user=app&password=p%20w%40%2F&dbname=main&", "sslmode=require", "search_path=app"},
		},
		// Query parameters override the DSN's userinfo and path, so the DSN
		// is kept as written and never re-parsed with net/url.
		"url dsn": {
			config.Postgres{DSN: config.NewSecret("postgres://u:old@h:5432/db?sslmode=disable"), Password: config.NewSecret("new"), Database: "other"},
			[]string{"postgres://u:old@h:5432/db?sslmode=disable&password=new&dbname=other"},
		},
		"keyword dsn": {
			config.Postgres{DSN: config.NewSecret("host=h user=u dbname=db"), Password: config.NewSecret("it's"), Params: map[string]string{"application_name": "svc"}},
			[]string{"host=h user=u dbname=db", `password='it\'s'`, "application_name='svc'"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := PostgresConnString(tc.cfg)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range tc.want {
				if !strings.Contains(got, want) {
					t.Errorf("%q missing %q", got, want)
				}
			}
		})
	}
}

func TestPostgresConfig(t *testing.T) {
	cc, err := PostgresConfig(config.Postgres{
		DSN:      config.NewSecret("host=h user=u dbname=db"),
		Password: config.NewSecret("it's"),
		Params:   map[string]string{"application_name": "svc", "search_path": "app,public", "connect_timeout": "3"},
		TLS:      config.TLS{Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cc.Host != "h" || cc.Port != 5432 || cc.User != "u" || cc.Password != "it's" || cc.Database != "db" || cc.ConnectTimeout != 3*time.Second {
		t.Errorf("config = %+v", cc.Config)
	}
	if cc.RuntimeParams["application_name"] != "svc" || cc.RuntimeParams["search_path"] != "app,public" {
		t.Errorf("runtime params = %v", cc.RuntimeParams)
	}
	if cc.TLSConfig == nil || cc.TLSConfig.ServerName != "h" || cc.Fallbacks != nil {
		t.Errorf("tls = %+v fallbacks = %v", cc.TLSConfig, cc.Fallbacks)
	}
}

func TestNewAppliesPoolWithoutConnecting(t *testing.T) {
	cfg := config.MySQL{Host: "db", SQLPool: config.SQLPool{MaxOpenConns: 7}}
	var called bool
	db, err := NewMySQL(cfg, WithMySQLConfig(func(*mysql.Config) { called = true }))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Stats().MaxOpenConnections != 7 || !called {
		t.Errorf("max open = %d, hook called = %v", db.Stats().MaxOpenConnections, called)
	}

	pg, err := NewPostgres(config.Postgres{Host: "pg", SQLPool: config.SQLPool{MaxOpenConns: 3}})
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	if pg.Stats().MaxOpenConnections != 3 {
		t.Errorf("pg max open = %d", pg.Stats().MaxOpenConnections)
	}
}

func TestOpenPingFailure(t *testing.T) {
	_, err := OpenMySQL(context.Background(), config.MySQL{Host: "127.0.0.1", Port: 1, Password: config.NewSecret("secret"), PingTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "ping mysql 127.0.0.1:1") || strings.Contains(err.Error(), "secret") {
		t.Errorf("got %v", err)
	}
	_, err = OpenPostgres(context.Background(), config.Postgres{Host: "127.0.0.1", Port: 1, Password: config.NewSecret("secret"), PingTimeout: time.Second})
	if err == nil || !strings.Contains(err.Error(), "ping postgres 127.0.0.1:1") || strings.Contains(err.Error(), "secret") {
		t.Errorf("got %v", err)
	}
}
