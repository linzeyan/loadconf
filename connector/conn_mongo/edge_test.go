package conn_mongo

import (
	"bytes"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/linzeyan/loadconf/config"
)

func TestConnStringParamCaseVariants(t *testing.T) {
	// URI option names are case-insensitive for the driver; a param must
	// replace every spelling, or the driver would see the option twice.
	got, err := connString(config.Mongo{
		URI:    config.NewSecret("mongodb://h/?MaxPoolSize=5&maxpoolsize=6&appName=keep&REPLICASET=rs0"),
		Params: map[string]string{"maxPoolSize": "50", "replicaSet": "rs1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	q := parseQuery(t, got)
	if len(q) != 3 || q.Get("maxPoolSize") != "50" || q.Get("replicaSet") != "rs1" || q.Get("appName") != "keep" {
		t.Errorf("conn string = %s", got)
	}
}

func TestConnStringKeepsBaseAndRepeatedOptions(t *testing.T) {
	// The part before '?' holds credentials and hosts; it must be kept byte
	// for byte. readPreferenceTags may legally repeat.
	uri := "mongodb://us%40er:p%3Ass@h1:27017,[::1]:27018/db%2Fx?readPreferenceTags=dc:ny&readPreferenceTags=&readPreference=secondary"
	got, err := connString(config.Mongo{URI: config.NewSecret(uri), Params: map[string]string{"appName": "svc"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "mongodb://us%40er:p%3Ass@h1:27017,[::1]:27018/db%2Fx?") {
		t.Errorf("base changed: %s", got)
	}
	if tags := parseQuery(t, got)["readPreferenceTags"]; len(tags) != 2 {
		t.Errorf("repeated option lost: %v in %s", tags, got)
	}
	o, err := ClientOptions(config.Mongo{URI: config.NewSecret(uri), Params: map[string]string{"appName": "svc"}})
	if err != nil {
		t.Fatal(err)
	}
	if o.Auth.Username != "us@er" || o.Auth.Password != "p:ss" || len(o.Hosts) != 2 || o.Hosts[1] != "[::1]:27018" {
		t.Errorf("auth %+v hosts %v", o.Auth, o.Hosts)
	}
}

func TestParamValuesCannotInject(t *testing.T) {
	// A value holding '&' or '=' must stay one option value.
	o, err := ClientOptions(config.Mongo{Hosts: []string{"h:27017"}, Params: map[string]string{"appName": "a&replicaSet=evil&directConnection=true"}})
	if err != nil {
		t.Fatal(err)
	}
	if *o.AppName != "a&replicaSet=evil&directConnection=true" || o.ReplicaSet != nil || o.Direct != nil {
		t.Errorf("app %q replicaSet %v direct %v", *o.AppName, o.ReplicaSet, o.Direct)
	}
}

func TestTypedFieldsBeatURIAndParams(t *testing.T) {
	o, err := ClientOptions(config.Mongo{
		URI:      config.NewSecret("mongodb://old:oldpw@h/?authSource=old&authMechanism=SCRAM-SHA-1&tls=false"),
		Username: "new", Password: config.NewSecret("p@ss:/?#"), AuthSource: "admin", AuthMechanism: "SCRAM-SHA-256",
		Params: map[string]string{"authSource": "params", "tls": "false"},
		TLS:    config.TLS{Enabled: true, ServerName: "db.internal"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if o.Auth.Username != "new" || o.Auth.Password != "p@ss:/?#" || !o.Auth.PasswordSet || o.Auth.AuthSource != "admin" || o.Auth.AuthMechanism != "SCRAM-SHA-256" {
		t.Errorf("auth = %+v", o.Auth)
	}
	if o.TLSConfig == nil || o.TLSConfig.ServerName != "db.internal" {
		t.Errorf("tls = %+v", o.TLSConfig)
	}
}

func TestHostsIPv6AndDefaults(t *testing.T) {
	o, err := ClientOptions(config.Mongo{Hosts: []string{"[::1]:27017", "db2"}, TLS: config.TLS{Enabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Hosts) != 2 || o.Hosts[0] != "[::1]:27017" || o.Hosts[1] != "db2" || o.TLSConfig == nil || o.TLSConfig.ServerName != "" {
		t.Errorf("hosts %v tls %+v", o.Hosts, o.TLSConfig)
	}
}

func TestClientOptionsErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.Mongo{
		"bad port":              {URI: config.NewSecret("mongodb://u:" + pw + "@h:bad/")},
		"bad escape":            {URI: config.NewSecret("mongodb://u:" + pw + "%zz@h/")},
		"bad option":            {URI: config.NewSecret("mongodb://u:" + pw + "@h/?maxPoolSize=many")},
		"bad option and params": {URI: config.NewSecret("mongodb://u:" + pw + "@h/?maxPoolSize=many"), Params: map[string]string{"appName": "x"}},
		"bad query and params":  {URI: config.NewSecret("mongodb://u:" + pw + "@h/?a=%zz"), Params: map[string]string{"appName": "x"}},
		"wrong scheme":          {URI: config.NewSecret("mongo://u:" + pw + "@h/")},
		"field and bad param":   {Hosts: []string{"h"}, Password: config.NewSecret(pw), Params: map[string]string{"w": "-1"}},
		"bad tls":               {Hosts: []string{"h"}, Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ClientOptions(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestDefaultLoggerIsSlog(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	o, err := ClientOptions(config.Mongo{Hosts: []string{"h"}})
	if err != nil {
		t.Fatal(err)
	}
	lo := o.LoggerOptions
	if lo == nil || lo.Sink == nil || lo.ComponentLevels[options.LogComponentAll] != options.LogLevelInfo {
		t.Fatalf("logger options = %+v", lo)
	}
	lo.Sink.Info(1, "mongo-default-marker")
	if !strings.Contains(buf.String(), "msg=mongo-default-marker") {
		t.Errorf("default sink output: %s", buf.String())
	}
}

// TestParamsCaseVariantsRejected: option names are case-insensitive and
// config does not normalize map keys (e.g. appName in a file and APPNAME
// from another source), so two spellings in Params name one option and
// either could win. connString rejects them, with the same error on every
// call.
func TestParamsCaseVariantsRejected(t *testing.T) {
	cfg := config.Mongo{Hosts: []string{"h"}, Params: map[string]string{"appName": "a", "APPNAME": "b", "appname": "c"}}
	_, first := connString(cfg)
	if first == nil {
		t.Fatal("case variants of one param accepted")
	}
	for range 50 {
		if _, err := connString(cfg); err == nil || err.Error() != first.Error() {
			t.Fatalf("error is not deterministic: %v vs %v", first, err)
		}
	}
}

// TestSemicolonSeparatedURIWithParams: the driver accepts ';' as well as
// '&' between URI options, so a URI that works on its own must keep working
// when Params are added.
func TestSemicolonSeparatedURIWithParams(t *testing.T) {
	const uri = "mongodb://h/?replicaSet=rs0;authSource=admin"
	if _, err := ClientOptions(config.Mongo{URI: config.NewSecret(uri)}); err != nil {
		t.Fatalf("the driver rejects %s itself: %v", uri, err)
	}
	o, err := ClientOptions(config.Mongo{URI: config.NewSecret(uri), Params: map[string]string{"appName": "svc"}})
	if err != nil {
		t.Fatalf("with params: %v", err)
	}
	if *o.ReplicaSet != "rs0" || *o.AppName != "svc" {
		t.Errorf("replicaSet %v appName %v", o.ReplicaSet, o.AppName)
	}
}

// TestUnixSocketHost: with Hosts, connString writes the hosts into a URI
// only so that the driver can parse the params. The driver supports a Unix
// socket path such as /tmp/mongodb-27017.sock percent-encoded in URIs, so
// the hosts must be escaped there, or the URI would have no host.
func TestUnixSocketHost(t *testing.T) {
	o, err := ClientOptions(config.Mongo{Hosts: []string{"/tmp/mongodb-27017.sock"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Hosts) != 1 || o.Hosts[0] != "/tmp/mongodb-27017.sock" {
		t.Errorf("hosts = %v", o.Hosts)
	}
}

func parseQuery(t *testing.T, uri string) url.Values {
	t.Helper()
	_, raw, _ := strings.Cut(uri, "?")
	q, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatalf("query of %s: %v", uri, err)
	}
	return q
}
