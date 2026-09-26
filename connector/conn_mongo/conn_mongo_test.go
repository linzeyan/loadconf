package conn_mongo

import (
	"context"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/linzeyan/loadconf/config"
)

func TestClientOptions(t *testing.T) {
	o, err := ClientOptions(config.Mongo{
		URI:      config.NewSecret("mongodb://urluser:urlpw@h1:27017,h2:27017/?authSource=admin&replicaSet=rs0&maxPoolSize=5"),
		Password: config.NewSecret("override"),
		Params:   map[string]string{"maxpoolsize": "50", "readPreference": "secondaryPreferred", "appName": "svc", "timeoutMS": "2000"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Hosts) != 2 || *o.ReplicaSet != "rs0" || *o.MaxPoolSize != 50 || *o.AppName != "svc" || *o.Timeout != 2*time.Second {
		t.Errorf("options = %+v", o)
	}
	if o.Auth.Username != "urluser" || o.Auth.Password != "override" || o.Auth.AuthSource != "admin" {
		t.Errorf("auth = %+v", o.Auth)
	}
	if o.ReadPreference.Mode() != readpref.SecondaryPreferredMode {
		t.Errorf("read preference = %v", o.ReadPreference.Mode())
	}

	o, err = ClientOptions(config.Mongo{Hosts: []string{"h:27017"}, Username: "u", Password: config.NewSecret("p"), Params: map[string]string{"directConnection": "true"}})
	if err != nil || o.Hosts[0] != "h:27017" || !*o.Direct || o.Auth.Username != "u" {
		t.Errorf("hosts options = %+v, %v", o, err)
	}

	if _, err := ClientOptions(config.Mongo{URI: config.NewSecret("http://not-mongo")}); err == nil {
		t.Error("bad uri should fail")
	}
	// A URI without a path still takes params.
	if o, err := ClientOptions(config.Mongo{URI: config.NewSecret("mongodb://h:27017"), Params: map[string]string{"appName": "svc"}}); err != nil || *o.AppName != "svc" {
		t.Errorf("uri without slash: %+v, %v", o, err)
	}
	if _, err := ClientOptions(config.Mongo{Hosts: []string{"h"}, Params: map[string]string{"maxPoolSize": "many"}}); err == nil {
		t.Error("a bad param should fail")
	}
}

// New must not wait for the servers, so that a service can start while
// MongoDB is down; Open is the one that checks.
func TestNewDoesNotWaitForServers(t *testing.T) {
	start := time.Now()
	c, err := New(config.Mongo{Hosts: []string{"127.0.0.1:1"}, PingTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Disconnect(context.Background())
	if d := time.Since(start); d > time.Second {
		t.Errorf("New took %v", d)
	}
}

func TestOpenFailsFast(t *testing.T) {
	start := time.Now()
	_, err := Open(context.Background(), config.Mongo{Hosts: []string{"127.0.0.1:1"}, PingTimeout: 300 * time.Millisecond})
	if err == nil {
		t.Fatal("expected ping failure")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("ping timeout not honored: %v", time.Since(start))
	}
	if _, err := OpenDatabase(context.Background(), config.Mongo{Hosts: []string{"h"}}); err == nil {
		t.Error("missing database should fail")
	}
}
