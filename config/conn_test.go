package config_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/linzeyan/loadconf/config"
)

type connConf struct {
	Postgres config.Named[config.Postgres] `config:"postgres"`
	MQTT     config.Named[config.MQTT]     `config:"mqtt"`
	REST     config.Named[config.MQTTREST] `config:"mqtt_rest"`
	Mongo    config.Named[config.Mongo]    `config:"mongo"`
}

func TestConnDefaultsAndMaps(t *testing.T) {
	cfg := mustYAML[connConf](t, `
postgres:
  main:
    host: pg
    params: {statement_timeout: "5000", TimeZone: UTC}
mqtt:
  edge:
    brokers: [tcp://broker:1883]
    options: {will_topic: status, keep_alive: 60}
mqtt_rest:
  emqx:
    base_url: http://emqx:18083
    auth: {username: key, password: secret}
  gw:
    provider: generic
    base_url: https://gw.example.com/mqtt
    auth: {token: t}
    publish: {path: /publish, payload_encoding: base64, extra: {source: svc, n: 1}}
`)
	pg := cfg.Postgres.MustGet("main")
	if pg.Port != 5432 || pg.Params["TimeZone"] != "UTC" || pg.Params["statement_timeout"] != "5000" {
		t.Errorf("postgres = %+v", pg)
	}
	m := cfg.MQTT.MustGet("edge")
	// Options stay raw until the connector decodes them against the driver's
	// own struct, which is where their keys are checked.
	if m.Options["will_topic"] != "status" || m.Options["keep_alive"] == nil {
		t.Errorf("mqtt = %+v", m)
	}
	e := cfg.REST.MustGet("emqx")
	if e.Provider != config.MQTTRESTEMQX || e.Auth.ResolvedType() != config.HTTPAuthBasic || e.MaxRetries != 2 {
		t.Errorf("emqx = %+v", e)
	}
	g := cfg.REST.MustGet("gw")
	if g.Auth.ResolvedType() != config.HTTPAuthBearer || g.Publish.Method != "POST" || g.Publish.TopicField != "topic" || g.Publish.Extra["source"] != "svc" {
		t.Errorf("generic = %+v", g)
	}
}

func TestConnValidation(t *testing.T) {
	_, err := loadYAML[connConf](t, `
postgres:
  a: {params: {sslmode: require}}
mqtt:
  a: {brokers: ["broker:1883"]}
mqtt_rest:
  a: {base_url: "emqx:18083", auth: {type: login}}
  b: {provider: generic, base_url: "http://x", auth: {type: login, username: u}}
mongo:
  a: {database: app}
`)
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{
		"postgres.a: either dsn or host is required",
		`mqtt.a: broker "broker:1883" must be a URL`,
		`mqtt_rest.a: base_url must be an http(s) URL`,
		"mqtt_rest.a: auth.username is required for login auth",
		"mqtt_rest.b: publish.path is required for the generic provider",
		"mqtt_rest.b: login auth is only supported by the emqx provider",
		"mongo.a: either uri or hosts is required",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error missing %q:\n%v", want, err)
		}
	}
}

func TestNamedHelpers(t *testing.T) {
	var n config.Named[int]
	n.Set(" A ", 1)
	n.Set("b", 2)
	if !n.Has("a") || n.Len() != 2 {
		t.Errorf("n = %v", n.Names())
	}
	var order []string
	for name, v := range n.All() {
		order = append(order, name)
		if v == 0 {
			t.Error("zero value")
		}
	}
	if strings.Join(order, ",") != "a,b" {
		t.Errorf("order = %v", order)
	}
	b, _ := json.Marshal(n)
	if string(b) != `{"a":1,"b":2}` {
		t.Errorf("json = %s", b)
	}
	n.Delete("A")
	if n.Has("a") {
		t.Error("delete failed")
	}
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), `"missing" not found (have b)`) {
			t.Errorf("panic = %v", r)
		}
	}()
	n.MustGet("missing")
}

func TestSecretLogValue(t *testing.T) {
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("x", "pw", config.NewSecret("hunter2"))
	if strings.Contains(buf.String(), "hunter2") {
		t.Errorf("secret logged: %s", buf.String())
	}
}

func TestWithoutTagValidator(t *testing.T) {
	type conf struct {
		Name string `config:"name" validate:"required"`
	}
	if _, err := config.Load[conf](context.Background(), quiet(), config.WithValidator(nil)); err != nil {
		t.Errorf("tag validation should be disabled: %v", err)
	}
}

func TestArraysAndTypedMaps(t *testing.T) {
	type conf struct {
		Pair   [2]int             `config:"pair"`
		Limits map[string]uint16  `config:"limits"`
		ByID   map[int]config.TLS `config:"by_id"`
	}
	cfg := mustYAML[conf](t, "pair: [1, 2]\nlimits: {a: 1}\nby_id: {7: {enabled: true}}")
	if cfg.Pair != [2]int{1, 2} || cfg.Limits["a"] != 1 || !cfg.ByID[7].Enabled {
		t.Errorf("cfg = %+v", cfg)
	}
	if _, err := loadYAML[conf](t, "pair: [1, 2, 3]\nlimits: {a: -1}"); err == nil ||
		!strings.Contains(err.Error(), "pair: expected at most 2 items") || !strings.Contains(err.Error(), "limits.a: -1 is negative") {
		t.Errorf("got %v", err)
	}
}
