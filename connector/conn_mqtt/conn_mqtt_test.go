package conn_mqtt

import (
	"context"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/linzeyan/loadconf/config"
)

func TestClientOptions(t *testing.T) {
	o, err := ClientOptions(config.MQTT{
		Brokers:              []string{"tcp://b1:1883", "ssl://b2:8883"},
		ClientID:             "svc",
		ClientIDRandomSuffix: true,
		Username:             "u",
		Password:             config.NewSecret("p"),
		TLS:                  config.TLS{Enabled: true, ServerName: "b2"},
		Options: map[string]any{
			"keep_alive":   15,
			"will_enabled": true, "will_topic": "status", "will_payload": "offline", "will_qos": 1, "will_retained": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(o.Servers) != 2 || o.Servers[1].Host != "b2:8883" {
		t.Errorf("servers = %v", o.Servers)
	}
	if !strings.HasPrefix(o.ClientID, "svc-") || len(o.ClientID) != len("svc-")+8 {
		t.Errorf("client id = %q", o.ClientID)
	}
	if o.Username != "u" || o.Password != "p" || !o.CleanSession || !o.AutoReconnect || o.KeepAlive != 15 || o.ProtocolVersion != 4 {
		t.Errorf("options = %+v", o)
	}
	if !o.WillEnabled || o.WillTopic != "status" || string(o.WillPayload) != "offline" || o.WillQos != 1 || !o.WillRetained {
		t.Errorf("will = %v %s %s %d %v", o.WillEnabled, o.WillTopic, o.WillPayload, o.WillQos, o.WillRetained)
	}
	if o.TLSConfig == nil || o.TLSConfig.ServerName != "b2" {
		t.Errorf("tls = %+v", o.TLSConfig)
	}
}

func TestInvalidConfig(t *testing.T) {
	if _, err := New(config.MQTT{Brokers: []string{"b1:1883"}}); err == nil {
		t.Error("broker without scheme should fail")
	}
	// Paho counts keep_alive in seconds; a duration string is a mistake.
	_, err := New(config.MQTT{Brokers: []string{"tcp://b1:1883"}, Options: map[string]any{"keep_alive": "30s", "servers": "tcp://x:1"}})
	if err == nil || !strings.Contains(err.Error(), `options.keep_alive: invalid integer "30s"`) ||
		!strings.Contains(err.Error(), "options.servers: set by its own config key") {
		t.Errorf("err = %v", err)
	}
}

func TestOpenFailure(t *testing.T) {
	var hooked bool
	_, err := Open(context.Background(), config.MQTT{Brokers: []string{"tcp://127.0.0.1:1"}, Options: map[string]any{"connect_timeout": "1s"}},
		func(o *mqtt.ClientOptions) { hooked = true })
	if err == nil || !hooked {
		t.Errorf("err = %v hooked = %v", err, hooked)
	}

	// With ConnectRetry the token never completes; ctx bounds the wait.
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = Open(ctx, config.MQTT{Brokers: []string{"tcp://127.0.0.1:1"}, Options: map[string]any{"connect_retry": true, "connect_retry_interval": "100ms"}})
	if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
		t.Errorf("err = %v", err)
	}
}
