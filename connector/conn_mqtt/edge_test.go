package conn_mqtt

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds URL delimiters and other characters a secret store may hand
// out; MQTT sends them as length-prefixed strings, so none is special.
var nasty = []string{
	"p@ss", "p:ss", "p/ss", "p?ss", "p#ss", "p&ss", "p=ss", "p+ss", "it's", `say "hi"`,
	`back\slash`, "100%", "%zz", "has space", " lead", "trail ", "密碼🔑", "a\x00b",
}

// fakeBroker accepts MQTT connections on loopback, hands each CONNECT
// packet to the test and accepts the session.
func fakeBroker(t *testing.T) (string, <-chan *packets.ConnectPacket) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	ch := make(chan *packets.ConnectPacket, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				p, err := packets.ReadPacket(c)
				cp, ok := p.(*packets.ConnectPacket)
				if err != nil || !ok {
					return
				}
				ch <- cp
				if packets.NewControlPacket(packets.Connack).Write(c) == nil {
					_, _ = io.Copy(io.Discard, c)
				}
			}()
		}
	}()
	return "tcp://" + ln.Addr().String(), ch
}

func connect(t *testing.T, cfg config.MQTT, ch <-chan *packets.ConnectPacket) *packets.ConnectPacket {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.Disconnect(250)
	select {
	case p := <-ch:
		return p
	case <-ctx.Done():
		t.Fatal("no CONNECT packet")
		return nil
	}
}

func TestConnectPacketCarriesCredentialsVerbatim(t *testing.T) {
	if os.Getenv("CONN_MQTT_WIRE_CHILD") == "" {
		// paho's goroutines outlive Disconnect and keep reading its global
		// loggers, which the SetLogger tests write: connect in a child
		// process so that this test cannot race with them.
		cmd := exec.Command(os.Args[0], "-test.run=^TestConnectPacketCarriesCredentialsVerbatim$", "-test.count=1")
		cmd.Env = append(os.Environ(), "CONN_MQTT_WIRE_CHILD=1")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v\n%s", err, out)
		}
		return
	}
	broker, ch := fakeBroker(t)
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			p := connect(t, config.MQTT{Brokers: []string{broker}, ClientID: "c" + s, Username: "u" + s, Password: config.NewSecret(s)}, ch)
			if p.ClientIdentifier != "c"+s || p.Username != "u"+s || string(p.Password) != s || !p.UsernameFlag || !p.PasswordFlag {
				t.Errorf("connect = client %q user %q password %q", p.ClientIdentifier, p.Username, p.Password)
			}
			if p.ProtocolVersion != 4 || p.ProtocolName != "MQTT" || !p.CleanSession {
				t.Errorf("connect = %v", p)
			}
		})
	}
}

func TestClientIDRandomSuffix(t *testing.T) {
	for name, tc := range map[string]struct {
		id     string
		suffix bool
		prefix string
	}{
		"suffix only":   {"", true, ""},
		"id and suffix": {"svc", true, "svc-"},
		"unicode id":    {"服務", true, "服務-"},
		"no suffix":     {"svc", false, "svc"},
		"nothing":       {"", false, ""},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := config.MQTT{Brokers: []string{"tcp://b:1883"}, ClientID: tc.id, ClientIDRandomSuffix: tc.suffix}
			a, err := ClientOptions(cfg)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := ClientOptions(cfg)
			rest, ok := strings.CutPrefix(a.ClientID, tc.prefix)
			if !tc.suffix {
				if a.ClientID != tc.id {
					t.Errorf("client id = %q", a.ClientID)
				}
				return
			}
			// Replicas share the config: each client needs its own id, or
			// the broker disconnects the previous holder.
			if _, err := hex.DecodeString(rest); !ok || err != nil || len(rest) != 8 || a.ClientID == b.ClientID {
				t.Errorf("client ids %q and %q", a.ClientID, b.ClientID)
			}
		})
	}
}

func TestBrokerSchemes(t *testing.T) {
	for broker, ok := range map[string]bool{
		"tcp://b:1883": true, "mqtt://b": true, "ssl://b:8883": true, "tls://b": true, "mqtts://b": true, "tcps://b": true,
		"ws://b/mqtt": true, "wss://b:443/mqtt": true, "unix:///run/mqtt.sock": true, "TCP://B:1883": true, "tcp://[::1]:1883": true,
		"http://b": false, "b:1883": false, "": false, "tcp//b:1883": false,
	} {
		t.Run(broker, func(t *testing.T) {
			o, err := ClientOptions(config.MQTT{Brokers: []string{broker}})
			if (err == nil) != ok {
				t.Fatalf("err = %v, want ok %v", err, ok)
			}
			// paho drops brokers it cannot parse with only a log line.
			if ok && len(o.Servers) != 1 {
				t.Errorf("servers = %v", o.Servers)
			}
		})
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for _, key := range []string{"SERVERS", "Client_ID", "USERNAME", "Password", "TLS_Config"} {
		t.Run(key, func(t *testing.T) {
			_, err := ClientOptions(config.MQTT{Brokers: []string{"tcp://b:1883"}, Options: map[string]any{key: "hunter2"}})
			if err == nil || !strings.Contains(err.Error(), "options."+strings.ToLower(key)+": set by its own config key") {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsCannotInstallDriverObjects(t *testing.T) {
	for _, key := range []string{"credentials_provider", "on_connect", "default_publish_handler", "store", "custom_open_connection_fn"} {
		t.Run(key, func(t *testing.T) {
			if _, err := ClientOptions(config.MQTT{Brokers: []string{"tcp://b:1883"}, Options: map[string]any{key: "x"}}); err == nil || !strings.Contains(err.Error(), "options."+key) {
				t.Errorf("err = %v", err)
			}
		})
	}
}

func TestProtocolAndTLS(t *testing.T) {
	for name, tc := range map[string]struct {
		tls  config.TLS
		want string // "" = no TLS config
	}{
		"no server name": {config.TLS{Enabled: true}, "<empty>"},
		"server name":    {config.TLS{Enabled: true, ServerName: "mqtt.internal"}, "mqtt.internal"},
		"disabled":       {config.TLS{ServerName: "ignored"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			o, err := ClientOptions(config.MQTT{Brokers: []string{"ssl://b:8883"}, TLS: tc.tls, Options: map[string]any{"keep_alive": 10}})
			if err != nil {
				t.Fatal(err)
			}
			if o.ProtocolVersion != 4 {
				t.Errorf("protocol = %d", o.ProtocolVersion)
			}
			got := ""
			if o.TLSConfig != nil {
				got = o.TLSConfig.ServerName
				if got == "" {
					got = "<empty>"
				}
				if o.TLSConfig.InsecureSkipVerify {
					t.Error("verification disabled")
				}
			}
			if got != tc.want {
				t.Errorf("tls server name = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientOptionsErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	for name, cfg := range map[string]config.MQTT{
		"no brokers":     {Password: config.NewSecret(pw)},
		"bad option":     {Brokers: []string{"tcp://b:1883"}, Password: config.NewSecret(pw), Options: map[string]any{"keep_alive": "30s"}},
		"unknown option": {Brokers: []string{"tcp://b:1883"}, Password: config.NewSecret(pw), Options: map[string]any{"pasword": pw}},
		"bad tls":        {Brokers: []string{"tcp://b:1883"}, Password: config.NewSecret(pw), TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
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

// TestInvalidBrokerErrorRedactsPassword: paho takes credentials from broker
// URLs, so config.MQTT.Validate must not print them for a scheme typo, or
// the password lands in startup logs.
func TestInvalidBrokerErrorRedactsPassword(t *testing.T) {
	_, err := ClientOptions(config.MQTT{Brokers: []string{"tpc://u:hunter2@b:1883"}})
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error leaks password: %v", err)
	}
}

// TestBrokerWithoutSlashesRejected: paho's AddBroker prefixes "tcp://" to
// anything without "://", so a typo such as "tcp:b:1883" or "tcp:/b:1883"
// would be dropped with only a log line (client without servers) or dialed
// at a host like "tcp:". ClientOptions rejects a broker without a host
// (unix sockets aside).
func TestBrokerWithoutSlashesRejected(t *testing.T) {
	for _, broker := range []string{"tcp:b", "tcp:/b:1883", "ssl:b:8883"} {
		o, err := ClientOptions(config.MQTT{Brokers: []string{broker}})
		if err == nil {
			t.Errorf("%q accepted; paho kept %v", broker, o.Servers)
		}
	}
}

// TestOpenErrorRedactsBrokerCredentials: paho reads credentials from broker
// URLs (they even win over Username/Password), so the brokers in Open's
// errors, printed on every failed connect or timeout, must be redacted like
// the other connectors' addresses.
func TestOpenErrorRedactsBrokerCredentials(t *testing.T) {
	_, err := Open(context.Background(), config.MQTT{Brokers: []string{"tcp://u:hunter2@127.0.0.1:1"}, Options: map[string]any{"connect_timeout": "1s"}})
	if err == nil {
		t.Fatal("want error")
	}
	if strings.Contains(err.Error(), "hunter2") || !strings.Contains(err.Error(), "mqtt connect [tcp://127.0.0.1:1]: ") {
		t.Errorf("err = %v", err)
	}
}
