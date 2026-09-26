package conn_kafka_sarama

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/IBM/sarama"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds SCRAM delimiters and other ASCII characters a secret store
// may hand out.
var nasty = []string{
	"p@ss", "p:ss", "p,ss", "p=ss", "a,r=evil", "=2C", "p/ss", "p?ss", "p#ss", "it's", `say "hi"`,
	`back\slash`, "100%", "has space", "~!$^&*()",
}

// scramFirst starts the SCRAM exchange sarama would run for sc and returns
// the client-first message.
func scramFirst(t *testing.T, sc *sarama.Config) (string, error) {
	t.Helper()
	c := sc.Net.SASL.SCRAMClientGeneratorFunc()
	if err := c.Begin(sc.Net.SASL.User, sc.Net.SASL.Password, ""); err != nil {
		return "", err
	}
	return c.Step("")
}

// scramUser extracts and unescapes the user name of a SCRAM client-first
// message, failing on any attribute besides n and r.
func scramUser(t *testing.T, msg string) string {
	t.Helper()
	bare, ok := strings.CutPrefix(msg, "n,,")
	fields := strings.Split(bare, ",")
	if !ok || len(fields) != 2 || !strings.HasPrefix(fields[0], "n=") || !strings.HasPrefix(fields[1], "r=") {
		t.Fatalf("client-first message %q", msg)
	}
	return strings.NewReplacer("=2C", ",", "=3D", "=").Replace(fields[0][2:])
}

func TestDurableProducerDefaults(t *testing.T) {
	// Options that do not mention idempotence keep the durable defaults;
	// franz-go writes the same way, so switching drivers must not weaken
	// delivery.
	for name, opts := range map[string]map[string]any{
		"none":          nil,
		"retry":         {"producer": map[string]any{"retry": map[string]any{"max": 10}}},
		"compression":   {"producer": map[string]any{"compression": "zstd"}},
		"consumer only": {"consumer": map[string]any{"offsets": map[string]any{"auto_commit": map[string]any{"interval": "1s"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			sc, err := Config(config.Kafka{Brokers: []string{"k"}, Options: opts})
			if err != nil {
				t.Fatal(err)
			}
			if !sc.Producer.Idempotent || sc.Producer.RequiredAcks != sarama.WaitForAll || sc.Net.MaxOpenRequests != 1 {
				t.Errorf("idempotent %v acks %v max open requests %d", sc.Producer.Idempotent, sc.Producer.RequiredAcks, sc.Net.MaxOpenRequests)
			}
		})
	}
	// Loosening one knob alone is refused rather than silently giving up
	// idempotence.
	for name, opts := range map[string]map[string]any{
		"max open requests": {"net": map[string]any{"max_open_requests": 5}},
		"no acks":           {"producer": map[string]any{"required_acks": 0}},
	} {
		if _, err := Config(config.Kafka{Brokers: []string{"k"}, Options: opts}); err == nil || !strings.Contains(err.Error(), "Idempotent") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestOwnedOptionKeysAnyCase(t *testing.T) {
	for name, tc := range map[string]struct {
		opts map[string]any
		want string
	}{
		"client id":      {map[string]any{"CLIENT_ID": "x"}, "options.client_id: set by its own config key"},
		"version":        {map[string]any{"Version": "3.6.0"}, "options.version: set by its own config key"},
		"tls":            {map[string]any{"Net": map[string]any{"TLS": map[string]any{"enable": false}}}, "options.net.tls: set by its own config key"},
		"sasl password":  {map[string]any{"NET": map[string]any{"Sasl": map[string]any{"password": "hunter2"}}}, "options.net.sasl: set by its own config key"},
		"initial offset": {map[string]any{"Consumer": map[string]any{"Offsets": map[string]any{"Initial": -2}}}, "options.consumer.offsets.initial: set by its own config key"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Config(config.Kafka{Brokers: []string{"k"}, Options: tc.opts})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want %q", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "hunter2") {
				t.Errorf("error leaks the value: %v", err)
			}
		})
	}
}

func TestOptionsCannotInstallDriverObjects(t *testing.T) {
	for name, opts := range map[string]map[string]any{
		"partitioner":     {"producer": map[string]any{"partitioner": "hash"}},
		"metric registry": {"metric_registry": "x"},
		"proxy dialer":    {"net": map[string]any{"proxy": map[string]any{"dialer": "socks5://evil"}}},
		"rebalance":       {"consumer": map[string]any{"group": map[string]any{"rebalance": map[string]any{"strategy": "range"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Config(config.Kafka{Brokers: []string{"k"}, Options: opts}); err == nil || !strings.Contains(err.Error(), "options.") {
				t.Errorf("err = %v", err)
			}
		})
	}
}

func TestSASLCredentialsOnTheWire(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
				sc, err := Config(config.Kafka{Brokers: []string{"k"}, SASL: config.KafkaSASL{Mechanism: mech, Username: "u" + s, Password: config.NewSecret(s)}})
				if err != nil {
					t.Fatal(err)
				}
				if !sc.Net.SASL.Enable || sc.Net.SASL.User != "u"+s || sc.Net.SASL.Password != s {
					t.Fatalf("%s: sasl = %+v", mech, sc.Net.SASL)
				}
				if mech == "PLAIN" {
					continue
				}
				first, err := scramFirst(t, sc)
				if err != nil {
					t.Fatalf("%s: %v", mech, err)
				}
				// A ',' or '=' in the name must not add SCRAM attributes.
				if got := scramUser(t, first); got != "u"+s {
					t.Errorf("%s user = %q", mech, got)
				}
			}
		})
	}
}

func TestSASLEmptyCredentialsFailEarly(t *testing.T) {
	// sarama's validation catches these before any connection, unlike a
	// failed handshake later.
	for name, s := range map[string]config.KafkaSASL{
		"no user":     {Mechanism: "SCRAM-SHA-256", Password: config.NewSecret("hunter2-S3cret")},
		"no password": {Mechanism: "PLAIN", Username: "u"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Config(config.Kafka{Brokers: []string{"k"}, SASL: s})
			if err == nil || strings.Contains(err.Error(), "hunter2") {
				t.Errorf("err = %v", err)
			}
		})
	}
	sc, err := Config(config.Kafka{Brokers: []string{"k"}, SASL: config.KafkaSASL{Username: "u", Password: config.NewSecret("p")}})
	if err != nil || sc.Net.SASL.Enable {
		t.Errorf("credentials without a mechanism enabled SASL: %+v, %v", sc.Net.SASL, err)
	}
}

func TestVersionSpellings(t *testing.T) {
	for v, ok := range map[string]bool{
		"3.6.0": true, "0.11.0.0": true, "4.1.0": true, "2.8.2": true,
		"v3.6.0": false, "3.6": false, " 3.6.0": false, "3.6.0-IV1": false, "banana": false,
	} {
		t.Run(v, func(t *testing.T) {
			if _, err := Config(config.Kafka{Brokers: []string{"k"}, Version: v}); (err == nil) != ok {
				t.Errorf("err = %v, want ok %v", err, ok)
			}
		})
	}
}

func TestInitialOffset(t *testing.T) {
	for off, want := range map[string]int64{"": sarama.OffsetNewest, "latest": sarama.OffsetNewest, "earliest": sarama.OffsetOldest} {
		sc, err := Config(config.Kafka{Brokers: []string{"k"}, Consumer: config.KafkaConsumer{InitialOffset: off}})
		if err != nil || sc.Consumer.Offsets.Initial != want {
			t.Errorf("%q: initial = %d, err %v", off, sc.Consumer.Offsets.Initial, err)
		}
	}
}

func TestTLSServerName(t *testing.T) {
	for name, tc := range map[string]struct {
		tls     config.TLS
		enabled bool
		want    string
	}{
		"no server name": {config.TLS{Enabled: true}, true, ""},
		"server name":    {config.TLS{Enabled: true, ServerName: "kafka.internal"}, true, "kafka.internal"},
		"disabled":       {config.TLS{ServerName: "ignored"}, false, ""},
	} {
		t.Run(name, func(t *testing.T) {
			sc, err := Config(config.Kafka{Brokers: []string{"k1:9093", "[::1]:9093"}, TLS: tc.tls})
			if err != nil {
				t.Fatal(err)
			}
			if sc.Net.TLS.Enable != tc.enabled || (tc.enabled && (sc.Net.TLS.Config == nil || sc.Net.TLS.Config.ServerName != tc.want || sc.Net.TLS.Config.InsecureSkipVerify)) {
				t.Errorf("tls = %v %+v", sc.Net.TLS.Enable, sc.Net.TLS.Config)
			}
		})
	}
}

func TestErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	sasl := config.KafkaSASL{Mechanism: "SCRAM-SHA-512", Username: "u", Password: config.NewSecret(pw)}
	for name, cfg := range map[string]config.Kafka{
		"no brokers":      {SASL: sasl},
		"bad mechanism":   {Brokers: []string{"k"}, SASL: config.KafkaSASL{Mechanism: "scram", Username: "u", Password: config.NewSecret(pw)}},
		"bad version":     {Brokers: []string{"k"}, Version: "x", SASL: sasl},
		"bad tls":         {Brokers: []string{"k"}, SASL: sasl, TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
		"bad option":      {Brokers: []string{"k"}, SASL: sasl, Options: map[string]any{"net": map[string]any{"dial_timeout": "soon"}}},
		"sarama validate": {Brokers: []string{"k"}, SASL: sasl, Options: map[string]any{"producer": map[string]any{"required_acks": 1}}},
		"unknown option":  {Brokers: []string{"k"}, SASL: sasl, Options: map[string]any{"sasl_password": pw}},
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

func TestSlogLoggerNilAndTrim(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	l := SlogLogger(nil, slog.LevelWarn)
	l.Printf("client/brokers %d%% down\n\n", 100)
	l.Println("  consumer", "closed ")
	if out := buf.String(); !strings.Contains(out, `level=WARN msg="client/brokers 100% down"`) || !strings.Contains(out, `msg="consumer closed"`) {
		t.Errorf("output = %q", out)
	}
}

// TestSCRAMCredentialsNotSASLprepped: Kafka brokers (ScramFormatter) and the
// franz-go connector use the raw UTF-8 bytes of SCRAM credentials. SASLprep
// would send "u p" (plain space) for "u\u00a0p" and "up" for "u\u00adp",
// which then fail to authenticate, and reject an emoji before sending
// anything, so the configured user name must go on the wire unchanged.
func TestSCRAMCredentialsNotSASLprepped(t *testing.T) {
	for _, user := range []string{"u\u00a0p", "u\u00adp", "u密碼🔑"} {
		sc, err := Config(config.Kafka{Brokers: []string{"k"}, SASL: config.KafkaSASL{Mechanism: "SCRAM-SHA-256", Username: user, Password: config.NewSecret("p")}})
		if err != nil {
			t.Fatal(err)
		}
		first, err := scramFirst(t, sc)
		if err != nil {
			t.Errorf("%q: %v", user, err)
			continue
		}
		if got := scramUser(t, first); got != user {
			t.Errorf("user %q sent as %q", user, got)
		}
	}
}
