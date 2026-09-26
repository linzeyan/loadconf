package conn_kafka_franz

import (
	"bytes"
	"context"
	"crypto/tls"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/linzeyan/loadconf/config"
)

// nasty holds SASL and SCRAM delimiters (',', '=', NUL-free) and other
// characters a secret store may hand out.
var nasty = []string{
	"p@ss", "p:ss", "p,ss", "p=ss", "a,r=evil", "=2C", "p/ss", "p?ss", "p#ss", "it's", `say "hi"`,
	`back\slash`, "100%", "has space", " lead", "trail ", "密碼🔑",
}

// firstMessage builds a client for cfg and returns the SASL mechanism's
// first client message, i.e. what goes on the wire.
func firstMessage(t *testing.T, cfg config.Kafka) (string, []byte, error) {
	t.Helper()
	cl, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
	if len(mechs) != 1 {
		t.Fatalf("mechanisms = %v", mechs)
	}
	_, msg, err := mechs[0].Authenticate(context.Background(), "k1:9092")
	return mechs[0].Name(), msg, err
}

// scramUser extracts and unescapes the user name of a SCRAM client-first
// message, failing on any attribute besides n and r.
func scramUser(t *testing.T, msg []byte) string {
	t.Helper()
	bare, ok := strings.CutPrefix(string(msg), "n,,")
	fields := strings.Split(bare, ",")
	if !ok || len(fields) != 2 || !strings.HasPrefix(fields[0], "n=") || !strings.HasPrefix(fields[1], "r=") {
		t.Fatalf("client-first message %q", msg)
	}
	return strings.NewReplacer("=2C", ",", "=3D", "=").Replace(fields[0][2:])
}

func TestSASLCredentialsOnTheWire(t *testing.T) {
	for _, s := range nasty {
		t.Run(s, func(t *testing.T) {
			for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
				name, msg, err := firstMessage(t, config.Kafka{Brokers: []string{"k1:9092"}, SASL: config.KafkaSASL{Mechanism: mech, Username: "u" + s, Password: config.NewSecret(s)}})
				if err != nil || name != mech {
					t.Fatalf("%s: mechanism %s, err %v", mech, name, err)
				}
				if mech == "PLAIN" {
					if string(msg) != "\x00u"+s+"\x00"+s {
						t.Errorf("PLAIN message %q", msg)
					}
					continue
				}
				// A ',' or '=' in the name must not add SCRAM attributes.
				if got := scramUser(t, msg); got != "u"+s {
					t.Errorf("%s user = %q", mech, got)
				}
			}
		})
	}
}

func TestSASLMechanismSpellings(t *testing.T) {
	const pw = "hunter2-S3cret"
	for _, mech := range []string{"plain", "Plain", "scram-sha-256", "SCRAM-SHA-1", "GSSAPI", "OAUTHBEARER", " PLAIN"} {
		t.Run(mech, func(t *testing.T) {
			_, err := Options(config.Kafka{Brokers: []string{"k"}, SASL: config.KafkaSASL{Mechanism: mech, Username: "u", Password: config.NewSecret(pw)}})
			if err == nil || !strings.Contains(err.Error(), "sasl.mechanism") {
				t.Errorf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestVersionSpellings(t *testing.T) {
	for v, ok := range map[string]bool{
		"3.6.0": true, "3.6": true, "v3.6.0": true, "0.11.0": true, "2.8.2": true,
		" 3.6.0": false, "3.6.0-IV1": false, "3": false, "99.0.0": false, "banana": false,
	} {
		t.Run(v, func(t *testing.T) {
			if _, err := Options(config.Kafka{Brokers: []string{"k"}, Version: v}); (err == nil) != ok {
				t.Errorf("err = %v, want ok %v", err, ok)
			}
		})
	}
}

func TestConsumerSettings(t *testing.T) {
	for name, tc := range map[string]struct {
		consumer config.KafkaConsumer
		offset   int64 // kgo: -2 start, -1 end
		group    string
	}{
		"earliest":            {config.KafkaConsumer{Topics: []string{"t"}, InitialOffset: "earliest"}, -2, ""},
		"latest":              {config.KafkaConsumer{Topics: []string{"t"}, InitialOffset: "latest"}, -1, ""},
		"unset means latest":  {config.KafkaConsumer{Topics: []string{"t"}}, -1, ""},
		"group":               {config.KafkaConsumer{Topics: []string{"t", "密碼"}, Group: "g,1"}, -1, "g,1"},
		"group with earliest": {config.KafkaConsumer{Topics: []string{"t"}, Group: "g", InitialOffset: "earliest"}, -2, "g"},
	} {
		t.Run(name, func(t *testing.T) {
			cl, err := New(config.Kafka{Brokers: []string{"k"}, Consumer: tc.consumer}, WithLogger(slog.New(slog.DiscardHandler)))
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			o, _ := cl.OptValue(kgo.ConsumeResetOffset).(kgo.Offset)
			if o.EpochOffset().Offset != tc.offset || cl.OptValue(kgo.ConsumerGroup) != tc.group {
				t.Errorf("offset %v group %v", o, cl.OptValue(kgo.ConsumerGroup))
			}
		})
	}
	for _, off := range []string{"Earliest", "oldest", "0"} {
		if _, err := Options(config.Kafka{Brokers: []string{"k"}, Consumer: config.KafkaConsumer{Topics: []string{"t"}, InitialOffset: off}}); err == nil {
			t.Errorf("initial offset %q accepted", off)
		}
	}
}

func TestBrokersParsedByClient(t *testing.T) {
	const pw = "hunter2-S3cret"
	for broker, ok := range map[string]bool{
		"[::1]:9092": true, "k1": true, "k1:65535": true, "127.0.0.1:9092": true,
		// Out-of-range ports pass here (kgo parses them as int32) and only
		// fail at dial time.
		"k1:port": false, "k1:": false,
	} {
		t.Run(broker, func(t *testing.T) {
			cl, err := New(config.Kafka{Brokers: []string{broker}, SASL: config.KafkaSASL{Mechanism: "PLAIN", Username: "u", Password: config.NewSecret(pw)}})
			if err == nil {
				cl.Close()
			}
			if (err == nil) != ok {
				t.Errorf("err = %v, want ok %v", err, ok)
			}
			if err != nil && strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestOptionsMapMustBeEmpty(t *testing.T) {
	if _, err := Options(config.Kafka{Brokers: []string{"k"}, Options: map[string]any{}}); err != nil {
		t.Errorf("empty options map: %v", err)
	}
	// The rejection must not echo the settings, which may hold credentials.
	_, err := Options(config.Kafka{Brokers: []string{"k"}, Options: map[string]any{"net": map[string]any{"sasl": map[string]any{"password": "hunter2"}}}})
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %v", err)
	}
}

func TestErrorsDoNotLeakPassword(t *testing.T) {
	const pw = "hunter2-S3cret"
	sasl := config.KafkaSASL{Mechanism: "SCRAM-SHA-512", Username: "u", Password: config.NewSecret(pw)}
	for name, cfg := range map[string]config.Kafka{
		"no brokers":          {SASL: sasl},
		"bad version":         {Brokers: []string{"k"}, Version: "x", SASL: sasl},
		"bad tls":             {Brokers: []string{"k"}, SASL: sasl, TLS: config.TLS{Enabled: true, MinVersion: "1.0"}},
		"group without topic": {Brokers: []string{"k"}, SASL: sasl, Consumer: config.KafkaConsumer{Group: "g"}},
		"bad offset":          {Brokers: []string{"k"}, SASL: sasl, Consumer: config.KafkaConsumer{Topics: []string{"t"}, InitialOffset: "first"}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Options(cfg)
			if err == nil {
				t.Fatal("want error")
			}
			if strings.Contains(err.Error(), pw) {
				t.Errorf("error leaks password: %v", err)
			}
		})
	}
}

func TestTLSServerName(t *testing.T) {
	for name, tc := range map[string]struct {
		tls  config.TLS
		want string
	}{
		// Empty lets kgo set each broker's host per dial; a fixed name is
		// used as is.
		"no server name": {config.TLS{Enabled: true}, ""},
		"server name":    {config.TLS{Enabled: true, ServerName: "kafka.internal"}, "kafka.internal"},
	} {
		t.Run(name, func(t *testing.T) {
			cl, err := New(config.Kafka{Brokers: []string{"k1:9093", "[::1]:9093"}, TLS: tc.tls})
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			c, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config)
			if c == nil || c.ServerName != tc.want || c.InsecureSkipVerify {
				t.Errorf("tls = %+v", c)
			}
		})
	}
	cl, err := New(config.Kafka{Brokers: []string{"k"}, TLS: config.TLS{ServerName: "ignored"}})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if c, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config); c != nil {
		t.Errorf("disabled tls dials with %+v", c)
	}
}

func TestDefaultLoggerFollowsSlogDefault(t *testing.T) {
	var buf bytes.Buffer
	var lv slog.LevelVar
	lv.Set(slog.LevelError)
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: &lv})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	cl, err := New(config.Kafka{Brokers: []string{"k"}})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	l, ok := cl.OptValue(kgo.WithLogger).(kgo.Logger)
	if !ok || l.Level() != kgo.LogLevelError {
		t.Fatalf("logger = %#v", cl.OptValue(kgo.WithLogger))
	}
	lv.Set(slog.LevelInfo)
	if l.Level() != kgo.LogLevelInfo {
		t.Errorf("level did not follow slog.Default: %v", l.Level())
	}
	l.Log(kgo.LogLevelWarn, "franz-default-marker", "broker", "k")
	if !strings.Contains(buf.String(), "level=WARN msg=franz-default-marker broker=k") {
		t.Errorf("output = %q", buf.String())
	}
}

func TestSlogLoggerOddKeyvals(t *testing.T) {
	// kgo passes key/value pairs; an odd count must not panic or drop the
	// message.
	var buf bytes.Buffer
	l := SlogLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	l.Log(kgo.LogLevelWarn, "odd", "k")
	l.Log(kgo.LogLevel(99), "unknown level")
	if out := buf.String(); !strings.Contains(out, "msg=odd !BADKEY=k") || strings.Contains(out, "unknown level") {
		t.Errorf("output = %q", out)
	}
}
