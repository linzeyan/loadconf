package conn_kafka_sarama

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/linzeyan/loadconf/config"
)

func TestConfigMapping(t *testing.T) {
	sc, err := Config(config.Kafka{
		Brokers:  []string{"k:9092"},
		ClientID: "svc",
		Version:  "3.6.0",
		SASL:     config.KafkaSASL{Mechanism: "SCRAM-SHA-256", Username: "u", Password: config.NewSecret("p")},
		TLS:      config.TLS{Enabled: true},
		Consumer: config.KafkaConsumer{InitialOffset: "earliest"},
		Options: map[string]any{
			"producer": map[string]any{"compression": "lz4", "flush": map[string]any{"frequency": "10ms"}, "retry": map[string]any{"max": 5}},
			"consumer": map[string]any{"isolation_level": 1, "offsets": map[string]any{"auto_commit": map[string]any{"enable": false}}, "fetch": map[string]any{"max": 1 << 20}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if sc.ClientID != "svc" || sc.Version != sarama.V3_6_0_0 || !sc.Net.TLS.Enable {
		t.Errorf("base = %s %v %v", sc.ClientID, sc.Version, sc.Net.TLS.Enable)
	}
	if !sc.Net.SASL.Enable || sc.Net.SASL.Mechanism != sarama.SASLTypeSCRAMSHA256 || sc.Net.SASL.SCRAMClientGeneratorFunc == nil {
		t.Errorf("sasl = %+v", sc.Net.SASL)
	}
	p := sc.Producer
	if p.RequiredAcks != sarama.WaitForAll || !p.Idempotent || sc.Net.MaxOpenRequests != 1 || p.Compression != sarama.CompressionLZ4 ||
		p.Flush.Frequency != 10*time.Millisecond || p.Retry.Max != 5 {
		t.Errorf("producer = %+v", p)
	}
	c := sc.Consumer
	if c.Offsets.Initial != sarama.OffsetOldest || c.IsolationLevel != sarama.ReadCommitted || c.Offsets.AutoCommit.Enable ||
		c.Fetch.Max != 1<<20 {
		t.Errorf("consumer = %+v", c)
	}
}

func TestSCRAMClient(t *testing.T) {
	sc, err := Config(config.Kafka{Brokers: []string{"k"}, SASL: config.KafkaSASL{Mechanism: "SCRAM-SHA-512", Username: "u", Password: config.NewSecret("p")}})
	if err != nil {
		t.Fatal(err)
	}
	client := sc.Net.SASL.SCRAMClientGeneratorFunc()
	if err := client.Begin("u", "p", ""); err != nil {
		t.Fatal(err)
	}
	first, err := client.Step("")
	if err != nil || first == "" || client.Done() {
		t.Errorf("first message = %q, %v", first, err)
	}
}

func TestConfigErrors(t *testing.T) {
	if _, err := Config(config.Kafka{Brokers: []string{"k"}, Version: "x.y"}); err == nil {
		t.Error("bad version should fail")
	}
	if _, err := OpenConsumerGroup(context.Background(), config.Kafka{Brokers: []string{"k"}}); err == nil {
		t.Error("missing group should fail")
	}
	// TLS and SASL come from their typed fields, where Secret keeps the
	// password out of logs.
	if _, err := Config(config.Kafka{Brokers: []string{"k"}, Options: map[string]any{"net": map[string]any{"sasl": map[string]any{"password": "p"}}}}); err == nil {
		t.Error("net.sasl in options should fail")
	}
	// Leader acks with the default idempotent producer is rejected by sarama.
	if _, err := Config(config.Kafka{Brokers: []string{"k"}, Options: map[string]any{"producer": map[string]any{"required_acks": 1}}}); err == nil {
		t.Error("idempotence without acks from all replicas should fail")
	}
	if _, err := Config(config.Kafka{Brokers: []string{"k"}, Options: map[string]any{"producer": map[string]any{"required_acks": 1, "idempotent": false}}}); err != nil {
		t.Errorf("leader acks without idempotence: %v", err)
	}
	// Options run before sarama's own validation.
	if _, err := Config(config.Kafka{Brokers: []string{"k"}}, func(sc *sarama.Config) { sc.Net.MaxOpenRequests = 0 }); err == nil {
		t.Error("invalid sarama config should fail")
	}
}

func TestSyncProducerFailsWithoutBroker(t *testing.T) {
	_, err := OpenSyncProducer(context.Background(), config.Kafka{Brokers: []string{"127.0.0.1:1"}, Options: map[string]any{"net": map[string]any{"dial_timeout": "200ms"}}},
		func(sc *sarama.Config) { sc.Metadata.Retry.Max = 0 })
	if err == nil {
		t.Error("expected connection failure")
	}
}

// sarama cannot be cancelled, so Open must return once ctx ends instead of
// waiting out sarama's own timeouts against a broker that never answers.
func TestOpenReturnsWhenContextEnds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c) // accepted, never answered
			mu.Unlock()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = OpenClient(ctx, config.Kafka{Brokers: []string{ln.Addr().String()}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("OpenClient took %v", d)
	}
}
