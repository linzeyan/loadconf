package conn_kafka_franz

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/linzeyan/loadconf/config"
)

func TestNewClientOptions(t *testing.T) {
	cl, err := New(config.Kafka{
		Brokers:  []string{"k1:9092", "k2:9092"},
		ClientID: "svc",
		Version:  "3.6.0",
		SASL:     config.KafkaSASL{Mechanism: "SCRAM-SHA-512", Username: "u", Password: config.NewSecret("p")},
		Consumer: config.KafkaConsumer{Group: "g", Topics: []string{"a", "b"}, InitialOffset: "earliest"},
	}, kgo.DefaultProduceTopic("events"), kgo.ProducerLinger(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	for opt, want := range map[string]any{
		"ClientID":            "svc",
		"DefaultProduceTopic": "events",
		"ConsumerGroup":       "g",
		"ProducerLinger":      5 * time.Millisecond,
	} {
		if got := cl.OptValue(opt); got != want {
			t.Errorf("%s = %v, want %v", opt, got, want)
		}
	}
	if seeds, _ := cl.OptValue("SeedBrokers").([]string); len(seeds) != 2 {
		t.Errorf("seeds = %v", cl.OptValue("SeedBrokers"))
	}
	if topics := fmt.Sprint(cl.OptValue("ConsumeTopics")); topics != "map[a:<nil> b:<nil>]" {
		t.Errorf("topics = %v", cl.OptValue("ConsumeTopics"))
	}
	if cl.OptValue("MaxVersions") == nil {
		t.Error("max versions not set")
	}
}

func TestOptionsErrors(t *testing.T) {
	for name, cfg := range map[string]config.Kafka{
		"no brokers":          {},
		"unknown version":     {Brokers: []string{"k"}, Version: "banana"},
		"group without topic": {Brokers: []string{"k"}, Consumer: config.KafkaConsumer{Group: "g"}},
		// Options would be silently ignored: kgo has no struct to decode them into.
		"sarama options": {Brokers: []string{"k"}, Options: map[string]any{"net": map[string]any{"dial_timeout": "1s"}}},
	} {
		if _, err := Options(cfg); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestProduceOnlyHasNoConsumer(t *testing.T) {
	cl, err := New(config.Kafka{Brokers: []string{"k"}, ClientID: "cfg"}, kgo.ClientID("extra-wins"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if cl.OptValue("ConsumerGroup") != "" || cl.OptValue("ClientID") != "extra-wins" || cl.OptValue("DisableIdempotentWrite") != false {
		t.Errorf("group = %v client id = %v", cl.OptValue("ConsumerGroup"), cl.OptValue("ClientID"))
	}
}

func TestOpenFailsFast(t *testing.T) {
	start := time.Now()
	if _, err := Open(context.Background(), config.Kafka{Brokers: []string{"127.0.0.1:1"}, PingTimeout: 500 * time.Millisecond}); err == nil {
		t.Fatal("expected ping failure")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("ping timeout not honored: %v", time.Since(start))
	}
}
