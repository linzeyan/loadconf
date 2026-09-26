package config

import (
	"errors"
	"fmt"
	"slices"
	"time"
)

// Kafka configures a Kafka client for the sarama and franz-go connectors.
//
// Options sets any other sarama setting by the snake_case path of its
// sarama.Config field, e.g. net: {dial_timeout: 5s}, producer: {compression:
// zstd, retry: {max: 5}} or consumer: {fetch: {max: 1048576}}; see
// [DecodeOptions]. franz-go has no config struct to decode into, so its
// connector rejects Options: pass kgo options in code instead.
type Kafka struct {
	Brokers  []string `config:"brokers"`
	ClientID string   `config:"client_id"`
	// Version is the broker protocol version, e.g. "3.6.0". sarama needs it
	// for newer features; franz-go negotiates versions and uses it as a cap.
	Version string `config:"version"`
	// PingTimeout bounds the connectivity check of the connectors' Open.
	PingTimeout time.Duration `config:"ping_timeout" default:"5s"`

	SASL     KafkaSASL      `config:"sasl"`
	TLS      TLS            `config:"tls"`
	Consumer KafkaConsumer  `config:"consumer"`
	Options  map[string]any `config:"options"`
}

// KafkaSASL configures SASL authentication; disabled when Mechanism is empty.
type KafkaSASL struct {
	// Mechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.
	Mechanism string `config:"mechanism"`
	Username  string `config:"username"`
	Password  Secret `config:"password"`
}

// KafkaConsumer names what to consume. Group consumption is used when Group
// is set.
type KafkaConsumer struct {
	Group  string   `config:"group"`
	Topics []string `config:"topics"`
	// InitialOffset is where to start without a committed offset: earliest
	// or latest.
	InitialOffset string `config:"initial_offset" default:"latest"`
}

func (k Kafka) Validate() error {
	var errs []error
	if len(k.Brokers) == 0 {
		errs = append(errs, errors.New("brokers is required"))
	}
	if !slices.Contains([]string{"", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}, k.SASL.Mechanism) {
		errs = append(errs, fmt.Errorf("unsupported sasl.mechanism %q", k.SASL.Mechanism))
	}
	if !slices.Contains([]string{"", "earliest", "latest"}, k.Consumer.InitialOffset) {
		errs = append(errs, fmt.Errorf("unsupported consumer.initial_offset %q", k.Consumer.InitialOffset))
	}
	return errors.Join(errs...)
}
