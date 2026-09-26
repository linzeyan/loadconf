// Package conn_kafka_sarama builds IBM/sarama clients from [config.Kafka].
//
//	producer, err := conn_kafka_sarama.OpenSyncProducer(ctx, cfg.Kafka.MustGet("main"))
//	group, err := conn_kafka_sarama.OpenConsumerGroup(ctx, cfg.Kafka.MustGet("main"))
//
// sarama constructors always connect, so every entry point is an Open and
// there is no New. sarama cannot cancel them: each attempt is bounded by the
// net.dial_timeout and metadata.retry options, and when ctx ends first the
// Open returns ctx.Err() and closes whatever sarama builds later.
//
// sarama logs through process-wide loggers; route them to slog with
//
//	sarama.Logger = conn_kafka_sarama.SlogLogger(logger, slog.LevelInfo)
package conn_kafka_sarama

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the sarama config after it is built from the config.
type Option func(*sarama.Config)

// Config converts cfg into a validated sarama config: sarama's defaults
// with a durable producer (acks from all replicas, idempotent), then
// cfg.Options, then the typed fields, then opts. To produce with
// producer.required_acks 1 or 0, also set producer.idempotent false.
func Config(cfg config.Kafka, opts ...Option) (*sarama.Config, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	sc := sarama.NewConfig()
	// The franz-go connector writes durably by default; match it so that
	// switching drivers does not weaken delivery.
	sc.Producer.RequiredAcks = sarama.WaitForAll
	sc.Producer.Idempotent = true
	sc.Net.MaxOpenRequests = 1 // required by sarama for idempotence
	if err := config.DecodeOptions(cfg.Options, sc, "client_id", "version", "net.tls", "net.sasl",
		"consumer.offsets.initial"); err != nil {
		return nil, err
	}
	if cfg.ClientID != "" {
		sc.ClientID = cfg.ClientID
	}
	if cfg.Version != "" {
		v, err := sarama.ParseKafkaVersion(cfg.Version)
		if err != nil {
			return nil, err
		}
		sc.Version = v
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		sc.Net.TLS.Enable, sc.Net.TLS.Config = true, tlsCfg
	}
	if s := cfg.SASL; s.Mechanism != "" {
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = s.Username
		sc.Net.SASL.Password = s.Password.Value()
		switch s.Mechanism {
		case "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scramClient{hash: scram.SHA256} }
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
			sc.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &scramClient{hash: scram.SHA512} }
		}
	}
	if cfg.Consumer.InitialOffset == "earliest" {
		sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	for _, opt := range opts {
		opt(sc)
	}
	if err := sc.Validate(); err != nil {
		return nil, fmt.Errorf("sarama config: %w", err)
	}
	return sc, nil
}

// OpenClient connects a client that producers, consumers and admins can
// share.
func OpenClient(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.Client, error) {
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.Client, error) {
		return sarama.NewClient(cfg.Brokers, sc)
	})
}

// OpenSyncProducer connects a synchronous producer. Return.Successes is
// enabled as sarama requires it.
func OpenSyncProducer(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.SyncProducer, error) {
	opts = append([]Option{func(sc *sarama.Config) { sc.Producer.Return.Successes = true }}, opts...)
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.SyncProducer, error) {
		return sarama.NewSyncProducer(cfg.Brokers, sc)
	})
}

// OpenAsyncProducer connects an asynchronous producer. Errors are returned
// on the Errors channel (sarama default); enable Return.Successes via an
// Option only if the Successes channel is drained.
func OpenAsyncProducer(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.AsyncProducer, error) {
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.AsyncProducer, error) {
		return sarama.NewAsyncProducer(cfg.Brokers, sc)
	})
}

// OpenConsumerGroup connects a consumer group for cfg.Consumer.Group. Pass
// cfg.Consumer.Topics to its Consume method.
func OpenConsumerGroup(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.ConsumerGroup, error) {
	if cfg.Consumer.Group == "" {
		return nil, errors.New("consumer.group is required")
	}
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.ConsumerGroup, error) {
		return sarama.NewConsumerGroup(cfg.Brokers, cfg.Consumer.Group, sc)
	})
}

// OpenConsumer connects a partition consumer (no group).
func OpenConsumer(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.Consumer, error) {
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.Consumer, error) {
		return sarama.NewConsumer(cfg.Brokers, sc)
	})
}

// OpenClusterAdmin connects an admin client.
func OpenClusterAdmin(ctx context.Context, cfg config.Kafka, opts ...Option) (sarama.ClusterAdmin, error) {
	return open(ctx, cfg, opts, func(sc *sarama.Config) (sarama.ClusterAdmin, error) {
		return sarama.NewClusterAdmin(cfg.Brokers, sc)
	})
}

// open builds the config and runs connect, which cannot be cancelled, in the
// background: when ctx ends first, open returns ctx.Err() and closes what
// connect returns later.
func open[T io.Closer](ctx context.Context, cfg config.Kafka, opts []Option, connect func(*sarama.Config) (T, error)) (T, error) {
	var zero T
	sc, err := Config(cfg, opts...)
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := connect(sc)
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		go func() {
			if r := <-done; r.err == nil {
				_ = r.v.Close()
			}
		}()
		return zero, fmt.Errorf("kafka %v: %w", cfg.Brokers, ctx.Err())
	}
}

// scramClient adapts github.com/xdg-go/scram to sarama.SCRAMClient.
type scramClient struct {
	hash scram.HashGeneratorFcn
	conv *scram.ClientConversation
}

func (c *scramClient) Begin(user, password, authzID string) error {
	// Kafka brokers and franz-go hash the raw bytes; SASLprep would alter or
	// reject some credentials (and print them in its error).
	client, err := c.hash.NewClientUnprepped(user, password, authzID)
	if err != nil {
		return err
	}
	c.conv = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) { return c.conv.Step(challenge) }

func (c *scramClient) Done() bool { return c.conv.Done() }
