// Package conn_kafka_franz opens franz-go (kgo) clients from [config.Kafka]. One
// client produces and, when consumer topics are configured, consumes.
//
//	cl, err := conn_kafka_franz.Open(ctx, cfg.Kafka.MustGet("main"),
//		kgo.DefaultProduceTopic("events"), kgo.ProducerBatchCompression(kgo.ZstdCompression()))
//	defer cl.Close()
//
// Settings beyond the typed config fields are kgo options passed in code:
// kgo has no config struct that the options of [config.Kafka] could fill.
//
// The client logs to slog.Default() and follows its level; [WithLogger]
// picks another logger.
package conn_kafka_franz

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/linzeyan/loadconf/config"
)

// Options converts cfg into kgo options. cfg.Options must be empty: kgo is
// configured through functions, so settings beyond the typed fields are
// passed as extra kgo options to [New] or [Open].
func Options(cfg config.Kafka) ([]kgo.Opt, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(cfg.Options) > 0 {
		return nil, errors.New("options apply to the sarama connector only; pass kgo options to New or Open instead")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...), WithLogger(nil)}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.Version != "" {
		v := kversion.FromString(cfg.Version)
		if v == nil {
			return nil, fmt.Errorf("unknown kafka version %q", cfg.Version)
		}
		opts = append(opts, kgo.MaxVersions(v))
	}
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	switch s := cfg.SASL; s.Mechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: s.Username, Pass: s.Password.Value()}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password.Value()}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password.Value()}.AsSha512Mechanism()))
	}

	c := cfg.Consumer
	if len(c.Topics) == 0 {
		if c.Group != "" {
			return nil, errors.New("consumer.topics is required when consumer.group is set")
		}
		return opts, nil
	}
	opts = append(opts, kgo.ConsumeTopics(c.Topics...))
	if c.InitialOffset == "earliest" {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	} else {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}
	if c.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(c.Group))
	}
	return opts, nil
}

// New creates a client without connecting. extra options are applied
// after the ones built from cfg and win on conflict.
func New(cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	opts, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	return kgo.NewClient(append(opts, extra...)...)
}

// Open creates a client and pings the brokers within cfg.PingTimeout. The
// client is closed if the ping fails.
func Open(ctx context.Context, cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	cl, err := New(cfg, extra...)
	if err != nil {
		return nil, err
	}
	if cfg.PingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.PingTimeout)
		defer cancel()
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka ping %v: %w", cfg.Brokers, err)
	}
	return cl, nil
}
