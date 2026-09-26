// Package conn_mqtt opens Eclipse Paho MQTT 3.1.1 clients from [config.MQTT].
//
//	c, err := conn_mqtt.Open(ctx, cfg.MQTT.MustGet("edge"))
//	defer c.Disconnect(250)
//
// Paho logs through process-wide loggers; [SetLogger] routes them to slog.
package conn_mqtt

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/linzeyan/loadconf/config"
)

// Option adjusts the Paho options after they are built from the config, e.g.
// to set OnConnect or ConnectionLost handlers.
type Option func(*mqtt.ClientOptions)

// ClientOptions converts cfg into Paho client options: Paho's defaults with
// MQTT 3.1.1, then cfg.Options, then the typed fields.
func ClientOptions(cfg config.MQTT) (*mqtt.ClientOptions, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := mqtt.NewClientOptions().SetProtocolVersion(4) // MQTT 3.1.1
	if err := config.DecodeOptions(cfg.Options, o, "servers", "client_id", "username", "password", "tls_config"); err != nil {
		return nil, err
	}
	for _, b := range cfg.Brokers {
		o.AddBroker(b)
	}

	clientID := cfg.ClientID
	if cfg.ClientIDRandomSuffix {
		suffix := make([]byte, 4)
		_, _ = rand.Read(suffix)
		if clientID != "" {
			clientID += "-"
		}
		clientID += hex.EncodeToString(suffix)
	}
	o.SetClientID(clientID)
	o.SetUsername(cfg.Username)
	o.SetPassword(cfg.Password.Value())
	if cfg.TLS.Enabled {
		tlsCfg, err := cfg.TLS.Config()
		if err != nil {
			return nil, err
		}
		o.SetTLSConfig(tlsCfg)
	}
	return o, nil
}

// New creates a client without connecting.
func New(cfg config.MQTT, opts ...Option) (mqtt.Client, error) {
	o, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(o)
	}
	return mqtt.NewClient(o), nil
}

// Open creates a client and waits until it is connected or ctx is done.
// With ConnectRetry enabled Paho keeps retrying, so give ctx a deadline.
func Open(ctx context.Context, cfg config.MQTT, opts ...Option) (mqtt.Client, error) {
	c, err := New(cfg, opts...)
	if err != nil {
		return nil, err
	}
	token := c.Connect()
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return nil, fmt.Errorf("mqtt connect %s: %w", target(cfg), err)
		}
		return c, nil
	case <-ctx.Done():
		c.Disconnect(0)
		return nil, fmt.Errorf("mqtt connect %s: %w", target(cfg), ctx.Err())
	}
}

// target describes the brokers for error messages, without the credentials
// paho accepts in broker URLs.
func target(cfg config.MQTT) string {
	brokers := make([]string, len(cfg.Brokers))
	for i, b := range cfg.Brokers {
		if u, err := url.Parse(b); err == nil {
			u.User = nil
			b = u.String()
		}
		brokers[i] = b
	}
	return fmt.Sprint(brokers)
}
