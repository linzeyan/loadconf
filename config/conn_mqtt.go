package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
)

// MQTT configures an MQTT 3.1.1 client connection (Eclipse Paho).
//
// Options sets any other Paho setting by the snake_case name of its
// mqtt.ClientOptions field, e.g. clean_session, order, connect_timeout,
// max_reconnect_interval, resume_subs, keep_alive (in seconds) or a last will
// with will_enabled, will_topic, will_payload, will_qos and will_retained;
// see [DecodeOptions]. Unset settings keep the Paho defaults.
type MQTT struct {
	// Brokers are URLs such as tcp://host:1883, ssl://host:8883, ws://host/mqtt
	// or wss://host/mqtt.
	Brokers  []string `config:"brokers"`
	ClientID string   `config:"client_id"`
	// ClientIDRandomSuffix appends a random suffix to ClientID so that
	// replicas of a service do not kick each other off the broker.
	ClientIDRandomSuffix bool   `config:"client_id_random_suffix"`
	Username             string `config:"username"`
	Password             Secret `config:"password"`

	TLS     TLS            `config:"tls"`
	Options map[string]any `config:"options"`
}

var mqttSchemes = []string{"tcp", "mqtt", "ssl", "tls", "mqtts", "tcps", "ws", "wss", "unix"}

func (m MQTT) Validate() error {
	var errs []error
	if len(m.Brokers) == 0 {
		errs = append(errs, errors.New("brokers is required"))
	}
	// Brokers may carry credentials, so they are printed without userinfo;
	// one that does not parse is not printed at all, as its userinfo cannot
	// be located.
	for i, b := range m.Brokers {
		u, err := url.Parse(b)
		if err != nil {
			errs = append(errs, fmt.Errorf("brokers[%d] must be a URL such as tcp://host:1883", i))
			continue
		}
		// Paho prefixes "tcp://" to a broker without "://", so "tcp:b" or
		// "unix:/x.sock" would be dropped or dialed at the wrong host. Only a
		// unix socket has no host.
		if !slices.Contains(mqttSchemes, strings.ToLower(u.Scheme)) || !strings.Contains(b, "://") || (u.Host == "" && u.Scheme != "unix") {
			u.User = nil
			errs = append(errs, fmt.Errorf("broker %q must be a URL such as tcp://host:1883", u.String()))
		}
	}
	return errors.Join(errs...)
}

// MQTT REST providers.
const (
	MQTTRESTEMQX    = "emqx"
	MQTTRESTGeneric = "generic"
)

// MQTTREST configures a broker's HTTP API: EMQX v5 (publish plus client,
// subscription, topic and retained message management) or a generic HTTP
// publish endpoint.
type MQTTREST struct {
	// Provider is emqx or generic.
	Provider string `config:"provider" default:"emqx"`
	// BaseURL is the API root, e.g. http://emqx:18083 (EMQX adds /api/v5) or
	// https://gateway.example.com/mqtt for a generic endpoint.
	BaseURL string            `config:"base_url"`
	Auth    HTTPAuth          `config:"auth"`
	Headers map[string]string `config:"headers"`
	Timeout time.Duration     `config:"timeout" default:"10s"`
	// MaxRetries retries idempotent requests (GET, DELETE) on network
	// errors, 429 and 5xx responses. Publishes are never retried.
	MaxRetries   int           `config:"max_retries"   default:"2"`
	RetryBackoff time.Duration `config:"retry_backoff" default:"200ms"`
	TLS          TLS           `config:"tls"`

	// Publish describes the publish endpoint of the generic provider.
	Publish MQTTRESTPublish `config:"publish"`
}

// HTTP auth types.
const (
	HTTPAuthNone   = "none"
	HTTPAuthBasic  = "basic"
	HTTPAuthBearer = "bearer"
	// HTTPAuthLogin exchanges Username/Password for a bearer token via the
	// EMQX dashboard login and renews it when it expires (EMQX only).
	HTTPAuthLogin = "login"
)

// HTTPAuth configures request authentication.
type HTTPAuth struct {
	// Type is none, basic, bearer or login. When empty it is basic if
	// Username is set, bearer if Token is set, else none.
	Type string `config:"type"`
	// Username and Password are the basic credentials; for EMQX use an API
	// key and secret.
	Username string `config:"username"`
	Password Secret `config:"password"`
	Token    Secret `config:"token"`
}

// ResolvedType returns Type, or the inferred type when Type is empty.
func (a HTTPAuth) ResolvedType() string {
	switch {
	case a.Type != "":
		return a.Type
	case a.Username != "":
		return HTTPAuthBasic
	case a.Token.Value() != "":
		return HTTPAuthBearer
	default:
		return HTTPAuthNone
	}
}

// MQTTRESTPublish describes a generic HTTP publish endpoint. The request body
// is a JSON object built from the field names below.
type MQTTRESTPublish struct {
	Method string `config:"method" default:"POST"`
	Path   string `config:"path"`
	// BatchPath accepts a JSON array of bodies; when empty, batches are sent
	// one message at a time.
	BatchPath    string `config:"batch_path"`
	TopicField   string `config:"topic_field"   default:"topic"`
	PayloadField string `config:"payload_field" default:"payload"`
	QoSField     string `config:"qos_field"     default:"qos"`
	RetainField  string `config:"retain_field"  default:"retain"`
	// PayloadEncoding is plain or base64.
	PayloadEncoding string `config:"payload_encoding" default:"plain"`
	// EncodingField, when set, carries the PayloadEncoding value.
	EncodingField string `config:"encoding_field"`
	// Extra holds static fields added to every body.
	Extra map[string]any `config:"extra"`
}

func (m MQTTREST) Validate() error {
	var errs []error
	// base_url may carry credentials: printed without userinfo, or not at all
	// when it does not parse and its userinfo cannot be located.
	u, err := url.Parse(m.BaseURL)
	switch {
	case err != nil:
		errs = append(errs, errors.New("base_url must be an http(s) URL"))
	case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		u.User = nil
		errs = append(errs, fmt.Errorf("base_url must be an http(s) URL (got %q)", u.String()))
	}
	switch m.Provider {
	case MQTTRESTEMQX:
	case MQTTRESTGeneric:
		if m.Publish.Path == "" {
			errs = append(errs, errors.New("publish.path is required for the generic provider"))
		}
		if !slices.Contains([]string{"", "plain", "base64"}, m.Publish.PayloadEncoding) {
			errs = append(errs, fmt.Errorf("unsupported publish.payload_encoding %q", m.Publish.PayloadEncoding))
		}
	default:
		errs = append(errs, fmt.Errorf("unsupported provider %q (want emqx or generic)", m.Provider))
	}
	switch auth := m.Auth.ResolvedType(); auth {
	case HTTPAuthNone:
	case HTTPAuthBasic, HTTPAuthLogin:
		if m.Auth.Username == "" {
			errs = append(errs, fmt.Errorf("auth.username is required for %s auth", auth))
		}
		if auth == HTTPAuthLogin && m.Provider != MQTTRESTEMQX {
			errs = append(errs, errors.New("login auth is only supported by the emqx provider"))
		}
	case HTTPAuthBearer:
		if m.Auth.Token.Value() == "" {
			errs = append(errs, errors.New("auth.token is required for bearer auth"))
		}
	default:
		errs = append(errs, fmt.Errorf("unsupported auth.type %q", m.Auth.Type))
	}
	return errors.Join(errs...)
}
