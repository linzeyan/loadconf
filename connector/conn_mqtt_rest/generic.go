package conn_mqtt_rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/linzeyan/loadconf/config"
)

// Generic publishes to an HTTP endpoint described by cfg.Publish, e.g. an
// in-house gateway or another broker's publish API. Other API calls go
// through Do with the configured auth, headers, TLS and retries.
type Generic struct {
	http *httpClient
	pub  config.MQTTRESTPublish
}

// NewGeneric creates a generic client.
func NewGeneric(cfg config.MQTTREST, opts ...Option) (*Generic, error) {
	if cfg.Provider != config.MQTTRESTGeneric {
		return nil, fmt.Errorf("provider %q is not generic", cfg.Provider)
	}
	c, err := newHTTPClient(cfg, "", opts)
	if err != nil {
		return nil, err
	}
	pub := cfg.Publish
	if pub.Method == "" {
		pub.Method = "POST"
	}
	return &Generic{http: c, pub: pub}, nil
}

// Do sends a raw request relative to the base URL.
func (g *Generic) Do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	return g.http.Do(ctx, method, path, query, body, out)
}

func (g *Generic) body(m Message) (map[string]any, error) {
	p := g.pub
	body := maps.Clone(p.Extra)
	if body == nil {
		body = map[string]any{}
	}
	set := func(field string, v any) {
		if field != "" {
			body[field] = v
		}
	}
	set(p.TopicField, m.Topic)
	switch {
	case p.PayloadEncoding == "base64":
		set(p.PayloadField, base64.StdEncoding.EncodeToString(m.Payload))
	case !utf8.Valid(m.Payload):
		// A JSON string holds UTF-8 only: encoding/json would replace the
		// other bytes with U+FFFD and the gateway would get another payload.
		return nil, errors.New("payload is not valid UTF-8; publish it with payload_encoding base64")
	default:
		set(p.PayloadField, string(m.Payload))
	}
	set(p.QoSField, m.QoS)
	set(p.RetainField, m.Retain)
	set(p.EncodingField, p.PayloadEncoding)
	return body, nil
}

// Publish sends one message. Any 2xx response is a success; an "id" field in
// a JSON response is reported as the result ID. With payload_encoding plain,
// a payload that is not UTF-8 is refused.
func (g *Generic) Publish(ctx context.Context, msg Message) (PublishResult, error) {
	body, err := g.body(msg)
	if err != nil {
		return PublishResult{}, err
	}
	// Not Do: it fails on a 2xx body that is not JSON, such as "accepted".
	status, data, err := g.http.roundTrip(ctx, strings.ToUpper(g.pub.Method), g.pub.Path, nil, body)
	if err != nil {
		return PublishResult{}, err
	}
	if status < 200 || status > 299 {
		return PublishResult{}, apiError(status, data)
	}
	return PublishResult{ID: responseID(data)}, nil
}

// PublishBatch sends all messages to BatchPath as a JSON array, or one by one
// when no BatchPath is configured.
func (g *Generic) PublishBatch(ctx context.Context, msgs []Message) ([]PublishResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if g.pub.BatchPath != "" {
		bodies := make([]map[string]any, len(msgs))
		for i, m := range msgs {
			var err error
			if bodies[i], err = g.body(m); err != nil {
				return nil, fmt.Errorf("message %d (%s): %w", i, m.Topic, err)
			}
		}
		if err := g.http.Do(ctx, strings.ToUpper(g.pub.Method), g.pub.BatchPath, nil, bodies, nil); err != nil {
			return nil, err
		}
		return make([]PublishResult, len(msgs)), nil
	}

	results := make([]PublishResult, len(msgs))
	var errs []error
	for i, m := range msgs {
		res, err := g.Publish(ctx, m)
		if err != nil {
			res.Err = err
			errs = append(errs, fmt.Errorf("message %d (%s): %w", i, m.Topic, err))
		}
		results[i] = res
	}
	return results, errors.Join(errs...)
}

func responseID(raw json.RawMessage) string {
	var v struct {
		ID any `json:"id"`
	}
	if json.Unmarshal(raw, &v) != nil || v.ID == nil {
		return ""
	}
	return fmt.Sprint(v.ID)
}
