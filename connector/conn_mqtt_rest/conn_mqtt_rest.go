// Package conn_mqtt_rest talks to MQTT brokers over their HTTP APIs, built from
// [config.MQTTREST]:
//
//   - [EMQX] covers the EMQX v5 REST API (5.x and 6.x): publish, clients,
//     subscriptions, topics, retained messages, nodes and stats.
//   - [Generic] publishes to any HTTP endpoint whose JSON body is described in
//     the config, and exposes Do for other calls.
//
// [Broker] and [Admin] are the provider-neutral interfaces; implement Admin
// for another broker's management API to plug it in.
//
//	c, err := conn_mqtt_rest.NewEMQX(cfg.MQTTREST.MustGet("emqx"))
//	res, err := c.Publish(ctx, conn_mqtt_rest.Message{Topic: "a/b", Payload: []byte("hi"), QoS: 1})
package conn_mqtt_rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// Message is an MQTT message to publish.
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	// Properties are MQTT 5 publish properties (EMQX only).
	Properties *Properties
}

// Properties are MQTT 5 publish properties.
type Properties struct {
	PayloadFormatIndicator *int              `json:"payload_format_indicator,omitempty"`
	MessageExpiry          time.Duration     `json:"-"`
	ResponseTopic          string            `json:"response_topic,omitempty"`
	CorrelationData        string            `json:"correlation_data,omitempty"`
	UserProperties         map[string]string `json:"user_properties,omitempty"`
	ContentType            string            `json:"content_type,omitempty"`
}

func (p Properties) MarshalJSON() ([]byte, error) {
	type plain Properties
	out := struct {
		plain
		MessageExpiryInterval int64 `json:"message_expiry_interval,omitempty"`
	}{plain: plain(p), MessageExpiryInterval: int64(p.MessageExpiry / time.Second)}
	return json.Marshal(out)
}

// PublishResult describes the outcome of one published message.
type PublishResult struct {
	// ID is the broker's message id when the message was dispatched.
	ID string
	// NoSubscribers reports that no subscriber matched. The message was
	// still published (and stored if retained).
	NoSubscribers bool
	// ReasonCode and Message carry the broker's reason, if any.
	ReasonCode int
	Message    string
	// Err is set for a message of a batch that failed.
	Err error
}

// Broker publishes messages over HTTP.
type Broker interface {
	Publish(ctx context.Context, msg Message) (PublishResult, error)
	// PublishBatch returns one result per message, in order. The error is
	// non-nil if the request failed or any message failed.
	PublishBatch(ctx context.Context, msgs []Message) ([]PublishResult, error)
	// Do sends a raw JSON request relative to the API base URL and decodes a
	// 2xx response into out.
	Do(ctx context.Context, method, path string, query url.Values, body, out any) error
}

// Admin manages clients, subscriptions, topics and retained messages.
type Admin interface {
	ListClients(ctx context.Context, q ClientQuery) (*Page[Client], error)
	GetClient(ctx context.Context, clientID string) (*Client, error)
	// KickClient disconnects the client and discards its session.
	KickClient(ctx context.Context, clientID string) error
	ClientSubscriptions(ctx context.Context, clientID string) ([]Subscription, error)
	// Subscribe subscribes a connected client to topics on its behalf.
	Subscribe(ctx context.Context, clientID string, subs ...Subscription) error
	Unsubscribe(ctx context.Context, clientID string, topics ...string) error
	ListSubscriptions(ctx context.Context, q SubscriptionQuery) (*Page[Subscription], error)
	ListTopics(ctx context.Context, q TopicQuery) (*Page[Route], error)
	ListRetained(ctx context.Context, q RetainedQuery) (*Page[RetainedMessage], error)
	GetRetained(ctx context.Context, topic string) (*RetainedMessage, error)
	DeleteRetained(ctx context.Context, topic string) error
}

var (
	_ Broker = (*EMQX)(nil)
	_ Admin  = (*EMQX)(nil)
	_ Broker = (*Generic)(nil)
)

// New returns the client for cfg.Provider: *EMQX or *Generic. Type-assert to
// [Admin] for management calls.
func New(cfg config.MQTTREST, opts ...Option) (Broker, error) {
	if cfg.Provider == config.MQTTRESTGeneric {
		return NewGeneric(cfg, opts...)
	}
	return NewEMQX(cfg, opts...)
}

// Page is one page of a list response.
type Page[T any] struct {
	Data []T      `json:"data"`
	Meta PageMeta `json:"meta"`
}

// PageMeta describes a page. Count is nil when the broker does not compute
// it (e.g. with fuzzy filters).
type PageMeta struct {
	Page    int  `json:"page"`
	Limit   int  `json:"limit"`
	HasNext bool `json:"hasnext"`
	Count   *int `json:"count,omitempty"`
}

// PageQuery selects a page; zero values use the broker defaults.
type PageQuery struct {
	Page  int
	Limit int
}

func (p PageQuery) values() url.Values {
	v := url.Values{}
	if p.Page > 0 {
		v.Set("page", strconv.Itoa(p.Page))
	}
	if p.Limit > 0 {
		v.Set("limit", strconv.Itoa(p.Limit))
	}
	return v
}

// ClientQuery filters ListClients. Zero fields are not sent.
type ClientQuery struct {
	PageQuery
	Node                       string
	ClientIDs                  []string // exact matches
	Usernames                  []string // exact matches
	IPAddress                  string
	ConnState                  string // connected, idle or disconnected
	CleanStart                 *bool
	ProtoVer                   int
	LikeClientID               string // substring match
	LikeUsername               string // substring match
	CreatedFrom, CreatedTo     time.Time
	ConnectedFrom, ConnectedTo time.Time
	// Fields limits the returned client fields.
	Fields []string
}

// Client is a client (connection or session) as reported by the broker.
type Client struct {
	ClientID       string `json:"clientid"`
	Username       string `json:"username"`
	Node           string `json:"node"`
	Connected      bool   `json:"connected"`
	IPAddress      string `json:"ip_address"`
	Port           int    `json:"port"`
	ProtoName      string `json:"proto_name"`
	ProtoVer       int    `json:"proto_ver"`
	KeepAlive      int    `json:"keepalive"`
	CleanStart     bool   `json:"clean_start"`
	ExpiryInterval int64  `json:"expiry_interval"`
	Listener       string `json:"listener"`
	IsBridge       bool   `json:"is_bridge"`
	Durable        bool   `json:"durable"`
	CreatedAt      Time   `json:"created_at"`
	ConnectedAt    Time   `json:"connected_at"`
	DisconnectedAt Time   `json:"disconnected_at"`

	SubscriptionsCnt int   `json:"subscriptions_cnt"`
	InflightCnt      int   `json:"inflight_cnt"`
	MqueueLen        int   `json:"mqueue_len"`
	MqueueDropped    int64 `json:"mqueue_dropped"`
	AwaitingRelCnt   int   `json:"awaiting_rel_cnt"`
	RecvMsg          int64 `json:"recv_msg"`
	SendMsg          int64 `json:"send_msg"`
	RecvOct          int64 `json:"recv_oct"`
	SendOct          int64 `json:"send_oct"`
}

// Subscription is a topic subscription. NL, RAP and RH are the MQTT 5
// no-local, retain-as-published and retain-handling options.
type Subscription struct {
	ClientID string `json:"clientid,omitempty"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
	NL       int    `json:"nl"`
	RAP      int    `json:"rap"`
	RH       int    `json:"rh"`
	Node     string `json:"node,omitempty"`
	Durable  bool   `json:"durable,omitempty"`
}

// SubscriptionQuery filters ListSubscriptions.
type SubscriptionQuery struct {
	PageQuery
	Node       string
	ClientID   string
	Topic      string // exact filter; use ShareGroup for shared subscriptions
	ShareGroup string
	MatchTopic string // subscriptions whose filter matches this topic
	QoS        *byte
}

// Route is a topic route.
type Route struct {
	Topic   string `json:"topic"`
	Node    string `json:"node,omitempty"`
	Session string `json:"session,omitempty"`
}

// TopicQuery filters ListTopics.
type TopicQuery struct {
	PageQuery
	Topic string
	Node  string
}

// RetainedMessage is a retained message. Payload is only filled by
// GetRetained.
type RetainedMessage struct {
	MsgID        string `json:"msgid"`
	Topic        string `json:"topic"`
	QoS          byte   `json:"qos"`
	PublishAt    Time   `json:"publish_at"`
	FromClientID string `json:"from_clientid"`
	FromUsername string `json:"from_username"`
	Payload      []byte `json:"payload"`
}

// RetainedQuery filters ListRetained.
type RetainedQuery struct {
	PageQuery
	Topic string // topic filter; wildcards allowed
}

// Time accepts RFC 3339 strings and epoch milliseconds, which EMQX versions
// use interchangeably.
type Time struct{ time.Time }

func (t *Time) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || string(b) == "null" {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			return nil
		}
		parsed, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return fmt.Errorf("parse time %q: %w", s, err)
		}
		t.Time = parsed
		return nil
	}
	ms, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("parse time %s: %w", b, err)
	}
	t.Time = time.UnixMilli(ms)
	return nil
}
