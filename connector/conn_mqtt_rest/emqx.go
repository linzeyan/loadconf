package conn_mqtt_rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/linzeyan/loadconf/config"
)

const emqxAPIPath = "/api/v5"

// EMQX reason codes returned by publish.
const (
	ReasonNoMatchingSubscribers = 16
	ReasonFailedToDispatch      = 131
)

// EMQX is a client for the EMQX v5 REST API.
type EMQX struct {
	http *httpClient
}

// NewEMQX creates an EMQX client. cfg.BaseURL is the dashboard/API root such
// as http://emqx:18083; /api/v5 is appended unless already present.
func NewEMQX(cfg config.MQTTREST, opts ...Option) (*EMQX, error) {
	if cfg.Provider == "" {
		cfg.Provider = config.MQTTRESTEMQX
	}
	if cfg.Provider != config.MQTTRESTEMQX {
		return nil, fmt.Errorf("provider %q is not emqx", cfg.Provider)
	}
	c, err := newHTTPClient(cfg, emqxAPIPath, opts)
	if err != nil {
		return nil, err
	}
	return &EMQX{http: c}, nil
}

// Do sends a raw request relative to /api/v5.
func (e *EMQX) Do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	return e.http.Do(ctx, method, path, query, body, out)
}

type emqxPublish struct {
	Topic           string      `json:"topic"`
	Payload         string      `json:"payload"`
	PayloadEncoding string      `json:"payload_encoding"`
	QoS             byte        `json:"qos"`
	Retain          bool        `json:"retain"`
	Properties      *Properties `json:"properties,omitempty"`
}

func toEMQXPublish(m Message) emqxPublish {
	p := emqxPublish{Topic: m.Topic, QoS: m.QoS, Retain: m.Retain, Properties: m.Properties}
	if utf8.Valid(m.Payload) {
		p.Payload, p.PayloadEncoding = string(m.Payload), "plain"
	} else {
		p.Payload, p.PayloadEncoding = base64.StdEncoding.EncodeToString(m.Payload), "base64"
	}
	return p
}

// emqxPublishResult is either {"id"} or {"reason_code","message"}.
type emqxPublishResult struct {
	ID         string `json:"id"`
	ReasonCode int    `json:"reason_code"`
	Message    string `json:"message"`
}

func (r emqxPublishResult) result(status int) PublishResult {
	out := PublishResult{ID: r.ID, ReasonCode: r.ReasonCode, Message: r.Message}
	switch r.ReasonCode {
	case 0:
	case ReasonNoMatchingSubscribers:
		out.NoSubscribers = true
	default:
		out.Err = &APIError{StatusCode: status, ReasonCode: r.ReasonCode, Message: r.Message}
	}
	return out
}

// Publish publishes one message. A message nobody subscribed to is not an
// error: the result has NoSubscribers set. Publishes are never retried.
func (e *EMQX) Publish(ctx context.Context, msg Message) (PublishResult, error) {
	status, data, err := e.http.roundTrip(ctx, http.MethodPost, "/publish", nil, toEMQXPublish(msg))
	if err != nil {
		return PublishResult{}, err
	}
	if status != http.StatusOK && status != http.StatusAccepted {
		return PublishResult{}, apiError(status, data)
	}
	var r emqxPublishResult
	if err := json.Unmarshal(data, &r); err != nil {
		return PublishResult{}, fmt.Errorf("decode publish response: %w", err)
	}
	res := r.result(status)
	return res, res.Err
}

// PublishBatch publishes messages in one request. If any message is invalid
// EMQX publishes none and an error is returned without results.
func (e *EMQX) PublishBatch(ctx context.Context, msgs []Message) ([]PublishResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	body := make([]emqxPublish, len(msgs))
	for i, m := range msgs {
		body[i] = toEMQXPublish(m)
	}
	status, data, err := e.http.roundTrip(ctx, http.MethodPost, "/publish/bulk", nil, body)
	if err != nil {
		return nil, err
	}
	var items []emqxPublishResult
	if (status != http.StatusOK && status != http.StatusAccepted && status != http.StatusServiceUnavailable) ||
		json.Unmarshal(data, &items) != nil {
		return nil, apiError(status, data)
	}
	results := make([]PublishResult, len(items))
	var errs []error
	for i, it := range items {
		results[i] = it.result(http.StatusServiceUnavailable)
		if results[i].Err != nil {
			errs = append(errs, fmt.Errorf("message %d (%s): %w", i, msgs[min(i, len(msgs)-1)].Topic, results[i].Err))
		}
	}
	return results, errors.Join(errs...)
}

// ListClients lists clients.
func (e *EMQX) ListClients(ctx context.Context, q ClientQuery) (*Page[Client], error) {
	v := q.values()
	setIf(v, "node", q.Node)
	for _, id := range q.ClientIDs {
		v.Add("clientid", id)
	}
	for _, u := range q.Usernames {
		v.Add("username", u)
	}
	setIf(v, "ip_address", q.IPAddress)
	setIf(v, "conn_state", q.ConnState)
	if q.CleanStart != nil {
		v.Set("clean_start", strconv.FormatBool(*q.CleanStart))
	}
	if q.ProtoVer > 0 {
		v.Set("proto_ver", strconv.Itoa(q.ProtoVer))
	}
	setIf(v, "like_clientid", q.LikeClientID)
	setIf(v, "like_username", q.LikeUsername)
	setTime(v, "gte_created_at", q.CreatedFrom)
	setTime(v, "lte_created_at", q.CreatedTo)
	setTime(v, "gte_connected_at", q.ConnectedFrom)
	setTime(v, "lte_connected_at", q.ConnectedTo)
	if len(q.Fields) > 0 {
		v.Set("fields", strings.Join(q.Fields, ","))
	}
	return getPage[Client](ctx, e.http, "/clients", v)
}

// GetClient returns one client; errors.Is(err, ErrNotFound) if unknown.
func (e *EMQX) GetClient(ctx context.Context, clientID string) (*Client, error) {
	var c Client
	if err := e.http.Do(ctx, http.MethodGet, "/clients/"+segment(clientID), nil, nil, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// KickClient disconnects a client and discards its session.
func (e *EMQX) KickClient(ctx context.Context, clientID string) error {
	return e.http.Do(ctx, http.MethodDelete, "/clients/"+segment(clientID), nil, nil, nil)
}

// KickClients disconnects several clients in one request.
func (e *EMQX) KickClients(ctx context.Context, clientIDs ...string) error {
	if len(clientIDs) == 0 {
		return nil
	}
	return e.http.Do(ctx, http.MethodPost, "/clients/kickout/bulk", nil, clientIDs, nil)
}

// ClientSubscriptions lists the subscriptions of a client.
func (e *EMQX) ClientSubscriptions(ctx context.Context, clientID string) ([]Subscription, error) {
	var subs []Subscription
	if err := e.http.Do(ctx, http.MethodGet, "/clients/"+segment(clientID)+"/subscriptions", nil, nil, &subs); err != nil {
		return nil, err
	}
	return subs, nil
}

type emqxSubscribe struct {
	Topic string `json:"topic"`
	QoS   byte   `json:"qos"`
	NL    int    `json:"nl"`
	RAP   int    `json:"rap"`
	RH    int    `json:"rh"`
}

// Subscribe subscribes a connected client on its behalf. EMQX applies it
// asynchronously and bypasses ACL checks.
func (e *EMQX) Subscribe(ctx context.Context, clientID string, subs ...Subscription) error {
	path := "/clients/" + segment(clientID) + "/subscribe"
	switch len(subs) {
	case 0:
		return nil
	case 1:
		s := subs[0]
		return e.http.Do(ctx, http.MethodPost, path, nil, emqxSubscribe{s.Topic, s.QoS, s.NL, s.RAP, s.RH}, nil)
	}
	body := make([]emqxSubscribe, len(subs))
	for i, s := range subs {
		body[i] = emqxSubscribe{s.Topic, s.QoS, s.NL, s.RAP, s.RH}
	}
	// All succeeded: an array. Partial failure: still 200 with an object.
	var raw json.RawMessage
	if err := e.http.Do(ctx, http.MethodPost, path+"/bulk", nil, body, &raw); err != nil {
		return err
	}
	var partial struct {
		Failed []struct {
			Data   json.RawMessage `json:"data"`
			Reason string          `json:"reason"`
		} `json:"failed"`
	}
	if json.Unmarshal(raw, &partial) == nil && len(partial.Failed) > 0 {
		errs := make([]error, len(partial.Failed))
		for i, f := range partial.Failed {
			errs[i] = fmt.Errorf("subscribe %s: %s", f.Data, f.Reason)
		}
		return errors.Join(errs...)
	}
	return nil
}

// Unsubscribe unsubscribes a connected client from topics.
func (e *EMQX) Unsubscribe(ctx context.Context, clientID string, topics ...string) error {
	path := "/clients/" + segment(clientID) + "/unsubscribe"
	type topic struct {
		Topic string `json:"topic"`
	}
	switch len(topics) {
	case 0:
		return nil
	case 1:
		return e.http.Do(ctx, http.MethodPost, path, nil, topic{topics[0]}, nil)
	}
	body := make([]topic, len(topics))
	for i, t := range topics {
		body[i] = topic{t}
	}
	return e.http.Do(ctx, http.MethodPost, path+"/bulk", nil, body, nil)
}

// ListSubscriptions lists subscriptions across clients.
func (e *EMQX) ListSubscriptions(ctx context.Context, q SubscriptionQuery) (*Page[Subscription], error) {
	v := q.values()
	setIf(v, "node", q.Node)
	setIf(v, "clientid", q.ClientID)
	setIf(v, "topic", q.Topic)
	setIf(v, "share_group", q.ShareGroup)
	setIf(v, "match_topic", q.MatchTopic)
	if q.QoS != nil {
		v.Set("qos", strconv.Itoa(int(*q.QoS)))
	}
	return getPage[Subscription](ctx, e.http, "/subscriptions", v)
}

// ListTopics lists topic routes.
func (e *EMQX) ListTopics(ctx context.Context, q TopicQuery) (*Page[Route], error) {
	v := q.values()
	setIf(v, "topic", q.Topic)
	setIf(v, "node", q.Node)
	return getPage[Route](ctx, e.http, "/topics", v)
}

// ListRetained lists retained messages without payloads (built-in database
// backend only).
func (e *EMQX) ListRetained(ctx context.Context, q RetainedQuery) (*Page[RetainedMessage], error) {
	v := q.values()
	setIf(v, "topic", q.Topic)
	return getPage[RetainedMessage](ctx, e.http, "/mqtt/retainer/messages", v)
}

// GetRetained returns the retained message of a topic, with payload.
func (e *EMQX) GetRetained(ctx context.Context, topic string) (*RetainedMessage, error) {
	var m RetainedMessage
	if err := e.http.Do(ctx, http.MethodGet, "/mqtt/retainer/message/"+segment(topic), nil, nil, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DeleteRetained deletes the retained message of a topic; a wildcard filter
// deletes all matching messages.
func (e *EMQX) DeleteRetained(ctx context.Context, topic string) error {
	return e.http.Do(ctx, http.MethodDelete, "/mqtt/retainer/message/"+segment(topic), nil, nil, nil)
}

// Node describes a cluster node.
type Node struct {
	Node             string  `json:"node"`
	NodeStatus       string  `json:"node_status"`
	Role             string  `json:"role"`
	Version          string  `json:"version"`
	Edition          string  `json:"edition"`
	Uptime           int64   `json:"uptime"` // milliseconds
	Connections      int64   `json:"connections"`
	LiveConnections  int64   `json:"live_connections"`
	Load1            float64 `json:"load1"`
	Load5            float64 `json:"load5"`
	Load15           float64 `json:"load15"`
	MemoryTotal      string  `json:"memory_total"`
	MemoryUsed       string  `json:"memory_used"`
	MaxFDs           int64   `json:"max_fds"`
	ProcessAvailable int64   `json:"process_available"`
	ProcessUsed      int64   `json:"process_used"`
}

// Nodes lists the cluster nodes.
func (e *EMQX) Nodes(ctx context.Context) ([]Node, error) {
	var nodes []Node
	if err := e.http.Do(ctx, http.MethodGet, "/nodes", nil, nil, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// Stats returns cluster-wide statistics keyed like "connections.count".
func (e *EMQX) Stats(ctx context.Context) (map[string]int64, error) {
	var stats map[string]int64
	if err := e.http.Do(ctx, http.MethodGet, "/stats", url.Values{"aggregate": {"true"}}, nil, &stats); err != nil {
		return nil, err
	}
	return stats, nil
}

func getPage[T any](ctx context.Context, h *httpClient, path string, v url.Values) (*Page[T], error) {
	var page Page[T]
	if err := h.Do(ctx, http.MethodGet, path, v, nil, &page); err != nil {
		return nil, err
	}
	return &page, nil
}

// segment escapes a client id or topic for use as one path segment. EMQX
// decodes each segment after splitting on "/", so "/" must become %2F; "+"
// is escaped too so it is never read as a space.
func segment(s string) string {
	return strings.ReplaceAll(url.PathEscape(s), "+", "%2B")
}

func setIf(v url.Values, key, value string) {
	if value != "" {
		v.Set(key, value)
	}
}

func setTime(v url.Values, key string, t time.Time) {
	if !t.IsZero() {
		v.Set(key, t.Format(time.RFC3339))
	}
}

// loginAuth exchanges dashboard credentials for a bearer token and renews it
// when EMQX reports it expired or unknown.
type loginAuth struct {
	client     *httpClient
	user, pass string

	mu    sync.Mutex
	token string
}

func (a *loginAuth) apply(ctx context.Context, req *http.Request) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token == "" {
		if err := a.login(ctx); err != nil {
			return err
		}
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	return nil
}

func (a *loginAuth) login(ctx context.Context) error {
	payload, _ := json.Marshal(map[string]string{"username": a.user, "password": a.pass})
	status, data, _, err := a.client.send(ctx, http.MethodPost, a.client.base+"/login", payload, false)
	if err != nil {
		return fmt.Errorf("emqx login: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("emqx login: %w", apiError(status, data))
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &resp); err != nil || resp.Token == "" {
		return errors.New("emqx login: response has no token")
	}
	a.token = resp.Token
	return nil
}

func (a *loginAuth) renew(_ context.Context, sent http.Header, e *APIError) bool {
	if e.Code != "TOKEN_TIME_OUT" && e.Code != "BAD_TOKEN" {
		return false
	}
	a.mu.Lock()
	// Every request in flight at expiry comes back rejected; only the first
	// drops the token, or each would throw away the one just renewed.
	if sent.Get("Authorization") == "Bearer "+a.token {
		a.token = ""
	}
	a.mu.Unlock()
	return true
}
