package logger

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linzeyan/loadconf/config"
)

// esSink ships records to Elasticsearch or OpenSearch with the bulk API.
// Records are queued and sent in batches by a background goroutine; when the
// queue is full, records are dropped and counted.
type esSink struct {
	hc       *http.Client
	addrs    []string
	next     atomic.Uint32
	auth     string
	headers  map[string]string
	index    []indexPart
	compress bool
	timeout  time.Duration
	batch    int
	interval time.Duration
	onError  func(error)

	mu      sync.RWMutex
	closed  bool
	queue   chan esDoc
	dropped atomic.Int64
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

type esDoc struct {
	index string
	doc   []byte
}

// indexPart is a literal or, when layout is set, a date.
type indexPart struct {
	text   string
	layout string
}

func parseIndex(tmpl, service string) ([]indexPart, error) {
	var parts []indexPart
	for tmpl != "" {
		i := strings.IndexByte(tmpl, '{')
		if i < 0 {
			parts = append(parts, indexPart{text: tmpl})
			break
		}
		j := strings.IndexByte(tmpl[i:], '}')
		if j < 0 {
			return nil, fmt.Errorf("unterminated { in index %q", tmpl)
		}
		parts = append(parts, indexPart{text: tmpl[:i]})
		switch expr := tmpl[i+1 : i+j]; {
		case expr == "service":
			parts = append(parts, indexPart{text: service})
		case strings.HasPrefix(expr, "date:") && len(expr) > len("date:"):
			parts = append(parts, indexPart{layout: expr[len("date:"):]})
		default:
			return nil, fmt.Errorf("unknown placeholder {%s} in index (want {service} or {date:LAYOUT})", expr)
		}
		tmpl = tmpl[i+j+1:]
	}
	return parts, nil
}

func formatIndex(parts []indexPart, t time.Time) string {
	var b strings.Builder
	for _, p := range parts {
		if p.layout != "" {
			b.WriteString(t.UTC().Format(p.layout))
		} else {
			b.WriteString(p.text)
		}
	}
	return strings.ToLower(b.String())
}

func openElasticsearch(_ context.Context, out config.LogOutput, env SinkEnv) (Sink, error) {
	index, err := parseIndex(cmp.Or(out.Index, "logs-{service}-default"), env.Service)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := out.TLS.Config()
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = tlsCfg
	s := &esSink{
		hc:       &http.Client{Transport: tr},
		addrs:    make([]string, len(out.Addresses)),
		headers:  out.Headers,
		index:    index,
		compress: out.Compress,
		timeout:  cmp.Or(out.Timeout, 5*time.Second),
		batch:    cmp.Or(out.BatchSize, 500),
		interval: cmp.Or(out.FlushInterval, time.Second),
		onError:  env.OnError,
		queue:    make(chan esDoc, cmp.Or(out.QueueSize, 10000)),
		done:     make(chan struct{}),
	}
	for i, a := range out.Addresses {
		s.addrs[i] = strings.TrimRight(a, "/")
	}
	switch {
	case out.APIKey.Value() != "":
		s.auth = "ApiKey " + out.APIKey.Value()
	case out.Username != "":
		s.auth = "Basic " + base64.StdEncoding.EncodeToString([]byte(out.Username+":"+out.Password.Value()))
	}
	if s.onError == nil {
		s.onError = func(error) {}
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	go s.run()
	return s, nil
}

func (s *esSink) Write(b []byte) (int, error) {
	// The document is rebuilt from the record's members as written, which
	// keeps numbers exact and the key order without decoding the values.
	rec, ms, dup, err := parseRecord(b, make([]member, 0, 32))
	if err != nil {
		return 0, fmt.Errorf("elasticsearch: %w", err)
	}
	t := rec.Time
	if t.IsZero() {
		t = time.Now()
	}
	doc := make([]byte, 0, len(b)+64)
	doc = append(doc, '{')
	// Data streams require a top-level @timestamp.
	if !slices.ContainsFunc(ms, func(m member) bool { return isKey(m.key, "@timestamp") }) {
		doc = append(doc, `"@timestamp":"`...)
		doc = t.UTC().AppendFormat(doc, time.RFC3339Nano)
		doc = append(doc, '"')
	}
	for i, m := range ms {
		// Elasticsearch rejects a document with a duplicate key, which slog
		// writes for an attr named like another attr or a static field.
		if dup && !kept(ms, i) {
			continue
		}
		if len(doc) > 1 {
			doc = append(doc, ',')
		}
		doc = append(append(doc, m.key...), ':')
		// ECS makes host an object (host.name), and the logs-*-* data streams
		// of Elasticsearch 9 enforce it, rejecting the host string that the
		// hostname setting adds. Other outputs keep the plain string.
		if m.val[0] == '"' && isKey(m.key, "host") {
			doc = append(append(append(doc, `{"name":`...), m.val...), '}')
		} else {
			doc = append(doc, m.val...)
		}
	}
	doc = append(doc, '}')

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("elasticsearch: sink closed")
	}
	select {
	case s.queue <- esDoc{index: formatIndex(s.index, t), doc: doc}:
	default:
		s.dropped.Add(1)
	}
	return len(b), nil
}

func (s *esSink) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	batch := make([]esDoc, 0, s.batch)
	flush := func() {
		if len(batch) > 0 {
			s.flush(batch)
			batch = batch[:0]
		}
		if n := s.dropped.Swap(0); n > 0 {
			s.onError(fmt.Errorf("elasticsearch: dropped %d records: queue full", n))
		}
	}
	for {
		select {
		case d, ok := <-s.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, d)
			if len(batch) >= s.batch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *esSink) flush(batch []esDoc) {
	var buf bytes.Buffer
	for _, d := range batch {
		buf.WriteString(`{"create":{"_index":`)
		idx, _ := json.Marshal(d.index)
		buf.Write(idx)
		buf.WriteString("}}\n")
		buf.Write(d.doc)
		buf.WriteByte('\n')
	}
	body := buf.Bytes()
	if s.compress {
		var zbuf bytes.Buffer
		zw := gzip.NewWriter(&zbuf)
		_, _ = zw.Write(body)
		_ = zw.Close()
		body = zbuf.Bytes()
	}

	var err error
	for attempt := range 3 {
		if attempt > 0 {
			select {
			case <-time.After(time.Duration(100<<attempt) * time.Millisecond):
			case <-s.ctx.Done():
			}
			if s.ctx.Err() != nil {
				break
			}
		}
		var retry bool
		if retry, err = s.send(body, len(batch)); !retry {
			break
		}
	}
	if err != nil {
		s.onError(err)
	}
}

// send posts one bulk request and reports whether a failure is worth
// retrying.
func (s *esSink) send(body []byte, n int) (retry bool, err error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	addr := s.addrs[int(s.next.Add(1)-1)%len(s.addrs)]
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/_bulk", bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("elasticsearch: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.compress {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if s.auth != "" {
		req.Header.Set("Authorization", s.auth)
	}
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	resp, err := s.hc.Do(req)
	if err != nil {
		return s.ctx.Err() == nil, fmt.Errorf("elasticsearch: bulk %d records: %w", n, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
		return true, fmt.Errorf("elasticsearch: bulk %d records: %s: %s", n, resp.Status, truncate(data))
	}
	if resp.StatusCode >= 300 {
		return false, fmt.Errorf("elasticsearch: bulk %d records: %s: %s", n, resp.Status, truncate(data))
	}
	var result struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}
	if err := json.Unmarshal(data, &result); err != nil || !result.Errors {
		return false, nil
	}
	failed, first := 0, ""
	for _, item := range result.Items {
		for _, r := range item {
			if r.Error != nil {
				if failed == 0 {
					first = r.Error.Type + ": " + r.Error.Reason
				}
				failed++
			}
		}
	}
	return false, fmt.Errorf("elasticsearch: %d of %d records rejected, first: %s", failed, n, first)
}

func truncate(b []byte) string {
	const limit = 512
	if len(b) > limit {
		return string(b[:limit]) + "..."
	}
	return string(b)
}

// Close stops accepting records and flushes the queue, giving up after the
// timeout plus the flush interval.
func (s *esSink) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.queue)
	s.mu.Unlock()

	select {
	case <-s.done:
	case <-time.After(s.timeout + s.interval):
		s.cancel()
		<-s.done
	}
	s.cancel()
	return nil
}
