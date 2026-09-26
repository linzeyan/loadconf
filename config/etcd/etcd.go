// Package etcd provides etcd v3 configuration sources for
// github.com/linzeyan/loadconf/config. It lives in its own module so that services not
// using etcd do not depend on the etcd client.
//
//	cli, _ := clientv3.New(clientv3.Config{Endpoints: []string{"etcd:2379"}})
//	cfg, err := config.Load[AppConf](ctx, config.From(
//		config.Profile("app"),
//		etcd.Prefix(cli, "/services/app/"), // /services/app/mysql/orders/dsn -> mysql.orders.dsn
//		config.Env("APP"),
//	))
package etcd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/linzeyan/loadconf/config"
)

// Client is the part of *clientv3.Client the sources use.
type Client interface {
	clientv3.KV
	clientv3.Watcher
}

// Option configures a source.
type Option func(*source)

// Format sets the document format of a [Key] source instead of inferring it
// from the key's extension.
func Format(format string) Option { return func(s *source) { s.format = format } }

// Optional makes a missing [Key] load as empty instead of failing.
func Optional() Option { return func(s *source) { s.optional = true } }

// Timeout bounds each read (default 5s).
func Timeout(d time.Duration) Option { return func(s *source) { s.timeout = d } }

// Prefix loads every key under prefix as a tree: the rest of the key, split
// on "/", is the key path and the value is a string, e.g.
//
//	/services/app/port                     8080
//	/services/app/mysql/orders/password  s3cret
//	/services/app/redis/cache/addrs        r1:6379,r2:6379
//
// The source is watchable: any change under the prefix triggers a reload.
func Prefix(cli Client, prefix string, opts ...Option) config.WatchableSource {
	return newSource(cli, prefix, true, opts)
}

// Key loads a single key holding a whole JSON, YAML or TOML document, e.g.
// /services/app/config.yaml. The source is watchable.
func Key(cli Client, key string, opts ...Option) config.WatchableSource {
	return newSource(cli, key, false, opts)
}

func newSource(cli Client, key string, prefix bool, opts []Option) *source {
	s := &source{cli: cli, key: key, prefix: prefix, timeout: 5 * time.Second}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type source struct {
	cli      Client
	key      string
	prefix   bool
	format   string
	optional bool
	timeout  time.Duration
	rev      atomic.Int64 // revision of the last load (or compaction); Watch resumes after it
}

func (s *source) String() string {
	if s.prefix {
		return "etcd-prefix(" + s.key + ")"
	}
	return "etcd-key(" + s.key + ")"
}

func (s *source) Load(ctx context.Context) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var opts []clientv3.OpOption
	if s.prefix {
		opts = append(opts, clientv3.WithPrefix())
	}
	resp, err := s.cli.Get(ctx, s.key, opts...)
	if err != nil {
		return nil, err
	}
	s.rev.Store(resp.Header.Revision)

	if s.prefix {
		out := map[string]any{}
		for _, kv := range resp.Kvs {
			if path := keyPath(strings.TrimPrefix(string(kv.Key), s.key)); len(path) > 0 {
				setPath(out, path, string(kv.Value))
			}
		}
		return out, nil
	}

	if len(resp.Kvs) == 0 {
		if s.optional {
			return nil, nil
		}
		return nil, fmt.Errorf("key %s not found", s.key)
	}
	format := s.format
	if format == "" {
		if format, err = config.FormatOf(s.key); err != nil {
			return nil, err
		}
	}
	return config.Parse(format, resp.Kvs[0].Value)
}

// Watch reports changes until ctx is done. It resumes from the revision of
// the last load so no update between loading and watching is missed, and
// re-establishes the watch after compaction or a lost leader.
func (s *source) Watch(ctx context.Context, changed func()) error {
	for ctx.Err() == nil {
		var opts []clientv3.OpOption
		if rev := s.rev.Load(); rev > 0 {
			opts = append(opts, clientv3.WithRev(rev+1))
		}
		if s.prefix {
			opts = append(opts, clientv3.WithPrefix())
		}
		wctx, cancel := context.WithCancel(clientv3.WithRequireLeader(ctx))
		for resp := range s.cli.Watch(wctx, s.key, opts...) {
			// History before CompactRevision is gone, so the next watch starts
			// there; the reload triggered below covers the writes it cannot replay.
			if rev := resp.CompactRevision - 1; rev > s.rev.Load() {
				s.rev.Store(rev)
			}
			if err := resp.Err(); err != nil {
				break
			}
			if len(resp.Events) > 0 {
				changed()
			}
		}
		cancel()
		if ctx.Err() != nil {
			break
		}
		// The watch ended (compaction, no leader, ...): events may have been
		// missed, so reload and watch again from the revision that reload
		// stores. Jumping to the store's current revision instead would skip
		// writes made between the reload and the new watch.
		changed()
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
		if err := s.ping(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	return nil
}

// ping reads the key so that a lasting failure, e.g. revoked permissions,
// ends Watch instead of being retried forever in silence.
func (s *source) ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	_, err := s.cli.Get(ctx, s.key, clientv3.WithCountOnly())
	return err
}

func keyPath(rest string) []string {
	var path []string
	for seg := range strings.SplitSeq(rest, "/") {
		if seg != "" {
			path = append(path, seg)
		}
	}
	return path
}

func setPath(m map[string]any, path []string, value any) {
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[k] = next
		}
		m = next
	}
	if _, isMap := m[path[len(path)-1]].(map[string]any); !isMap {
		m[path[len(path)-1]] = value
	}
}
