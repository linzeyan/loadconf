package etcd

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/linzeyan/loadconf/config"
)

// fakeClient serves Get from a map and lets tests push watch responses.
type fakeClient struct {
	clientv3.KV
	clientv3.Watcher

	mu      sync.Mutex
	data    map[string]string
	rev     int64
	watches chan chan clientv3.WatchResponse
	lastOps []clientv3.OpOption
}

func newFake(data map[string]string) *fakeClient {
	return &fakeClient{data: data, rev: 10, watches: make(chan chan clientv3.WatchResponse, 4)}
}

func (f *fakeClient) Get(_ context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	op := clientv3.OpGet(key, opts...)
	resp := &clientv3.GetResponse{Header: &pb.ResponseHeader{Revision: f.rev}}
	for k, v := range f.data {
		if k == key || (op.IsOptsWithPrefix() && strings.HasPrefix(k, key)) {
			resp.Kvs = append(resp.Kvs, &mvccpb.KeyValue{Key: []byte(k), Value: []byte(v)})
		}
	}
	return resp, nil
}

func (f *fakeClient) Watch(ctx context.Context, _ string, opts ...clientv3.OpOption) clientv3.WatchChan {
	f.mu.Lock()
	f.lastOps = opts
	f.mu.Unlock()
	ch := make(chan clientv3.WatchResponse)
	out := make(chan clientv3.WatchResponse)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-ch:
				if !ok {
					return
				}
				out <- resp
			}
		}
	}()
	f.watches <- ch
	return out
}

func (f *fakeClient) set(key, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = value
	f.rev++
}

type appConf struct {
	Port  int                        `config:"port"`
	MySQL config.Named[config.MySQL] `config:"mysql"`
	Redis config.Named[config.Redis] `config:"redis"`
}

func TestPrefix(t *testing.T) {
	cli := newFake(map[string]string{
		"/svc/app/port":                  "8080",
		"/svc/app/mysql/orders/host":     "db1",
		"/svc/app/mysql/orders/password": "pw",
		"/svc/app/redis/cache/addrs":     "r1:6379,r2:6379",
		"/svc/other/port":                "1",
	})
	cfg, err := config.Load[appConf](t.Context(), config.WithLogger(nil), config.From(Prefix(cli, "/svc/app/")))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 8080 || cfg.MySQL.MustGet("orders").Password.Value() != "pw" || len(cfg.Redis.MustGet("cache").Addrs) != 2 {
		t.Errorf("cfg = %+v", cfg)
	}
}

func TestKey(t *testing.T) {
	cli := newFake(map[string]string{
		"/svc/app/config.yaml": "port: 9000\nmysql: [{name: core, host: db2}]",
		"/svc/app/raw":         `{"port": 1}`,
	})
	cfg, err := config.Load[appConf](t.Context(), config.WithLogger(nil), config.From(Key(cli, "/svc/app/config.yaml")))
	if err != nil || cfg.Port != 9000 || cfg.MySQL.MustGet("core").Host != "db2" {
		t.Fatalf("cfg = %+v, %v", cfg, err)
	}
	if cfg, err := config.Load[appConf](t.Context(), config.WithLogger(nil), config.From(Key(cli, "/svc/app/raw", Format("json")))); err != nil || cfg.Port != 1 {
		t.Errorf("json key = %+v, %v", cfg, err)
	}
	if _, err := Key(cli, "/missing.yaml").Load(t.Context()); err == nil {
		t.Error("missing key should fail")
	}
	if m, err := Key(cli, "/missing.yaml", Optional()).Load(t.Context()); err != nil || m != nil {
		t.Errorf("optional missing = %v, %v", m, err)
	}
}

func TestWatchReloads(t *testing.T) {
	cli := newFake(map[string]string{"/svc/app/port": "1"})
	l := config.New[appConf](config.WithLogger(nil), config.WithDebounce(10*time.Millisecond), config.From(Prefix(cli, "/svc/app/")))
	changed := make(chan int, 4)
	l.OnChange(func(_, next *appConf) { changed <- next.Port })

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- l.Watch(ctx) }()

	ch := <-cli.watches
	cli.mu.Lock()
	op := clientv3.OpGet("", cli.lastOps...)
	cli.mu.Unlock()
	if op.Rev() != 11 {
		t.Errorf("watch should resume after the loaded revision, got rev %d", op.Rev())
	}

	cli.set("/svc/app/port", "2")
	ch <- clientv3.WatchResponse{Events: []*clientv3.Event{{Type: mvccpb.PUT}}}
	select {
	case port := <-changed:
		if port != 2 {
			t.Errorf("port = %d", port)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload")
	}

	// A broken watch (e.g. compaction) triggers a reload and a new watch.
	cli.set("/svc/app/port", "3")
	close(ch)
	select {
	case port := <-changed:
		if port != 3 {
			t.Errorf("port = %d", port)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload after broken watch")
	}
	select {
	case <-cli.watches:
	case <-time.After(5 * time.Second):
		t.Fatal("watch not re-established")
	}

	cancel()
	if err := <-done; err != nil {
		t.Errorf("watch returned %v", err)
	}
}
