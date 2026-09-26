package etcd

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/linzeyan/loadconf/config"
)

func TestKeyPathEdges(t *testing.T) {
	for in, want := range map[string][]string{
		"":                nil,
		"/":               nil,
		"port":            {"port"},
		"//a///b/":        {"a", "b"},
		"MySQL/Main/Host": {"MySQL", "Main", "Host"},
	} {
		if got := keyPath(in); !slices.Equal(got, want) {
			t.Errorf("keyPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// /app/redis next to /app/redis/cache/addrs must resolve the same way in
// whatever order etcd returns them: the deeper key wins, as with environment
// variables.
func TestSetPathDeeperKeyWinsInAnyOrder(t *testing.T) {
	for _, order := range [][][]string{{{"a"}, {"a", "b"}}, {{"a", "b"}, {"a"}}} {
		m := map[string]any{}
		for _, p := range order {
			setPath(m, p, strings.Join(p, "."))
		}
		if want := map[string]any{"a": map[string]any{"b": "a.b"}}; !reflect.DeepEqual(m, want) {
			t.Errorf("order %v: got %v, want %v", order, m, want)
		}
	}
}

func TestPrefixIgnoresBareKeyAndEmptySegments(t *testing.T) {
	cli := newFake(map[string]string{
		"/svc/app/":                  "the prefix itself",
		"/svc/app//port":             "8080",
		"/svc/app/mysql//main/host/": "db1",
	})
	m, err := Prefix(cli, "/svc/app/").Load(t.Context())
	want := map[string]any{"port": "8080", "mysql": map[string]any{"main": map[string]any{"host": "db1"}}}
	if err != nil || !reflect.DeepEqual(m, want) {
		t.Errorf("got %v, %v; want %v", m, err, want)
	}
}

func TestKeyFormatErrors(t *testing.T) {
	cli := newFake(map[string]string{"/svc/app/raw": "port: 1", "/svc/app/bad.json": "{"})
	if _, err := Key(cli, "/svc/app/raw").Load(t.Context()); err == nil || !strings.Contains(err.Error(), "cannot infer config format") {
		t.Errorf("no extension: %v", err)
	}
	if _, err := Key(cli, "/svc/app/bad.json").Load(t.Context()); err == nil || !strings.Contains(err.Error(), "parse json") {
		t.Errorf("broken document: %v", err)
	}
	// Optional covers a missing key, not a broken one: a typo in the stored
	// document must not silently fall back to defaults.
	if _, err := Key(cli, "/svc/app/bad.json", Optional()).Load(t.Context()); err == nil {
		t.Error("optional broken document loaded")
	}
}

// flakyClient fails reads on demand, e.g. after permissions were revoked.
type flakyClient struct {
	*fakeClient
	failGet atomic.Bool
}

func (c *flakyClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if c.failGet.Load() {
		return nil, errors.New("etcdserver: permission denied")
	}
	return c.fakeClient.Get(ctx, key, opts...)
}

func TestWatchStartsWithoutRevisionBeforeLoad(t *testing.T) {
	cli := newFake(map[string]string{"/p/port": "1"})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = Prefix(cli, "/p/").Watch(ctx, func() {}) }()
	<-cli.watches
	cli.mu.Lock()
	defer cli.mu.Unlock()
	// Without a loaded revision the watch starts at the current one; resuming
	// "after revision 0" would replay the whole key history.
	if op := clientv3.OpGet("", cli.lastOps...); op.Rev() != 0 {
		t.Errorf("watch before any load starts at rev %d, want the current revision (0)", op.Rev())
	}
}

// When the watch breaks and etcd then refuses reads for a reason other than a
// timeout, Watch must give up with that error instead of retrying forever in
// silence.
func TestWatchReturnsPersistentError(t *testing.T) {
	cli := &flakyClient{fakeClient: newFake(map[string]string{"/p/port": "1"})}
	done := make(chan error, 1)
	go func() { done <- Prefix(cli, "/p/").Watch(t.Context(), func() {}) }()
	ch := <-cli.watches
	cli.failGet.Store(true)
	close(ch)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("Watch = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Watch kept retrying")
	}
}

// Shutdown must not wait out the one-second retry backoff.
func TestWatchCancelDuringBackoff(t *testing.T) {
	cli := newFake(map[string]string{"/p/port": "1"})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Prefix(cli, "/p/").Watch(ctx, func() {}) }()
	close(<-cli.watches)
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Watch = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Watch did not return promptly after cancel")
	}
}

// Watch resumes from the revision of the last load, also after a broken watch,
// so no update between loading and watching is missed. Resuming from the
// store's current revision instead would skip writes made after the reload but
// before the new watch: the config would stay stale until another key under the
// prefix changed.
func TestWatchResumesAfterLoadedRevision(t *testing.T) {
	cli := newFake(map[string]string{"/svc/app/port": "1"})
	l := config.New[appConf](config.WithLogger(nil), config.WithDebounce(10*time.Millisecond), config.From(Prefix(cli, "/svc/app/")))
	changed := make(chan int, 4)
	l.OnChange(func(_, next *appConf) { changed <- next.Port })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { _ = l.Watch(ctx) }()

	ch := <-cli.watches
	cli.set("/svc/app/port", "2") // rev 11
	close(ch)                     // the watch breaks; the source triggers a reload
	select {
	case port := <-changed:
		if port != 2 {
			t.Fatalf("port = %d", port)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no reload after the broken watch")
	}
	cli.set("/svc/app/port", "3") // rev 12: after the reload, before the new watch
	select {
	case <-cli.watches:
	case <-time.After(5 * time.Second):
		t.Fatal("watch not re-established")
	}
	cli.mu.Lock()
	rev := clientv3.OpGet("", cli.lastOps...).Rev()
	cli.mu.Unlock()
	if rev > 12 {
		t.Errorf("new watch starts at rev %d, after the unloaded write at rev 12; port stays %d", rev, l.Current().Port)
	}
}
