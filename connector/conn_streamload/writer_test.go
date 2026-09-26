package conn_streamload

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWriterBatches(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) {
		var rows []map[string]any
		if err := json.Unmarshal([]byte(r.body), &rows); err != nil {
			return 200, `{"Status":"Fail","Message":"bad json"}`
		}
		return 200, success(r, len(rows))
	})
	c := newClient(t, testCfg(cl.addrs()))
	var (
		mu      sync.Mutex
		flushed []int
	)
	w, err := c.NewWriter("", WriterOptions{MaxRows: 2, OnFlush: func(res *Result, rows int, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil || int(res.NumberLoadedRows) != rows {
			t.Errorf("flush: %+v %v", res, err)
		}
		flushed = append(flushed, rows)
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := range 5 {
		if err := w.WriteJSON(ctx, map[string]int{"id": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ctx, []byte("{}")); !errors.Is(err, ErrClosed) {
		t.Errorf("write after close = %v", err)
	}
	if err := w.Close(ctx); err != nil {
		t.Errorf("second close = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(flushed) != 3 || flushed[0] != 2 || flushed[1] != 2 || flushed[2] != 1 {
		t.Errorf("flushed = %v", flushed)
	}
	be := cl.requests("be")
	if be[0].body != `[{"id":0},{"id":1}]` || be[2].body != `[{"id":4}]` || be[0].header.Get("strip_outer_array") != "true" {
		t.Errorf("bodies = %q %q", be[0].body, be[2].body)
	}
	if be[0].header.Get("label") == be[1].header.Get("label") {
		t.Error("batches must have their own labels")
	}
}

func TestWriterMaxBytesAndCSV(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, success(r, strings.Count(r.body, "\n")) })
	cfg := testCfg(cl.addrs())
	cfg.Format = "csv"
	c := newClient(t, cfg)
	w, err := c.NewWriter("", WriterOptions{MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, row := range []string{"aaaa", "bbbb", "cccc"} {
		if err := w.Write(ctx, []byte(row)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for _, r := range cl.requests("be") {
		bodies = append(bodies, r.body)
	}
	// "aaaa\nbbbb\n" is 10 bytes: flushed when reached; "cccc" on Flush.
	if strings.Join(bodies, "|") != "aaaa\nbbbb\n|cccc\n" {
		t.Errorf("bodies = %q", bodies)
	}
	if err := w.Flush(ctx); err != nil || len(cl.requests("be")) != 2 {
		t.Errorf("an empty flush must not load: %v", err)
	}
	_ = w.Close(ctx)
}

func TestWriterInterval(t *testing.T) {
	loaded := make(chan string, 4)
	cl := newCluster(t, 1, func(r req, _ int) (int, string) {
		loaded <- r.body
		return 200, success(r, 1)
	})
	c := newClient(t, testCfg(cl.addrs()))
	w, err := c.NewWriter("", WriterOptions{FlushInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(context.Background())
	if err := w.Write(context.Background(), []byte(`{"id":1}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-loaded:
		if body != `[{"id":1}]` {
			t.Errorf("body = %s", body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no interval flush")
	}
}

func TestWriterReportsIntervalFailure(t *testing.T) {
	cl := newCluster(t, 1, func(r req, _ int) (int, string) { return 200, `{"Status":"Fail","Message":"schema mismatch"}` })
	c := newClient(t, testCfg(cl.addrs()))
	failed := make(chan int, 1)
	w, _ := c.NewWriter("", WriterOptions{FlushInterval: 10 * time.Millisecond, OnFlush: func(_ *Result, rows int, err error) {
		if err != nil {
			failed <- rows
		}
	}})
	ctx := context.Background()
	_ = w.Write(ctx, []byte(`{"id":1}`))
	select {
	case rows := <-failed:
		if rows != 1 {
			t.Errorf("rows = %d", rows)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no failed flush")
	}
	err := w.Write(ctx, []byte(`{"id":2}`))
	if err == nil || !strings.Contains(err.Error(), "schema mismatch") || !strings.Contains(err.Error(), "batch of 1 rows dropped") {
		t.Errorf("next write = %v", err)
	}
	// Reported once.
	_ = w.Close(ctx)
}

func TestWriterConcurrent(t *testing.T) {
	var (
		mu   sync.Mutex
		rows int
	)
	cl := newCluster(t, 1, func(r req, _ int) (int, string) {
		var batch []json.RawMessage
		_ = json.Unmarshal([]byte(r.body), &batch)
		mu.Lock()
		rows += len(batch)
		mu.Unlock()
		return 200, success(r, len(batch))
	})
	c := newClient(t, testCfg(cl.addrs()))
	w, _ := c.NewWriter("", WriterOptions{MaxRows: 7, FlushInterval: 5 * time.Millisecond})
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range 25 {
				if err := w.WriteJSON(ctx, map[string]int{"g": g, "i": i}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if rows != 100 {
		t.Errorf("loaded %d rows, want 100", rows)
	}
}

func TestWriterLockHonorsContext(t *testing.T) {
	block := make(chan struct{})
	cl := newCluster(t, 1, func(r req, _ int) (int, string) {
		<-block
		return 200, success(r, 1)
	})
	c := newClient(t, testCfg(cl.addrs()))
	w, _ := c.NewWriter("", WriterOptions{MaxRows: 1})
	go func() { _ = w.Write(context.Background(), []byte(`{}`)) }() // loads and blocks
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := w.Write(ctx, []byte(`{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("write while loading = %v", err)
	}
	close(block)
	_ = w.Close(context.Background())
}
