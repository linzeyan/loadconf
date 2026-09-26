package conn_streamload

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestDorisTwoPhaseCommit(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		switch {
		case strings.HasSuffix(r.path, "/_stream_load"):
			return 200, success(r, 1)
		case r.header.Get("txn_operation") == "commit" && n == 1:
			return 503, "master changing"
		case r.header.Get("txn_operation") == "commit":
			// The first attempt went through after all.
			return 200, `{"status":"Fail","msg":"transaction [42] is already visible, not pre-committed."}`
		default:
			return 200, `{"status":"Success","msg":"transaction [42] abort successfully."}`
		}
	})
	c := newClient(t, testCfg(cl.addrs()))
	ctx := context.Background()

	tx, err := c.Begin(ctx, "", LoadOptions{Label: "tx1"})
	if err != nil || len(cl.requests("")) != 0 {
		t.Fatalf("Doris Begin must not call the server: %v", err)
	}
	if err := tx.Prepare(ctx); err == nil {
		t.Error("Prepare before a load should fail")
	}
	res, err := tx.Load(ctx, strings.NewReader(`[{"id":1}]`))
	if err != nil || res.TxnID != 42 || tx.TxnID() != 42 {
		t.Fatalf("load: %+v %v", res, err)
	}
	if _, err := tx.Load(ctx, strings.NewReader(`[]`)); err == nil {
		t.Error("a second Doris load should fail")
	}
	if err := tx.Prepare(ctx); err != nil {
		t.Error(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, ErrTxnDone) {
		t.Errorf("second commit = %v", err)
	}

	be := cl.requests("be")
	if h := be[0].header; h.Get("two_phase_commit") != "true" || h.Get("label") != "tx1" {
		t.Errorf("load headers = %v", h)
	}
	for _, r := range be[1:] {
		if r.method != http.MethodPut || r.path != "/api/db/events/_stream_load_2pc" || r.header.Get("txn_id") != "42" || r.auth() != "root:pw" {
			t.Errorf("2pc request = %s %s %v", r.method, r.path, r.header)
		}
	}

	// Abort without a load is a no-op; with one it calls the server.
	tx, _ = c.Begin(ctx, "")
	if err := tx.Abort(ctx); err != nil || len(cl.requests("be")) != 3 {
		t.Errorf("abort without load: %v", err)
	}
	tx, _ = c.Begin(ctx, "")
	if _, err := tx.Load(ctx, strings.NewReader(`[]`)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if last := cl.requests("be")[4]; last.header.Get("txn_operation") != "abort" {
		t.Errorf("abort request = %v", last.header)
	}
}

func TestStarRocksTransaction(t *testing.T) {
	var loads int
	cl := newCluster(t, 2, func(r req, n int) (int, string) {
		switch r.path {
		case "/api/transaction/begin":
			return 200, `{"Status":"OK","Label":"` + r.header.Get("label") + `","TxnId":7}`
		case "/api/transaction/load":
			loads++
			if strings.Contains(r.body, "fail") {
				return 500, "internal error"
			}
			return 200, `{"Status":"OK","NumberLoadedRows":1,"LoadBytes":10}`
		case "/api/transaction/prepare", "/api/transaction/commit", "/api/transaction/rollback":
			return 200, `{"Status":"OK","Message":""}`
		}
		return 404, "unknown"
	})
	cfg := testCfg(cl.addrs())
	cfg.Flavor = "starrocks"
	c := newClient(t, cfg)
	ctx := context.Background()

	tx, err := c.Begin(ctx, "orders", LoadOptions{Label: "sr1", Headers: map[string]string{"timeout": "600"}})
	if err != nil || tx.TxnID() != 7 {
		t.Fatalf("begin: %v", err)
	}
	for _, body := range []string{`[{"id":1}]`, `[{"id":2}]`} {
		if res, err := tx.Load(ctx, strings.NewReader(body)); err != nil || res.NumberLoadedRows != 1 {
			t.Fatalf("load: %+v %v", res, err)
		}
	}
	if err := tx.Prepare(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	be := cl.requests("be")
	var paths []string
	for _, r := range be {
		paths = append(paths, r.method+" "+r.path)
		if r.header.Get("label") != "sr1" || r.header.Get("db") != "db" || r.header.Get("table") != "orders" || r.auth() != "root:pw" {
			t.Errorf("%s headers = %v", r.path, r.header)
		}
	}
	want := "POST /api/transaction/begin,PUT /api/transaction/load,PUT /api/transaction/load,POST /api/transaction/prepare,POST /api/transaction/commit"
	if strings.Join(paths, ",") != want {
		t.Errorf("requests = %v", paths)
	}
	if be[0].header.Get("timeout") != "600" || be[1].header.Get("format") != "json" {
		t.Errorf("begin/load headers = %v / %v", be[0].header, be[1].header)
	}
	// Every request of the transaction went through the FE it began on.
	if a, b := len(cl.requests("fe0")), len(cl.requests("fe1")); a*b != 0 || a+b != 5 {
		t.Errorf("fe0 = %d fe1 = %d requests", a, b)
	}

	// A failed load inside a transaction is not retried.
	tx, _ = c.Begin(ctx, "orders")
	before := loads
	if _, err := tx.Load(ctx, strings.NewReader("fail")); err == nil {
		t.Fatal("load should fail")
	}
	if loads-before != 1 {
		t.Errorf("load attempts = %d", loads-before)
	}
	if err := tx.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if last := cl.requests("be"); last[len(last)-1].path != "/api/transaction/rollback" {
		t.Errorf("abort path = %s", last[len(last)-1].path)
	}
	if _, err := tx.Load(ctx, strings.NewReader("[]")); !errors.Is(err, ErrTxnDone) {
		t.Errorf("load after abort = %v", err)
	}
}

func TestStarRocksBeginRetryAcceptsOwnLabel(t *testing.T) {
	cl := newCluster(t, 1, func(r req, n int) (int, string) {
		if n == 0 {
			return 502, "bad gateway"
		}
		return 200, `{"Status":"LABEL_ALREADY_EXISTS","Message":"label sr2 already exists","TxnId":9}`
	})
	cfg := testCfg(cl.addrs())
	cfg.Flavor = "starrocks"
	tx, err := newClient(t, cfg).Begin(context.Background(), "", LoadOptions{Label: "sr2"})
	if err != nil || tx.TxnID() != 9 {
		t.Fatalf("begin: %v", err)
	}

	cl.mu.Lock()
	cl.handle = func(req, int) (int, string) {
		return 200, `{"Status":"FAILED","Message":"label sr3 already exists"}`
	}
	cl.mu.Unlock()
	_, err = newClient(t, cfg).Begin(context.Background(), "", LoadOptions{Label: "sr3"})
	var le *LoadError
	if !errors.As(err, &le) || le.Op != "begin" || le.Status != "FAILED" {
		t.Errorf("first-attempt conflict must fail: %v", err)
	}
}
