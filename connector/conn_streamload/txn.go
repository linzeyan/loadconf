package conn_streamload

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/linzeyan/loadconf/config"
)

// Txn is a two-phase commit load: loaded data becomes visible on Commit and
// is dropped on Abort. A Txn is not safe for concurrent use.
//
// Doris runs one load per transaction (two_phase_commit); Prepare is a
// no-op there since the load leaves the transaction pre-committed. StarRocks
// uses its transaction API (/api/transaction/...) and accepts several loads
// before Prepare and Commit. A StarRocks transaction stays on the FE it
// began on, and its loads are not retried, since a repeated load inside a
// transaction would add the rows twice: on a failed load, Abort and start
// over.
//
// Commit and Abort are retried; when a retry, or a call after a failed one,
// finds the transaction already committed or aborted, the call succeeds.
type Txn struct {
	c     *Client
	table string
	label string
	opts  LoadOptions
	fe    *url.URL
	txnID int64
	loads int
	done  bool
	// sent holds the operations sent before: their answers may have been
	// lost, so finding the work done means it was done by this Txn.
	sent map[string]bool
}

// ErrTxnDone is returned by calls on a committed or aborted Txn.
var ErrTxnDone = errors.New("stream load: transaction already committed or aborted")

// Begin starts a transaction on table (empty: the configured table). opts
// apply to every load of the transaction; opts.Headers["timeout"] (seconds)
// also sets the StarRocks transaction timeout.
func (c *Client) Begin(ctx context.Context, table string, opts ...LoadOptions) (*Txn, error) {
	o, err := optional(opts)
	if err != nil {
		return nil, err
	}
	table, err = c.table(table)
	if err != nil {
		return nil, err
	}
	t := &Txn{
		c: c, table: table, label: cmp.Or(o.Label, c.label(table)), opts: o,
		fe:   c.fes[(c.next.Add(1)-1)%uint64(len(c.fes))],
		sent: map[string]bool{},
	}
	if c.starRocks() {
		h := t.txnHeaders()
		if timeout := c.loadHeaders(t.label, o).Get("timeout"); timeout != "" {
			h.Set("timeout", timeout)
		}
		res, err := t.call(ctx, "begin", "/api/transaction/begin", h, t.fe)
		if err != nil {
			return nil, err
		}
		t.txnID = res.TxnID
	}
	return t, nil
}

func (c *Client) starRocks() bool { return c.cfg.Flavor == config.StreamLoadStarRocks }

// Label returns the transaction label.
func (t *Txn) Label() string { return t.label }

// TxnID returns the server's transaction id, once known.
func (t *Txn) TxnID() int64 { return t.txnID }

// Load loads body in the transaction.
func (t *Txn) Load(ctx context.Context, body io.Reader) (*Result, error) {
	if t.done {
		return nil, ErrTxnDone
	}
	p, err := newPayload(body)
	if err != nil {
		return nil, err
	}
	h := t.c.loadHeaders(t.label, t.opts)
	if !t.c.starRocks() {
		if t.loads > 0 {
			return nil, errors.New("stream load: a Doris transaction takes one load")
		}
		h.Set("two_phase_commit", "true")
		// Doris keys the pre-committed load by label, not by FE, so the
		// load fails over like any other.
		res, err := t.c.streamLoad(ctx, t.table, t.label, h, p, nil, true)
		if err != nil {
			return nil, err
		}
		t.loads++
		t.txnID = cmp.Or(res.TxnID, t.txnID)
		return res, nil
	}

	h.Set("db", t.c.cfg.Database)
	h.Set("table", t.table)
	if t.c.cfg.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t.c.cfg.Timeout)
		defer cancel()
	}
	resp, err := t.c.send(ctx, t.fe, http.MethodPut, "/api/transaction/load", h, p)
	if err != nil {
		return nil, fmt.Errorf("stream load load %s: %w", t.label, err)
	}
	res, _, err := txnResult("load", t.label, resp, false)
	if err != nil {
		return nil, err
	}
	t.loads++
	return &res.Result, nil
}

// Prepare pre-commits a StarRocks transaction; for Doris it only checks
// that the transaction holds a load.
func (t *Txn) Prepare(ctx context.Context) error {
	if t.done {
		return ErrTxnDone
	}
	if !t.c.starRocks() {
		if t.loads == 0 {
			return errors.New("stream load: nothing loaded in the transaction")
		}
		return nil
	}
	_, err := t.call(ctx, "prepare", "/api/transaction/prepare", t.txnHeaders(), t.fe)
	return err
}

// Commit makes the loaded data visible.
func (t *Txn) Commit(ctx context.Context) error {
	if t.done {
		return ErrTxnDone
	}
	var err error
	if t.c.starRocks() {
		_, err = t.call(ctx, "commit", "/api/transaction/commit", t.txnHeaders(), t.fe)
	} else {
		if t.loads == 0 {
			return errors.New("stream load: nothing loaded in the transaction")
		}
		err = t.doris2PC(ctx, "commit")
	}
	if err == nil {
		t.done = true
	}
	return err
}

// Abort drops the transaction. Aborting a Doris transaction without a load
// does nothing.
func (t *Txn) Abort(ctx context.Context) error {
	if t.done {
		return ErrTxnDone
	}
	var err error
	switch {
	case t.c.starRocks():
		_, err = t.call(ctx, "abort", "/api/transaction/rollback", t.txnHeaders(), t.fe)
	case t.loads > 0:
		err = t.doris2PC(ctx, "abort")
	}
	if err == nil {
		t.done = true
	}
	return err
}

func (t *Txn) txnHeaders() http.Header {
	h := http.Header{}
	h.Set("label", t.label)
	h.Set("db", t.c.cfg.Database)
	h.Set("table", t.table)
	return h
}

// doris2PC commits or aborts a Doris pre-committed load, by transaction id
// when known (the label otherwise). Any FE forwards it to the master.
func (t *Txn) doris2PC(ctx context.Context, op string) error {
	h := http.Header{}
	h.Set("txn_operation", op)
	if t.txnID != 0 {
		h.Set("txn_id", strconv.FormatInt(t.txnID, 10))
	} else {
		h.Set("label", t.label)
	}
	_, err := t.call(ctx, op, "/api/"+t.c.cfg.Database+"/"+t.table+"/_stream_load_2pc", h, nil)
	return err
}

// call runs a transaction operation with retries. Doris 2PC uses PUT, the
// StarRocks transaction API POST.
func (t *Txn) call(ctx context.Context, op, path string, h http.Header, pinned *url.URL) (*txnResponse, error) {
	method := http.MethodPost
	if !t.c.starRocks() {
		method = http.MethodPut
	}
	var out *txnResponse
	err := t.c.retry(ctx, op, t.label, pinned, func(ctx context.Context, fe *url.URL) (bool, error) {
		retried := t.sent[op]
		t.sent[op] = true
		resp, err := t.c.send(ctx, fe, method, path, h, nil)
		if err != nil {
			return true, fmt.Errorf("stream load %s %s: %w", op, t.label, err)
		}
		r, retry, err := txnResult(op, t.label, resp, retried)
		out = r
		return retry, err
	})
	return out, err
}

// txnResponse covers the StarRocks transaction API ({"Status": "OK", ...})
// and Doris 2PC ({"status": "Success", "msg": ...}).
type txnResponse struct {
	Result
	Msg string `json:"msg"`
}

// txnResult interprets a transaction response. A retried operation that
// finds its work already done (label exists on begin, already committed or
// aborted) succeeds.
func txnResult(op, label string, resp *response, retried bool) (*txnResponse, bool, error) {
	if resp.status != http.StatusOK {
		return nil, resp.status >= 500, &LoadError{Op: op, Label: label, HTTPStatus: resp.status, Message: snippet(resp.body)}
	}
	var r txnResponse
	if err := json.Unmarshal(resp.body, &r); err != nil {
		return nil, false, &LoadError{Op: op, Label: label, HTTPStatus: resp.status, Message: "invalid response: " + snippet(resp.body)}
	}
	r.Label = cmp.Or(r.Label, label)
	r.Message = cmp.Or(r.Message, r.Msg)
	status := strings.ToLower(r.Status)
	switch {
	case status == "ok" || status == "success":
		return &r, false, nil
	case retried && op == "begin" && strings.Contains(strings.ReplaceAll(status, "_", " "), "label already exists"):
		return &r, false, nil
	case retried && alreadyDone(op, r.Message):
		return &r, false, nil
	}
	return nil, false, loadError(op, label, &r.Result)
}

func alreadyDone(op, msg string) bool {
	msg = strings.ToLower(msg)
	if !strings.Contains(msg, "already") {
		return false
	}
	switch op {
	case "commit":
		return strings.Contains(msg, "commit") || strings.Contains(msg, "visible")
	case "prepare":
		return strings.Contains(msg, "prepare")
	case "abort":
		return strings.Contains(msg, "abort") || strings.Contains(msg, "rollback")
	}
	return false
}
