package conn_streamload

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
)

// LoadOptions are per-load settings, passed at most once and only when
// needed.
type LoadOptions struct {
	// Label identifies the load; empty generates one. Loading a label
	// again is a no-op on the server.
	Label string
	// Format is json or csv; empty uses the configured format.
	Format string
	// Headers are load properties merged over the configured headers,
	// e.g. columns, where, partial_columns or timeout (seconds).
	Headers map[string]string
}

// Result is the server's answer to a load.
type Result struct {
	TxnID  int64  `json:"TxnId"`
	Label  string `json:"Label"`
	Status string `json:"Status"`
	// ExistingJobStatus is set when Status is "Label Already Exists".
	ExistingJobStatus    string `json:"ExistingJobStatus"`
	Message              string `json:"Message"`
	NumberTotalRows      int64  `json:"NumberTotalRows"`
	NumberLoadedRows     int64  `json:"NumberLoadedRows"`
	NumberFilteredRows   int64  `json:"NumberFilteredRows"`
	NumberUnselectedRows int64  `json:"NumberUnselectedRows"`
	LoadBytes            int64  `json:"LoadBytes"`
	LoadTimeMs           int64  `json:"LoadTimeMs"`
	// ErrorURL shows the rejected rows.
	ErrorURL string `json:"ErrorURL"`
}

// LoadError is a load, begin, commit, ... the server rejected.
type LoadError struct {
	Op    string
	Label string
	// HTTPStatus is set when the server answered with an HTTP error.
	HTTPStatus int
	Status     string
	Message    string
	ErrorURL   string
	// Result is the server's answer to a rejected load, if it sent one.
	Result *Result
}

func (e *LoadError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "stream load %s %s: ", e.Op, e.Label)
	if e.Status != "" {
		b.WriteString(e.Status)
	} else {
		fmt.Fprintf(&b, "HTTP %d", e.HTTPStatus)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	if e.ErrorURL != "" {
		b.WriteString(" (rejected rows: " + e.ErrorURL + ")")
	}
	return b.String()
}

// Load loads body into table (empty: the configured table). A load counts
// as successful when the server reports Success or Publish Timeout (the
// data is committed and becomes visible shortly), or when the label
// already loaded; Result.Status tells which. A label that is still loading
// is waited for with the retries. Body is not read after Load returns,
// unless it failed because ctx or the timeout ended.
func (c *Client) Load(ctx context.Context, table string, body io.Reader, opts ...LoadOptions) (*Result, error) {
	o, err := optional(opts)
	if err != nil {
		return nil, err
	}
	table, err = c.table(table)
	if err != nil {
		return nil, err
	}
	label := cmp.Or(o.Label, c.label(table))
	p, err := newPayload(body)
	if err != nil {
		return nil, err
	}
	return c.streamLoad(ctx, table, label, c.loadHeaders(label, o), p, nil, false)
}

// optional returns the options given at most once, or the zero value.
// Several are rejected rather than merged: no merge order would be obvious.
func optional[T any](opts []T) (T, error) {
	var zero T
	switch len(opts) {
	case 0:
		return zero, nil
	case 1:
		return opts[0], nil
	default:
		return zero, fmt.Errorf("stream load: %T given %d times, want at most one", zero, len(opts))
	}
}

// LoadJSON loads rows, a slice or array of objects, as one JSON array with
// strip_outer_array.
func (c *Client) LoadJSON(ctx context.Context, table string, rows any, opts ...LoadOptions) (*Result, error) {
	o, err := optional(opts)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("stream load: %w", err)
	}
	if len(data) == 0 || data[0] != '[' {
		return nil, errors.New("stream load: LoadJSON needs a slice or array")
	}
	o.Format = "json"
	o.Headers = withHeader(o.Headers, "strip_outer_array", "true")
	return c.Load(ctx, table, bytes.NewReader(data), o)
}

// LoadCSV loads records as lines of fields joined by the column separator
// (the column_separator header, "\t" by default, as on the server; \x
// followed by hex digits, such as Hive's \x01, stands for those bytes).
// Fields are not quoted, so a field containing the separator or a line
// break is an error, as is one that ends in part of a multi-character
// separator the next separator would complete; use Load with enclose
// settings for such data.
func (c *Client) LoadCSV(ctx context.Context, table string, records [][]string, opts ...LoadOptions) (*Result, error) {
	o, err := optional(opts)
	if err != nil {
		return nil, err
	}
	sep := separator(cmp.Or(o.Headers["column_separator"], c.cfg.Headers["column_separator"], "\t"))
	// Checking every field first gives the body's exact size, so that it
	// is built without growing.
	size := len(records)
	for i, rec := range records {
		for j, f := range rec {
			// The server cuts a line at each separator from the left: with
			// "||", "a|" then "b" reads as "a" and "|b". Only the last
			// len(sep)-1 bytes of a field can start such a separator, so
			// they are checked instead of copying the whole field.
			tail := f[len(f)-min(len(f), len(sep)-1):]
			if strings.Contains(f, sep) || strings.ContainsAny(f, "\r\n") ||
				j < len(rec)-1 && strings.Index(tail+sep, sep) < len(tail) {
				return nil, fmt.Errorf("stream load: record %d field %d contains the separator or a line break, or runs into the separator after it", i, j)
			}
			if j > 0 {
				size += len(sep)
			}
			size += len(f)
		}
	}
	var b bytes.Buffer
	b.Grow(size)
	for _, rec := range records {
		for j, f := range rec {
			if j > 0 {
				b.WriteString(sep)
			}
			b.WriteString(f)
		}
		b.WriteByte('\n')
	}
	o.Format = "csv"
	return c.Load(ctx, table, bytes.NewReader(b.Bytes()), o)
}

// separator returns the bytes a column_separator stands for: like Doris and
// StarRocks, \x (or \X) followed by hex digits means those bytes. Anything
// else, including invalid hex the server will reject, is taken as is.
func separator(s string) string {
	if len(s) > 2 && (s[:2] == `\x` || s[:2] == `\X`) {
		if b, err := hex.DecodeString(s[2:]); err == nil {
			return string(b)
		}
	}
	return s
}

func withHeader(h map[string]string, k, v string) map[string]string {
	h = maps.Clone(h)
	if h == nil {
		h = map[string]string{}
	}
	if _, ok := h[k]; !ok {
		h[k] = v
	}
	return h
}

func (c *Client) loadHeaders(label string, opts LoadOptions) http.Header {
	h := http.Header{}
	for k, v := range c.cfg.Headers {
		h.Set(k, v)
	}
	for k, v := range opts.Headers {
		h.Set(k, v)
	}
	h.Set("format", cmp.Or(opts.Format, c.cfg.Format))
	h.Set("label", label)
	return h
}

// streamLoad runs a load (with two_phase_commit when twoPC), retrying with
// the same label.
func (c *Client) streamLoad(ctx context.Context, table, label string, h http.Header, p *payload, pinned *url.URL, twoPC bool) (*Result, error) {
	path := "/api/" + c.cfg.Database + "/" + table + "/_stream_load"
	var res *Result
	err := c.retry(ctx, "load", label, pinned, func(ctx context.Context, fe *url.URL) (bool, error) {
		resp, err := c.send(ctx, fe, http.MethodPut, path, h, p)
		if err != nil {
			return !errors.Is(err, errNotRewindable), fmt.Errorf("stream load %s: %w", label, err)
		}
		var retry bool
		res, retry, err = loadResult(label, resp, twoPC)
		return retry, err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// loadResult interprets a load response.
func loadResult(label string, resp *response, twoPC bool) (*Result, bool, error) {
	if resp.status != http.StatusOK {
		return nil, resp.status >= 500, &LoadError{Op: "load", Label: label, HTTPStatus: resp.status, Message: snippet(resp.body)}
	}
	var res Result
	if err := json.Unmarshal(resp.body, &res); err != nil {
		return nil, false, &LoadError{Op: "load", Label: label, HTTPStatus: resp.status, Message: "invalid response: " + snippet(resp.body)}
	}
	res.Label = cmp.Or(res.Label, label)
	switch strings.ToLower(res.Status) {
	case "success", "publish timeout", "ok":
		return &res, false, nil
	case "label already exists":
		switch strings.ToUpper(res.ExistingJobStatus) {
		case "FINISHED", "VISIBLE", "COMMITTED":
			return &res, false, nil
		case "PRECOMMITTED", "PREPARED":
			if twoPC {
				return &res, false, nil
			}
		case "RUNNING", "PREPARE", "LOADING":
			// Still loading (possibly an earlier attempt): wait and ask again.
			return nil, true, loadError("load", label, &res)
		}
	}
	return nil, false, loadError("load", label, &res)
}

func loadError(op, label string, res *Result) *LoadError {
	e := &LoadError{Op: op, Label: label, Status: res.Status, Message: res.Message, ErrorURL: res.ErrorURL, Result: res}
	if res.ExistingJobStatus != "" {
		e.Message = strings.TrimSpace("existing job " + res.ExistingJobStatus + ". " + e.Message)
	}
	return e
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300] + "..."
	}
	return s
}
