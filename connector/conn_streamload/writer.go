package conn_streamload

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// WriterOptions configure a Writer. Zero limits use the configured batch
// settings.
type WriterOptions struct {
	// LoadOptions apply to every batch; the label is generated per batch.
	LoadOptions
	MaxRows       int
	MaxBytes      int64
	FlushInterval time.Duration
	// OnFlush is called after each batch load with its result, row count
	// and error. It runs while the Writer is locked and must not call it.
	OnFlush func(res *Result, rows int, err error)
}

// Writer batches rows into loads, flushing when a batch reaches MaxRows or
// MaxBytes, every FlushInterval, and on Flush and Close. Loads run in the
// goroutine that triggers them, so a Write that fills a batch waits for its
// load. A batch that fails after the retries is dropped: its error is
// passed to OnFlush and returned by the Write or Flush that loaded it, or,
// for an interval flush, by the next call. A Writer is safe for concurrent
// use.
type Writer struct {
	c     *Client
	table string
	opts  WriterOptions
	json  bool

	mu     chanMutex
	buf    bytes.Buffer
	rows   int
	err    error
	closed bool

	stop chan struct{}
	done chan struct{}
}

// chanMutex is a mutex whose Lock can give up when a context ends.
type chanMutex chan struct{}

func (m chanMutex) lock(ctx context.Context) error {
	select {
	case m <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m chanMutex) unlock() { <-m }

// ErrClosed is returned by calls on a closed Writer.
var ErrClosed = errors.New("stream load: writer closed")

// NewWriter returns a Writer loading into table (empty: the configured
// table). JSON rows are loaded as one array per batch (strip_outer_array),
// CSV rows as lines.
func (c *Client) NewWriter(table string, options ...WriterOptions) (*Writer, error) {
	opts, err := optional(options)
	if err != nil {
		return nil, err
	}
	table, err = c.table(table)
	if err != nil {
		return nil, err
	}
	opts.MaxRows = cmp.Or(opts.MaxRows, c.cfg.Batch.MaxRows)
	opts.MaxBytes = cmp.Or(opts.MaxBytes, int64(c.cfg.Batch.MaxBytes))
	opts.FlushInterval = cmp.Or(opts.FlushInterval, c.cfg.Batch.FlushInterval)
	opts.Format = cmp.Or(opts.Format, c.cfg.Format)
	opts.Label = ""
	w := &Writer{c: c, table: table, opts: opts, json: opts.Format == "json", mu: make(chanMutex, 1)}
	if w.json {
		w.opts.Headers = withHeader(opts.Headers, "strip_outer_array", "true")
	}
	if opts.FlushInterval > 0 {
		w.stop, w.done = make(chan struct{}), make(chan struct{})
		go w.loop()
	}
	return w, nil
}

func (w *Writer) loop() {
	defer close(w.done)
	t := time.NewTicker(w.opts.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			if w.mu.lock(context.Background()) != nil {
				continue
			}
			if !w.closed && w.rows > 0 {
				if err := w.flush(context.Background()); err != nil {
					w.err = errors.Join(w.err, err)
				}
			}
			w.mu.unlock()
		}
	}
}

// Write adds one row: a JSON object for the json format, a line without
// its line break for csv.
func (w *Writer) Write(ctx context.Context, row []byte) error {
	if err := w.mu.lock(ctx); err != nil {
		return err
	}
	defer w.mu.unlock()
	if w.closed {
		return ErrClosed
	}
	err := w.takeErr()
	// A row adds its line break, or its comma and the closing bracket.
	grow := len(row) + 1
	if w.json {
		grow++
	}
	if w.rows > 0 && w.opts.MaxBytes > 0 && int64(w.buf.Len()+grow) > w.opts.MaxBytes {
		err = errors.Join(err, w.flush(ctx))
	}
	switch {
	case !w.json:
		w.buf.Write(row)
		w.buf.WriteByte('\n')
	case w.rows == 0:
		w.buf.WriteByte('[')
		w.buf.Write(row)
	default:
		w.buf.WriteByte(',')
		w.buf.Write(row)
	}
	w.rows++
	if w.opts.MaxRows > 0 && w.rows >= w.opts.MaxRows ||
		w.opts.MaxBytes > 0 && int64(w.buf.Len()) >= w.opts.MaxBytes {
		err = errors.Join(err, w.flush(ctx))
	}
	return err
}

// WriteJSON adds v, marshaled to JSON, as one row.
func (w *Writer) WriteJSON(ctx context.Context, v any) error {
	row, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("stream load: %w", err)
	}
	return w.Write(ctx, row)
}

// Flush loads the buffered rows.
func (w *Writer) Flush(ctx context.Context) error {
	if err := w.mu.lock(ctx); err != nil {
		return err
	}
	defer w.mu.unlock()
	if w.closed {
		return ErrClosed
	}
	return errors.Join(w.takeErr(), w.flush(ctx))
}

// Close loads the buffered rows and stops the interval flushes.
func (w *Writer) Close(ctx context.Context) error {
	if err := w.mu.lock(ctx); err != nil {
		return err
	}
	if w.closed {
		w.mu.unlock()
		return nil
	}
	w.closed = true
	err := errors.Join(w.takeErr(), w.flush(ctx))
	w.mu.unlock()
	if w.stop != nil {
		close(w.stop)
		<-w.done
	}
	return err
}

func (w *Writer) takeErr() error {
	err := w.err
	w.err = nil
	return err
}

// flush loads the batch; the caller holds the lock.
func (w *Writer) flush(ctx context.Context) error {
	if w.rows == 0 {
		return nil
	}
	if w.json {
		w.buf.WriteByte(']')
	}
	rows := w.rows
	res, err := w.c.Load(ctx, w.table, bytes.NewReader(w.buf.Bytes()), w.opts.LoadOptions)
	if err != nil {
		// Load may have stopped waiting for the transport to let go of the
		// batch (ctx ended): leave it the old buffer.
		w.buf = bytes.Buffer{}
	}
	w.buf.Reset()
	w.rows = 0
	if w.opts.OnFlush != nil {
		w.opts.OnFlush(res, rows, err)
	}
	if err != nil {
		return fmt.Errorf("stream load: batch of %d rows dropped: %w", rows, err)
	}
	return nil
}
