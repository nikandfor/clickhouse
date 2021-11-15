package batcher

import (
	"context"
	"sync"
	"time"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Batcher struct {
		pool click.ClientPool

		MaxRows     int
		MaxBytes    int64
		MaxInterval time.Duration

		mu sync.Mutex
		bs map[string]*batch

		now func() time.Time

		committerCtx    context.Context
		committerCancel func()
		committerErr    error
		committerDone   chan struct{}
	}

	batch struct {
		q    *click.Query
		meta click.QueryMeta

		block *click.Block

		lastCommit time.Time
	}

	client struct {
		p *Batcher

		// if transparent
		cl click.Client

		// if not
		b *batch

		transparent bool
	}
)

var (
	_ click.ClientPool = &Batcher{}
	_ click.Client     = &client{}
)

func New(ctx context.Context, cl click.ClientPool) (p *Batcher) {
	p = &Batcher{
		pool: cl,

		MaxRows:     1000000,
		MaxBytes:    100 << 20, // 100MiB
		MaxInterval: 1 * time.Minute,

		bs: make(map[string]*batch),

		committerCtx:  ctx,
		committerDone: make(chan struct{}),

		now: time.Now,
	}

	p.committerCancel = func() { close(p.committerDone) }

	return p
}

func (p *Batcher) Get(ctx context.Context) (_ click.Client, err error) {
	return &client{
		p: p,
	}, nil
}

func (p *Batcher) Put(ctx context.Context, cl click.Client, err error) error {
	return nil
}

func (p *Batcher) Close() error {
	p.mu.Lock()
	p.committerCancel()
	p.mu.Unlock()

	<-p.committerDone

	return p.committerErr
}

func (p *Batcher) committer(ctx context.Context) {
	tr := tlog.SpawnFromContext(ctx, "batch_committer")
	defer tr.Finish()

	defer close(p.committerDone)

	ctx = tlog.ContextWithSpan(ctx, tr)

	t := time.NewTicker(5 * time.Second)

loop:
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			break loop
		}

		p.mu.Lock()

		for q, b := range p.bs {
			err := p.commitIfNeeded(ctx, b, nil)
			if err != nil {
				tr.Printw("commit batch", "query", q, "err", err)
			}
		}

		p.mu.Unlock()
	}

	defer p.mu.Unlock()
	p.mu.Lock()

	for q, b := range p.bs {
		err := p.commitBatch(ctx, b)
		if err != nil {
			tr.Printw("commit batch", "query", q, "err", err)
		}
		if p.committerErr == nil {
			p.committerErr = err
		}
	}
}

func (p *Batcher) batch(ctx context.Context, q *click.Query) (b *batch, err error) {
	defer p.mu.Unlock()
	p.mu.Lock()

	if p.committerCtx != nil {
		if p.MaxInterval != 0 {
			ctx, cancel := context.WithCancel(p.committerCtx)

			p.committerCancel = cancel

			go p.committer(ctx)
		}

		p.committerCtx = nil
	}

	b, ok := p.bs[q.Query]
	if ok {
		return b, nil
	}

	b = &batch{
		q:     q,
		block: &click.Block{},
	}

	err = p.commitIfNeeded(ctx, b, nil)
	if err != nil {
		return nil, errors.Wrap(err, "start batch")
	}

	p.bs[q.Query] = b

	return b, nil
}

func (p *Batcher) addBlock(ctx context.Context, batch *batch, b *click.Block) (err error) {
	defer p.mu.Unlock()
	p.mu.Lock()

	err = p.commitIfNeeded(ctx, batch, b)
	if err != nil {
		return
	}

	bl := batch.block

	if len(b.Cols) != len(bl.Cols) {
		return errors.New("unequal blocks structure")
	}

	for i, col := range b.Cols {
		cc := bl.Cols[i]

		if cc.Name != col.Name || cc.Type != col.Type {
			return errors.New("unequal blocks structure")
		}

		bl.Cols[i].RawData = append(cc.RawData, col.RawData...)
	}

	bl.Rows += b.Rows

	return nil
}

func (p *Batcher) commitIfNeeded(ctx context.Context, batch *batch, b *click.Block) (err error) {
	bl := batch.block

	now := p.now()

	if !(now.After(batch.lastCommit.Add(p.MaxInterval)) ||
		b != nil &&
			(p.MaxRows != 0 && bl.Rows+b.Rows > p.MaxRows ||
				p.MaxBytes != 0 && 0x28+bl.DataSize()+b.DataSize() > p.MaxBytes)) {
		return nil
	}

	err = p.commitBatch(ctx, batch)
	if err != nil {
		return errors.Wrap(err, "commit batch")
	}

	batch.lastCommit = now

	return nil
}

func (p *Batcher) commitBatch(ctx context.Context, b *batch) (err error) {
	cl, err := p.pool.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	defer func() { p.pool.Put(ctx, cl, err) }()

	meta, err := cl.SendQuery(ctx, b.q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	tr := tlog.SpanFromContext(ctx).V("batch")
	if tr.Logger != nil {
		defer func() {
			tr.Printw("commit batch", "rows", b.block.Rows, "err", err, "from", loc.Callers(1, 3))
		}()
	}

	// update meta anyway
	defer func() {
		b.meta = meta

		bl := b.block
		bl.Rows = 0

		if len(bl.Cols) != len(meta) {
			bl.Cols = meta
		}

		for i, col := range meta {
			var buf []byte

			if i < len(bl.Cols) {
				buf = bl.Cols[i].RawData
			}

			bl.Cols[i] = click.Column{
				Name:    col.Name,
				Type:    col.Type,
				RawData: buf[:0],
			}
		}
	}()

	if b.block.Rows == 0 {
		err = cl.CancelQuery(ctx)
		if err != nil {
			return errors.Wrap(err, "send cancel")
		}
	} else {
		err = cl.SendBlock(ctx, b.block, b.q.Compressed)
		if err != nil {
			return errors.Wrap(err, "send block")
		}

		err = cl.SendBlock(ctx, nil, b.q.Compressed)
		if err != nil {
			return errors.Wrap(err, "send block")
		}
	}

	err = p.consumeResponse(ctx, cl)
	if err != nil {
		return errors.Wrap(err, "get meta")
	}

	return nil
}

func (p *Batcher) consumeResponse(ctx context.Context, cl click.Client) (err error) {
	for {
		tp, err := cl.NextPacket(ctx)
		if err != nil {
			return err
		}

		switch tp {
		case click.ServerEndOfStream:
			return nil
		default:
			return errors.New("unexpected packet: %x", tp)
		}
	}
}

//

func (c *client) Hello(ctx context.Context) error {
	return nil
}

func (c *client) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	if c.transparent {
		return c.cl.NextPacket(ctx)
	}

	return click.ServerEndOfStream, nil
}

func (c *client) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	c.transparent = !q.IsInsert()

	if c.transparent {
		c.cl, err = c.p.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "get client")
		}

		return c.cl.SendQuery(ctx, q)
	}

	c.b, err = c.p.batch(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "batch")
	}

	return c.b.meta, nil
}

func (c *client) CancelQuery(ctx context.Context) (err error) {
	return c.cl.CancelQuery(ctx)
}

func (c *client) RecvBlock(ctx context.Context, compr bool) (b *click.Block, err error) {
	if c.transparent {
		return c.cl.RecvBlock(ctx, compr)
	}

	panic("nea")
}

func (c *client) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	if c.transparent {
		return c.cl.SendBlock(ctx, b, compr)
	}

	if b.IsEmpty() {
		return nil
	}

	return c.p.addBlock(ctx, c.b, b)
}

func (c *client) RecvException(ctx context.Context) error {
	return c.cl.RecvException(ctx)
}

func (c *client) RecvProgress(ctx context.Context) (click.Progress, error) {
	return c.cl.RecvProgress(ctx)
}

func (c *client) RecvProfileInfo(ctx context.Context) (click.ProfileInfo, error) {
	return c.cl.RecvProfileInfo(ctx)
}

func (c *client) Close() error { panic("nah") }
