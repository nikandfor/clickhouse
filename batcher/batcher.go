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
		committerDone   chan struct{}

		tr tlog.Span
	}

	batch struct {
		q    *click.Query
		meta click.QueryMeta

		block *click.Block

		lastCommit time.Time

		totalRows int

		tr tlog.Span
	}

	client struct {
		p *Batcher

		// if transparent
		click.Client

		// if not
		b *batch

		blocks []*click.Block
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

func (p *Batcher) Get(ctx context.Context, opts ...click.ClientOption) (_ click.Client, err error) {
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

	return nil
}

func (p *Batcher) committer(ctx context.Context, startedc chan struct{}) {
	tr := tlog.SpawnFromContext(ctx, "batch_committer")
	defer tr.Finish()

	ctx = tlog.ContextWithSpan(ctx, tr)

	p.tr = tr

	close(startedc)

	defer close(p.committerDone)

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()

loop:
	for {
		select {
		case <-t.C:
		case <-ctx.Done():
			break loop
		}

		p.commitOrDelete(ctx)
	}

	p.commitAll(ctx)
}

func (p *Batcher) commitOrDelete(ctx context.Context) {
	tr := tlog.SpanFromContext(ctx)

	defer p.mu.Unlock()
	p.mu.Lock()

	limit := p.now().Add(-time.Minute)

	for q, b := range p.bs {
		if b.lastCommit.Before(limit) && b.block.Rows == 0 {
			b.tr.Finish("total_rows", b.totalRows)

			delete(p.bs, q)

			continue
		}

		err := p.commitIfNeeded(ctx, b, nil, false)
		if err != nil {
			tr.Printw("commit batch", "query", q, "err", err)
		}
	}
}

func (p *Batcher) commitAll(ctx context.Context) {
	tr := tlog.SpanFromContext(ctx)

	defer p.mu.Unlock()
	p.mu.Lock()

	for q, b := range p.bs {
		err := p.commitIfNeeded(ctx, b, nil, true)
		if err != nil {
			tr.Printw("commit batch", "query", q, "err", err)
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

			c := make(chan struct{})

			go p.committer(ctx, c)

			<-c
		}

		p.committerCtx = nil
	}

	b, ok := p.bs[q.Query]
	if ok {
		return b, nil
	}

	parent := tlog.SpanFromContext(ctx)

	b = &batch{
		q:     q,
		block: &click.Block{},

		lastCommit: p.now(),

		tr: p.tr.Spawn("batch", tlog.KeyParent, parent.ID, "query", q.Query),
	}

	err = p.commitIfNeeded(ctx, b, nil, false)
	if err != nil {
		return nil, errors.Wrap(err, "start batch")
	}

	p.bs[q.Query] = b

	return b, nil
}

func (p *Batcher) addBlocks(ctx context.Context, batch *batch, blocks []*click.Block) (err error) {
	for _, b := range blocks {
		err = p.addBlock(ctx, batch, b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Batcher) addBlock(ctx context.Context, batch *batch, b *click.Block) (err error) {
	defer p.mu.Unlock()
	p.mu.Lock()

	err = p.commitIfNeeded(ctx, batch, b, false)
	if err != nil {
		return
	}

	bb := batch.block

	if len(b.Cols) != len(bb.Cols) {
		return errors.New("unequal blocks structure")
	}

	for i, col := range b.Cols {
		cc := bb.Cols[i]

		if cc.Name != col.Name || cc.Type != col.Type {
			return errors.New("unequal blocks structure")
		}

		bb.Cols[i].RawData = append(cc.RawData, col.RawData...)
	}

	bb.Rows += b.Rows

	return nil
}

func (p *Batcher) commitIfNeeded(ctx context.Context, batch *batch, b *click.Block, final bool) (err error) {
	bb := batch.block

	now := p.now()

	if !(batch.meta == nil ||
		final && bb.Rows != 0 ||
		now.After(batch.lastCommit.Add(p.MaxInterval)) ||
		b != nil &&
			(p.MaxRows != 0 && bb.Rows+b.Rows > p.MaxRows ||
				p.MaxBytes != 0 && 0x28+bb.DataSize()+b.DataSize() > p.MaxBytes)) {
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
	tr := b.tr.Spawn("batch_query", "rows", b.block.Rows, "initialize", b.meta == nil, tlog.KeyParent, tlog.IDFromContext(ctx), "from", loc.Callers(1, 3))
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	cl, err := p.pool.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	defer func() { p.pool.Put(ctx, cl, err) }()

	meta, err := cl.SendQuery(ctx, b.q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	// update meta anyway
	defer func() {
		b.meta = meta

		bb := b.block
		bb.Rows = 0

		if len(bb.Cols) != len(meta) {
			bb.Cols = meta
		}

		for i, col := range meta {
			var buf []byte

			if i < len(bb.Cols) {
				buf = bb.Cols[i].RawData
			}

			bb.Cols[i] = click.Column{
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

	b.totalRows += b.block.Rows

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

func (c *client) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	if c.Client != nil {
		return c.Client.NextPacket(ctx)
	}

	return click.ServerEndOfStream, nil
}

func (c *client) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	if !q.IsInsert() {
		c.Client, err = c.p.pool.Get(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "get client")
		}

		return c.Client.SendQuery(ctx, q)
	}

	c.b, err = c.p.batch(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "batch")
	}

	return c.b.meta, nil
}

func (c *client) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	if c.Client != nil {
		return c.Client.SendBlock(ctx, b, compr)
	}

	if b.IsEmpty() {
		return c.p.addBlocks(ctx, c.b, c.blocks)
	}

	c.blocks = append(c.blocks, b)

	return nil
}

func (c *client) CancelQuery(ctx context.Context) error {
	c.blocks = c.blocks[:0]

	return nil
}
