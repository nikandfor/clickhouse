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
		bs map[key]*batch

		now func() time.Time
	}

	key struct {
		creds click.Credentials
		q     string
	}

	batch struct {
		q    *click.Query
		meta click.QueryMeta

		block *click.Block

		opts []click.ClientOption

		tr tlog.Span
	}

	client struct {
		p *Batcher
		b *batch

		blocks []*click.Block

		click.Client

		creds click.Credentials
		opts  []click.ClientOption
	}
)

func New(ctx context.Context, cl click.ClientPool) (p *Batcher) {
	p = &Batcher{
		pool: cl,

		MaxRows:     1000000,
		MaxBytes:    100 << 20, // 100MiB
		MaxInterval: 1 * time.Minute,

		bs: make(map[key]*batch),

		now: time.Now,
	}

	return p
}

func (p *Batcher) Get(ctx context.Context, opts ...click.ClientOption) (_ click.Client, err error) {
	var creds click.Credentials

	for _, o := range opts {
		if o, ok := o.(click.ApplyToCredentialser); ok {
			err = o.ApplyToCredentials(&creds)
			if err != nil {
				return nil, errors.Wrap(err, "option: %v", o)
			}
		}
	}

	return &client{
		p: p,

		creds: creds,
		opts:  opts,
	}, nil
}

func (p *Batcher) Put(ctx context.Context, cl click.Client, err error) error {
	return nil
}

func (p *Batcher) Close() error {
	// TODO: flush batches

	return nil
}

func (p *Batcher) batch(ctx context.Context, c *client, q *click.Query) (b *batch, err error) {
	defer p.mu.Unlock()
	p.mu.Lock()

	k := key{
		creds: c.creds,
		q:     q.Query,
	}

	b, ok := p.bs[k]
	if ok {
		return b, nil
	}

	b, err = p.newBatch(ctx, c, q)
	if err != nil {
		return nil, errors.Wrap(err, "new batch")
	}

	p.bs[k] = b

	return b, nil
}

func (p *Batcher) newBatch(ctx context.Context, c *client, q *click.Query) (b *batch, err error) {
	tr := tlog.SpawnFromContext(ctx, "batch", "db", c.creds.Database, "query", q.Query)
	defer func() {
		if err == nil {
			return
		}

		tr.Finish("err", err, "", loc.Caller(1))
	}()

	cl, err := p.pool.Get(ctx, c.opts...)
	if err != nil {
		return nil, errors.Wrap(err, "get client")
	}

	defer func() { p.pool.Put(ctx, cl, err) }()

	meta, err := cl.SendQuery(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, "send query")
	}

	err = cl.CancelQuery(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "send cancel")
	}

	err = p.consumeResponse(ctx, cl)
	if err != nil {
		return nil, errors.Wrap(err, "get meta")
	}

	b = &batch{
		q:    q,
		meta: meta,
		block: &click.Block{
			Cols: make([]click.Column, len(meta)),
		},
		opts: c.opts,

		tr: tr,
	}

	for i, c := range meta {
		b.block.Cols[i] = click.Column{
			Name: c.Name,
			Type: c.Type,
		}
	}

	return b, nil
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

func (p *Batcher) addBlocks(ctx context.Context, batch *batch, blocks []*click.Block) (err error) {
	defer p.mu.Unlock()
	p.mu.Lock()

	rows := 0
	for _, b := range blocks {
		rows += b.Rows
	}

	batch.tr.Printw("merge blocks", "blocks", len(blocks), "rows", rows, "", tlog.IDFromContext(ctx))

	bb := batch.block

	for _, b := range blocks {
		for i, c := range b.Cols {
			bb.Cols[i].RawData = append(bb.Cols[i].RawData, c.RawData...)
		}

		bb.Rows += b.Rows
	}

	err = p.flushBatch(ctx, batch)
	if err != nil {
		return errors.Wrap(err, "flush batch")
	}

	return nil
}

func (p *Batcher) flushBatch(ctx context.Context, b *batch) (err error) {
	tr := b.tr.Spawn("flush", "rows", b.block.Rows)
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	cl, err := p.pool.Get(ctx, b.opts...)
	if err != nil {
		return errors.Wrap(err, "get client")
	}

	defer func() { _ = p.pool.Put(ctx, cl, err) }()

	meta, err := cl.SendQuery(ctx, b.q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	bb := b.block

	err = cl.SendBlock(ctx, bb, b.q.Compressed)
	if err != nil {
		return errors.Wrap(err, "send cancel")
	}

	err = cl.SendBlock(ctx, nil, b.q.Compressed)
	if err != nil {
		return errors.Wrap(err, "send cancel")
	}

	err = p.consumeResponse(ctx, cl)
	if err != nil {
		return errors.Wrap(err, "get meta")
	}

	for i := range bb.Cols {
		bb.Cols[i].RawData = bb.Cols[i].RawData[:0]
	}

	bb.Rows = 0

	b.meta = meta

	return nil
}

//

func (c *client) NextPacket(ctx context.Context) (click.ServerPacket, error) {
	if c.Client != nil {
		return c.Client.NextPacket(ctx)
	}

	return click.ServerEndOfStream, nil
}

func (c *client) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	if !q.IsInsert() {
		c.Client, err = c.p.pool.Get(ctx, c.opts...)
		if err != nil {
			return nil, errors.Wrap(err, "get client")
		}

		return c.Client.SendQuery(ctx, q)
	}

	c.b, err = c.p.batch(ctx, c, q)
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
	if c.Client != nil {
		return c.Client.CancelQuery(ctx)
	}

	c.blocks = c.blocks[:0]

	return nil
}
