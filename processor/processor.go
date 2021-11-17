package processor

import (
	"context"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	Processor struct {
		pool click.ClientPool

		OnQuery func(ctx context.Context, q *click.Query) (*click.Query, error)
		OnMeta  func(ctx context.Context, m click.QueryMeta) (click.QueryMeta, error)

		OnSendBlock func(ctx context.Context, b *click.Block) (*click.Block, error)
		OnRecvBlock func(ctx context.Context, b *click.Block) (*click.Block, error)
	}

	client struct {
		p *Processor

		cl click.Client
	}
)

var (
	_ click.ClientPool = &Processor{}
	_ click.Client     = &client{}
)

func New(cl click.ClientPool) *Processor {
	return &Processor{pool: cl}
}

func (p *Processor) Get(ctx context.Context, opts ...click.ClientOption) (_ click.Client, err error) {
	cl, err := p.pool.Get(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &client{
		p:  p,
		cl: cl,
	}, nil
}

func (p *Processor) Put(ctx context.Context, cl click.Client, err error) error {
	return p.pool.Put(ctx, cl.(*client).cl, err)
}

func (p *Processor) Close() (err error) {
	err = p.pool.Close()

	return errors.Wrap(err, "connections pool")
}

//

func (c *client) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	return c.cl.NextPacket(ctx)
}

func (c *client) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	if c.p.OnQuery != nil {
		q, err = c.p.OnQuery(ctx, q)
		if err != nil {
			return nil, err
		}
	}

	meta, err = c.cl.SendQuery(ctx, q)
	if err != nil {
		return
	}

	if c.p.OnMeta != nil {
		meta, err = c.p.OnMeta(ctx, meta)
		if err != nil {
			return nil, err
		}
	}

	return meta, nil
}

func (c *client) CancelQuery(ctx context.Context) error {
	return c.cl.CancelQuery(ctx)
}

func (c *client) RecvBlock(ctx context.Context, compr bool) (b *click.Block, err error) {
	b, err = c.cl.RecvBlock(ctx, compr)
	if err != nil {
		return
	}

	if c.p.OnRecvBlock != nil {
		b, err = c.p.OnRecvBlock(ctx, b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (c *client) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	if c.p.OnSendBlock != nil {
		b, err = c.p.OnSendBlock(ctx, b)
		if err != nil {
			return err
		}
	}

	return c.cl.SendBlock(ctx, b, compr)
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
