package processor

import (
	"context"

	click "github.com/nikandfor/clickhouse"
)

type (
	Processor struct {
		pool click.ClientPool
	}

	client struct {
	}
)

var (
	_ click.ClientPool = &Processor{}
	_ click.Client     = &client{}
)

func New(cl click.ClientPool) *Processor {
	return &Processor{pool: cl}
}

func (b *Processor) Get(ctx context.Context) (_ click.Client, err error) {
	return &client{}, nil
}

func (b *Processor) Put(ctx context.Context, cl click.Client, err error) error {
	return nil
}

//

func (c *client) Hello(ctx context.Context) error {
	return nil
}

func (c *client) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	panic("nea")
}

func (c *client) SendQuery(ctx context.Context, q *click.Query) (click.QueryMeta, error) {
	panic("nea")
}

func (c *client) CancelQuery(ctx context.Context) error {
	panic("nea")
}

func (c *client) SendBlock(ctx context.Context, b *click.Block, compr bool) error {
	panic("nea")
}

func (c *client) RecvBlock(ctx context.Context) (b *click.Block, compr bool, err error) {
	panic("nea")
}

func (c *client) RecvException(context.Context) (*click.Exception, error) {
	panic("nea")
}

func (c *client) RecvProgress(context.Context) (click.Progress, error) {
	panic("nea")
}

func (c *client) RecvProfileInfo(context.Context) (click.ProfileInfo, error) {
	panic("nea")
}

func (c *client) Close() error { panic("nah") }
