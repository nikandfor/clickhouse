package clpool

import (
	"context"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/wire"
)

type (
	DumpPool struct {
		click.ClientPool

		Callers int
	}

	dumpClient struct {
		click.Client

		Callers int
	}
)

var _ click.ClientPool = &DumpPool{}

func NewDumpPool(p click.ClientPool, callers int) *DumpPool {
	return &DumpPool{
		ClientPool: p,
		Callers:    callers,
	}
}

func (p *DumpPool) Get(ctx context.Context, opts ...click.ClientOption) (cl click.Client, err error) {
	cl, err = p.ClientPool.Get(ctx, opts...)

	tlog.SpanFromContext(ctx).Printw("Get client", "opts", opts, "err", err, dumpFrom(p.Callers))

	return dumpClient{
		Client:  cl,
		Callers: p.Callers,
	}, nil
}

func (p *DumpPool) Put(ctx context.Context, cl click.Client, err error) (rerr error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("Put client", "err", err, "res_err", rerr, dumpFrom(p.Callers))
	}()

	return p.ClientPool.Put(ctx, cl.(dumpClient).Client, err)
}

func (p *DumpPool) Close() (err error) {
	return p.ClientPool.Close()
}

func (c dumpClient) NextPacket(ctx context.Context) (pk click.ServerPacket, err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("NextPacket", "pk", pk, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.NextPacket(ctx)
}

func (c dumpClient) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("SendQuery", "q", q, "meta", meta, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.SendQuery(ctx, q)
}

func (c dumpClient) CancelQuery(ctx context.Context) (err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("CancelQuery", "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.CancelQuery(ctx)
}

func (c dumpClient) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("SendBlock", "block", b, "compr", compr, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.SendBlock(ctx, b, compr)
}

func (c dumpClient) RecvBlock(ctx context.Context, compr bool) (b *click.Block, err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("RecvBlock", "block", b, "compr", compr, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.RecvBlock(ctx, compr)
}

func (c dumpClient) RecvException(ctx context.Context) (err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("RecvException", "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.RecvException(ctx)
}

func (c dumpClient) RecvProgress(ctx context.Context) (p click.Progress, err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("RecvProgress", "progress", p, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.RecvProgress(ctx)
}

func (c dumpClient) RecvProfileInfo(ctx context.Context) (p click.ProfileInfo, err error) {
	defer func() {
		tlog.SpanFromContext(ctx).Printw("RecvProfileInfo", "profile_info", p, "err", err, dumpFrom(c.Callers))
	}()

	return c.Client.RecvProfileInfo(ctx)
}

func dumpFrom(c int) (b tlog.RawMessage) {
	if c <= 0 {
		return nil
	}

	var e wire.Encoder

	return e.AppendKeyValue(b, "from", loc.Callers(3, c))
}
