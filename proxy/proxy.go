package proxy

import (
	"context"
	"io"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Proxy struct {
		pool click.ClientPool
	}
)

var (
	_ click.Server = &Proxy{}
)

func New(pool click.ClientPool) *Proxy {
	return &Proxy{
		pool: pool,
	}
}

func (p *Proxy) HandleConn(ctx context.Context, srv click.ServerConn) (err error) {
	tr := tlog.SpawnFromContext(ctx, "connection")
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	ctx = tlog.ContextWithSpan(ctx, tr)

	err = srv.Hello(ctx)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	for err == nil {
		err = p.HandleRequest(ctx, srv)
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return
}

func (p *Proxy) HandleRequest(ctx context.Context, srv click.ServerConn) (err error) {
	pk, err := srv.NextPacket(ctx)
	if err != nil {
		return errors.Wrap(err, "reading next request")
	}

	tr := tlog.SpawnFromContext(ctx, "request")
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	ctx = tlog.ContextWithSpan(ctx, tr)

	defer func() {
		if err == nil {
			return
		}

		if _, ok := err.(*click.Exception); ok {
			return // already sent
		}

		_ = srv.SendException(ctx, err)
	}()

	cl, err := p.pool.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "client")
	}

	defer func() { p.pool.Put(ctx, cl, err) }()

	switch pk {
	case click.ClientQuery:
	default:
		return errors.New("client: unexpected packet: %x", pk)
	}

	q, err := srv.RecvQuery(ctx)
	if err != nil {
		return errors.Wrap(err, "recv query")
	}

	tr.Printw("query", "query", q.Query, "compressed", q.Compressed, "qid", q.ID, "quota_key", q.QuotaKey)

	meta, err := cl.SendQuery(ctx, q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	err = srv.SendQueryMeta(ctx, meta, q.Compressed)
	if err != nil {
		return errors.Wrap(err, "send query meta")
	}

	if q.IsInsert() {
		for {
			pk, err := srv.NextPacket(ctx)
			if err != nil {
				return errors.Wrap(err, "client: recv packet")
			}

			if pk != click.ClientData {
				return errors.Wrap(err, "client: unexpected packet: %x", pk)
			}

			b, err := srv.RecvBlock(ctx, q.Compressed)
			if err != nil {
				return errors.Wrap(err, "client: recv block")
			}

			err = cl.SendBlock(ctx, b, q.Compressed)
			if err != nil {
				return errors.Wrap(err, "server: send block")
			}

			if b.IsEmpty() {
				break
			}
		}
	}

	for {
		pk, err := cl.NextPacket(ctx)
		if err != nil {
			return errors.Wrap(err, "server: recv packet")
		}

		switch pk {
		case click.ServerEndOfStream:
			err = srv.SendEndOfStream(ctx)

			// end of request
			return errors.Wrap(err, "client: send eos")
		case click.ServerData:
			b, err := cl.RecvBlock(ctx, q.Compressed)
			if err != nil {
				return errors.Wrap(err, "server: recv block")
			}

			err = srv.SendBlock(ctx, b, q.Compressed)
		case click.ServerException:
			err = cl.RecvException(ctx)
			if _, ok := err.(*click.Exception); !ok {
				return errors.Wrap(err, "server: recv exception")
			}

			err = srv.SendException(ctx, err)
		case click.ServerProgress:
			p, err := cl.RecvProgress(ctx)
			if err != nil {
				return errors.Wrap(err, "server: recv progress")
			}

			err = srv.SendProgress(ctx, p)
		case click.ServerProfileInfo:
			p, err := cl.RecvProfileInfo(ctx)
			if err != nil {
				return errors.Wrap(err, "server: recv profile info")
			}

			err = srv.SendProfileInfo(ctx, p)
		default:
			return errors.New("server: unexpected packet: %x", pk)
		}

		if err != nil {
			return errors.Wrap(err, "client: send %x", pk)
		}
	}
}
