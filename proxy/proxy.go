package proxy

import (
	"context"
	"io"
	"net"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/clickhouse/binary"
	"github.com/nikandfor/clickhouse/clpool"
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

func (p *Proxy) Serve(ctx context.Context, l net.Listener) (err error) {
	tr := tlog.SpawnFromContext(ctx, "binary_proxy")
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	for {
		conn, err := l.Accept()
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		if err != nil {
			return errors.Wrap(err, "accept")
		}

		go p.HandleConn(ctx, conn)
	}
}

func (p *Proxy) HandleConn(ctx context.Context, conn net.Conn) (err error) {
	tr := tlog.SpawnFromContext(ctx, "connection")
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	ctx = tlog.ContextWithSpan(ctx, tr)

	if tr.If("dump_server,dump_conn") {
		dc := binary.ConnDump(conn, tr)
		dc.Callers = 5
		conn = dc
	}

	defer conn.Close()

	srv := binary.NewServerConn(conn)

	srv.Server.Name = "gh/nikandfor/clickhouse"

	srv.Auth = nil // TODO

	err = srv.Hello(ctx)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	var clopts []click.ClientOption

	//	clopts = append(clopts, clpool.WithDatabase(srv.Database))
	clopts = append(clopts, clpool.WithCredentials(srv.Database, srv.User, srv.Password))

	for err == nil {
		err = p.HandleRequest(ctx, srv, clopts...)
	}

	if errors.Is(err, io.EOF) {
		return nil
	}

	return
}

func (p *Proxy) HandleRequest(ctx context.Context, srv click.ServerConn, clopts ...click.ClientOption) (err error) {
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

	cl, err := p.pool.Get(ctx, clopts...)
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
		err = p.sendData(ctx, srv, cl, q)
		if err != nil {
			return errors.Wrap(err, "send client data")
		}
	}

	err = p.recvResponse(ctx, srv, cl, q)
	if err != nil {
		return errors.Wrap(err, "recv response")
	}

	return nil
}

func (p *Proxy) sendData(ctx context.Context, srv click.ServerConn, cl click.Client, q *click.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)

	for {
		pk, err := srv.NextPacket(ctx)
		if err != nil {
			return errors.Wrap(err, "client: recv packet")
		}

		switch pk {
		case click.ClientData:
		case click.ClientCancel:
			err = cl.CancelQuery(ctx)
			if err != nil {
				return errors.Wrap(err, "send cancel")
			}

			return nil
		default:
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
			return nil
		}

		tr.V("blocks").Printw("client block", "rows", b.Rows)
	}
}

func (p *Proxy) recvResponse(ctx context.Context, srv click.ServerConn, cl click.Client, q *click.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)

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

			if b.Rows != 0 {
				tr.V("blocks").Printw("server block", "rows", b.Rows)
			}
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

func (p *Proxy) Close() (err error) {
	err = p.pool.Close()

	return errors.Wrap(err, "connections pool")
}
