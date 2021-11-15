package proxy

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/nikandfor/errors"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/netpoll"
	"github.com/nikandfor/tlog"

	"github.com/nikandfor/clickhouse/conn"
	"github.com/nikandfor/clickhouse/dsn"
	"github.com/nikandfor/clickhouse/protocol"
	"github.com/nikandfor/clickhouse/wire"
)

type (
	Proxy struct {
		dsn *dsn.DSN

		mu sync.Mutex
		bs map[string]*batch

		Auth func(ctx context.Context, c *conn.Client) error

		ProcessQuery func(ctx context.Context, q *wire.Query) (context.Context, *wire.Query, error)
		ProcessMeta  func(ctx context.Context, m wire.QueryMeta) (context.Context, wire.QueryMeta, error)
		//	ProcessBlock func(q *wire.Query, b *wire.Block) error

		stopc chan struct{}
	}

	batch struct {
	}
)

var ErrSkipQuery = errors.New("skip query")

func New(d string) (p *Proxy, err error) {
	dd, err := dsn.Parse(d)
	if err != nil {
		return nil, errors.Wrap(err, "dsn")
	}

	p = &Proxy{
		dsn:   dd,
		bs:    make(map[string]*batch),
		stopc: make(chan struct{}),
	}

	return p, nil
}

func (p *Proxy) Shutdown() {
	close(p.stopc)
}

func (p *Proxy) ServeContext(ctx context.Context, l net.Listener) (err error) {
	tr := tlog.SpawnFromContext(ctx, "serve_listener")
	defer func() {
		tr.Finish("err", err, "", loc.Caller(1))
	}()

	ctx = tlog.ContextWithSpan(ctx, tr)

	d, ok := l.(interface {
		SetDeadline(time.Time) error
	})

	var wg sync.WaitGroup

	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.stopc:
			return nil
		default:
		}

		if ok {
			err = d.SetDeadline(time.Now().Add(100 * time.Millisecond))
			if err != nil {
				return errors.Wrap(err, "set deadline")
			}
		}

		conn, err := l.Accept()

		if errors.Is(err, os.ErrDeadlineExceeded) {
			continue
		}

		if err != nil {
			return errors.Wrap(err, "accept")
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			defer func() {
				_ = conn.Close()
			}()

			_ = p.HandleConn(ctx, conn)
		}()
	}
}

func (p *Proxy) HandleConn(ctx context.Context, c net.Conn) (err error) {
	tr := tlog.SpawnFromContext(ctx, "handle_conn", "local_addr", c.LocalAddr(), "remote_addr", c.RemoteAddr())
	defer func() {
		tr.Finish("err", err, "", loc.Caller(1))
	}()

	ctx = tlog.ContextWithSpan(ctx, tr)

	if tlog.If("dump_conn,dump_client_conn") {
		c = conn.NewDump(tr, "client", c)
	}

	s := conn.NewClient(c)

	s.Auth = p.Auth

	err = s.Hello(ctx)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	//	tr.Printw("client", "client", s)

	for err == nil {
		err = p.handleReq(ctx, s)
	}

	if errors.Is(err, io.EOF) {
		err = nil
	}

	return err
}

func (p *Proxy) handleReq(ctx context.Context, c *conn.Client) (err error) {
	err = p.waitForData(ctx, c.Conn)
	if err != nil {
		return
	}

	q, err := c.NextQuery(ctx)
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if err != nil {
		return errors.Wrap(err, "query")
	}

	tr := tlog.SpawnFromContext(ctx, "request", "query", q.Query, "compressed", q.Compressed)
	defer func() {
		tr.Finish("err", err, "", loc.Caller(1))
	}()

	ctx = tlog.ContextWithSpan(ctx, tr)

	if p.ProcessQuery != nil {
		ctx, q, err = p.ProcessQuery(ctx, q)
		if err != nil {
			return errors.Wrap(err, "process")
		}
	}

	//return p.transparentQuery(ctx, c, q)
	return p.skipQuery(ctx, c, q)

	//

	if !q.IsInsert() {
		return p.transparentQuery(ctx, c, q)
	}

	p.mu.Lock()
	b, ok := p.bs[q.Query]
	if !ok {
		b = &batch{}

		p.bs[q.Query] = b
	}
	p.mu.Unlock()

	return p.processQuery(ctx, b, c, q)
}

func (p *Proxy) waitForData(ctx context.Context, c *conn.Conn) (err error) {
	for c.Buffered() == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		_, err = netpoll.Select([]io.Reader{c.RawConn()}, 100*time.Millisecond)
		switch err {
		case nil:
		case netpoll.ErrNoEvents:
			continue
		default:
			return errors.Wrap(err, "select")
		}

		break
	}

	return nil
}

func (p *Proxy) processQuery(ctx context.Context, bc *batch, c *conn.Client, q *wire.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)
	tr.Printw("process query")

	for {
		b, err := c.RecvBlock(ctx, q.Compressed)
		if err != nil {
			return errors.Wrap(err, "block")
		}

		if b.IsEmpty() {
			break
		}

		_ = b
	}

	return nil
}

func (p *Proxy) skipQuery(ctx context.Context, c *conn.Client, q *wire.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)
	tr.Printw("skip query")

	srv, err := p.conn(ctx, c)
	if err != nil {
		return errors.Wrap(err, "connect")
	}

	defer func() { p.reuse(srv, err) }()

	defer func() {
		var exc *wire.Exception

		if errors.As(err, &exc) {
			tlog.Printw("error is exception", "err", err, "exc", exc)
			err = c.SendException(ctx, err)
		}
	}()

	meta, err := srv.SendQuery(ctx, q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	err = srv.CancelQuery(ctx)
	if err != nil {
		return errors.Wrap(err, "cancel")
	}

	err = c.SendQueryMeta(ctx, meta, q.Compressed)
	if err != nil {
		return errors.Wrap(err, "resp meta")
	}

	if !q.IsInsert() {
		err = c.SendEndOfStream(ctx)
		if err != nil {
			return errors.Wrap(err, "send oes")
		}

		return nil
	}

	for {
		tp, err := c.RecvPacket()
		tr.V("packets").Printw("client packet", "tp", tlog.Hex(tp), "err", err)
		if err != nil {
			return errors.Wrap(err, "recv")
		}

		switch tp {
		case protocol.ClientData:
			b, err := c.RecvBlock(ctx, q.Compressed)
			if err != nil {
				return errors.Wrap(err, "recv block")
			}

			tr.V("blocks").Printw("client block", "cols", len(b.Cols), "rows", b.Rows)

			if b.Rows == 0 && len(b.Cols) == 0 {
				err = c.SendEndOfStream(ctx)
				if err != nil {
					return errors.Wrap(err, "send oes")
				}

				return nil
			}
		default:
			return errors.New("unexpected packet: %x", tp)
		}
	}

	return nil
}

func (p *Proxy) transparentQuery(ctx context.Context, cl *conn.Client, q *wire.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)
	tr.Printw("transparent query")

	srv, err := p.conn(ctx, cl)
	if err != nil {
		return errors.Wrap(err, "connect")
	}

	defer func() { p.reuse(srv, err) }()

	defer func() {
		var exc *wire.Exception

		if errors.As(err, &exc) {
			tlog.Printw("error is exception", "err", err, "exc", exc)
			err = cl.SendException(ctx, err)
		}
	}()

	meta, err := srv.SendQuery(ctx, q)
	if err != nil {
		return errors.Wrap(err, "send query")
	}

	if p.ProcessMeta != nil {
		ctx, meta, err = p.ProcessMeta(ctx, meta)
		if err != nil {
			return errors.Wrap(err, "process meta")
		}
	}

	err = cl.SendQueryMeta(ctx, meta, q.Compressed)
	if err != nil {
		return errors.Wrap(err, "resp meta")
	}

	return p.syncProxy(ctx, cl, srv, q)
}

func (p *Proxy) syncProxy(ctx context.Context, cl *conn.Client, srv *conn.Server, q *wire.Query) (err error) {
	clConn, srvConn := cl.RawConn(), srv.RawConn()

	var done bool

	for !done {
		if cl.Buffered() != 0 {
			done, err = p.procClient(ctx, cl, srv, q)
			if err != nil {
				return errors.Wrap(err, "client")
			}

			continue
		}

		if srv.Buffered() != 0 {
			done, err = p.procServer(ctx, cl, srv, q)
			if err != nil {
				return errors.Wrap(err, "server")
			}

			continue
		}

		r, err := netpoll.Select([]io.Reader{clConn, srvConn}, 100*time.Millisecond)
		switch err {
		case nil:
		case netpoll.ErrNoEvents:
			continue
		default:
			return errors.Wrap(err, "select")
		}

		//	tr.Printw("select", "cl", r == clConn)

		if r == clConn {
			done, err = p.procClient(ctx, cl, srv, q)
			if err != nil {
				return errors.Wrap(err, "client")
			}
		} else {
			done, err = p.procServer(ctx, cl, srv, q)
			if err != nil {
				return errors.Wrap(err, "server")
			}
		}
	}

	return nil
}

func (p *Proxy) procClient(ctx context.Context, cl *conn.Client, srv *conn.Server, q *wire.Query) (done bool, err error) {
	tr := tlog.SpanFromContext(ctx)

	tp, err := cl.RecvPacket()
	tr.V("packets").Printw("client packet", "tp", tlog.Hex(tp), "err", err)
	if err != nil {
		return false, errors.Wrap(err, "client: recv")
	}

	switch tp {
	case protocol.ClientData:
		b, err := cl.RecvBlock(ctx, q.Compressed)
		if err != nil {
			return false, errors.Wrap(err, "recv block")
		}

		tr.V("blocks").Printw("client block", "cols", len(b.Cols), "rows", b.Rows)

		err = srv.SendBlock(ctx, b, q.Compressed)
		if err != nil {
			return false, errors.Wrap(err, "send block")
		}

	default:
		return false, errors.New("unexpected packet: %x", tp)
	}

	if cl.Buffered() == 0 {
		err = srv.Enc.Flush()
		if err != nil {
			return false, errors.Wrap(err, "flush")
		}
	}

	return false, nil
}

func (p *Proxy) procServer(ctx context.Context, cl *conn.Client, srv *conn.Server, q *wire.Query) (done bool, err error) {
	tr := tlog.SpanFromContext(ctx)

	tp, err := srv.RecvPacket()
	tr.V("packets").Printw("server packet", "tp", tlog.Hex(tp), "err", err)
	if err != nil {
		return false, errors.Wrap(err, "server: recv")
	}

	switch tp {
	case protocol.ServerEndOfStream:
		err = cl.SendEndOfStream(ctx)
		if err != nil {
			return false, errors.Wrap(err, "send resp")
		}

		return true, nil
	case protocol.ServerData:
		b, err := srv.RecvBlock(ctx, q.Compressed)
		if err != nil {
			return false, errors.Wrap(err, "recv block")
		}

		tr.V("blocks").Printw("server block", "cols", len(b.Cols), "rows", b.Rows)

		err = cl.SendBlock(ctx, b, q.Compressed)
		if err != nil {
			return false, errors.Wrap(err, "send block")
		}
	case protocol.ServerProgress:
		p, err := srv.RecvProgress(ctx)
		if err != nil {
			return false, errors.Wrap(err, "recv progress")
		}

		err = cl.SendProgress(ctx, p)
		if err != nil {
			return false, errors.Wrap(err, "send progress")
		}
	case protocol.ServerProfileInfo:
		p, err := srv.RecvProfileInfo(ctx)
		if err != nil {
			return false, errors.Wrap(err, "recv profile")
		}

		err = cl.SendProfileInfo(ctx, p)
		if err != nil {
			return false, errors.Wrap(err, "send profile")
		}
	default:
		return false, errors.New("unexpected packet: %x", tp)
	}

	if srv.Buffered() == 0 {
		err = cl.Enc.Flush()
		if err != nil {
			return false, errors.Wrap(err, "flush")
		}
	}

	return false, nil
}

func (p *Proxy) conn(ctx context.Context, cl *conn.Client) (srv *conn.Server, err error) {
	tr := tlog.SpanFromContext(ctx)

	c, err := net.Dial("tcp", p.dsn.Hosts[0])
	if err != nil {
		return
	}

	defer func() {
		if err == nil {
			return
		}

		_ = c.Close()
	}()

	if tlog.If("dump_conn,dump_server_conn") {
		c = conn.NewDump(tr, "server", c)
	}

	srv = conn.NewServer(c)

	//srv.Database = p.dsn.Database
	srv.Database = cl.Database
	srv.User = p.dsn.User

	srv.Compress = p.dsn.Compress

	err = srv.Hello(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "hello")
	}

	//	tr.Printw("server", "server", srv)

	return
}

func (p *Proxy) reuse(cl *conn.Server, err error) {
	_ = cl.Close()
}
