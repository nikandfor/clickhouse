package proxy

import (
	"context"
	"io"
	"net"
	"time"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/clickhouse/binary"
	"github.com/nikandfor/clickhouse/clpool"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	Proxy struct {
		pool click.ClientPool
	}

	netCounter struct {
		net.Conn

		read    int
		written int
	}

	blocksRows struct {
		blocks int
		rows   int
	}
)

var _ click.Server = &Proxy{}

var (
	reqsElapsed = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "clickhouse",
		Subsystem: "proxy",
		Name:      "request_elapsed_sec",
		Help:      "client reques elapsed seconds",

		Objectives: map[float64]float64{0.1: 0.1, 0.5: 0.1, 0.9: 0.1, 0.95: 0.1, 0.99: 0.1, 1: 0.1},
	}, []string{"remote_host", "err"})

	reqsRead = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "clickhouse",
		Subsystem: "proxy",
		Name:      "request_bytes_read",
		Help:      "client reques bytes read",

		Objectives: map[float64]float64{0.1: 0.1, 0.5: 0.1, 0.9: 0.1, 0.95: 0.1, 0.99: 0.1, 1: 0.1},
	}, []string{"remote_host", "err"})

	reqsWrite = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "clickhouse",
		Subsystem: "proxy",
		Name:      "request_bytes_written",
		Help:      "client reques bytes written",

		Objectives: map[float64]float64{0.1: 0.1, 0.5: 0.1, 0.9: 0.1, 0.95: 0.1, 0.99: 0.1, 1: 0.1},
	}, []string{"remote_host", "err"})

	reqsRows = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "clickhouse",
		Subsystem: "proxy",
		Name:      "request_rows",
		Help:      "client reques rows written",

		Objectives: map[float64]float64{0.1: 0.1, 0.5: 0.1, 0.9: 0.1, 0.95: 0.1, 0.99: 0.1, 1: 0.1},
	}, []string{"remote_host", "err"})

	reqsBlocks = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: "clickhouse",
		Subsystem: "proxy",
		Name:      "request_blocks",
		Help:      "client reques rows written",

		Objectives: map[float64]float64{0.1: 0.1, 0.5: 0.1, 0.9: 0.1, 0.95: 0.1, 0.99: 0.1, 1: 0.1},
	}, []string{"remote_host", "err"})
)

func init() {
	prometheus.MustRegister(reqsElapsed, reqsRead, reqsWrite, reqsRows, reqsBlocks)
}

func New(ctx context.Context, pool click.ClientPool) *Proxy {
	if tr := tlog.SpanFromContext(ctx); tr.If("dump_client") {
		pool = clpool.NewDumpPool(pool, 3)
	}

	if l := tlog.LoggerFromContext(ctx); l != nil {
		l.RegisterMetric("clickhouse_proxy_request_read_bytes", tlog.MetricCounter, "bytes read from client per request")
		l.RegisterMetric("clickhouse_proxy_request_written_bytes", tlog.MetricCounter, "bytes written from client per request")

		l.RegisterMetric("clickhouse_proxy_request_elapsed_ns", tlog.MetricSummary, "request duration")
	}

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
	tr := tlog.SpawnFromContext(ctx, "connection", "remote_host", host(conn.RemoteAddr()), "local_host", host(conn.LocalAddr()))
	defer func() { tr.Finish("err", err, "", loc.Caller(1)) }()

	ctx = tlog.ContextWithSpan(ctx, tr)

	if tr.If("dump_server_conn,dump_conn") {
		dc := binary.NewDumpConn(conn, tr)
		dc.Callers = 5
		conn = dc
	}

	defer conn.Close()

	conn = &netCounter{
		Conn: conn,
	}

	srv := binary.NewServerConn(ctx, conn)

	srv.Server.Name = "gh/nikandfor/clickhouse"

	srv.Auth = nil // TODO

	err = srv.Hello(ctx)
	if err != nil {
		return errors.Wrap(err, "hello")
	}

	tlog.SpanFromContext(ctx).V("hello").Printw("server hello", "client_conn", srv)

	var clopts []click.ClientOption

	//	clopts = append(clopts, click.WithDatabase(srv.Database))
	clopts = append(clopts, click.WithCredentials(srv.Credentials))

	for err == nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

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

	var remoteHost string
	var mm blocksRows

	defer func() {
		e := "nil"
		if err != nil {
			e = err.Error()
		}

		reqsElapsed.WithLabelValues(remoteHost, e).Observe(time.Since(tr.StartedAt).Seconds())

		reqsBlocks.WithLabelValues(remoteHost, e).Observe(float64(mm.blocks))
		reqsRows.WithLabelValues(remoteHost, e).Observe(float64(mm.rows))
	}()

	if c, ok := srv.(interface{ Conn() net.Conn }); ok {
		conn := c.Conn()

		remoteHost = host(conn.RemoteAddr())

		if cnt, ok := conn.(*netCounter); ok {
			r, w := cnt.read, cnt.written

			defer func() {
				r = cnt.read - r
				w = cnt.written - w

				tr.Observe("clickhouse_proxy_request_read_bytes", r, "remote_host", remoteHost)
				tr.Observe("clickhouse_proxy_request_written_bytes", w, "remote_host", remoteHost)

				e := "nil"
				if err != nil {
					e = err.Error()
				}

				reqsRead.WithLabelValues(remoteHost, e).Observe(float64(r))
				reqsWrite.WithLabelValues(remoteHost, e).Observe(float64(w))
			}()
		}
	}

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

	tr.V("is_insert").Printw("is insert", "is_insert", q.IsInsert())

	if q.IsInsert() {
		err = p.sendData(ctx, srv, cl, q, &mm)
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

func (p *Proxy) sendData(ctx context.Context, srv click.ServerConn, cl click.Client, q *click.Query, mm *blocksRows) (err error) {
	tr := tlog.SpanFromContext(ctx)

	var blocks, rows int

	defer func() {
		tr.Printw("client-to-server blocks", "blocks", blocks, "rows", rows)

		if mm != nil {
			mm.blocks = blocks
			mm.rows = rows
		}
	}()

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

		blocks++
		rows += b.Rows

		tr.V("blocks").Printw("client block", "rows", b.Rows)
	}
}

func (p *Proxy) recvResponse(ctx context.Context, srv click.ServerConn, cl click.Client, q *click.Query) (err error) {
	tr := tlog.SpanFromContext(ctx)

	var blocks, rows int

	defer func() {
		if blocks != 0 {
			tr.Printw("server-to-client blocks", "blocks", blocks, "rows", rows)
		}
	}()

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

			if !b.IsEmpty() {
				tr.V("blocks").Printw("server block", "rows", b.Rows)

				blocks++
				rows += b.Rows
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

func (c *netCounter) Read(p []byte) (n int, err error) {
	n, err = c.Conn.Read(p)

	c.read += n

	return
}

func (c *netCounter) Write(p []byte) (n int, err error) {
	n, err = c.Conn.Write(p)

	c.written += n

	return
}

func host(a net.Addr) (h string) {
	switch a := a.(type) {
	case *net.TCPAddr:
		return a.IP.String()
	}

	s := a.String()

	h, _, err := net.SplitHostPort(s)
	if err != nil {
		h = s
	}
	return
}
