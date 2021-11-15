package conn

import (
	"bufio"
	"context"
	"net"

	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"

	"github.com/nikandfor/clickhouse/binary"
	"github.com/nikandfor/clickhouse/protocol"
	"github.com/nikandfor/clickhouse/wire"
)

type (
	// Client is connection with client
	Client struct {
		*Conn

		Name string
		Ver  [3]int

		TimeZone string

		Database string
		User     string
		Password string

		Auth func(context.Context, *Client) error
	}

	Conn struct {
		Enc *binary.Encoder
		Dec *binary.Decoder

		r *bufio.Reader
		w *bufio.Writer

		rev int

		c net.Conn
	}
)

func NewClient(conn net.Conn) (c *Client) {
	c = &Client{
		Conn: newConn(conn),
	}

	//	c.TimeZone, _ = time.Now().Zone()
	//	if c.TimeZone == "" {
	c.TimeZone = "UTC"
	//	}

	return c
}

func (c *Client) Hello(ctx context.Context) (err error) {
	tp, err := c.Dec.Uvarint()
	if err != nil {
		return err
	}

	switch tp {
	case protocol.ClientHello:
	default:
		return errors.New("unexpected packet: %x", tp)
	}

	err = c.recvHello()
	if err != nil {
		return errors.Wrap(err, "recv")
	}

	if c.Auth != nil {
		err = c.Auth(ctx, c)
		if err != nil {
			return errors.Wrap(err, "auth")
		}
	}

	err = c.sendHello()
	if err != nil {
		return errors.Wrap(err, "send")
	}

	err = c.Enc.Flush()
	if err != nil {
		return errors.Wrap(err, "send")
	}

	return nil
}

func (c *Client) recvHello() (err error) {
	c.Name, c.Ver, err = c.recvClientInfo()
	if err != nil {
		return
	}

	c.Database, err = c.Dec.String()
	if err != nil {
		return
	}

	c.User, err = c.Dec.String()
	if err != nil {
		return
	}

	c.Password, err = c.Dec.String()
	if err != nil {
		return
	}

	return nil
}

func (c *Client) sendHello() (err error) {
	err = c.SendPacket(protocol.ClientHello)
	if err != nil {
		return
	}

	err = c.sendClientInfo(ServerName, [3]int{21, 11, ServerRevision})
	if err != nil {
		return
	}

	if ServerRevision >= protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		err = c.Enc.String(c.TimeZone)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Client) NextQuery(ctx context.Context) (q *wire.Query, err error) {
loop:
	for {
		tp, err := c.RecvPacket()
		if err != nil {
			return nil, err
		}

		tr := tlog.SpanFromContext(ctx)
		tr.V("all_packets").Printw("packet", "packet", tlog.Hex(tp))

		switch tp {
		case protocol.ClientQuery:
			break loop
		case protocol.ClientPing:
			err = c.SendPong(ctx)
		default:
			return q, errors.New("unexpected packet: %x", tp)
		}

		if err != nil {
			return nil, errors.Wrap(err, "packet: %x", tp)
		}
	}

	return c.recvQuery(ctx)
}

func (c *Client) recvQuery(ctx context.Context) (q *wire.Query, err error) {
	tr := tlog.SpanFromContext(ctx)

	q = new(wire.Query)

	q.ID, err = c.Dec.String()
	if err != nil {
		return
	}

	err = c.recvQueryInfo(ctx, q)
	if err != nil {
		return
	}

	q.QuotaKey, err = c.Dec.String()
	if err != nil {
		return
	}

	for {
		var s string

		s, err = c.Dec.String()
		if err != nil {
			return
		}

		if s == "" {
			break
		}

		panic(s)
	}

	sc, err := c.Dec.Uvarint()
	if err != nil {
		return
	}

	if sc != 2 {
		return q, errors.New("state: %x", sc)
	}

	q.Compressed, err = c.Dec.Bool()
	if err != nil {
		return
	}

	q.Query, err = c.Dec.String()
	if err != nil {
		return
	}

	tr.V("query").Printw("query", "q", q.Query, "compressed", q.Compressed)

	err = c.recvExtTables(ctx, q)
	if err != nil {
		return q, errors.Wrap(err, "ext tables")
	}

	return q, nil
}

func (c *Client) recvExtTables(ctx context.Context, q *wire.Query) (err error) {
	for {
		var tp int
		tp, err = c.RecvPacket()
		if err != nil {
			return
		}

		if tp != protocol.ClientData {
			return errors.New("unexpected packet: %x", tp)
		}

		tab, _, cols, rows, err := c.recvBlockHeader(q.Compressed)
		if err != nil {
			return err
		}

		c.Dec.SetCompressed(false)

		if tab == "" && cols == 0 && rows == 0 {
			break
		}

		return errors.New("implement ext tables please")
	}

	return nil
}

func (c *Client) recvQueryInfo(ctx context.Context, q *wire.Query) (err error) {
	var x []byte

	x, err = c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadString()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadString()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadString()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadString()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	x, err = c.Dec.ReadString()
	if err != nil {
		return
	}

	q.Info = append(q.Info, x...)

	q.Client, q.Ver, err = c.recvClientInfo()
	if err != nil {
		return
	}

	return nil
}

func (c *Client) SendQueryMeta(ctx context.Context, meta wire.QueryMeta, compr bool) (err error) {
	defer c.Enc.SetCompressed(false)

	err = c.sendBlockHeader(protocol.ServerData, "", nil, len(meta), 0, compr)
	if err != nil {
		return
	}

	for _, col := range meta {
		err = c.Enc.String(col.Name)
		if err != nil {
			return
		}

		err = c.Enc.String(col.Type)
		if err != nil {
			return
		}
	}

	err = c.Enc.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (c *Client) SendPong(ctx context.Context) (err error) {
	err = c.Enc.Uvarint(protocol.ServerPong)
	if err != nil {
		return
	}

	return c.Enc.Flush()
}

func (c *Client) SendEndOfStream(ctx context.Context) (err error) {
	err = c.Enc.Uvarint(protocol.ServerEndOfStream)
	if err != nil {
		return
	}

	return c.Enc.Flush()
}

func (c *Client) SendException(ctx context.Context, exc error) (err error) {
	err = c.SendPacket(protocol.ServerException)
	if err != nil {
		return
	}

	err = c.sendException(exc)
	if err != nil {
		return
	}

	err = c.Enc.Flush()
	if err != nil {
		return
	}

	return nil
}

func (c *Client) sendException(exc error) (err error) {
	var e *wire.Exception

	if !errors.As(exc, &e) {
		e = &wire.Exception{
			Code:    -1,
			Name:    "error",
			Message: exc.Error(),
		}
	}

	for e != nil {
		err = c.Enc.Int32(e.Code)
		if err != nil {
			return
		}

		err = c.Enc.String(e.Name)
		if err != nil {
			return
		}

		err = c.Enc.String(e.Message)
		if err != nil {
			return
		}

		err = c.Enc.String(e.StackTrace) // stack
		if err != nil {
			return
		}

		err = c.Enc.Bool(e.Cause != nil)
		if err != nil {
			return
		}

		e = e.Cause
	}

	return nil
}

func (c *Client) SendProgress(ctx context.Context, p wire.Progress) (err error) {
	err = c.SendPacket(protocol.ServerProgress)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.Rows)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.Bytes)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.TotalRows)
	if err != nil {
		return
	}

	err = c.Enc.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (c *Client) SendProfileInfo(ctx context.Context, p wire.ProfileInfo) (err error) {
	err = c.SendPacket(protocol.ServerProfileInfo)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.Rows)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.Blocks)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.Bytes)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.AppliedLimit)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.RowsBeforeLimit)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint64(p.CalcRowsBeforeLimit)
	if err != nil {
		return
	}

	err = c.Enc.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func newConn(c net.Conn) *Conn {
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	return &Conn{
		Dec: binary.NewDecoder(r),
		Enc: binary.NewEncoder(w),
		r:   r,
		w:   w,
		c:   c,
	}
}

func (c *Conn) Buffered() int {
	return c.r.Buffered()
}

func (c *Conn) RecvPacket() (tp int, err error) {
	return c.Dec.Uvarint()
}

func (c *Conn) SendPacket(tp int) (err error) {
	return c.Enc.Uvarint(tp)
}

func (c *Conn) recvClientInfo() (n string, v [3]int, err error) {
	n, err = c.Dec.String()
	if err != nil {
		return
	}

	v[0], err = c.Dec.Uvarint()
	if err != nil {
		return
	}

	v[1], err = c.Dec.Uvarint()
	if err != nil {
		return
	}

	v[2], err = c.Dec.Uvarint()
	if err != nil {
		return
	}

	c.rev = v[2]

	return
}

func (c *Conn) sendClientInfo(n string, v [3]int) (err error) {
	err = c.Enc.String(n)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(v[0])
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(v[1])
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(v[2])
	if err != nil {
		return
	}

	return nil
}

func (c *Conn) RawConn() net.Conn { return c.c }

func (c *Conn) Close() error {
	if c.c == nil {
		return nil
	}

	return c.c.Close()
}
