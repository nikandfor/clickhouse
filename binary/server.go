package binary

import (
	"context"
	"net"

	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	Server struct {
		conn

		Auth func(context.Context, *Server) error

		// server response
		Server   click.Agent
		TimeZone string

		// client request
		Client click.Agent

		Database string
		User     string
		Password string
	}
)

var _ click.ServerConn = &Server{}

func NewServerConn(conn net.Conn) *Server {
	return &Server{
		conn: newConn(conn),
		Server: click.Agent{
			Name: "Clickhouse",
			Ver:  click.Ver{21, 11, 54450},
		},
	}
}

func (c *Server) Hello(ctx context.Context) error {
	tp, err := c.NextPacket(ctx)
	if err != nil {
		return err
	}

	if tp != protocol.ClientHello {
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

	err = c.e.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (c *Server) recvHello() (err error) {
	n, v, err := c.recvClientInfo()
	if err != nil {
		return
	}

	c.Client.Name = n
	c.Client.Ver = v

	c.Database, err = c.d.String()
	if err != nil {
		return
	}

	c.User, err = c.d.String()
	if err != nil {
		return
	}

	c.Password, err = c.d.String()
	if err != nil {
		return
	}

	return nil
}

func (c *Server) sendHello() (err error) {
	err = c.sendPacket(protocol.ClientHello)
	if err != nil {
		return
	}

	err = c.sendClientInfo(c.Server.Name, c.Server.Ver)
	if err != nil {
		return
	}

	if c.Server.Ver[2] >= click.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		err = c.e.String(c.TimeZone)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Server) NextPacket(ctx context.Context) (pk click.ClientPacket, err error) {
	tp, err := c.d.Uvarint()

	return click.ClientPacket(tp), err
}

func (c *Server) RecvQuery(ctx context.Context) (q *click.Query, err error) {
	q = new(click.Query)

	q.ID, err = c.d.String()
	if err != nil {
		return
	}

	err = c.recvQueryInfo(ctx, q)
	if err != nil {
		return
	}

	q.QuotaKey, err = c.d.String()
	if err != nil {
		return
	}

	for { // settings
		var s string

		s, err = c.d.String()
		if err != nil {
			return
		}

		if s == "" {
			break
		}

		panic(s)
	}

	sc, err := c.d.Uvarint()
	if err != nil {
		return
	}

	if sc != 2 {
		return q, errors.New("state: %x", sc)
	}

	q.Compressed, err = c.d.Bool()
	if err != nil {
		return
	}

	q.Query, err = c.d.String()
	if err != nil {
		return
	}

	err = c.recvExtTables(ctx, q)
	if err != nil {
		return q, errors.Wrap(err, "ext tables")
	}

	return q, nil
}

func (c *Server) recvExtTables(ctx context.Context, q *click.Query) (err error) {
	var last bool

	for !last {
		var tp click.ClientPacket

		tp, err = c.NextPacket(ctx)
		if err != nil {
			return
		}

		if tp != protocol.ClientData {
			return errors.New("unexpected packet: %x", tp)
		}

		var tab string

		tab, err = c.d.String()
		if err != nil {
			return
		}

		last, err = c.recvExcTable(ctx, q, tab)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Server) recvExcTable(ctx context.Context, q *click.Query, tab string) (last bool, err error) {
	if q.Compressed {
		c.d.SetCompressed(true)
		defer c.d.SetCompressed(false)
	}

	cols, rows, err := c.recvBlockHeader()
	if err != nil {
		return false, err
	}

	if tab == "" && cols == 0 && rows == 0 {
		return true, nil
	}

	return false, errors.New("implement ext tables please")
}

func (c *Server) recvQueryInfo(ctx context.Context, q *click.Query) (err error) {
	var x []byte

	_ = x

	x, err = c.d.ReadUvarint()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadString()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadString()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadString()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadUvarint()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadString()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	x, err = c.d.ReadString()
	if err != nil {
		return
	}

	//	q.Info = append(q.Info, x...)

	q.Client.Name, q.Client.Ver, err = c.recvClientInfo()
	if err != nil {
		return
	}

	return nil
}

func (c *Server) SendQueryMeta(ctx context.Context, meta click.QueryMeta, compr bool) (err error) {
	err = c.sendPacket(int(click.ServerData))
	if err != nil {
		return
	}

	err = c.e.String("") // table
	if err != nil {
		return
	}

	if compr {
		c.e.SetCompressed(true)
		defer c.e.SetCompressed(false)
	}

	err = c.sendBlockHeader(len(meta), 0)
	if err != nil {
		return
	}

	for _, col := range meta {
		err = c.e.String(col.Name)
		if err != nil {
			return
		}

		err = c.e.String(col.Type)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Server) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	return c.sendBlock(ctx, int(click.ServerData), b, compr)
}

func (c *Server) SendEndOfStream(ctx context.Context) (err error) {
	err = c.sendPacket(int(click.ServerEndOfStream))
	if err != nil {
		return
	}

	return c.e.Flush()
}

func (c *Server) sendPong(ctx context.Context) (err error) {
	err = c.sendPacket(int(click.ServerPong))
	if err != nil {
		return
	}

	return c.e.Flush()
}

func (c *Server) SendException(ctx context.Context, exc error) (err error) {
	err = c.sendPacket(int(protocol.ServerException))
	if err != nil {
		return
	}

	err = c.sendException(exc)
	if err != nil {
		return
	}

	err = c.e.Flush()
	if err != nil {
		return
	}

	return nil
}

func (c *Server) sendException(exc error) (err error) {
	var e *click.Exception

	if !errors.As(exc, &e) {
		e = &click.Exception{
			Code:    -1,
			Name:    "error",
			Message: exc.Error(),
		}
	}

	for e != nil {
		err = c.e.Int32(e.Code)
		if err != nil {
			return
		}

		err = c.e.String(e.Name)
		if err != nil {
			return
		}

		err = c.e.String(e.Message)
		if err != nil {
			return
		}

		err = c.e.String(e.StackTrace) // stack
		if err != nil {
			return
		}

		err = c.e.Bool(e.Cause != nil)
		if err != nil {
			return
		}

		e = e.Cause
	}

	return nil
}

func (c *Server) SendProgress(ctx context.Context, p click.Progress) (err error) {
	err = c.sendPacket(int(click.ServerProgress))
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.Rows)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.Bytes)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.TotalRows)
	if err != nil {
		return
	}

	err = c.e.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}

func (c *Server) SendProfileInfo(ctx context.Context, p click.ProfileInfo) (err error) {
	err = c.sendPacket(int(click.ServerProfileInfo))
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.Rows)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.Blocks)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.Bytes)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.AppliedLimit)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.RowsBeforeLimit)
	if err != nil {
		return
	}

	err = c.e.Uvarint64(p.CalcRowsBeforeLimit)
	if err != nil {
		return
	}

	err = c.e.Flush()
	if err != nil {
		return errors.Wrap(err, "flush")
	}

	return nil
}
