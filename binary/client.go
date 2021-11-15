package binary

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/lib/protocol"
	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	Client struct {
		conn

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

var _ click.Client = &Client{}

var hostname, _ = os.Hostname()

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn: newConn(conn),
		Client: click.Agent{
			Name: "Clickhouse clien",
			Ver:  click.Ver{1, 1, 54213},
		},
		Database: "default",
		User:     "default",
	}
}

func (c *Client) Hello(ctx context.Context) (err error) {
	err = c.sendHello()
	if err != nil {
		return
	}

	err = c.e.Flush()
	if err != nil {
		return
	}

	tp, err := c.NextPacket(ctx)
	if err != nil {
		return
	}

	switch tp {
	case protocol.ServerHello:
		return c.recvHello()
	case protocol.ServerException:
		return c.RecvException(ctx)
	default:
		return errors.New("unexpected packet: %x", tp)
	}

	return nil
}

func (c *Client) sendHello() (err error) {
	err = c.sendPacket(int(click.ClientHello))
	if err != nil {
		return
	}

	err = c.sendClientInfo(c.Client.Name, c.Client.Ver)
	if err != nil {
		return
	}

	err = c.e.String(c.Database)
	if err != nil {
		return
	}

	err = c.e.String(c.User)
	if err != nil {
		return
	}

	err = c.e.String(c.Password)
	if err != nil {
		return
	}

	return nil
}

func (c *Client) recvHello() (err error) {
	c.Server.Name, c.Server.Ver, err = c.recvClientInfo()
	if err != nil {
		return
	}

	if c.Server.Ver[2] >= protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		c.TimeZone, err = c.d.String()
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Client) SendQuery(ctx context.Context, q *click.Query) (_ click.QueryMeta, err error) {
	err = c.sendQuery(ctx, q)
	if err != nil {
		return
	}

	err = c.e.Flush()
	if err != nil {
		return
	}

	tp, err := c.NextPacket(ctx)
	if err != nil {
		return
	}

	switch tp {
	case protocol.ServerData:
	case protocol.ServerException:
		return nil, c.RecvException(ctx)
	default:
		return nil, errors.New("unexpected packet: %x", tp)
	}

	return c.recvMeta(ctx, q)
}

func (c *Client) sendQuery(ctx context.Context, q *click.Query) (err error) {
	err = c.sendPacket(int(protocol.ClientQuery))
	if err != nil {
		return
	}

	err = c.e.String(q.ID)
	if err != nil {
		return
	}

	err = c.sendQueryInfo(ctx, q)
	if err != nil {
		return
	}

	if c.Server.Ver[2] >= click.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		err = c.e.String(q.QuotaKey)
		if err != nil {
			return
		}
	}

	err = c.e.String("") // settings
	if err != nil {
		return
	}

	err = c.e.Uvarint(2) // state complete
	if err != nil {
		return
	}

	err = c.e.Bool(q.Compressed)
	if err != nil {
		return
	}

	err = c.e.String(q.Query)
	if err != nil {
		return
	}

	err = c.sendEmptyData(protocol.ClientData, q.Compressed)
	if err != nil {
		return
	}

	return
}

func (c *Client) sendQueryInfo(ctx context.Context, q *click.Query) (err error) {
	err = c.e.Uvarint(1)
	if err != nil {
		return
	}

	err = c.e.String("")
	if err != nil {
		return
	}

	err = c.e.String("")
	if err != nil {
		return
	}

	err = c.e.String("localhost:0")
	if err != nil {
		return
	}

	err = c.e.Uvarint(1)
	if err != nil {
		return
	}

	err = c.e.String(hostname)
	if err != nil {
		return
	}

	err = c.e.String(hostname)
	if err != nil {
		return
	}

	err = c.sendClientInfo(c.Client.Name, c.Client.Ver)
	if err != nil {
		return
	}

	return nil
}

func (c *Client) recvMeta(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	_, err = c.d.String()
	if err != nil {
		return
	}

	if q.Compressed {
		c.d.SetCompressed(true)
		defer c.d.SetCompressed(false)
	}

	cols, rows, err := c.recvBlockHeader()
	if err != nil {
		return
	}

	if rows != 0 {
		return nil, errors.New("unexpected rows: %v", rows)
	}

	meta = make(click.QueryMeta, cols)

	for col := 0; col < cols; col++ {
		meta[col].Name, err = c.d.String()
		if err != nil {
			return
		}

		meta[col].Type, err = c.d.String()
		if err != nil {
			return
		}
	}

	return meta, nil
}

func (c *Client) CancelQuery(ctx context.Context) (err error) {
	err = c.sendPacket(int(click.ClientCancel))
	if err != nil {
		return
	}

	return c.e.Flush()
}

func (c *Client) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	return c.sendBlock(ctx, int(click.ClientData), b, compr)
}

func (c *Client) RecvException(ctx context.Context) (err error) {
	root := &click.Exception{}
	exc := root

	for {
		exc.Code, err = c.d.Int32()
		if err != nil {
			return
		}

		exc.Name, err = c.d.String()
		if err != nil {
			return
		}

		exc.Message, err = c.d.String()
		if err != nil {
			return
		}

		exc.Message = strings.TrimSpace(strings.TrimPrefix(exc.Message, exc.Name+": "))

		exc.StackTrace, err = c.d.String()
		if err != nil {
			return
		}

		var more bool
		more, err = c.d.Bool()
		if err != nil {
			return
		}

		if !more {
			break
		}

		exc.Cause = &click.Exception{}
		exc = exc.Cause
	}

	return root
}

func (c *Client) RecvProgress(context.Context) (p click.Progress, err error) {
	p.Rows, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.Bytes, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.TotalRows, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	return
}

func (c *Client) RecvProfileInfo(context.Context) (p click.ProfileInfo, err error) {
	p.Rows, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.Blocks, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.Bytes, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.AppliedLimit, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.RowsBeforeLimit, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	p.CalcRowsBeforeLimit, err = c.d.Uvarint64()
	if err != nil {
		return
	}

	return
}
