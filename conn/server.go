package conn

import (
	"bytes"
	"context"
	"net"
	"os"
	"strings"

	"github.com/nikandfor/errors"

	"github.com/nikandfor/clickhouse/protocol"
	"github.com/nikandfor/clickhouse/wire"
)

type (
	// Server is connection with server
	Server struct {
		*Conn

		ServerName string
		ServerVer  [3]int
		TimeZone   string

		Database string
		User     string
		Password string

		Compress bool
	}
)

const (
	ClientRevision = 54213
	ServerRevision = 54450
)

var (
	ClientName = "Clickhouse clien"
	ServerName = "Clickhouse"
)

var hostname, _ = os.Hostname()

func NewServer(conn net.Conn) (c *Server) {
	c = &Server{
		Conn:     newConn(conn),
		Database: "default",
		User:     "default",
	}

	return c
}

func (c *Server) Hello(ctx context.Context) (err error) {
	err = c.sendHello()
	if err != nil {
		return
	}

	err = c.Enc.Flush()
	if err != nil {
		return
	}

	tp, err := c.RecvPacket()
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

func (c *Server) sendHello() (err error) {
	err = c.SendPacket(protocol.ClientHello)
	if err != nil {
		return
	}

	err = c.sendClientInfo(ClientName, [3]int{1, 1, ClientRevision})
	if err != nil {
		return
	}

	err = c.Enc.String(c.Database)
	if err != nil {
		return
	}

	err = c.Enc.String(c.User)
	if err != nil {
		return
	}

	err = c.Enc.String(c.Password)
	if err != nil {
		return
	}

	return nil
}

func (c *Server) recvHello() (err error) {
	c.ServerName, c.ServerVer, err = c.recvClientInfo()
	if err != nil {
		return
	}

	if c.ServerVer[2] >= protocol.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
		c.TimeZone, err = c.Dec.String()
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Server) SendQuery(ctx context.Context, q *wire.Query) (meta wire.QueryMeta, err error) {
	err = c.sendQuery(ctx, q)
	if err != nil {
		return
	}

	err = c.Enc.Flush()
	if err != nil {
		return
	}

	tp, err := c.RecvPacket()
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

	defer c.Dec.SetCompressed(false)

	tab, _, cols, rows, err := c.recvBlockHeader(q.Compressed)
	if err != nil {
		return
	}

	if tab != "" || rows != 0 {
		return nil, errors.New("unexpeted meta: %v %v", tab, rows)
	}

	meta = make(wire.QueryMeta, cols)

	for col := 0; col < cols; col++ {
		meta[col].Name, err = c.Dec.String()
		if err != nil {
			return
		}

		meta[col].Type, err = c.Dec.String()
		if err != nil {
			return
		}
	}

	return meta, nil
}

func (c *Server) sendQuery(ctx context.Context, q *wire.Query) (err error) {
	err = c.SendPacket(protocol.ClientQuery)
	if err != nil {
		return
	}

	err = c.Enc.String(q.ID)
	if err != nil {
		return
	}

	err = c.sendQueryInfo(ctx, q)
	if err != nil {
		return
	}

	if c.ServerVer[2] >= protocol.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
		err = c.Enc.String(q.QuotaKey)
		if err != nil {
			return
		}
	}

	err = c.Enc.String("") // settings
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(2) // state complete
	if err != nil {
		return
	}

	err = c.Enc.Bool(q.Compressed)
	if err != nil {
		return
	}

	err = c.Enc.String(q.Query)
	if err != nil {
		return
	}

	defer c.Enc.SetCompressed(false)

	err = c.sendBlockHeader(protocol.ClientData, "", nil, 0, 0, q.Compressed)
	if err != nil {
		return
	}

	return
}

func (c *Server) sendQueryInfo(ctx context.Context, q *wire.Query) (err error) {
	if len(q.Info) != 0 {
		_, err = c.Enc.Write(q.Info)
		if err != nil {
			return
		}
	} else {
		err = c.Enc.Uvarint(1)
		if err != nil {
			return
		}

		err = c.Enc.String("")
		if err != nil {
			return
		}

		err = c.Enc.String("")
		if err != nil {
			return
		}

		err = c.Enc.String("localhost:0")
		if err != nil {
			return
		}

		err = c.Enc.Uvarint(1)
		if err != nil {
			return
		}

		err = c.Enc.String(hostname)
		if err != nil {
			return
		}

		err = c.Enc.String(hostname)
		if err != nil {
			return
		}
	}

	err = c.sendClientInfo(ClientName, [3]int{1, 1, ClientRevision})
	if err != nil {
		return
	}

	return nil
}

func (c *Server) RecvException(ctx context.Context) (err error) {
	root := &wire.Exception{}
	exc := root

	for {
		exc.Code, err = c.Dec.Int32()
		if err != nil {
			return err
		}

		exc.Name, err = c.Dec.String()
		if err != nil {
			return err
		}

		exc.Message, err = c.Dec.String()
		if err != nil {
			return err
		}

		exc.Message = strings.TrimSpace(strings.TrimPrefix(exc.Message, exc.Name+": "))

		exc.StackTrace, err = c.Dec.String()
		if err != nil {
			return err
		}

		more, err := c.Dec.Bool()
		if err != nil {
			return err
		}

		if !more {
			break
		}

		exc.Cause = &wire.Exception{}
		exc = exc.Cause
	}

	return root
}

func (c *Server) RecvProgress(ctx context.Context) (p wire.Progress, err error) {
	p.Rows, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.Bytes, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.TotalRows, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	return
}

func (c *Server) RecvProfileInfo(ctx context.Context) (p wire.ProfileInfo, err error) {
	p.Rows, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.Blocks, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.Bytes, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.AppliedLimit, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.RowsBeforeLimit, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	p.CalcRowsBeforeLimit, err = c.Dec.Uvarint64()
	if err != nil {
		return
	}

	return
}

func (c *Server) CancelQuery(ctx context.Context) (err error) {
	err = c.Enc.Uvarint(protocol.ClientCancel)
	if err != nil {
		return
	}

	return c.Enc.Flush()
}

func (c *Conn) RecvBlock(ctx context.Context, compr bool) (b *wire.Block, err error) {
	defer c.Dec.SetCompressed(false)

	tab, info, cols, rows, err := c.recvBlockHeader(compr)
	if err != nil {
		return
	}

	b = &wire.Block{
		Table: tab,
		Info:  info,
		Rows:  rows,
		Cols:  make([]wire.Column, cols),
	}

	for i := 0; i < cols; i++ {
		var name, tp string

		name, err = c.Dec.String()
		if err != nil {
			return
		}

		tp, err = c.Dec.String()
		if err != nil {
			return
		}

		var d []byte

		switch {
		case tp == "String":
			for j := 0; j < rows; j++ {
				x, err := c.Dec.ReadString()
				if err != nil {
					return nil, err
				}

				d = append(d, x...)
			}
		case tp == "UInt64" || tp == "Int64", strings.HasPrefix(tp, "DateTime64("):
			d = make([]byte, 8*rows)

			_, err = c.Dec.Read(d)
		case tp == "UInt32" || tp == "Int32":
			d = make([]byte, 4*rows)

			_, err = c.Dec.Read(d)
		case tp == "UInt16" || tp == "Int16" || tp == "Date":
			d = make([]byte, 2*rows)

			_, err = c.Dec.Read(d)
		case tp == "UInt8" || tp == "Int8":
			d = make([]byte, rows)

			_, err = c.Dec.Read(d)
		default:
			return nil, errors.New("unsupported type: %v (col %v)", tp, name)
		}

		if err != nil {
			return nil, err
		}

		b.Cols[i] = wire.Column{
			Name: name,
			Type: tp,

			Data: d,
		}
	}

	return
}

func (c *Server) SendBlock(ctx context.Context, b *wire.Block, compr bool) (err error) {
	return c.sendBlock(ctx, protocol.ClientData, b, compr)
}

func (c *Client) SendBlock(ctx context.Context, b *wire.Block, compr bool) (err error) {
	return c.sendBlock(ctx, protocol.ServerData, b, compr)
}

func (c *Conn) sendBlock(ctx context.Context, pk int, b *wire.Block, compr bool) (err error) {
	defer c.Enc.SetCompressed(false)

	err = c.sendBlockHeader(pk, b.Table, b.Info, len(b.Cols), b.Rows, compr)
	if err != nil {
		return
	}

	for _, col := range b.Cols {
		err = c.Enc.String(col.Name)
		if err != nil {
			return
		}

		err = c.Enc.String(col.Type)
		if err != nil {
			return
		}

		_, err = c.Enc.Write(col.Data)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *Conn) sendBlockHeader(pk int, tab string, info []byte, cols, rows int, compr bool) (err error) {
	err = c.SendPacket(pk)
	if err != nil {
		return
	}

	err = c.Enc.String(tab)
	if err != nil {
		return
	}

	c.Enc.SetCompressed(compr)

	if c.rev != 0 {
		if len(info) != 0 {
			_, err = c.Enc.Write(info)
		} else {
			err = c.sendBlockInfo()
		}
		if err != nil {
			return
		}
	}

	err = c.Enc.Uvarint(cols)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(rows)
	if err != nil {
		return
	}

	return nil
}

func (c *Conn) sendBlockInfo() (err error) {
	err = c.Enc.Uvarint(1)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(0)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(2)
	if err != nil {
		return
	}

	err = c.Enc.Int32(-1)
	if err != nil {
		return
	}

	err = c.Enc.Uvarint(0)
	if err != nil {
		return
	}

	return nil
}

func (c *Conn) recvBlockHeader(compr bool) (tab string, info []byte, cols, rows int, err error) {
	tab, err = c.Dec.String()
	if err != nil {
		return
	}

	c.Dec.SetCompressed(compr)

	if c.rev != 0 {
		info, err = c.recvBlockInfo()
		if err != nil {
			return
		}
	}

	cols, err = c.Dec.Uvarint()
	if err != nil {
		return
	}

	rows, err = c.Dec.Uvarint()
	if err != nil {
		return
	}

	return
}

func (c *Conn) recvBlockInfo() (info []byte, err error) {
	info = make([]byte, 8)[:0]

	// 1
	t, err := c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{1}) {
		return nil, errors.New("tag: %x", t)
	}

	info = append(info, t...)

	t, err = c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	info = append(info, t...)

	// 2
	t, err = c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{2}) {
		return nil, errors.New("tag: %x", t)
	}

	info = append(info, t...)

	t, err = c.Dec.ReadFixed(4)
	if err != nil {
		return
	}

	info = append(info, t...)

	// 0
	t, err = c.Dec.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{0}) {
		return nil, errors.New("tag: %x", t)
	}

	info = append(info, t...)

	return info, nil
}
