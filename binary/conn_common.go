package binary

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"strings"

	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	conn struct {
		d *Decoder
		e *Encoder

		r *bufio.Reader
		w *bufio.Writer

		c net.Conn
	}
)

func newConn(ctx context.Context, c net.Conn) conn {
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)

	return conn{
		d: NewDecoder(ctx, r),
		e: NewEncoder(ctx, w),
		r: r,
		w: w,
		c: c,
	}
}

func (c *conn) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	x, err := c.d.Uvarint()

	return click.ServerPacket(x), err
}

func (c *conn) sendPacket(tp int) (err error) {
	return c.e.Uvarint(tp)
}

func (c *conn) RecvBlock(ctx context.Context, compr bool) (b *click.Block, err error) {
	tab, err := c.d.String()
	if err != nil {
		return
	}

	if compr {
		c.d.SetCompressed(true)
		defer c.d.SetCompressed(false)
	}

	cols, rows, err := c.recvBlockHeader()
	if err != nil {
		return
	}

	b = &click.Block{
		Table: tab,
		//	Info:  info,
		Rows: rows,
		Cols: make([]click.Column, cols),
	}

	for i := 0; i < cols; i++ {
		var name, tp string

		name, err = c.d.String()
		if err != nil {
			return
		}

		tp, err = c.d.String()
		if err != nil {
			return
		}

		var d []byte

		switch {
		case tp == "String":
			for j := 0; j < rows; j++ {
				x, err := c.d.ReadString()
				if err != nil {
					return nil, err
				}

				d = append(d, x...)
			}
		case tp == "UInt64" || tp == "Int64", strings.HasPrefix(tp, "DateTime64("):
			d = make([]byte, 8*rows)

			_, err = c.d.ReadFull(d)
		case tp == "UInt32" || tp == "Int32":
			d = make([]byte, 4*rows)

			_, err = c.d.ReadFull(d)
		case tp == "UInt16" || tp == "Int16" || tp == "Date":
			d = make([]byte, 2*rows)

			_, err = c.d.ReadFull(d)
		case tp == "UInt8" || tp == "Int8":
			d = make([]byte, rows)

			_, err = c.d.ReadFull(d)
		default:
			return nil, errors.New("unsupported type: %v (col %v)", tp, name)
		}

		if err != nil {
			return nil, err
		}

		b.Cols[i] = click.Column{
			Name: name,
			Type: tp,

			RawData: d,
		}
	}

	return
}

func (c *conn) sendBlock(ctx context.Context, pk int, b *click.Block, compr bool) (err error) {
	err = c.sendPacket(pk)
	if err != nil {
		return
	}

	tab := ""
	cols, rows := 0, 0

	if b != nil {
		tab = b.Table
		cols, rows = len(b.Cols), b.Rows
	}

	err = c.e.String(tab)
	if err != nil {
		return
	}

	if compr {
		c.e.SetCompressed(true)
		defer c.e.SetCompressed(false)
	}

	err = c.sendBlockHeader(cols, rows)
	if err != nil {
		return
	}

	if b.IsEmpty() {
		return c.e.Flush()
	}

	for _, col := range b.Cols {
		err = c.e.String(col.Name)
		if err != nil {
			return
		}

		err = c.e.String(col.Type)
		if err != nil {
			return
		}

		_, err = c.e.Write(col.RawData)
		if err != nil {
			return
		}
	}

	return nil
}

func (c *conn) Close() error {
	return c.c.Close()
}

func (c *conn) recvClientInfo() (n string, v [3]int, err error) {
	n, err = c.d.String()
	if err != nil {
		return
	}

	v[0], err = c.d.Uvarint()
	if err != nil {
		return
	}

	v[1], err = c.d.Uvarint()
	if err != nil {
		return
	}

	v[2], err = c.d.Uvarint()
	if err != nil {
		return
	}

	return
}

func (c *conn) sendClientInfo(n string, v [3]int) (err error) {
	err = c.e.String(n)
	if err != nil {
		return
	}

	err = c.e.Uvarint(v[0])
	if err != nil {
		return
	}

	err = c.e.Uvarint(v[1])
	if err != nil {
		return
	}

	err = c.e.Uvarint(v[2])
	if err != nil {
		return
	}

	return nil
}

func (c *conn) recvBlockHeader() (cols, rows int, err error) {
	_, err = c.recvBlockInfo()
	if err != nil {
		return
	}

	cols, err = c.d.Uvarint()
	if err != nil {
		return
	}

	rows, err = c.d.Uvarint()
	if err != nil {
		return
	}

	return
}

func (c *conn) sendBlockHeader(cols, rows int) (err error) {
	err = c.sendBlockInfo()
	if err != nil {
		return
	}

	err = c.e.Uvarint(cols)
	if err != nil {
		return
	}

	err = c.e.Uvarint(rows)
	if err != nil {
		return
	}

	return nil
}

func (c *conn) recvBlockInfo() (info []byte, err error) {
	//	info = make([]byte, 8)[:0]

	// 1
	t, err := c.d.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{1}) {
		return nil, errors.New("tag: %x", t)
	}

	//	info = append(info, t...)

	t, err = c.d.ReadUvarint()
	if err != nil {
		return
	}

	//	info = append(info, t...)

	// 2
	t, err = c.d.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{2}) {
		return nil, errors.New("tag: %x", t)
	}

	//	info = append(info, t...)

	t, err = c.d.ReadFixed(4)
	if err != nil {
		return
	}

	//	info = append(info, t...)

	// 0
	t, err = c.d.ReadUvarint()
	if err != nil {
		return
	}

	if !bytes.Equal(t, []byte{0}) {
		return nil, errors.New("tag: %x", t)
	}

	//	info = append(info, t...)

	return info, nil
}

func (c *conn) sendBlockInfo() (err error) {
	err = c.e.Uvarint(1)
	if err != nil {
		return
	}

	err = c.e.Uvarint(0)
	if err != nil {
		return
	}

	err = c.e.Uvarint(2)
	if err != nil {
		return
	}

	err = c.e.Int32(-1)
	if err != nil {
		return
	}

	err = c.e.Uvarint(0)
	if err != nil {
		return
	}

	return nil
}

func (c *conn) sendEmptyData(pk int, compr bool) (err error) {
	err = c.sendPacket(pk)
	if err != nil {
		return
	}

	err = c.e.String("")
	if err != nil {
		return
	}

	if compr {
		c.e.SetCompressed(true)
		defer c.e.SetCompressed(false)
	}

	return c.sendBlockHeader(0, 0)
}
