package clpool

import (
	"context"
	"net"

	"github.com/nikandfor/clickhouse"
	"github.com/nikandfor/clickhouse/binary"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/tlog"
)

type (
	BinaryPool struct {
		addr string

		AgentName string

		net.Dialer
	}

	DialOption struct {
		Option
		F func(addr *string, d *net.Dialer) error
	}

	BinaryOption struct {
		Option
		F func(*binary.Client) error
	}
)

func NewBinaryPool(addr string) *BinaryPool {
	return &BinaryPool{
		addr:      addr,
		AgentName: "gh/nikandfor/clickhouse",
	}
}

func (p *BinaryPool) Get(ctx context.Context, opts ...clickhouse.ClientOption) (_ clickhouse.Client, err error) {
	addr := p.addr
	d := p.Dialer

	var more []clickhouse.ClientOption

	for _, o := range opts {
		if o, ok := o.(DialOption); ok {
			err = o.F(&addr, &d)
			if err != nil {
				return nil, errors.Wrap(err, "dial option")
			}
		} else {
			more = append(more, o)
		}
	}

	opts = more

	conn, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	tr := tlog.SpanFromContext(ctx)

	if tr.If("dump_client,dump_conn") {
		dc := binary.ConnDump(conn, tr)
		dc.Callers = 5
		conn = dc
	}

	defer func() {
		if err == nil {
			return
		}

		_ = conn.Close()
	}()

	cl := binary.NewClient(conn)

	cl.Client.Name = p.AgentName

	more = more[:0]

	for _, o := range opts {
		if o, ok := o.(BinaryOption); ok {
			err = o.F(cl)
			if err != nil {
				return nil, errors.Wrap(err, "binary option")
			}
		} else {
			more = append(more, o)
		}
	}

	err = cl.Hello(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "hello")
	}

	if len(more) != 0 {
		return nil, errors.New("unused options: %v", more)
	}

	return cl, nil
}

func (p *BinaryPool) Put(ctx context.Context, cl clickhouse.Client, err error) error {
	return cl.(*binary.Client).Close()
}

func (p *BinaryPool) Close() error { return nil }
