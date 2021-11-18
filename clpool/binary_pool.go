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

		Credentials clickhouse.Credentials

		net.Dialer
	}
)

var _ clickhouse.ClientPool = &BinaryPool{}

func NewBinaryPool(addr string) *BinaryPool {
	return &BinaryPool{
		addr:      addr,
		AgentName: "gh/nikandfor/clickhouse",
		Credentials: clickhouse.Credentials{
			Database: "default",
			User:     "default",
		},
	}
}

func (p *BinaryPool) Get(ctx context.Context, opts ...clickhouse.ClientOption) (_ clickhouse.Client, err error) {
	creds := p.Credentials

	for _, o := range opts {
		if o, ok := o.(clickhouse.ApplyToCredentialser); ok {
			err = o.ApplyToCredentials(&creds)
			if err != nil {
				return nil, errors.Wrap(err, "credentials option")
			}
		}
	}

	conn, err := net.Dial("tcp", p.addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}

	tr := tlog.SpanFromContext(ctx)

	if tr.If("dump_client_conn,dump_conn") {
		dc := binary.NewDumpConn(conn, tr)
		dc.Callers = 5
		conn = dc
	}

	defer func() {
		if err == nil {
			return
		}

		_ = conn.Close()
	}()

	cl := binary.NewClient(ctx, conn)

	cl.Client.Name = p.AgentName
	cl.Credentials = creds

	err = cl.Hello(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "hello")
	}

	tlog.SpanFromContext(ctx).V("hello").Printw("client hello", "server_conn", cl)

	return cl, nil
}

func (p *BinaryPool) Put(ctx context.Context, cl clickhouse.Client, err error) error {
	return cl.(*binary.Client).Close()
}

func (p *BinaryPool) Close() error { return nil }
