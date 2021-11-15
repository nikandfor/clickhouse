package clpool

import (
	"context"
	"sync/atomic"

	"github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	Getter func(context.Context) (clickhouse.Client, error)

	ConnectionsPool struct {
		New Getter

		// TODO: reuse connections

		closed int32
	}
)

func (p *ConnectionsPool) Get(ctx context.Context) (cl clickhouse.Client, err error) {
	cl, err = p.New(ctx)
	if err != nil {
		return
	}

	err = cl.Hello(ctx)
	if err != nil {
		_ = cl.Close()

		return nil, errors.Wrap(err, "hello")
	}

	return cl, nil
}

func (p *ConnectionsPool) Put(ctx context.Context, cl clickhouse.Client, err error) error {
	if atomic.LoadInt32(&p.closed) != 0 {
		return cl.Close()
	}

	return cl.Close()
}

func (p *ConnectionsPool) Close() error {
	atomic.StoreInt32(&p.closed, 1)

	return nil
}
