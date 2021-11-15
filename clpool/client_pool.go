package clpool

import (
	"context"

	"github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
)

type (
	Getter func(context.Context) (clickhouse.Client, error)

	ConnectionsPool struct {
		New Getter
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
	return cl.Close()
}
