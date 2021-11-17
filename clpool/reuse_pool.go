package clpool

import (
	"context"
	"sync/atomic"

	"github.com/nikandfor/clickhouse"
)

type (
	ReusePool struct {
		clickhouse.ClientPool

		// TODO: reuse connections

		closed int32
	}
)

var _ clickhouse.ClientPool = &ReusePool{}

func NewReusePool(pool clickhouse.ClientPool) *ReusePool {
	return &ReusePool{ClientPool: pool}
}

func (p *ReusePool) Get(ctx context.Context, opts ...clickhouse.ClientOption) (cl clickhouse.Client, err error) {
	cl, err = p.ClientPool.Get(ctx, opts...)
	if err != nil {
		return
	}

	return cl, nil
}

func (p *ReusePool) Put(ctx context.Context, cl clickhouse.Client, err error) error {
	if atomic.LoadInt32(&p.closed) != 0 {
		return p.ClientPool.Put(ctx, cl, err)
	}

	return p.ClientPool.Put(ctx, cl, err)
}

func (p *ReusePool) Close() error {
	atomic.StoreInt32(&p.closed, 1)

	return nil
}
