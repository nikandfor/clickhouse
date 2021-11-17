//go:build ignore

package processor

import (
	"context"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common"
	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

type (
	CelProcessor struct {
		p    cel.Program
		pool click.ClientPool
	}

	celClient struct {
		p *CelProcessor

		cl click.Client
	}
)

var (
	_ click.ClientPool = &CelProcessor{}
	_ click.Client     = &celClient{}
)

func NewCelProcessor(pool click.ClientPool, prog common.Source) (p *CelProcessor, err error) {
	prg, err := compileCel(prog)
	if err != nil {
		return nil, errors.Wrap(err, "cel")
	}

	p = &CelProcessor{
		p:    prg,
		pool: pool,
	}

	return p, nil
}

func (p *CelProcessor) Get(ctx context.Context) (_ click.Client, err error) {
	cl, err := p.pool.Get(ctx)
	if err != nil {
		return nil, err
	}

	return &celClient{
		p:  p,
		cl: cl,
	}, nil
}

func (p *CelProcessor) Put(ctx context.Context, cl click.Client, err error) error {
	return p.pool.Put(ctx, cl.(*celClient).cl, err)
}

func compileCel(prog common.Source) (prg cel.Program, err error) {
	ds := cel.Declarations(
		decls.NewVar("query", decls.String),
		decls.NewFunction("service",
			decls.NewInstanceOverload("service", []*exprpb.Type{decls.String}, decls.String),
		),
	)

	env, err := cel.NewEnv(ds)
	if err != nil {
		return nil, errors.Wrap(err, "new env")
	}

	ast, iss := env.CompileSource(prog)
	if iss.Err() != nil {
		return nil, errors.Wrap(iss.Err(), "compile")
	}

	prg, err = env.Program(ast)
	if err != nil {
		return nil, errors.Wrap(err, "program")
	}

	return prg, nil
}

func (c *celClient) Hello(ctx context.Context) error {
	return nil
}

func (c *celClient) NextPacket(ctx context.Context) (tp click.ServerPacket, err error) {
	return c.cl.NextPacket(ctx)
}

func (c *celClient) SendQuery(ctx context.Context, q *click.Query) (meta click.QueryMeta, err error) {
	meta, err = c.cl.SendQuery(ctx, q)
	if err != nil {
		return
	}

	return meta, nil
}

func (c *celClient) CancelQuery(ctx context.Context) error {
	return c.cl.CancelQuery(ctx)
}

func (c *celClient) RecvBlock(ctx context.Context, compr bool) (b *click.Block, err error) {
	b, err = c.cl.RecvBlock(ctx, compr)
	if err != nil {
		return
	}

	return b, nil
}

func (c *celClient) SendBlock(ctx context.Context, b *click.Block, compr bool) (err error) {
	return c.cl.SendBlock(ctx, b, compr)
}

func (c *celClient) RecvException(ctx context.Context) error {
	return c.cl.RecvException(ctx)
}

func (c *celClient) RecvProgress(ctx context.Context) (click.Progress, error) {
	return c.cl.RecvProgress(ctx)
}

func (c *celClient) RecvProfileInfo(ctx context.Context) (click.ProfileInfo, error) {
	return c.cl.RecvProfileInfo(ctx)
}

func (c *celClient) Close() error { panic("nah") }
