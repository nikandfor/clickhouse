package clickhouse

import (
	"context"
	"io"
)

type (
	Client interface {
		Hello(context.Context) error

		NextPacket(context.Context) (ServerPacket, error)

		SendQuery(context.Context, *Query) (QueryMeta, error)
		CancelQuery(context.Context) error

		SendBlock(ctx context.Context, b *Block, compr bool) error
		RecvBlock(ctx context.Context, compr bool) (b *Block, err error)

		RecvException(context.Context) error
		RecvProgress(context.Context) (Progress, error)
		RecvProfileInfo(context.Context) (ProfileInfo, error)

		io.Closer
	}

	ServerConn interface {
		Hello(context.Context) error

		NextPacket(context.Context) (ClientPacket, error)

		RecvQuery(context.Context) (*Query, error)
		SendQueryMeta(ctx context.Context, m QueryMeta, compr bool) error

		SendBlock(ctx context.Context, b *Block, compr bool) error
		RecvBlock(ctx context.Context, compr bool) (b *Block, err error)

		SendEndOfStream(context.Context) error

		SendException(context.Context, error) error
		SendProgress(context.Context, Progress) error
		SendProfileInfo(context.Context, ProfileInfo) error

		//	SendPong(context.Context) error

		io.Closer
	}

	Server interface {
		HandleConn(context.Context, ServerConn) error
		HandleRequest(context.Context, ServerConn) error
	}

	ClientPool interface {
		Get(context.Context) (Client, error)
		Put(context.Context, Client, error) error
	}
)
