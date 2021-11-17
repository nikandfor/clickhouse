package clpool

import (
	"path"

	"github.com/nikandfor/clickhouse"
	"github.com/nikandfor/clickhouse/binary"
	"github.com/nikandfor/loc"
)

type (
	Option string
)

func (o Option) String() string { return string(o) }

func OptFunc(d int) Option {
	pc := loc.Caller(1 + d)

	name, _, _ := pc.NameFileLine()

	name = path.Ext(name)[1:]

	return Option(name)
}

func WithAgentName(agent string) clickhouse.ClientOption {
	return &BinaryOption{
		Option: OptFunc(0),
		F: func(cl *binary.Client) error {
			cl.Client.Name = agent

			return nil
		},
	}
}

func WithDatabase(db string) clickhouse.ClientOption {
	return &BinaryOption{
		Option: OptFunc(0),
		F: func(cl *binary.Client) error {
			cl.Database = db

			return nil
		},
	}
}

func WithCredentials(db, user, pass string) clickhouse.ClientOption {
	return &BinaryOption{
		Option: OptFunc(0),
		F: func(cl *binary.Client) error {
			cl.Database = db
			cl.User = user
			cl.Password = pass

			return nil
		},
	}
}
