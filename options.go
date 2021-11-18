package clickhouse

import (
	"path"

	"github.com/nikandfor/loc"
)

type (
	BaseOption string

	ApplyToCredentialser interface {
		ApplyToCredentials(*Credentials) error
	}

	CredentialsOption struct {
		BaseOption
		F func(*Credentials) error
	}
)

func (o BaseOption) String() string { return string(o) }

func OptFunc(d int) BaseOption {
	pc := loc.Caller(1 + d)

	name, _, _ := pc.NameFileLine()

	name = path.Ext(name)[1:]

	return BaseOption(name)
}

func WithDatabase(db string) ClientOption {
	return CredentialsOption{
		BaseOption: OptFunc(0),
		F: func(c *Credentials) error {
			c.Database = db

			return nil
		},
	}
}

func WithCredentials(c Credentials) ClientOption {
	return CredentialsOption{
		BaseOption: OptFunc(0),
		F: func(cr *Credentials) error {
			*cr = c

			return nil
		},
	}
}

func GetDatabase(opts ...ClientOption) string {
	var c Credentials

	for _, o := range opts {
		if o, ok := o.(ApplyToCredentialser); ok {
			err := o.ApplyToCredentials(&c)
			if err != nil {
				panic(err)
			}
		}
	}

	return c.Database
}

func (o CredentialsOption) ApplyToCredentials(c *Credentials) (err error) {
	return o.F(c)
}
