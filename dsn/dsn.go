package dsn

import (
	"net/url"
	"strings"
)

type DSN struct {
	Scheme string

	Hosts []string

	Database string
	User     string
	Password string

	Compress bool
}

func Parse(dsn string) (d *DSN, err error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	d = &DSN{
		Scheme:   u.Scheme,
		Hosts:    []string{u.Host},
		Database: "default",
		User:     "default",
	}

	q := u.Query()

	if x := q.Get("database"); x != "" {
		d.Database = x
	} else if x = q.Get("db"); x != "" {
		d.Database = x
	} else if x = u.Path; x != "" && x != "/" {
		x = strings.TrimPrefix(x, "/")

		xx := strings.SplitN(x, "/", 2)

		if xx[0] != "" {
			d.Database = xx[0]
		}
	}

	if x := q.Get("username"); x != "" {
		d.User = x
	} else if x = q.Get("user"); x != "" {
		d.User = x
	} else if x = u.User.Username(); x != "" {
		d.User = x
	}

	if x := q.Get("password"); x != "" {
		d.Password = x
	} else if x = q.Get("pass"); x != "" {
		d.Password = x
	} else if x, ok := u.User.Password(); ok && x != "" {
		d.Password = x
	}

	if x := q.Get("compress"); x != "" || q.Has("compress") {
		d.Compress = true
	}

	if x := q.Get("alt_hosts"); x != "" {
		d.Hosts = append(d.Hosts, strings.Split(x, ",")...)
	}

	return d, nil
}
