package binary

import (
	"encoding/hex"
	"io"
	"net"

	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Conn struct {
		io.Reader
		io.Writer
		io.Closer

		net.Conn

		tlog.Span
		Callers int
	}
)

func ConnDump(c net.Conn, tr tlog.Span) *Conn {
	return &Conn{
		Reader: c,
		Writer: c,
		Closer: c,
		Conn:   c,
		Span:   tr,

		Callers: 3,
	}
}

func (c *Conn) Read(p []byte) (n int, err error) {
	n, err = c.Reader.Read(p)

	c.Printf("read    %v %v%s\n%s", n, err, c.from(), hex.Dump(p[:n]))

	return
}

func (c *Conn) Write(p []byte) (n int, err error) {
	n, err = c.Writer.Write(p)

	c.Printf("written %v %v%s\n%s", n, err, c.from(), hex.Dump(p[:n]))

	return
}

func (c *Conn) Close() (err error) {
	err = c.Closer.Close()

	c.Printf("closed  %v%s", err, c.from())

	return
}

func (c *Conn) from() string {
	if c.Callers == 0 {
		return ""
	}

	return "  from " + loc.Callers(2, c.Callers).String()
}
