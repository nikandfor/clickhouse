package conn

import (
	"encoding/hex"
	"net"

	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Dump struct {
		net.Conn

		tlog.Span
		Prefix string

		Callers int
	}
)

func NewDump(tr tlog.Span, p string, c net.Conn) (d Dump) {
	d = Dump{
		Conn:   c,
		Span:   tr,
		Prefix: p,
	}

	return d
}

func (c Dump) Read(p []byte) (n int, err error) {
	n, err = c.Conn.Read(p)

	cs := ""

	if c.Callers != 0 {
		cs = loc.Callers(1, c.Callers).String()
	}

	c.Span.Printf("read  %s  buf %x => %x %v%s\n%s", c.Prefix, len(p), n, err, cs, hex.Dump(p[:n]))

	return
}

func (c Dump) Write(p []byte) (n int, err error) {
	n, err = c.Conn.Write(p)

	c.Span.Printf("write %s  buf %x => %x %v\n%s", c.Prefix, len(p), n, err, hex.Dump(p[:n]))

	return
}

func (c Dump) Close() (err error) {
	err = c.Conn.Close()

	c.Span.Printf("close %s => %v", c.Prefix, err)

	return
}

func (c Dump) Flush() error {
	if f, ok := c.Conn.(interface{ Flush() error }); ok {
		return f.Flush()
	}

	return nil
}
