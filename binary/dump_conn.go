package binary

import (
	"encoding/hex"
	"io"
	"net"

	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Flusher interface {
		Flush() error
	}

	DumpConn struct {
		io.Reader
		io.Writer
		io.Closer
		Flusher

		net.Conn

		tlog.Span
		Callers int
	}
)

func NewDumpConn(c interface{}, tr tlog.Span) *DumpConn {
	dc := &DumpConn{
		Span: tr,

		Callers: 3,
	}

	dc.Reader, _ = c.(io.Reader)
	dc.Writer, _ = c.(io.Writer)
	dc.Closer, _ = c.(io.Closer)
	dc.Flusher, _ = c.(Flusher)
	dc.Conn, _ = c.(net.Conn)

	return dc
}

func (c *DumpConn) Read(p []byte) (n int, err error) {
	n, err = c.Reader.Read(p)

	c.Printf("read    %v %v%s\n%s", n, err, c.from(), hex.Dump(p[:n]))

	return
}

func (c *DumpConn) Write(p []byte) (n int, err error) {
	n, err = c.Writer.Write(p)

	c.Printf("written %v %v%s\n%s", n, err, c.from(), hex.Dump(p[:n]))

	return
}

func (c *DumpConn) Close() (err error) {
	err = c.Closer.Close()

	c.Printf("closed  %v%s", err, c.from())

	return
}

func (c *DumpConn) from() string {
	if c.Callers == 0 {
		return ""
	}

	return "  from " + loc.Callers(2, c.Callers).String()
}
