package binary

import (
	"encoding/hex"
	"io"

	"github.com/nikandfor/loc"
	"github.com/nikandfor/tlog"
)

type (
	Dump struct {
		io.Reader
		io.Writer
		io.Closer

		tlog.Span
		Prefix string

		Callers int
	}
)

func NewDump(tr tlog.Span, p string, c interface{}) (d Dump) {
	d = Dump{
		Span:   tr,
		Prefix: p,
	}

	d.Reader, _ = c.(io.Reader)
	d.Writer, _ = c.(io.Writer)
	d.Closer, _ = c.(io.Closer)

	return d
}

func (c Dump) from() string {
	if c.Callers == 0 {
		return ""
	}

	return "  from " + loc.Callers(2, c.Callers).String()
}

func (c Dump) Read(p []byte) (n int, err error) {
	n, err = c.Reader.Read(p)

	c.Span.Printf("read  %s  buf %x => %x %v%s\n%s", c.Prefix, len(p), n, err, c.from(), hex.Dump(p[:n]))

	return
}

func (c Dump) Write(p []byte) (n int, err error) {
	n, err = c.Writer.Write(p)

	c.Span.Printf("write %s  buf %x => %x %v%s\n%s", c.Prefix, len(p), n, err, c.from(), hex.Dump(p))

	return
}

func (c Dump) Close() (err error) {
	err = c.Closer.Close()

	c.Span.Printf("close %s => %v%s", c.Prefix, err, c.from())

	return
}

func (c Dump) Flush() (err error) {
	if f, ok := c.Writer.(interface{ Flush() error }); ok {
		err = f.Flush()

		c.Span.Printf("flush %s  => %v%s", c.Prefix, err, c.from())

		return err
	}

	return nil
}
