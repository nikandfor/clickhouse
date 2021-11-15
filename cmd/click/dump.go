package main

import (
	"context"
	"encoding/hex"
	"io"
	"net"
	"sync"
	"time"

	"github.com/nikandfor/cli"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/graceful"
	"github.com/nikandfor/loc"
	"github.com/nikandfor/netpoll"
	"github.com/nikandfor/tlog"
)

type (
	Dumper struct {
	}
)

func dumpRun(c *cli.Command) (err error) {
	l, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	// closed on interrupt

	tlog.Printw("listening", "listen", l.Addr())

	ctx := context.Background()

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var lerr error

	err = graceful.Shutdown(context.Background(), func(ctx context.Context) error {
		var wg sync.WaitGroup

		defer wg.Wait()

		for {
			conn, err := l.Accept()
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			if err != nil {
				return errors.Wrap(err, "accept")
			}

			wg.Add(1)
			go func() {
				defer wg.Done()

				dumpConn(connCtx, c, conn)
			}()
		}
	}, graceful.WithStop(func() {
		tlog.Printw("stopping listener, waiting for connections to finish")

		lerr = l.Close()
		lerr = errors.Wrap(lerr, "close listener")
	}), graceful.WithForceStop(func(i int) {
		tlog.Printw("killing connections")

		cancel()
	}))

	if err == nil {
		err = lerr
	} else if err != nil {
		tlog.Printw("close listener", "err", err)
	}

	return err
}

func dumpConn(ctx context.Context, c *cli.Command, conn net.Conn) (err error) {
	tr := tlog.Start("dump_conn", "addr", conn.RemoteAddr(), "local_addr", conn.LocalAddr())
	defer func() {
		tr.Finish("err", err, "", loc.Caller(1))
	}()

	ctx = tlog.ContextWithSpan(ctx, tr)

	defer func() {
		e := conn.Close()
		if err == nil {
			err = errors.Wrap(e, "close client conn")
		}
	}()

	dst, err := net.Dial("tcp", c.String("dst"))
	if err != nil {
		return errors.Wrap(err, "dial dst")
	}

	defer func() {
		e := dst.Close()
		if err == nil {
			err = errors.Wrap(e, "close server conn")
		}
	}()

	buf := make([]byte, 0x1000)

	list := []io.Reader{conn, dst}

	for len(list) != 0 {
		s, err := netpoll.Select(list, 100*time.Millisecond)

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		switch err {
		case nil:
		case netpoll.ErrNoEvents:
			continue
		default:
			return errors.Wrap(err, "select")
		}

		var prefix string
		var w io.Writer
		if s == conn {
			w = dst
			prefix = "src to dst >>>"
		} else {
			w = conn
			prefix = "dst to src <<<"
		}

		n, err := s.Read(buf)
		if err == io.EOF {
			tr.Printf("closed  %v", prefix)

			list = list[:len(list)-1]

			if len(list) == 0 {
				continue
			}

			if s == conn {
				list[0] = dst
			} else {
				list[0] = conn
			}

			if cw, ok := list[0].(interface {
				CloseWrite() error
			}); ok {
				err = cw.CloseWrite()
				if err != nil {
					return errors.Wrap(err, "close write")
				}
			}

			continue
		}

		tr.Printf("read    %v  %v %v\n%s", prefix, n, err, hex.Dump(buf[:n]))
		if err != nil {
			return errors.Wrap(err, "read %v", prefix)
		}

		m, err := w.Write(buf[:n])
		tr.Printf("written %v  %v %v", prefix, m, err)
		if err != nil {
			return errors.Wrap(err, "write %v", prefix)
		}
	}

	return nil
}
