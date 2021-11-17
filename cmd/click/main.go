package main

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/nikandfor/cli"
	click "github.com/nikandfor/clickhouse"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/graceful"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/ext/tlflag"

	"github.com/nikandfor/clickhouse/batcher"
	"github.com/nikandfor/clickhouse/clpool"
	"github.com/nikandfor/clickhouse/dsn"
	"github.com/nikandfor/clickhouse/proxy"
)

func main() {
	proxyCmd := &cli.Command{
		Name:        "proxy",
		Description: "clickhouse reverse proxy. batching, metrics, processing",
		Action:      proxyRun,
		Flags: []*cli.Flag{
			cli.NewFlag("listen,l", ":9000", "address to listen to"),
			cli.NewFlag("dsn,dst,db,d", "tcp://:8900", "clickhouse address"),

			cli.NewFlag("user", "default", ""),
			cli.NewFlag("pass", "", ""),

			cli.NewFlag("batch-max-interval", time.Minute, "max time to wait for batch to commit. 0 to no batching"),
			cli.NewFlag("batch-max-rows", 1000000, "max rows in the batch"),
			cli.NewFlag("batch-max-size", "100MiB", "max batch size"),
		},
	}

	dumpCmd := &cli.Command{
		Name:        "dump",
		Description: "clickhouse reverse proxy. dump all data to logs.",
		Action:      dumpRun,
		Flags: []*cli.Flag{
			cli.NewFlag("listen,l", ":9000", "address to listen to"),
			cli.NewFlag("dsn,dst,d", "tcp://:8900", "clickhouse address"),

			cli.NewFlag("user", "default", ""),
			cli.NewFlag("pass", "", ""),
		},
	}

	testCmd := &cli.Command{
		Name:        "test",
		Description: "test commands",
		Flags: []*cli.Flag{
			cli.NewFlag("driver", "clickhouse", "sql driver"),
			cli.NewFlag("dsn,d", "tcp://:9000", "address to connect to"),
		},
		Commands: []*cli.Command{{
			Name:   "query",
			Action: testQuery,
			Flags: []*cli.Flag{
				cli.NewFlag("query,q", "SELECT colA, colB, colC FROM table LIMIT 3", "query to send"),
			},
		}, {
			Name:   "exec,insert",
			Action: testExec,
			Args:   cli.Args{},
			Flags: []*cli.Flag{
				cli.NewFlag("query,q", "INSERT INTO table (colA, colB, colC)", "query to send"),
			},
		}},
	}

	cli.App = cli.Command{
		Name:   "clickhouse tools",
		Before: before,
		Flags: []*cli.Flag{
			cli.NewFlag("log", "stderr+dm", "log output file (or stderr)"),
			cli.NewFlag("verbosity,v", "", "logger verbosity topics"),
			cli.NewFlag("debug", "", "debug address"),

			cli.FlagfileFlag,
			cli.HelpFlag,
		},
		Commands: []*cli.Command{
			proxyCmd,
			dumpCmd,
			testCmd,
		},
	}

	cli.RunAndExit(os.Args)
}

func before(c *cli.Command) error {
	w, err := tlflag.OpenWriter(c.String("log"))
	if err != nil {
		return errors.Wrap(err, "open log file")
	}

	tlog.DefaultLogger = tlog.New(w)

	tlog.SetFilter(c.String("verbosity"))

	if q := c.String("debug"); q != "" {
		go func() {
			tlog.Printw("start debug interface", "addr", q)

			err := http.ListenAndServe(q, nil)
			if err != nil {
				tlog.Printw("debug", "addr", q, "err", err, "", tlog.Fatal)
				os.Exit(1)
			}
		}()
	}

	return nil
}

func proxyRun(c *cli.Command) (err error) {
	tr := tlog.Start("clickhouse_proxy")
	defer func() { tr.Finish("err", err) }()

	ctx := context.Background()
	ctx = tlog.ContextWithSpan(ctx, tr)

	d, err := dsn.Parse(c.String("dsn"))
	if err != nil {
		return errors.Wrap(err, "parse dsn")
	}

	var pool click.ClientPool

	pool = clpool.NewBinaryPool(d.Hosts[0])

	if q := c.Duration("batch-max-interval"); q != 0 {
		b := batcher.New(ctx, pool)

		b.MaxInterval = q
		b.MaxRows = c.Int("batch-max-rows")

		b.MaxBytes, err = parseSize(c.String("batch-max-size"))
		if err != nil {
			return errors.Wrap(err, "parse batch size")
		}

		pool = b
	}

	p := proxy.New(pool)

	defer func() {
		e := p.Close()
		if err == nil {
			err = errors.Wrap(e, "close proxy")
		}
	}()

	l, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	tr.Printw("listening", "listen", l.Addr())

	err = graceful.Shutdown(ctx, func(ctx context.Context) error {
		return p.Serve(ctx, l)
	}, graceful.WithStop(func() {
		err := l.Close()
		if err != nil {
			tr.Printw("close listener", "err", err)
		}
	}), graceful.WithForceStop(func(i int) {
		tr.Printw("Ctrl-C more to kill...", "more_to_kill", i+1)
	}))

	return err
}

func testQuery(c *cli.Command) (err error) {
	db, err := sql.Open(c.String("driver"), c.String("dsn"))
	if err != nil {
		return errors.Wrap(err, "open")
	}

	defer func() {
		e := db.Close()
		if err == nil {
			err = errors.Wrap(e, "close db")
		}
	}()

	args := make([]interface{}, c.Args.Len())
	for i, a := range c.Args {
		args[i] = a
	}

	rows, err := db.Query(c.String("query"), args...)
	if err != nil {
		return errors.Wrap(err, "query")
	}

	defer func() {
		e := rows.Close()
		if err == nil {
			err = errors.Wrap(e, "close rows")
		}
	}()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return errors.Wrap(err, "col types")
	}

	for i, tp := range cols {
		tlog.Printw("columns", "i", i, "name", tp.Name(), "db_type", tp.DatabaseTypeName(), "go_type", tp.ScanType())
	}

	buf := make([]string, len(cols))
	ptrs := make([]interface{}, len(cols))

	for i := range buf {
		ptrs[i] = &buf[i]
	}

	for rows.Next() {
		err = rows.Scan(ptrs...)
		if err != nil {
			return errors.Wrap(err, "scan")
		}

		tlog.Printw("row", "row", buf)
	}

	err = rows.Err()
	if err != nil {
		return errors.Wrap(err, "rows")
	}

	return nil
}

func testExec(c *cli.Command) (err error) {
	db, err := sql.Open(c.String("driver"), c.String("dsn"))
	if err != nil {
		return errors.Wrap(err, "open")
	}

	defer func() {
		e := db.Close()
		if err == nil {
			err = errors.Wrap(e, "close db")
		}
	}()

	tx, err := db.Begin()
	if err != nil {
		return errors.Wrap(err, "begin")
	}

	defer func() {
		if err == nil {
			return
		}

		e := tx.Rollback()
		if err == nil {
			err = errors.Wrap(e, "rollback")
		}
	}()

	s, err := tx.Prepare(c.String("query"))
	if err != nil {
		return errors.Wrap(err, "prepare")
	}

	for i, a := range c.Args {
		cols := strings.Split(a, ",")
		colsi := make([]interface{}, len(cols))

		for j := range cols {
			colsi[j] = cols[j]
		}

		_, err = s.Exec(colsi...)
		if err != nil {
			return errors.Wrap(err, "exec arg %d", i)
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "commit")
	}

	tlog.Printw("committed")

	return nil
}

func parseSize(s string) (sz int64, err error) {
	parts := regexp.MustCompile(`^(\d+)((?:[KMG]i?)?B)$`).FindStringSubmatch(s)

	tlog.Printw("parts", "parts", parts)

	if len(parts) != 3 {
		return 0, errors.New("bad value: %v", s)
	}

	sz, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return
	}

	if parts[2] == "" {
		return sz, nil
	}

	switch parts[2][0] {
	case 'B':
	case 'K':
		sz <<= 10
	case 'M':
		sz <<= 20
	case 'G':
		sz <<= 30
	default:
		panic(parts[2])
	}

	return sz, nil
}
