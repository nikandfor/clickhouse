package main

import (
	"context"
	"database/sql"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/nikandfor/cli"
	"github.com/nikandfor/errors"
	"github.com/nikandfor/graceful"
	"github.com/nikandfor/tlog"
	"github.com/nikandfor/tlog/ext/tlflag"

	"github.com/nikandfor/clickhouse/proxy"
)

func main() {
	proxyCmd := &cli.Command{
		Name:        "proxy",
		Description: "clickhouse reverse proxy. batching, metrics, processing",
		Action:      proxyRun,
		Flags: []*cli.Flag{
			cli.NewFlag("listen,l", ":9000", "address to listen to"),
			cli.NewFlag("dsn,dst,d", "tcp://:8900", "clickhouse address"),

			cli.NewFlag("user", "default", ""),
			cli.NewFlag("pass", "", ""),
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
	p, err := proxy.New(c.String("dsn"))
	if err != nil {
		return errors.Wrap(err, "new proxy")
	}

	l, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		return errors.Wrap(err, "listen")
	}

	tlog.Printw("listening", "listen", l.Addr())

	ctx := context.Background()

	ctx = tlog.ContextWithSpan(ctx, tlog.Span{Logger: tlog.DefaultLogger})

	err = graceful.Shutdown(ctx, func(ctx context.Context) error {
		return p.ServeContext(ctx, l)
	},
		graceful.WithStop(p.Shutdown),
		graceful.WithForceStop(func(i int) {
			tlog.Printw("Ctrl-C more to kill...")
		}),
	)

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
