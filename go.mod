module github.com/nikandfor/clickhouse

go 1.17

require (
	github.com/ClickHouse/clickhouse-go v1.5.1
	github.com/nikandfor/cli v0.0.0-20210105003942-afe14413f747
	github.com/nikandfor/errors v0.4.0
	github.com/nikandfor/graceful v0.0.0-0-0
	github.com/nikandfor/loc v0.1.1-0.20210914135013-829520244234
	github.com/nikandfor/netpoll v0.0.0-00010101000000-000000000000
	github.com/nikandfor/tlog v0.12.2-0.20211110181733-1de114389e87
)

require (
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/sys v0.0.0-20210415045647-66c3f260301c // indirect
	golang.org/x/term v0.0.0-20201117132131-f5c789dd3221 // indirect
)

replace github.com/nikandfor/graceful => ../graceful

replace github.com/nikandfor/tlog => ../tlog

replace github.com/nikandfor/netpoll => ../netpoll
