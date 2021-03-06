module github.com/nikandfor/clickhouse

go 1.17

require (
	github.com/ClickHouse/clickhouse-go v1.5.1
	github.com/google/cel-go v0.9.0
	github.com/nikandfor/cli v0.0.0-20210105003942-afe14413f747
	github.com/nikandfor/errors v0.4.0
	github.com/nikandfor/graceful v0.0.0-20211115215916-d1e69cb51d77
	github.com/nikandfor/loc v0.1.1-0.20210914135013-829520244234
	github.com/nikandfor/netpoll v0.0.0-20211124145858-9739b0b763d8
	github.com/nikandfor/tlog v0.12.2-0.20211123200322-8880f72871a2
	github.com/stretchr/testify v1.7.0
	google.golang.org/genproto v0.0.0-20210831024726-fe130286e0e2
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr v0.0.0-20210826220005-b48c857c3a0e // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cloudflare/golz4 v0.0.0-20150217214814-ef862a3cdc58 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v1.11.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	golang.org/x/sys v0.0.0-20210831042530-f4d43177bf5e // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200605160147-a5ece683394c // indirect
)

replace github.com/ClickHouse/clickhouse-go => github.com/nikandfor/clickhouse-go v1.5.2-0.20211123125730-7b296aa1f0ea
