package clickhouse

import (
	"fmt"
	"regexp"

	"github.com/nikandfor/tlog/wire"
)

type (
	Query struct {
		Query string

		ID       string
		QuotaKey string

		Compressed bool

		//	Tables []ExtTable

		//	Info []byte

		Client Agent
	}

	QueryMeta []Column

	Column struct {
		Name string
		Type string

		RawData []byte
	}

	Block struct {
		Table string
		//	Info  []byte

		Rows int
		Cols []Column
	}

	Exception struct {
		Code       int32
		Name       string
		Message    string
		StackTrace string

		Cause *Exception
	}

	Progress struct {
		Rows      uint64
		Bytes     uint64
		TotalRows uint64
	}

	ProfileInfo struct {
		Rows   uint64
		Blocks uint64
		Bytes  uint64

		AppliedLimit        uint64
		RowsBeforeLimit     uint64
		CalcRowsBeforeLimit uint64
	}

	Credentials struct {
		Database string
		User     string
		Password string
	}

	Agent struct {
		Name string
		Ver  Ver
	}

	Ver [3]int
)

var (
	insertRE = regexp.MustCompile(`^(?i)INSERT INTO`) // TODO: WITH
	execRE   = regexp.MustCompile(`^(?i)(?:CREATE|ALTER|DROP)`)
)

func (q *Query) IsInsert() bool { return insertRE.MatchString(q.Query) }

func (q *Query) IsExec() bool { return execRE.MatchString(q.Query) }

func (q *Query) Copy() *Query {
	return &Query{
		Query:      q.Query,
		ID:         q.ID,
		QuotaKey:   q.QuotaKey,
		Compressed: q.Compressed,
		//	Info:       append([]byte{}, q.Info...),
		Client: q.Client,
	}
}

func (b *Block) IsEmpty() bool { return b == nil || b.Rows == 0 && len(b.Cols) == 0 }

func (b *Block) DataSize() (size int64) {
	for _, col := range b.Cols {
		size += int64(1 + len(col.Name))
		size += int64(1 + len(col.Type))

		size += int64(len(col.RawData))
	}

	return
}

func (b *Block) TlogAppend(e *wire.Encoder, buf []byte) []byte {
	if b == nil {
		return e.AppendNil(buf)
	}

	buf = e.AppendMap(buf, 3)

	//	buf = e.AppendKeyInt(buf, "table", b.Table)
	buf = e.AppendKeyInt(buf, "cols", len(b.Cols))
	buf = e.AppendKeyInt(buf, "rows", b.Rows)
	buf = e.AppendKeyInt64(buf, "size", b.DataSize())

	return buf
}

func (m QueryMeta) TlogAppend(e *wire.Encoder, b []byte) []byte {
	b = e.AppendMap(b, len(m))

	for _, c := range m {
		b = e.AppendKeyString(b, c.Name, c.Type)
	}

	return b
}

func (e *Exception) Error() string {
	return fmt.Sprintf("%v (%x): %v", e.Name, e.Code, e.Message)
}
