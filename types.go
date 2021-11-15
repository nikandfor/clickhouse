package clickhouse

import (
	"fmt"
	"regexp"
)

type (
	Query struct {
		Query string

		ID       string
		QuotaKey string

		Compressed bool

		//	Tables []ExtTable

		Info []byte

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

	Agent struct {
		Name string
		Ver  Ver
	}

	Ver [3]int
)

var insertRE = regexp.MustCompile(`^(?i)INSERT INTO`)

func (q *Query) IsInsert() bool { return insertRE.MatchString(q.Query) }

func (b *Block) IsEmpty() bool { return b.Rows == 0 && len(b.Cols) == 0 }

func (b *Block) DataSize() (size int64) {
	for _, col := range b.Cols {
		size += int64(1 + len(col.Name))
		size += int64(1 + len(col.Type))

		size += int64(len(col.RawData))
	}

	return
}

func (e *Exception) Error() string {
	return fmt.Sprintf("%v (%x): %v", e.Name, e.Code, e.Message)
}
