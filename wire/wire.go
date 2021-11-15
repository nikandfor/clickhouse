package wire

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

		Client string
		Ver    [3]int
	}

	QueryMeta []Column

	Column struct {
		Name string
		Type string

		Data []byte
	}

	Block struct {
		Table string
		Info  []byte

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
)

var insertRE = regexp.MustCompile(`^(?i)INSERT INTO`)

func (q *Query) IsInsert() bool { return insertRE.MatchString(q.Query) }

func (b *Block) IsEmpty() bool { return true }

func (e *Exception) Error() string {
	return fmt.Sprintf("%v (%x): %v", e.Name, e.Code, e.Message)
}
