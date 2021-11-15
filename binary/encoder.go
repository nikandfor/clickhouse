package binary

import (
	"encoding/binary"
	"io"

	clbinary "github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/nikandfor/tlog"
)

type (
	Encoder struct {
		w io.Writer

		o io.Writer
		z flushWriter

		b []byte
	}

	flushWriter interface {
		io.Writer
		Flush() error
	}
)

func NewEncoder(w io.Writer) *Encoder {
	if tlog.If("dump_encoder") {
		w = Dump{
			Writer:  w,
			Span:    tlog.Root(),
			Callers: 5,
		}
	}

	return &Encoder{
		w: w,
		o: w,
		z: clbinary.NewCompressWriter(w),
		b: make([]byte, 16),
	}
}

func (e *Encoder) SetCompressed(c bool) (err error) {
	if !c {
		err = e.z.Flush()
		if err != nil {
			return
		}
	}

	if c {
		e.w = e.z
	} else {
		e.w = e.o
	}

	return nil
}

func (e *Encoder) Write(p []byte) (int, error) {
	return e.w.Write(p)
}

func (e *Encoder) Flush() error {
	if f, ok := e.w.(interface{ Flush() error }); ok {
		return f.Flush()
	}

	return nil
}

func (e *Encoder) Uvarint(x int) (err error) {
	n := binary.PutUvarint(e.b, uint64(x))

	_, err = e.Write(e.b[:n])

	return
}

func (e *Encoder) Uvarint64(x uint64) (err error) {
	n := binary.PutUvarint(e.b, x)

	_, err = e.Write(e.b[:n])

	return
}

func (e *Encoder) UInt64(x uint64) (err error) {
	binary.LittleEndian.PutUint64(e.b, x)

	_, err = e.w.Write(e.b[:8])

	return
}

func (e *Encoder) UInt32(x uint32) (err error) {
	binary.LittleEndian.PutUint32(e.b, x)

	_, err = e.w.Write(e.b[:4])

	return
}

func (e *Encoder) UInt16(x uint16) (err error) {
	binary.LittleEndian.PutUint16(e.b, x)

	_, err = e.w.Write(e.b[:2])

	return
}

func (e *Encoder) UInt8(x uint8) (err error) {
	e.b[0] = x

	_, err = e.w.Write(e.b[:1])

	return
}

func (e *Encoder) Int64(x int64) (err error) {
	binary.LittleEndian.PutUint64(e.b, uint64(x))

	_, err = e.w.Write(e.b[:8])

	return
}

func (e *Encoder) Int32(x int32) (err error) {
	binary.LittleEndian.PutUint32(e.b, uint32(x))

	_, err = e.w.Write(e.b[:4])

	return
}

func (e *Encoder) Int16(x int16) (err error) {
	binary.LittleEndian.PutUint16(e.b, uint16(x))

	_, err = e.w.Write(e.b[:2])

	return
}

func (e *Encoder) Int8(x int8) (err error) {
	e.b[0] = uint8(x)

	_, err = e.w.Write(e.b[:1])

	return
}

func (e *Encoder) String(x string) (err error) {
	err = e.Uvarint(len(x))
	if err != nil {
		return
	}

	if w, ok := e.w.(io.StringWriter); ok {
		_, err = w.WriteString(x)
		return
	}

	e.b = append(e.b[:0], x...)

	_, err = e.w.Write(e.b[:len(x)])

	e.b = e.b[:cap(e.b)]

	return
}

func (e *Encoder) RawString(x []byte) (err error) {
	err = e.Uvarint(len(x))
	if err != nil {
		return
	}

	_, err = e.w.Write(x)

	return
}

func (e *Encoder) Bool(x bool) (err error) {
	if x {
		e.b[0] = 1
	} else {
		e.b[0] = 0
	}

	_, err = e.w.Write(e.b[:1])

	return
}
