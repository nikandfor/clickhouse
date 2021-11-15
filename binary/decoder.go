package binary

import (
	"encoding/binary"
	"io"

	clbinary "github.com/ClickHouse/clickhouse-go/lib/binary"
	"github.com/nikandfor/tlog"
)

type Decoder struct {
	r io.Reader

	o, z io.Reader

	b []byte
}

func NewDecoder(r io.Reader) *Decoder {
	if tlog.If("dump_decoder") {
		// TODO
	}

	return &Decoder{
		r: r,
		o: r,
		z: clbinary.NewCompressReader(r),
		b: make([]byte, 16),
	}
}

func (d *Decoder) SetCompressed(c bool) {
	if c {
		d.r = d.z
	} else {
		d.r = d.o
	}
}

func (d *Decoder) Read(p []byte) (int, error) {
	return d.r.Read(p)
}

func (d *Decoder) ReadByte() (b byte, err error) {
	if r, ok := d.r.(io.ByteReader); ok {
		return r.ReadByte()
	}

	_, err = d.r.Read(d.b[:1])
	if err != nil {
		return 0, err
	}

	return d.b[0], nil
}

func (d *Decoder) Uvarint() (_ int, err error) {
	x, err := binary.ReadUvarint(d)

	return int(x), err
}

func (d *Decoder) Uvarint64() (_ uint64, err error) {
	return binary.ReadUvarint(d)
}

func (d *Decoder) UInt64() (x uint64, err error) {
	_, err = io.ReadFull(d.r, d.b[:8])
	if err != nil {
		return
	}

	return binary.LittleEndian.Uint64(d.b[:]), nil
}

func (d *Decoder) UInt32() (x uint32, err error) {
	_, err = io.ReadFull(d.r, d.b[:4])
	if err != nil {
		return
	}

	return binary.LittleEndian.Uint32(d.b[:]), nil
}

func (d *Decoder) UInt16() (x uint16, err error) {
	_, err = io.ReadFull(d.r, d.b[:2])
	if err != nil {
		return
	}

	return binary.LittleEndian.Uint16(d.b[:]), nil
}

func (d *Decoder) UInt8() (x uint8, err error) {
	_, err = d.r.Read(d.b[:1])
	if err != nil {
		return
	}

	return d.b[0], nil
}

func (d *Decoder) Int64() (x int64, err error) {
	_, err = io.ReadFull(d.r, d.b[:8])
	if err != nil {
		return
	}

	return int64(binary.LittleEndian.Uint64(d.b[:])), nil
}

func (d *Decoder) Int32() (x int32, err error) {
	_, err = io.ReadFull(d.r, d.b[:4])
	if err != nil {
		return
	}

	return int32(binary.LittleEndian.Uint32(d.b[:])), nil
}

func (d *Decoder) Int16() (x int16, err error) {
	_, err = io.ReadFull(d.r, d.b[:2])
	if err != nil {
		return
	}

	return int16(binary.LittleEndian.Uint16(d.b[:])), nil
}

func (d *Decoder) Int8() (x int8, err error) {
	_, err = d.r.Read(d.b[:1])
	if err != nil {
		return
	}

	return int8(d.b[0]), nil
}

func (d *Decoder) Bytes() (x []byte, err error) {
	l, err := d.Uvarint()
	if err != nil {
		return
	}

	d.grow(int(l))

	_, err = io.ReadFull(d.r, d.b[:l])
	if err != nil {
		return
	}

	return d.b[:l], nil
}

func (d *Decoder) String() (x string, err error) {
	s, err := d.Bytes()
	if err != nil {
		return "", err
	}

	return string(s), nil
}

func (d *Decoder) Bool() (x bool, err error) {
	_, err = d.r.Read(d.b[:1])
	if err != nil {
		return
	}

	return d.b[0] != 0, nil
}

func (d *Decoder) ReadUvarint() (x []byte, err error) {
	v, err := binary.ReadUvarint(d)
	if err != nil {
		return
	}

	m := binary.PutUvarint(d.b, v)

	return d.b[:m], err
}

func (d *Decoder) ReadString() (x []byte, err error) {
	l, err := d.Uvarint()
	if err != nil {
		return
	}

	m := binary.PutUvarint(d.b, uint64(l))

	d.grow(m + l)

	_, err = io.ReadFull(d.r, d.b[m:m+l])
	if err != nil {
		return
	}

	return d.b[:m+l], nil
}

func (d *Decoder) ReadFixed(n int) (x []byte, err error) {
	d.grow(n)

	_, err = io.ReadFull(d.r, d.b[:n])
	if err != nil {
		return
	}

	return d.b[:n], nil
}

func (d *Decoder) grow(l int) {
	for len(d.b) < l {
		d.b = append(d.b, 0, 0, 0, 0, 0, 0, 0, 0)
		d.b = d.b[:cap(d.b)]
	}
}
