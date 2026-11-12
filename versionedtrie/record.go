package versionedtrie

import (
	"encoding/binary"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

const MaxRecordBytes = 1 << 20

type Kind uint8

const (
	Branch Kind = iota
	Extension
	Leaf
)

// Path contains canonical compact hex-prefix bytes (not nibble bytes). It is
// structural metadata, never a substitute for authentication of rebuilt RLP.
type Record struct {
	Kind        Kind
	Bitmap      uint16
	Hash        mpt.Hash
	Inline      bool
	Path, Value []byte
}

func pathNibbles(p []byte, kind Kind) (int, error) {
	if len(p) == 0 || len(p) > 33 {
		return 0, ErrCorrupt
	}
	flag := p[0] >> 4
	if flag > 3 || (flag&2 != 0) != (kind == Leaf) || flag&1 == 0 && p[0]&15 != 0 {
		return 0, ErrCorrupt
	}
	n := (len(p)-1)*2 + int(flag&1)
	if n > 64 || kind == Extension && n == 0 {
		return 0, ErrCorrupt
	}
	return n, nil
}
func (r Record) validate(depth int) error {
	if depth < 0 || depth > 64 || r.Kind > Leaf || len(r.Value) > MaxRecordBytes {
		return ErrCorrupt
	}
	if r.Kind == Branch {
		if depth == 64 && r.Bitmap != 0 {
			return ErrCorrupt
		}
		if len(r.Path) != 0 {
			return ErrCorrupt
		}
		return nil
	}
	if r.Bitmap != 0 {
		return ErrCorrupt
	}
	n, e := pathNibbles(r.Path, r.Kind)
	if e != nil || depth+n > 64 {
		return ErrCorrupt
	}
	if r.Kind == Extension && len(r.Value) != 0 || r.Kind == Leaf && len(r.Value) == 0 {
		return ErrCorrupt
	}
	return nil
}
func EncodeRecord(r Record) ([]byte, error) {
	if e := r.validate(0); e != nil {
		return nil, e
	}
	b := make([]byte, 35)
	binary.BigEndian.PutUint16(b, r.Bitmap)
	copy(b[2:34], r.Hash[:])
	b[34] = byte(r.Kind)
	if r.Inline {
		b[34] |= 4
	}
	if r.Kind == Branch && len(r.Value) > 0 {
		b[34] |= 8
	}
	if r.Kind != Branch {
		b = append(b, byte(len(r.Path)))
		b = append(b, r.Path...)
	}
	if r.Kind == Leaf || r.Kind == Branch && len(r.Value) > 0 {
		b = append(b, be32(uint32(len(r.Value)))...)
		b = append(b, r.Value...)
	}
	if len(b) > MaxRecordBytes {
		return nil, ErrLimit
	}
	return b, nil
}
func DecodeRecord(b []byte) (Record, error) {
	var r Record
	if len(b) < 35 || len(b) > MaxRecordBytes {
		return r, ErrCorrupt
	}
	flags := b[34]
	if flags&0xf0 != 0 || flags&3 == 3 || flags&8 != 0 && Kind(flags&3) != Branch {
		return r, ErrCorrupt
	}
	r.Kind = Kind(flags & 3)
	r.Inline = flags&4 != 0
	r.Bitmap = binary.BigEndian.Uint16(b)
	copy(r.Hash[:], b[2:34])
	tail := b[35:]
	if r.Kind != Branch {
		if len(tail) < 1 || int(tail[0]) > len(tail)-1 {
			return r, ErrCorrupt
		}
		n := int(tail[0])
		r.Path = append([]byte(nil), tail[1:1+n]...)
		tail = tail[1+n:]
	}
	if r.Kind == Leaf || flags&8 != 0 {
		if len(tail) < 4 {
			return r, ErrCorrupt
		}
		n := binary.BigEndian.Uint32(tail)
		tail = tail[4:]
		if uint64(n) != uint64(len(tail)) || n == 0 {
			return r, ErrCorrupt
		}
		r.Value = append([]byte(nil), tail...)
		tail = nil
	}
	if len(tail) != 0 {
		return r, ErrCorrupt
	}
	if e := r.validate(0); e != nil {
		return Record{}, e
	}
	return r, nil
}

type Lifecycle struct {
	Active bool
	TrieID uint32
}

func encodeLife(v Lifecycle) ([]byte, error) {
	if !v.Active {
		if v.TrieID != 0 {
			return nil, ErrCorrupt
		}
		return []byte{0}, nil
	}
	if v.TrieID == 0 {
		return nil, ErrCorrupt
	}
	return append([]byte{1}, be32(v.TrieID)...), nil
}
func decodeLife(b []byte) (Lifecycle, error) {
	if len(b) == 1 && b[0] == 0 {
		return Lifecycle{}, nil
	}
	if len(b) == 5 && b[0] == 1 {
		q := binary.BigEndian.Uint32(b[1:])
		if q > 0 {
			return Lifecycle{true, q}, nil
		}
	}
	return Lifecycle{}, ErrCorrupt
}
