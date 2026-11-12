package patharchive

// This codec is local to the Path backend. Canonical MPT bytes are unchanged.
import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/rlp"
)

const absentNode byte = 0
const fullNode byte = 1
const deltaNode byte = 2

var ErrHistoryLimit = errors.New("path archive: history recovery limit")

func nodeElements(blob []byte) ([]rlp.RawValue, error) {
	content, tail, err := rlp.SplitList(blob)
	if err != nil || len(tail) != 0 {
		return nil, ErrCorrupt
	}
	elems := make([]rlp.RawValue, 0, 17)
	for len(content) > 0 {
		if len(elems) == 17 {
			return nil, ErrCorrupt
		}
		_, _, rest, e := rlp.Split(content)
		if e != nil {
			return nil, ErrCorrupt
		}
		elems = append(elems, content[:len(content)-len(rest)])
		content = rest
	}
	if len(elems) != 2 && len(elems) != 17 {
		return nil, ErrCorrupt
	}
	child := func(b []byte, allowEmpty bool) error {
		kind, value, _, e := rlp.Split(b)
		if e != nil {
			return ErrCorrupt
		}
		if kind == rlp.List {
			if len(b) >= 32 {
				return ErrCorrupt
			}
			_, e = nodeElements(b)
			return e
		}
		if len(value) != 32 && !(allowEmpty && len(value) == 0) {
			return ErrCorrupt
		}
		return nil
	}
	if len(elems) == 2 {
		p, _, e := rlp.SplitString(elems[0])
		if e != nil || len(p) == 0 || p[0]>>4 > 3 || (p[0]&16 == 0 && p[0]&15 != 0) {
			return nil, ErrCorrupt
		}
		if p[0]&32 != 0 {
			if _, _, e = rlp.SplitString(elems[1]); e != nil {
				return nil, ErrCorrupt
			}
		} else {
			if len(p) == 1 && p[0] == 0 {
				return nil, ErrCorrupt
			}
			if e = child(elems[1], false); e != nil {
				return nil, e
			}
		}
	} else {
		for _, b := range elems[:16] {
			if e := child(b, true); e != nil {
				return nil, e
			}
		}
		if _, _, e := rlp.SplitString(elems[16]); e != nil {
			return nil, ErrCorrupt
		}
	}
	return elems, nil
}
func nodeKind(e []rlp.RawValue) byte {
	if len(e) == 17 {
		return 4
	}
	p, _, _ := rlp.SplitString(e[0])
	return p[0] >> 5
}
func fullHistory(old []byte) []byte {
	if len(old) == 0 {
		return []byte{absentNode}
	}
	return append([]byte{fullNode}, old...)
}

// encodeNodeHistory only emits a delta when smaller and exactly reversible.
func encodeNodeHistory(old, next []byte, force bool) ([]byte, string, error) {
	full := fullHistory(old)
	if len(old) == 0 {
		return full, "absent", nil
	}
	a, e := nodeElements(old)
	if e != nil {
		return nil, "", e
	}
	if force {
		return full, "checkpoint", nil
	}
	if len(next) == 0 {
		return full, "structure", nil
	}
	b, e := nodeElements(next)
	if e != nil {
		return nil, "", e
	}
	if len(a) != len(b) || nodeKind(a) != nodeKind(b) {
		return full, "structure", nil
	}
	flag := deltaNode
	out := []byte{flag}
	if len(a) == 17 {
		out[0] |= 4
		out = append(out, 0, 0)
	}
	for i := range a {
		if bytes.Equal(a[i], b[i]) {
			continue
		}
		if len(a) == 2 {
			out[0] |= 1 << uint(i+3)
		} else if i == 16 {
			out[0] |= 8
		} else {
			out[1+i/8] |= 1 << uint(i%8)
		}
		out = binary.AppendUvarint(out, uint64(len(a[i])))
		out = append(out, a[i]...)
	}
	if len(out) >= len(full) {
		return full, "no-gain", nil
	}
	restored, e := restoreNodeHistory(out, next)
	if e != nil || !bytes.Equal(restored, old) {
		return nil, "", fmt.Errorf("%w: delta round trip", ErrCorrupt)
	}
	return out, "delta", nil
}

func restoreNodeHistory(record, base []byte) ([]byte, error) {
	if len(record) == 0 {
		return nil, ErrCorrupt
	}
	switch record[0] & 3 {
	case absentNode:
		if len(record) != 1 || record[0] != 0 {
			return nil, ErrCorrupt
		}
		return nil, nil
	case fullNode:
		if record[0] != 1 {
			return nil, ErrCorrupt
		}
		if _, e := nodeElements(record[1:]); e != nil {
			return nil, e
		}
		return bytes.Clone(record[1:]), nil
	case deltaNode:
	default:
		return nil, ErrCorrupt
	}
	flag := record[0]
	branch := flag&4 != 0
	mask := uint32(0)
	rest := record[1:]
	if branch {
		if flag&0xf0 != 0 || len(rest) < 2 {
			return nil, ErrCorrupt
		}
		mask = uint32(rest[0]) | uint32(rest[1])<<8
		if flag&8 != 0 {
			mask |= 1 << 16
		}
		rest = rest[2:]
	} else {
		if flag&0xe0 != 0 {
			return nil, ErrCorrupt
		}
		mask = uint32(flag>>3) & 3
	}
	if mask == 0 {
		return nil, ErrCorrupt
	}
	elems, e := nodeElements(base)
	if e != nil {
		return nil, e
	}
	if branch != (len(elems) == 17) {
		return nil, ErrCorrupt
	}
	kind := nodeKind(elems)
	for i := range elems {
		if mask&(1<<uint(i)) == 0 {
			continue
		}
		n, used := binary.Uvarint(rest)
		if used <= 0 || n == 0 || used != len(binary.AppendUvarint(nil, n)) || n > uint64(len(rest)-used) {
			return nil, ErrCorrupt
		}
		rest = rest[used:]
		v := rest[:int(n)]
		if _, _, tail, e := rlp.Split(v); e != nil || len(tail) != 0 {
			return nil, ErrCorrupt
		}
		elems[i] = bytes.Clone(v)
		rest = rest[int(n):]
	}
	if len(rest) != 0 {
		return nil, ErrCorrupt
	}
	out, e := rlp.EncodeToBytes(elems)
	if e != nil {
		return nil, ErrCorrupt
	}
	checked, e := nodeElements(out)
	if e != nil || nodeKind(checked) != kind {
		return nil, ErrCorrupt
	}
	return out, nil
}
