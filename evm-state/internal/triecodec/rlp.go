package triecodec

import "fmt"

// These bounded read-only RLP helpers describe canonical nodes; trie mutation
// remains entirely in the original mpt package. Slices borrow the input.
type rlpElement struct {
	raw, payload []byte
	list         bool
}

func splitRLP(b []byte) (rlpElement, []byte, error) {
	bad := func() (rlpElement, []byte, error) { return rlpElement{}, nil, fmt.Errorf("%w: RLP", ErrFormat) }
	if len(b) == 0 {
		return bad()
	}
	p := b[0]
	offset, n := 1, uint64(0)
	list := p >= 0xc0
	switch {
	case p < 0x80:
		return rlpElement{b[:1], b[:1], false}, b[1:], nil
	case p <= 0xb7:
		n = uint64(p - 0x80)
	case p <= 0xbf || p > 0xf7:
		width := int(p - 0xb7)
		if list {
			width = int(p - 0xf7)
		}
		if len(b) < 1+width || b[1] == 0 {
			return bad()
		}
		for _, v := range b[1 : 1+width] {
			n = n<<8 | uint64(v)
		}
		if n < 56 {
			return bad()
		}
		offset += width
	default:
		n = uint64(p - 0xc0)
	}
	if n > uint64(len(b)-offset) {
		return bad()
	}
	end := offset + int(n)
	if !list && n == 1 && b[offset] < 0x80 {
		return bad()
	}
	return rlpElement{b[:end], b[offset:end], list}, b[end:], nil
}

func encodeRLP(payload []byte, list bool) []byte {
	if !list && len(payload) == 1 && payload[0] < 0x80 {
		return append([]byte(nil), payload...)
	}
	short, long := byte(0x80), byte(0xb7)
	if list {
		short, long = 0xc0, 0xf7
	}
	if len(payload) < 56 {
		return append([]byte{short + byte(len(payload))}, payload...)
	}
	var length [8]byte
	n := uint64(len(payload))
	i := 8
	for n > 0 {
		i--
		length[i] = byte(n)
		n >>= 8
	}
	out := []byte{long + byte(8-i)}
	out = append(out, length[i:]...)
	return append(out, payload...)
}
func encodeList(items [][]byte) []byte {
	var b []byte
	for _, v := range items {
		b = append(b, v...)
	}
	return encodeRLP(b, true)
}

type nodeView struct {
	items       [][]byte
	leaf        bool
	path, value []byte
}

func parseNode(b []byte, c Config) (nodeView, error) { return parseNodeAt(b, c, 0, 0) }
func parseNodeAt(b []byte, c Config, nesting int, prefix uint64) (nodeView, error) {
	var out nodeView
	if uint64(len(b)) > c.MaxNodeBytes {
		return out, ErrLimit
	}
	if nesting > 32 {
		return out, ErrLimit
	} // an inline node must be shorter than 32 bytes
	e, rest, err := splitRLP(b)
	if err != nil {
		return out, err
	}
	if len(rest) != 0 || !e.list {
		return out, ErrFormat
	}
	var items []rlpElement
	for tail := e.payload; len(tail) > 0; {
		var x rlpElement
		x, tail, err = splitRLP(tail)
		if err != nil {
			return out, err
		}
		items = append(items, x)
		if len(items) > 17 {
			return out, ErrFormat
		}
	}
	for _, x := range items {
		out.items = append(out.items, x.raw)
	}
	switch len(items) {
	case 2:
		p := items[0]
		if p.list || len(p.payload) == 0 {
			return out, ErrFormat
		}
		flag := p.payload[0] >> 4
		if flag > 3 || flag&1 == 0 && p.payload[0]&15 != 0 {
			return out, ErrFormat
		}
		pathlen := uint64(len(p.payload)-1)*2 + uint64(flag&1)
		if prefix+pathlen > uint64(c.MaxPathNibbles) {
			return out, ErrLimit
		}
		out.leaf = flag&2 != 0
		out.path = p.payload
		if out.leaf {
			if items[1].list {
				return out, ErrFormat
			}
			out.value = items[1].payload
		} else {
			if pathlen == 0 || !items[1].list && len(items[1].payload) == 0 {
				return out, ErrFormat
			}
			if err = validateRef(items[1], c, nesting, prefix+pathlen); err != nil {
				return out, err
			}
		}
	case 17:
		if prefix > uint64(c.MaxPathNibbles) {
			return out, ErrLimit
		}
		for _, x := range items[:16] {
			if err = validateRef(x, c, nesting, prefix+1); err != nil {
				return out, err
			}
		}
		if items[16].list {
			return out, ErrFormat
		}
		out.value = items[16].payload
	default:
		return out, ErrFormat
	}
	return out, nil
}
func validateRef(e rlpElement, c Config, nesting int, prefix uint64) error {
	if e.list {
		if len(e.raw) >= 32 {
			return ErrFormat
		}
		_, err := parseNodeAt(e.raw, c, nesting+1, prefix)
		return err
	}
	if len(e.payload) != 0 && len(e.payload) != 32 {
		return ErrFormat
	}
	return nil
}

// listEncodedLen is also the length of a string encoding except for a single
// byte below 0x80. Callers bound payload before performing any allocation.
func listEncodedLen(payload uint64) uint64 {
	if payload < 56 {
		return 1 + payload
	}
	width := uint64(0)
	for n := payload; n > 0; n >>= 8 {
		width++
	}
	return 1 + width + payload
}
