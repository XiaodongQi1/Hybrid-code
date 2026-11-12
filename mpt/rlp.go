package mpt

import "fmt"

type rlpKind byte

const (
	rlpString rlpKind = iota
	rlpList
)

type rlpItem struct {
	kind    rlpKind
	payload []byte
	raw     []byte
}

func rlpEncodeBytes(value []byte) []byte {
	if len(value) == 1 && value[0] < 0x80 {
		return cloneBytes(value)
	}
	return rlpWithPrefix(0x80, 0xb7, value)
}

func rlpEncodeList(elements ...[]byte) []byte {
	length := 0
	for _, element := range elements {
		length += len(element)
	}
	payload := make([]byte, 0, length)
	for _, element := range elements {
		payload = append(payload, element...)
	}
	return rlpWithPrefix(0xc0, 0xf7, payload)
}

func rlpWithPrefix(shortBase, longBase byte, payload []byte) []byte {
	if len(payload) <= 55 {
		out := make([]byte, 1+len(payload))
		out[0] = shortBase + byte(len(payload))
		copy(out[1:], payload)
		return out
	}
	length := uintToBytes(len(payload))
	out := make([]byte, 1+len(length)+len(payload))
	out[0] = longBase + byte(len(length))
	copy(out[1:], length)
	copy(out[1+len(length):], payload)
	return out
}

func uintToBytes(value int) []byte {
	var buf [8]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte(value)
		value >>= 8
	}
	return cloneBytes(buf[i:])
}

func rlpSplit(input []byte) (rlpItem, []byte, error) {
	if len(input) == 0 {
		return rlpItem{}, nil, fmt.Errorf("mpt: truncated rlp")
	}
	prefix := input[0]
	var kind rlpKind
	var offset, size int
	switch {
	case prefix < 0x80:
		kind, offset, size = rlpString, 0, 1
	case prefix <= 0xb7:
		kind, offset, size = rlpString, 1, int(prefix-0x80)
	case prefix <= 0xbf:
		lengthSize := int(prefix - 0xb7)
		length, err := rlpReadLength(input, 1, lengthSize)
		if err != nil {
			return rlpItem{}, nil, err
		}
		kind, offset, size = rlpString, 1+lengthSize, length
	case prefix <= 0xf7:
		kind, offset, size = rlpList, 1, int(prefix-0xc0)
	default:
		lengthSize := int(prefix - 0xf7)
		length, err := rlpReadLength(input, 1, lengthSize)
		if err != nil {
			return rlpItem{}, nil, err
		}
		kind, offset, size = rlpList, 1+lengthSize, length
	}
	// Check by subtraction before adding: an encoded length can approach
	// MaxInt even when the input contains only its nine-byte header.
	if offset > len(input) || size < 0 || size > len(input)-offset {
		return rlpItem{}, nil, fmt.Errorf("mpt: truncated rlp payload")
	}
	end := offset + size
	raw := cloneBytes(input[:end])
	return rlpItem{kind: kind, payload: cloneBytes(input[offset:end]), raw: raw}, input[end:], nil
}

func rlpReadLength(input []byte, offset, size int) (int, error) {
	if size <= 0 || size > 8 || offset < 0 || offset >= len(input) || size > len(input)-offset || input[offset] == 0 {
		return 0, fmt.Errorf("mpt: invalid rlp length")
	}
	var length uint64
	for _, b := range input[offset : offset+size] {
		length = length<<8 | uint64(b)
	}
	if length < 56 {
		return 0, fmt.Errorf("mpt: non-canonical rlp length")
	}
	if length > uint64(len(input)-offset-size) {
		return 0, fmt.Errorf("mpt: truncated rlp payload")
	}
	return int(length), nil
}

func rlpListItems(item rlpItem) ([]rlpItem, error) {
	if item.kind != rlpList {
		return nil, fmt.Errorf("mpt: expected rlp list")
	}
	var items []rlpItem
	input := item.payload
	for len(input) > 0 {
		next, rest, err := rlpSplit(input)
		if err != nil {
			return nil, err
		}
		items = append(items, next)
		input = rest
	}
	return items, nil
}
