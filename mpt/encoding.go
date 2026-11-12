package mpt

import "fmt"

func keyToNibbles(key []byte) []byte {
	nibbles := make([]byte, len(key)*2)
	for i, b := range key {
		nibbles[2*i] = b >> 4
		nibbles[2*i+1] = b & 0x0f
	}
	return nibbles
}

func compactEncode(path []byte, leaf bool) []byte {
	odd := len(path)%2 == 1
	flag := byte(0)
	if leaf {
		flag = 2
	}
	if odd {
		flag++
	}

	encoded := make([]byte, (len(path)+2)/2)
	pos := 0
	if odd {
		encoded[0] = flag<<4 | path[0]
		pos = 1
	} else {
		encoded[0] = flag << 4
	}
	for i := 1; pos < len(path); i++ {
		encoded[i] = path[pos]<<4 | path[pos+1]
		pos += 2
	}
	return encoded
}

func compactDecode(encoded []byte) (path []byte, leaf bool, err error) {
	if len(encoded) == 0 {
		return nil, false, fmt.Errorf("mpt: empty compact path")
	}
	flag := encoded[0] >> 4
	if flag > 3 {
		return nil, false, fmt.Errorf("mpt: invalid compact path flag %d", flag)
	}
	leaf = flag&2 != 0
	odd := flag&1 != 0
	path = make([]byte, 0, len(encoded)*2)
	if odd {
		path = append(path, encoded[0]&0x0f)
	} else if encoded[0]&0x0f != 0 {
		return nil, false, fmt.Errorf("mpt: invalid compact path padding")
	}
	for _, b := range encoded[1:] {
		path = append(path, b>>4, b&0x0f)
	}
	return path, leaf, nil
}

func commonPrefix(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func concatPath(prefix []byte, nibble byte, suffix []byte) []byte {
	path := make([]byte, 0, len(prefix)+1+len(suffix))
	path = append(path, prefix...)
	path = append(path, nibble)
	path = append(path, suffix...)
	return path
}
