package mpt

import (
	"bytes"
	"errors"
)

var ErrInvalidPath = errors.New("mpt: invalid path or node hash")

// NodeAtPath returns only an independently hash-addressed node at an exact
// canonical nibble path. Embedded nodes are not eligible storage references.
// Every loaded node is authenticated; this never updates the trie.
func NodeAtPath(root Hash, path []byte, store Store) (Hash, []byte, bool, error) {
	if store == nil {
		return Hash{}, nil, false, ErrNilStore
	}
	for _, b := range path {
		if b > 15 {
			return Hash{}, nil, false, ErrInvalidPath
		}
	}
	if root == EmptyRootHash {
		return Hash{}, nil, false, nil
	}
	var n node = hashNode(root)
	var prefix []byte
	for {
		if h, ok := referenceHash(n); ok {
			b, err := ReadNode(store, prefix, Hash(h))
			if err != nil {
				return Hash{}, nil, false, err
			}
			if hashBytes(b) != Hash(h) {
				return Hash{}, nil, false, ErrInvalidPath
			}
			if bytes.Equal(prefix, path) {
				return Hash(h), b, true, nil
			}
			n, err = decodeNode(b)
			if err != nil {
				return Hash{}, nil, false, err
			}
		}
		if len(prefix) >= len(path) {
			return Hash{}, nil, false, nil
		}
		switch v := n.(type) {
		case *extensionNode:
			if !bytes.HasPrefix(path[len(prefix):], v.path) {
				return Hash{}, nil, false, nil
			}
			prefix = append(prefix, v.path...)
			n = v.child
		case *branchNode:
			next := path[len(prefix)]
			prefix = append(prefix, next)
			n = v.children[next]
		default:
			return Hash{}, nil, false, nil
		}
	}
}
