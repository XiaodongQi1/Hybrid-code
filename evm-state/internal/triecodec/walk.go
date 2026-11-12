package triecodec

import (
	"context"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// CheckNode checks canonical encoding and immediate hashed children (including
// those inside inline nodes). Old child closures were checked when committed.
func CheckNode(raw []byte, s mpt.Store) error {
	c := DefaultConfig()
	c.MaxPathNibbles = 64
	var check func([]byte) error
	var ref func([]byte) error
	ref = func(item []byte) error {
		e, _, err := splitRLP(item)
		if err != nil {
			return err
		}
		if e.list {
			return check(e.raw)
		}
		if len(e.payload) == 0 {
			return nil
		}
		b, err := s.Get(e.payload)
		if err != nil {
			return err
		}
		var h mpt.Hash
		copy(h[:], e.payload)
		if NodeHash(b) != h || len(b) < 32 {
			return ErrHashMismatch
		}
		_, err = parseNode(b, c)
		return err
	}
	check = func(b []byte) error {
		n, e := parseNode(b, c)
		if e != nil {
			return e
		}
		if len(n.items) == 17 {
			for _, item := range n.items[:16] {
				if e = ref(item); e != nil {
					return e
				}
			}
		} else if !n.leaf {
			return ref(n.items[1])
		}
		return nil
	}
	return check(raw)
}

// Walk traverses a fixed 32-byte-key trie and calls leaf on each logical value.
// The counter is shared across forests/heights, making audit limits global.
// All hashed and inline references are checked. It never follows value payloads.
func Walk(ctx context.Context, root mpt.Hash, store mpt.Store, visits *uint64, limit uint64, leaf func([]byte) error) error {
	if root == mpt.EmptyRootHash {
		return ctx.Err()
	}
	c := DefaultConfig()
	c.MaxPathNibbles = 64
	var walk func([]byte, uint64) error
	var ref func([]byte, uint64) error
	ref = func(item []byte, prefix uint64) error {
		e, _, err := splitRLP(item)
		if err != nil {
			return err
		}
		if e.list {
			return walk(e.raw, prefix)
		}
		if len(e.payload) == 0 {
			return nil
		}
		if len(e.payload) != 32 {
			return ErrFormat
		}
		b, err := store.Get(e.payload)
		if err != nil {
			return err
		}
		var h mpt.Hash
		copy(h[:], e.payload)
		if NodeHash(b) != h {
			return ErrHashMismatch
		}
		if len(b) < 32 {
			return ErrFormat
		}
		return walk(b, prefix)
	}
	walk = func(b []byte, prefix uint64) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if *visits >= limit {
			return ErrLimit
		}
		*visits++
		n, e := parseNodeAt(b, c, 0, prefix)
		if e != nil {
			return e
		}
		if len(n.items) == 17 {
			if len(n.value) > 0 {
				if prefix != 64 {
					return ErrFormat
				}
				if e = leaf(n.value); e != nil {
					return e
				}
			}
			for _, item := range n.items[:16] {
				if e = ref(item, prefix+1); e != nil {
					return e
				}
			}
			return nil
		}
		end := prefix + uint64(len(n.path)-1)*2 + uint64(n.path[0]>>4&1)
		if n.leaf {
			if end != 64 || len(n.value) == 0 {
				return ErrFormat
			}
			return leaf(n.value)
		}
		return ref(n.items[1], end)
	}
	b, e := store.Get(root[:])
	if e != nil {
		return e
	}
	if NodeHash(b) != root {
		return ErrHashMismatch
	}
	return walk(b, 0)
}
