// Package versionedtrie stores versioned physical trie records. It does not yet
// rebuild MPT nodes or authenticate roots; those belong to the next adapter.
package versionedtrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

var (
	ErrCorrupt   = errors.New("versionedtrie: corrupt record")
	ErrLimit     = errors.New("versionedtrie: limit exceeded")
	ErrNotFound  = errors.New("versionedtrie: no version")
	ErrReadOnly  = errors.New("versionedtrie: read-only")
	ErrClosed    = errors.New("versionedtrie: closed")
	ErrConflict  = errors.New("versionedtrie: stale or noncontinuous commit")
	ErrUncertain = errors.New("versionedtrie: commit outcome unknown; reopen required")
	ErrBusy      = errors.New("versionedtrie: release snapshots before close")
)

func u32(v uint64) (uint32, error) {
	if v > math.MaxUint32 {
		return 0, ErrLimit
	}
	return uint32(v), nil
}
func be32(v uint32) []byte { b := make([]byte, 4); binary.BigEndian.PutUint32(b, v); return b }
func pack(path []byte) ([]byte, error) {
	if len(path) > 64 {
		return nil, ErrLimit
	}
	out := make([]byte, (len(path)+1)/2)
	for i, n := range path {
		if n > 15 {
			return nil, ErrCorrupt
		}
		if i%2 == 0 {
			out[i/2] = n << 4
		} else {
			out[i/2] |= n
		}
	}
	return out, nil
}
func nodeHeader(id uint64, path []byte) ([]byte, error) {
	q, e := u32(id)
	if e != nil {
		return nil, e
	}
	p, e := pack(path)
	if e != nil {
		return nil, e
	}
	out := append([]byte{'N'}, be32(q)...)
	out = append(out, byte(len(path)))
	return append(out, p...), nil
}
func EncodeNodeKey(id uint64, path []byte, height uint64) ([]byte, error) {
	h, e := u32(height)
	if e != nil {
		return nil, e
	}
	b, e := nodeHeader(id, path)
	if e != nil {
		return nil, e
	}
	return append(b, be32(h)...), nil
}

type NodeKey struct {
	TrieID uint32
	Path   []byte
	Height uint32
}

func DecodeNodeKey(key []byte) (NodeKey, error) {
	var out NodeKey
	if len(key) < 10 || key[0] != 'N' {
		return out, ErrCorrupt
	}
	d := int(key[5])
	if d > 64 || len(key) != 10+(d+1)/2 {
		return out, ErrCorrupt
	}
	packed := key[6 : len(key)-4]
	if d%2 != 0 && packed[len(packed)-1]&15 != 0 {
		return out, ErrCorrupt
	}
	out.TrieID = binary.BigEndian.Uint32(key[1:5])
	out.Height = binary.BigEndian.Uint32(key[len(key)-4:])
	out.Path = make([]byte, d)
	for i := range out.Path {
		out.Path[i] = packed[i/2] >> 4
		if i%2 != 0 {
			out.Path[i] = packed[i/2] & 15
		}
	}
	return out, nil
}
func lifecycleHeader(owner [32]byte) []byte        { return append([]byte{'L'}, owner[:]...) }
func lifecycleKey(owner [32]byte, h uint32) []byte { return append(lifecycleHeader(owner), be32(h)...) }
func rootKey(h uint32) []byte                      { return append([]byte{'R'}, be32(h)...) }
func sameHeader(key, head []byte) bool {
	return len(key) == len(head)+4 && bytes.Equal(key[:len(head)], head)
}
