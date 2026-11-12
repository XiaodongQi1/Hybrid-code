package patharchive

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// WithOwner is a value copy: concurrent account/contract readers cannot change
// each other's namespace. nil is the account trie; 32 bytes is a contract key.
func (r reader) WithOwner(key []byte) forest.Store {
	r.owner = append([]byte{1}, key...)
	if key == nil {
		r.owner = []byte{0}
	}
	return r
}
func (r reader) GetNode(path []byte, h mpt.Hash) ([]byte, error) {
	if r.format == LegacyFormat {
		return r.Get(h[:])
	}
	if e := r.ctx.Err(); e != nil {
		return nil, e
	}
	if (len(r.owner) != 1 || r.owner[0] != 0) && (len(r.owner) != 33 || r.owner[0] != 1) {
		return nil, ErrCorrupt
	}
	if len(path) > 64 {
		return nil, ErrCorrupt
	}
	for _, n := range path {
		if n > 15 {
			return nil, ErrCorrupt
		}
	}
	k := r.key('P', r.owner, path)
	var b []byte
	var e error
	if r.current {
		b, e = r.get(k)
	} else {
		b, e = r.value(k, r.height)
	}
	if e != nil {
		return nil, e
	}
	if !r.servingProof && !bytes.Equal(crypto.Keccak256(b), h[:]) {
		return nil, ErrCorrupt
	}
	return b, nil
}
func (v staged) GetNode(path []byte, h mpt.Hash) ([]byte, error) {
	if b, ok := v.p.Nodes[h]; ok {
		return bytes.Clone(b), nil
	}
	return v.reader.GetNode(path, h)
}
