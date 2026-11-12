package versionedtrie

import (
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

const MaxProjectionNodes = 1 << 20

// ProjectTrie stages one FINAL logical tree per namespace. A new trieID must
// use EmptyRootHash as before, even when it copies another instance's content.
// Storage before roots must come from the trusted previous account; the global
// account before/after roots are checked against the transaction's heads here.
// A failed projection poisons this update: abort it, never commit partial rows.
func (u *Update) ProjectTrie(id uint64, beforeRoot mpt.Hash, after *mpt.Trie) (stats mpt.DiffStats, err error) {
	if e := u.check(); e != nil {
		return stats, e
	}
	defer func() {
		if err != nil {
			u.projectionError = fmt.Errorf("projection failed: %w", err)
		}
	}()
	q, e := u32(id)
	if e != nil {
		return stats, e
	}
	if id >= u.nextID || after == nil {
		return stats, ErrCorrupt
	}
	if u.projected[q] {
		return stats, ErrConflict
	}
	isNew := id >= u.baseNextID || id == 0 && !u.baseHead.Initialized
	if isNew && beforeRoot != mpt.EmptyRootHash {
		return stats, ErrConflict
	}
	if q == 0 {
		expected := mpt.EmptyRootHash
		if u.baseHead.Initialized {
			expected = u.baseHead.Root
		}
		if beforeRoot != expected {
			return stats, ErrConflict
		}
		root, e := after.Hash()
		if e != nil {
			return stats, e
		}
		if root != u.root {
			return stats, ErrConflict
		}
	}
	var old *mpt.Trie
	if beforeRoot == mpt.EmptyRootHash {
		old, e = mpt.NewEmpty(mpt.NewMemoryStore())
	} else {
		if !u.baseHead.Initialized {
			return stats, ErrConflict
		}
		v, err := u.store.ViewAt(uint64(u.baseHead.Height))
		if err != nil {
			return stats, err
		}
		defer v.Close()
		reader, err := v.TrieReader(id)
		if err != nil {
			return stats, err
		}
		old, e = mpt.Open(beforeRoot, reader)
	}
	if e != nil {
		return stats, e
	}
	stats, err = after.DiffLogicalNodes(old, MaxProjectionNodes, func(change mpt.LogicalChange) error {
		desc, e := mpt.InspectCanonicalNode(change.RLP)
		if e != nil {
			return e
		}
		rec := Record{Kind: Kind(desc.Kind), Hash: change.Hash, Inline: len(change.RLP) < 32, Path: desc.Path, Value: desc.Value}
		if desc.Kind == mpt.CanonicalBranch {
			for i, ref := range desc.Children {
				if ref.Present {
					rec.Bitmap |= 1 << uint(i)
				}
			}
		}
		return u.PutNode(id, change.Path, rec)
	})
	if err == nil {
		u.projected[q] = true
	}
	return stats, err
}
