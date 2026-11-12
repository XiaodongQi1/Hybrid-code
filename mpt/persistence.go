package mpt

import (
	"fmt"
	"sync/atomic"
)

// In-memory generations implement persistDirty without mutating shared COW
// nodes. They are never encoded, hashed, or used as a storage version/height.
// A decoded node has generation zero; an edit creates fresh dirty objects.
var nextPersistenceRevision atomic.Uint64

func persistenceRevision(n node) uint64 {
	switch n := n.(type) {
	case *leafNode:
		return n.persistRevision
	case *extensionNode:
		return n.persistRevision
	case *branchNode:
		return n.persistRevision
	default:
		return 0
	}
}

// AcceptLogicalPersistence acknowledges only this Trie's pending versions,
// after an external versioned store has durably published root. It does not
// consume native hash-store staging, modify COW copies, or discard node fields.
// The caller must supply the root just committed and must not mutate the Trie
// between staging and acknowledgment.
func (t *Trie) AcceptLogicalPersistence(root Hash) {
	t.persistedThrough = t.revision
	t.persistenceBase = root
}

// PendingLogicalNodes exports dirty reachable positions, parent before child.
// Hash() does not clear this state. No old tree is opened or compared. If the
// destination base differs from this Trie's acknowledged base, the caller is
// importing a replacement tree and we conservatively export the whole tree.
// Removed positions need no tombstone: historical readers follow authenticated
// parent links, and removed children are absent from the emitted parent.
func (t *Trie) PendingLogicalNodes(base Hash, maxVisited uint64, emit func(LogicalChange) error) (DiffStats, error) {
	var stats DiffStats
	if t == nil || emit == nil || maxVisited == 0 {
		return stats, ErrInvalidPath
	}
	all := t.persistenceBase != base
	var walk func(node, []byte) error
	walk = func(n node, path []byte) error {
		if n == nil {
			return nil
		}
		if !all && persistenceRevision(n) <= t.persistedThrough {
			stats.Pruned++
			return nil
		}
		if len(path) > 64 || stats.Visited >= maxVisited {
			return fmt.Errorf("mpt: pending projection budget exceeded")
		}
		stats.Visited++
		var err error
		if all {
			n, err = t.resolve(n, path)
			if err != nil {
				return err
			}
		}
		raw, hash, err := encodeNode(n, nil)
		if err != nil {
			return err
		}
		if err = emit(LogicalChange{Path: cloneBytes(path), Hash: hash, RLP: raw}); err != nil {
			return err
		}
		stats.Emitted++
		switch n := n.(type) {
		case *extensionNode:
			return walk(n.child, append(cloneBytes(path), n.path...))
		case *branchNode:
			for i, child := range n.children {
				if err = walk(child, append(cloneBytes(path), byte(i))); err != nil {
					return err
				}
			}
		}
		return nil
	}
	err := walk(t.root, nil)
	return stats, err
}
