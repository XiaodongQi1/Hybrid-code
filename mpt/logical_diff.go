package mpt

import (
	"bytes"
	"fmt"
)

type LogicalChange struct {
	Path []byte
	Hash Hash
	RLP  []byte
}
type DiffStats struct{ Visited, Emitted, Pruned, OldLookups uint64 }

// CanonicalNodeAtPath inspects an exact reachable starting position in this
// tree, including uncommitted COW nodes and inline nodes. It never treats an
// orphan disk record as reachable and does not commit or consume pending writes.
func (t *Trie) CanonicalNodeAtPath(path []byte) (Hash, []byte, bool, error) {
	if t == nil {
		return Hash{}, nil, false, ErrInvalidPath
	}
	n, found, err := t.logicalAt(path)
	if err != nil || !found {
		return Hash{}, nil, found, err
	}
	raw, hash, err := encodeNode(n, nil)
	return hash, cloneBytes(raw), err == nil, err
}

// logicalAt returns only a node at the exact reachable starting path. Unlike
// NodeAtPath it includes inline nodes and never consults orphan path records.
func (t *Trie) logicalAt(target []byte) (node, bool, error) {
	if len(target) > 64 {
		return nil, false, ErrInvalidPath
	}
	for _, x := range target {
		if x > 15 {
			return nil, false, ErrInvalidPath
		}
	}
	n := t.root
	var prefix []byte
	for {
		resolved, e := t.resolve(n, prefix)
		if e != nil {
			return nil, false, e
		}
		n = resolved
		if n == nil {
			return nil, false, nil
		}
		if equalPath(prefix, target) {
			return n, true, nil
		}
		if len(prefix) >= len(target) {
			return nil, false, nil
		}
		switch cur := n.(type) {
		case *extensionNode:
			rest := target[len(prefix):]
			if len(cur.path) > len(rest) || !equalPath(cur.path, rest[:len(cur.path)]) {
				return nil, false, nil
			}
			prefix = append(prefix, cur.path...)
			n = cur.child
		case *branchNode:
			i := target[len(prefix)]
			prefix = append(prefix, i)
			n = cur.children[i]
		default:
			return nil, false, nil
		}
	}
}

// A compressed edge may land beyond the requested position. Keep its endpoint
// for the next descent: a deeper position can still align in both trees.
type diffCursor struct {
	n    node
	path []byte
}

func (c diffCursor) advance(t *Trie, target []byte) (diffCursor, error) {
	for c.n != nil && len(c.path) < len(target) {
		if !bytes.HasPrefix(target, c.path) {
			return diffCursor{}, nil
		}
		n, err := t.resolve(c.n, c.path)
		if err != nil {
			return diffCursor{}, err
		}
		switch n := n.(type) {
		case *branchNode:
			i := target[len(c.path)]
			c.path = target[:len(c.path)+1]
			c.n = n.children[i]
		case *extensionNode:
			c.path = append(cloneBytes(c.path), n.path...)
			c.n = n.child
		default:
			return diffCursor{}, nil
		}
	}
	if !bytes.HasPrefix(c.path, target) {
		return diffCursor{}, nil
	}
	return c, nil
}

// DiffLogicalNodes exports changed reachable positions (including inline nodes).
// Both cursors descend from authenticated parents; equal digests only prune
// subtrees at the same position. Removed nodes remain historical orphans.
func (t *Trie) DiffLogicalNodes(before *Trie, maxVisited uint64, emit func(LogicalChange) error) (DiffStats, error) {
	var stats DiffStats
	if t == nil || before == nil || emit == nil || maxVisited == 0 {
		return stats, ErrInvalidPath
	}
	var visit func(node, []byte, diffCursor) error
	visit = func(n node, path []byte, old diffCursor) error {
		if n == nil {
			return nil
		}
		if len(path) > 64 {
			return ErrInvalidPath
		}
		if stats.Visited >= maxVisited {
			return fmt.Errorf("mpt: logical diff budget exceeded")
		}
		stats.Visited++
		old, e := old.advance(before, path)
		if e != nil {
			return e
		}
		if old.n != nil && len(old.path) == len(path) {
			stats.OldLookups++
			newHash, _, e := hashCached(n, nil)
			if e != nil {
				return e
			}
			oldHash, _, e := hashCached(old.n, nil)
			if e != nil {
				return e
			}
			if newHash == oldHash {
				stats.Pruned++
				return nil
			}
			old.n, e = before.resolve(old.n, path)
			if e != nil {
				return e
			}
		}
		n, e = t.resolve(n, path)
		if e != nil {
			return e
		}
		raw, hash, e := encodeNode(n, nil)
		if e != nil {
			return e
		}
		if e = emit(LogicalChange{Path: cloneBytes(path), Hash: hash, RLP: cloneBytes(raw)}); e != nil {
			return e
		}
		stats.Emitted++
		switch cur := n.(type) {
		case *extensionNode:
			return visit(cur.child, append(cloneBytes(path), cur.path...), old)
		case *branchNode:
			for i, ch := range cur.children {
				if e = visit(ch, append(cloneBytes(path), byte(i)), old); e != nil {
					return e
				}
			}
		}
		return nil
	}
	e := visit(t.root, nil, diffCursor{n: before.root})
	return stats, e
}
