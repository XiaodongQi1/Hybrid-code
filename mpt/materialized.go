package mpt

// WalkMaterialized visits only in-memory object nodes, stopping at hash
// references without loading them. Inline children are part of their host and
// are not emitted independently. RLP passed to emit is borrowed and read-only.
// This supports bounded-cache preparation without prefetching the logical tree.
func (t *Trie) WalkMaterialized(maxNodes uint64, emit func(LogicalChange) error) error {
	if t == nil || maxNodes == 0 || emit == nil {
		return ErrInvalidPath
	}
	var visited uint64
	var walk func(node, []byte) error
	walk = func(n node, path []byte) error {
		if n == nil {
			return nil
		}
		if independentReference(n) {
			return nil
		}
		if len(path) > 64 || visited >= maxNodes {
			return ErrInvalidPath
		}
		visited++
		raw, h, e := encodeNode(n, nil)
		if e != nil {
			return e
		}
		if len(path) == 0 || len(raw) >= 32 {
			if e = emit(LogicalChange{Path: path, Hash: h, RLP: raw}); e != nil {
				return e
			}
		}
		switch v := n.(type) {
		case *branchNode:
			for i, ch := range v.children {
				if e = walk(ch, append(cloneBytes(path), byte(i))); e != nil {
					return e
				}
			}
		case *extensionNode:
			return walk(v.child, append(cloneBytes(path), v.path...))
		}
		return nil
	}
	return walk(t.root, nil)
}
