package mpt

import "bytes"

// CanonicalCursor retains only the ancestor chain of the last lookup. It pins
// a COW root, reuses shared ancestors for subsequent paths, and never mutates
// the tree. It must not outlive the backing store's read view.
type CanonicalCursor struct {
	tree   *Trie
	frames []canonicalFrame
}
type canonicalFrame struct {
	path []byte
	n    node
}

func (t *Trie) CanonicalCursor() *CanonicalCursor {
	return &CanonicalCursor{tree: t.Copy()}
}

func (c *CanonicalCursor) AtPath(path []byte) (Hash, []byte, bool, error) {
	if len(path) > 64 {
		return Hash{}, nil, false, ErrInvalidPath
	}
	for _, b := range path {
		if b > 15 {
			return Hash{}, nil, false, ErrInvalidPath
		}
	}
	for len(c.frames) > 0 && !bytes.HasPrefix(path, c.frames[len(c.frames)-1].path) {
		c.frames = c.frames[:len(c.frames)-1]
	}
	if len(c.frames) == 0 {
		c.frames = append(c.frames, canonicalFrame{n: c.tree.root})
	}
	for {
		f := &c.frames[len(c.frames)-1]
		n, err := c.tree.resolve(f.n, f.path)
		if err != nil {
			return Hash{}, nil, false, err
		}
		f.n = n
		if n == nil {
			return Hash{}, nil, false, nil
		}
		if bytes.Equal(f.path, path) {
			raw, h, e := encodeNode(n, nil)
			return h, raw, e == nil, e
		}
		var next node
		var at []byte
		switch n := n.(type) {
		case *extensionNode:
			at = append(cloneBytes(f.path), n.path...)
			if !bytes.HasPrefix(path, at) {
				return Hash{}, nil, false, nil
			}
			next = n.child
		case *branchNode:
			slot := path[len(f.path)]
			at = append(cloneBytes(f.path), slot)
			next = n.children[slot]
		default:
			return Hash{}, nil, false, nil
		}
		c.frames = append(c.frames, canonicalFrame{at, next})
	}
}
