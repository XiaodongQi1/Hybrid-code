package mpt

import "bytes"

// IsEmpty inspects the normalized trie shape without computing a root hash.
func (t *Trie) IsEmpty() bool { return t.root == nil }

// Equal compares canonical trie contents without encoding or hashing dirty
// nodes. Shared immutable nodes and identical authenticated hashes short-circuit.
// Resolving persisted nodes still performs the usual integrity verification.
func (t *Trie) Equal(other *Trie) (bool, error) {
	var equal func(node, node, []byte) (bool, error)
	equal = func(a, b node, path []byte) (bool, error) {
		if a == b {
			return true, nil
		}
		if a == nil || b == nil {
			return false, nil
		}
		if independentReference(a) && independentReference(b) {
			ah, _, err := resolvedReferenceHash(a)
			if err != nil {
				return false, err
			}
			bh, _, err := resolvedReferenceHash(b)
			return ah == bh, err
		}
		a, e := t.resolve(a, path)
		if e != nil {
			return false, e
		}
		b, e = other.resolve(b, path)
		if e != nil {
			return false, e
		}
		switch x := a.(type) {
		case *leafNode:
			y, ok := b.(*leafNode)
			return ok && bytes.Equal(x.path, y.path) && bytes.Equal(x.value, y.value), nil
		case *extensionNode:
			y, ok := b.(*extensionNode)
			if !ok || !bytes.Equal(x.path, y.path) {
				return false, nil
			}
			return equal(x.child, y.child, append(cloneBytes(path), x.path...))
		case *branchNode:
			y, ok := b.(*branchNode)
			if !ok || !bytes.Equal(x.value, y.value) {
				return false, nil
			}
			for i := range x.children {
				ok, e := equal(x.children[i], y.children[i], append(cloneBytes(path), byte(i)))
				if e != nil || !ok {
					return ok, e
				}
			}
			return true, nil
		default:
			return false, ErrMissingNode
		}
	}
	return equal(t.root, other.root, nil)
}
