package mpt

import "fmt"

// Walk visits reachable persisted nodes and leaves in key order with O(depth)
// working memory. Node callbacks receive authenticated hash-addressed blobs;
// embedded nodes are visited through their parent. It never updates the trie.
func Walk(root Hash, store Store, nodeFn func(Hash, []byte) error, leafFn func([]byte, []byte) error) error {
	var fn func([]byte, Hash, []byte) (bool, error)
	if nodeFn != nil {
		fn = func(_ []byte, h Hash, b []byte) (bool, error) { return true, nodeFn(h, b) }
	}
	return WalkPaths(root, store, fn, leafFn)
}

// WalkPaths adds canonical nibble paths and subtree pruning to Walk. Returning
// false skips an unchanged subtree. Embedded nodes are part of their parent's blob.
func WalkPaths(root Hash, store Store, nodeFn func([]byte, Hash, []byte) (bool, error), leafFn func([]byte, []byte) error) error {
	if store == nil {
		return ErrNilStore
	}
	if root == EmptyRootHash {
		return nil
	}
	var visit func(node, []byte) error
	leaf := func(path, value []byte) error {
		if len(path)%2 != 0 {
			return fmt.Errorf("mpt: odd leaf key")
		}
		key := make([]byte, len(path)/2)
		for i := range key {
			key[i] = path[2*i]<<4 | path[2*i+1]
		}
		if leafFn != nil {
			return leafFn(key, cloneBytes(value))
		}
		return nil
	}
	visit = func(n node, path []byte) error {
		if len(path) > 4096 {
			return fmt.Errorf("mpt: walk path too long")
		}
		if h, ok := referenceHash(n); ok {
			blob, e := ReadNode(store, path, Hash(h))
			if e != nil {
				return e
			}
			if hashBytes(blob) != Hash(h) {
				return fmt.Errorf("mpt: walk node hash mismatch")
			}
			var e2 error
			n, e2 = decodeNode(blob)
			if e2 != nil {
				return e2
			}
			if nodeFn != nil {
				descend, err := nodeFn(cloneBytes(path), Hash(h), cloneBytes(blob))
				if err != nil {
					return err
				}
				if !descend {
					return nil
				}
			}
		}
		switch n := n.(type) {
		case nil:
			return nil
		case *leafNode:
			return leaf(append(cloneBytes(path), n.path...), n.value)
		case *extensionNode:
			return visit(n.child, append(cloneBytes(path), n.path...))
		case *branchNode:
			if len(n.value) > 0 {
				if e := leaf(path, n.value); e != nil {
					return e
				}
			}
			for i, c := range n.children {
				if c != nil {
					if e := visit(c, append(cloneBytes(path), byte(i))); e != nil {
						return e
					}
				}
			}
			return nil
		default:
			return fmt.Errorf("mpt: invalid walk node")
		}
	}
	return visit(hashNode(root), nil)
}
