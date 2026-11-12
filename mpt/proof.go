package mpt

import (
	"errors"
	"fmt"
)

// ErrProofNodeHashMismatch indicates that the fetched proof node bytes do not
// hash to the digest required by the trusted root or its parent.
var ErrProofNodeHashMismatch = errors.New("mpt: proof node hash mismatch")

// Proof contains RLP-encoded nodes needed to authenticate a key.
type Proof [][]byte

// Prove returns an inclusion proof for an existing key or a non-inclusion
// proof ending at the first node that excludes the key.
func (t *Trie) Prove(key []byte) (Proof, error) {
	r, e := walkProof(t.root, t.store, key, false)
	return r.Nodes, e
}

// ProofResult owns the terminal value and the path proving its presence or absence.
type ProofResult struct {
	Value  []byte
	Exists bool
	Nodes  Proof
}

// ProveValue collects the value at the terminal node of the same proof walk.
func (t *Trie) ProveValue(key []byte) (ProofResult, error) {
	return walkProof(t.root, t.store, key, true)
}

// ProveValueAt reads a pinned root directly. Its first path read checks the
// root digest, so opening and reading the same root separately is unnecessary.
func ProveValueAt(root Hash, store Store, key []byte) (ProofResult, error) {
	if store == nil {
		return ProofResult{}, ErrNilStore
	}
	if root == EmptyRootHash {
		return ProofResult{Nodes: Proof{}}, nil
	}
	return walkProof(hashNode(root), store, key, true)
}
func walkProof(root node, store Store, key []byte, wantValue bool) (ProofResult, error) {
	nibbles := keyToNibbles(key)
	full := nibbles
	n := root
	proof := make(Proof, 0)
	result := func(value []byte) ProofResult {
		r := ProofResult{Nodes: proof, Exists: true}
		if wantValue {
			r.Value = cloneBytes(value)
		}
		return r
	}
	seen := make(map[Hash]bool)
	for n != nil {
		var deferredExpected *Hash
		if d, ok := n.(*deferredNode); ok {
			v, err := d.ref.Materialize()
			if err != nil {
				return ProofResult{}, err
			}
			deferredExpected = &v.hash
			n = v.root
		}
		var encoded []byte
		// Change O04 (2026-10-21): reuse the encoding/read-validation hash.
		var nodeHash Hash
		if hash, ok := referenceHash(n); ok {
			resolvedHash := Hash(hash)
			blob, err := ReadNode(store, full[:len(full)-len(nibbles)], resolvedHash)
			if err != nil {
				return ProofResult{}, fmt.Errorf("%w: %x", ErrMissingNode, Hash(hash))
			}
			nodeHash = hashBytes(blob)
			if nodeHash != resolvedHash {
				return ProofResult{}, ErrProofNodeHashMismatch
			}
			encoded = blob
		} else {
			var err error
			encoded, nodeHash, err = encodeNode(n, nil)
			if err != nil {
				return ProofResult{}, err
			}
		}
		if deferredExpected != nil {
			nodeHash = hashBytes(encoded)
			if nodeHash != *deferredExpected {
				return ProofResult{}, ErrProofNodeHashMismatch
			}
		}
		if !seen[nodeHash] {
			proof = append(proof, cloneBytes(encoded))
			seen[nodeHash] = true
		}
		resolved := n
		if _, persisted := referenceHash(n); persisted {
			var err error
			resolved, err = decodeNode(encoded)
			if err != nil {
				return ProofResult{}, err
			}
		}
		switch current := resolved.(type) {
		case *leafNode:
			if equalPath(current.path, nibbles) {
				return result(current.value), nil
			}
			return ProofResult{Nodes: proof}, nil
		case *extensionNode:
			if len(nibbles) < len(current.path) || !equalPath(current.path, nibbles[:len(current.path)]) {
				return ProofResult{Nodes: proof}, nil
			}
			nibbles = nibbles[len(current.path):]
			n = current.child
		case *branchNode:
			if len(nibbles) == 0 {
				if len(current.value) > 0 {
					return result(current.value), nil
				}
				return ProofResult{Nodes: proof}, nil
			}
			n = current.children[nibbles[0]]
			nibbles = nibbles[1:]
		default:
			return ProofResult{}, fmt.Errorf("mpt: invalid node %T", resolved)
		}
	}
	return ProofResult{Nodes: proof}, nil
}

// VerifyProof authenticates key against root. exists is false for a valid
// non-inclusion proof. Any missing, malformed, or tampered node returns an error.
func VerifyProof(root Hash, key []byte, proof Proof) (value []byte, exists bool, err error) {
	if root == EmptyRootHash {
		return nil, false, nil
	}
	nodes := make(map[Hash][]byte, len(proof))
	for _, encoded := range proof {
		nodes[hashBytes(encoded)] = encoded
	}
	// Change O04 (2026-10-21): every entry is indexed by its computed hash. Looking
	// up the expected root/child hash authenticates it without hashing it again.
	return verifyProofPath(root, key, func(expected Hash) ([]byte, error) {
		encoded, present := nodes[expected]
		if !present {
			return nil, fmt.Errorf("mpt: proof node %x missing", expected)
		}
		return encoded, nil
	})
}

// verifyProofPath consumes only independently hashed nodes. Inline nodes are
// decoded from their authenticated parent, using the same logical traversal as
// ordinary trie reads. The resolver decides proof ordering/unused-item policy.
func verifyProofPath(root Hash, key []byte, resolve func(Hash) ([]byte, error)) (value []byte, exists bool, err error) {
	if root == EmptyRootHash {
		return nil, false, nil
	}
	nibbles := keyToNibbles(key)
	var n node = hashNode(root)
	for {
		if hash, ok := referenceHash(n); ok {
			encoded, e := resolve(Hash(hash))
			if e != nil {
				return nil, false, e
			}
			n, err = decodeNode(encoded)
			if err != nil {
				return nil, false, err
			}
		}
		switch current := n.(type) {
		case *leafNode:
			if equalPath(current.path, nibbles) {
				return cloneBytes(current.value), true, nil
			}
			return nil, false, nil
		case *extensionNode:
			if len(nibbles) < len(current.path) || !equalPath(current.path, nibbles[:len(current.path)]) {
				return nil, false, nil
			}
			nibbles = nibbles[len(current.path):]
			n = current.child
		case *branchNode:
			if len(nibbles) == 0 {
				if len(current.value) == 0 {
					return nil, false, nil
				}
				return cloneBytes(current.value), true, nil
			}
			n = current.children[nibbles[0]]
			nibbles = nibbles[1:]
			if n == nil {
				return nil, false, nil
			}
		default:
			return nil, false, fmt.Errorf("mpt: invalid proof node %T", n)
		}
	}
}
