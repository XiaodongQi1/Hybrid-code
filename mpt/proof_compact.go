package mpt

import "fmt"

// CompactProof copies a locally generated expanded proof into the shared
// compact wire format: root first, then independently hashed path nodes in
// traversal order. Inline children already live inside their parent's RLP.
// It is a format conversion, not validation of an untrusted proof.
func CompactProof(proof Proof) Proof {
	out := make(Proof, 0, len(proof))
	for i, raw := range proof {
		if i == 0 || len(raw) >= 32 {
			out = append(out, cloneBytes(raw))
		}
	}
	return out
}

func (t *Trie) ProveCompact(key []byte) (Proof, error) {
	p, e := t.Prove(key)
	if e != nil {
		return nil, e
	}
	return CompactProof(p), nil
}

// VerifyCompactProof authenticates a compact proof and requires precisely the
// ordered hashed path. It rejects missing, reordered, duplicate and surplus
// items, including empty-root junk and separately supplied inline children.
// Applications accepting untrusted nodes should also impose their byte/node
// budgets and canonical-node validation (evm-state/query does both).
func VerifyCompactProof(root Hash, key []byte, proof Proof) ([]byte, bool, error) {
	next := 0
	value, exists, e := verifyProofPath(root, key, func(want Hash) ([]byte, error) {
		if next >= len(proof) {
			return nil, fmt.Errorf("mpt: compact proof node %x missing", want)
		}
		raw := proof[next]
		if len(raw) == 0 || next > 0 && len(raw) < 32 || hashBytes(raw) != want {
			return nil, fmt.Errorf("mpt: compact proof node %d mismatch", next)
		}
		next++
		return raw, nil
	})
	if e != nil {
		return nil, false, e
	}
	if next != len(proof) {
		return nil, false, fmt.Errorf("mpt: surplus compact proof nodes")
	}
	return value, exists, nil
}
