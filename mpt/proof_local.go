package mpt

import "bytes"

// AssembleValueProofAt collects an expanded proof from a pinned local Ethereum
// trie. References supply lookup keys. The client must authenticate the result
// against an independent root before using its value or absence claim.
func AssembleValueProofAt(root Hash, store Store, key []byte) (ProofResult, error) {
	result := ProofResult{Nodes: Proof{}}
	if store == nil {
		return ProofResult{}, ErrNilStore
	}
	if len(key) > 32 {
		return ProofResult{}, ErrInvalidPath
	}
	if root == EmptyRootHash {
		return result, nil
	}
	path := keyToNibbles(key)
	depth := 0
	ref := CanonicalReference{Present: true, Hash: root}
	for ref.Present {
		raw := ref.Inline
		if len(raw) == 0 {
			var err error
			raw, err = ReadNode(store, path[:depth], ref.Hash)
			if err != nil {
				return ProofResult{}, err
			}
			if depth > 0 && len(raw) < 32 {
				return ProofResult{}, ErrInvalidPath
			}
		}
		fields, err := DecodeLocalNodeFields(raw)
		if err != nil {
			return ProofResult{}, err
		}
		result.Nodes = append(result.Nodes, cloneBytes(raw))
		switch fields.Kind {
		case CanonicalBranch:
			if depth == len(path) {
				result.Value = cloneBytes(fields.Value)
				result.Exists = len(result.Value) != 0
				return result, nil
			}
			ref = fields.Children[path[depth]]
			depth++
		case CanonicalExtension, CanonicalLeaf:
			fragment, leaf, err := DecodeCompactPath(fields.Path)
			if err != nil {
				return ProofResult{}, err
			}
			if !bytes.HasPrefix(path[depth:], fragment) {
				return result, nil
			}
			depth += len(fragment)
			if leaf {
				if depth == len(path) {
					result.Value = cloneBytes(fields.Value)
					result.Exists = true
				}
				return result, nil
			}
			ref = fields.Children[0]
		}
	}
	return result, nil
}
