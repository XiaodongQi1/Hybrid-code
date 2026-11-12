package hybridtrie

import "github.com/XiaodongQi1/HybridTrie/mpt"

// ChildLocator preserves the physical source declared by a parent. Prefix
// locators have no known hash; Hash locators carry a digest, and Embed locators
// carry canonical child RLP. Target.Height is the query/execution height, not
// an invented update height or the parent's last-write height.
type ChildLocator struct {
	Kind   ReferenceKind
	Target NodeKey
	Hash   mpt.Hash
	Inline []byte
}

// LocateChild computes the actual starting position without reading storage.
// Empty branch slots produce Empty locators. Leaves and nonzero extension slots
// are invalid requests. Returned paths/inline bytes never alias the input.
func LocateChild(ctx NodeKey, r Record, slot int) (ChildLocator, error) {
	if err := r.ValidateAt(ctx); err != nil {
		return ChildLocator{}, err
	}
	return locateChild(ctx, r, slot)
}

// locateChild is used after validation, so rebuilding a branch does not
// revalidate every embedded sibling separately for every prefix reference.
func locateChild(ctx NodeKey, r Record, slot int) (ChildLocator, error) {
	if slot < 0 || slot >= 16 || r.Kind == mpt.CanonicalLeaf || r.Kind == mpt.CanonicalExtension && slot != 0 {
		return ChildLocator{}, ErrCorrupt
	}
	path := append([]byte(nil), ctx.Path...)
	if r.Kind == mpt.CanonicalExtension {
		path = append(path, r.Fragment...)
	} else {
		if len(path) == MaxDepth {
			return ChildLocator{}, ErrCorrupt
		}
		path = append(path, byte(slot))
	}
	ref := r.Children[slot]
	return ChildLocator{ref.Kind, NodeKey{ctx.TrieID, path, ctx.Height}, ref.Hash, append([]byte(nil), ref.Inline...)}, nil
}
