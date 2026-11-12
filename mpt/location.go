package mpt

// NodeLocation is a trusted local store's physical child locator. Kind is
// store-defined; zero means unspecified. Height is meaningful only with Exact.
// It belongs to the pinned reader that supplied it, not to the logical hash.
// Locations never enter canonical RLP, NodeImage, proofs, or persisted records.
type NodeLocation struct {
	Height uint64
	Kind   uint8
	Exact  bool
}

type locatedHashNode struct {
	hash     Hash
	location NodeLocation
}

func (*locatedHashNode) isNode() {}

func referenceHash(n node) (Hash, bool) {
	switch n := n.(type) {
	case hashNode:
		return Hash(n), true
	case *locatedHashNode:
		return n.hash, true
	default:
		return Hash{}, false
	}
}

// LocatedMaterializedStore consumes locators supplied by the same pinned
// local store. This is not an interface for authenticating untrusted hints.
type LocatedMaterializedStore interface {
	GetLocatedMaterializedNode(path []byte, expected Hash, location NodeLocation) (MaterializedNode, error)
}

type LocatedValueReader interface {
	LocatedLocalValue(path []byte, expected Hash, remaining []byte, location NodeLocation, observer TrieAccessObserver) ([]byte, bool, error)
}

// WithChildLocations detaches the parent before attaching locators, preserving
// existing COW copies. A reference moves with its subtree through edits; no
// complete path or trie ID is stored on nodes. Inline children need no locator.
func (m MaterializedNode) WithChildLocations(locations [16]NodeLocation) MaterializedNode {
	attach := func(n node, loc NodeLocation) node {
		if h, ok := referenceHash(n); ok && loc.Kind != 0 {
			return &locatedHashNode{hash: h, location: loc}
		}
		return n
	}
	switch n := m.root.(type) {
	case *extensionNode:
		copy := &extensionNode{persistRevision: n.persistRevision, path: n.path, child: attach(n.child, locations[0])}
		copy.cache.Store(n.cache.Load())
		m.root = copy
	case *branchNode:
		copy := &branchNode{persistRevision: n.persistRevision, value: n.value, children: n.children}
		copy.cache.Store(n.cache.Load())
		for i, ch := range copy.children {
			copy.children[i] = attach(ch, locations[i])
		}
		m.root = copy
	}
	return m
}

// LocalLookupReader may return a detached node on a resident hit, or a terminal
// value on a cold point walk. One lookup replaces probe-then-resolve. A nonempty
// materialized node takes precedence over handled; metadata remains local only.
type LocalLookupReader interface {
	LookupLocalValue(path []byte, expected Hash, remaining []byte, location *NodeLocation, observer TrieAccessObserver) ([]byte, MaterializedNode, bool, error)
}
