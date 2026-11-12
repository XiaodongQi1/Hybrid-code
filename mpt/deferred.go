package mpt

import (
	"sync/atomic"
	"unsafe"
)

// LocalNodeReference addresses one immutable node in a pinned local snapshot.
// Digest reads only its stored summary; Materialize loads that node's fields,
// not its descendants. Implementations must preserve the original physical
// position when COW edits move a reference to another logical position.
type LocalNodeReference interface {
	KnownDigest() (Hash, bool)
	Location() NodeLocation
	Digest() (Hash, error)
	Materialize() (MaterializedNode, error)
	Prefetch()
}

type deferredNode struct {
	ref        LocalNodeReference
	digest     atomic.Pointer[Hash]
	prefetched atomic.Bool
}

func (*deferredNode) isNode() {}
func (n *deferredNode) hash() (Hash, error) {
	if h := n.digest.Load(); h != nil {
		return *h, nil
	}
	h, err := n.ref.Digest()
	if err == nil {
		n.digest.CompareAndSwap(nil, &h)
	}
	return h, err
}

func independentReference(n node) bool {
	if _, ok := n.(*deferredNode); ok {
		return true
	}
	_, ok := referenceHash(n)
	return ok
}

func resolvedReferenceHash(n node) (Hash, bool, error) {
	if d, ok := n.(*deferredNode); ok {
		h, err := d.hash()
		return h, true, err
	}
	h, ok := referenceHash(n)
	return h, ok, nil
}

// Prefetch only after a real edit. There is no worker or store knowledge in
// the MPT; the reader owns the bounded queue. Shared COW references submit once.
func prefetchChildren(n node) {
	visit := func(c node) {
		if d, ok := c.(*deferredNode); ok && d.digest.Load() == nil && d.prefetched.CompareAndSwap(false, true) {
			d.ref.Prefetch()
		}
	}
	switch n := n.(type) {
	case *branchNode:
		for _, c := range n.children {
			visit(c)
		}
	case *extensionNode:
		visit(n.child)
	}
}

// MaterializeDeferredLocal constructs a trusted local object without reading
// child summaries. refs replace independent child references in d. No pending
// state is encoded into canonical bytes, proofs, or persistent NodeImages.
func MaterializeDeferredLocal(d CanonicalNode, digest Hash, refs [16]LocalNodeReference) (MaterializedNode, error) {
	n, err := nodeFromFieldsWithReferences(d, true, &refs)
	if err != nil {
		return MaterializedNode{}, err
	}
	size := localEncodedSize(d)
	if size == 0 || size > MaxCanonicalNodeBytes {
		return MaterializedNode{}, ErrInvalidPath
	}
	publishDigest(n, digest, size)
	return MaterializedNode{n, digest}, nil
}

func localEncodedSize(d CanonicalNode) uint32 {
	prefix := func(n int) int {
		if n <= 55 {
			return 1
		}
		k := 0
		for x := n; x > 0; x >>= 8 {
			k++
		}
		return 1 + k
	}
	str := func(v []byte) int {
		if len(v) == 1 && v[0] < 128 {
			return 1
		}
		return len(v) + prefix(len(v))
	}
	ref := func(r CanonicalReference) int {
		if !r.Present {
			return 1
		}
		if len(r.Inline) > 0 {
			return len(r.Inline)
		}
		return 33
	}
	size := 0
	switch d.Kind {
	case CanonicalLeaf:
		size = str(d.Path) + str(d.Value)
	case CanonicalExtension:
		size = str(d.Path) + ref(d.Children[0])
	case CanonicalBranch:
		size = str(d.Value)
		for _, r := range d.Children {
			size += ref(r)
		}
	default:
		return 0
	}
	return uint32(size + prefix(size))
}

// Resolve summaries before parallel hashing. Clean subtrees stop immediately;
// this never materializes or revisits each reference from the root.
func prepareDeferred(n node) error {
	if d, ok := n.(*deferredNode); ok {
		_, err := d.hash()
		return err
	}
	if c := digestCache(n); c != nil && c.Load() != nil {
		return nil
	}
	switch n := n.(type) {
	case *branchNode:
		for _, c := range n.children {
			if err := prepareDeferred(c); err != nil {
				return err
			}
		}
	case *extensionNode:
		return prepareDeferred(n.child)
	}
	return nil
}

func usesDeferredReads(s Store) bool {
	v, ok := s.(interface{ DeferredReads() bool })
	return ok && v.DeferredReads()
}

// LocalRLP serializes this detached local node on explicit demand, resolving
// only child digests. It is not a retained encoding cache.
func (m MaterializedNode) LocalRLP() ([]byte, error) {
	raw, _, err := encodeNode(m.root, nil)
	return raw, err
}

// RetainedBytes counts this detached object's fields and local references, not
// implementations behind LocalNodeReference or allocator bookkeeping. Consumers
// bill those separately. It never reads descendants from disk.
func (m MaterializedNode) RetainedBytes() uint64 {
	var size func(node) uint64
	size = func(n node) uint64 {
		switch n := n.(type) {
		case *leafNode:
			return uint64(unsafe.Sizeof(*n)) + uint64(cap(n.path)+cap(n.value)) + uint64(unsafe.Sizeof(cachedDigest{}))
		case *extensionNode:
			return uint64(unsafe.Sizeof(*n)) + uint64(cap(n.path)) + uint64(unsafe.Sizeof(cachedDigest{})) + size(n.child)
		case *branchNode:
			v := uint64(unsafe.Sizeof(*n)) + uint64(cap(n.value)) + uint64(unsafe.Sizeof(cachedDigest{}))
			for _, c := range n.children {
				v += size(c)
			}
			return v
		case *deferredNode:
			return uint64(unsafe.Sizeof(*n)) + 32
		case *locatedHashNode:
			return uint64(unsafe.Sizeof(*n))
		case hashNode:
			return 32
		default:
			return 0
		}
	}
	return size(m.root)
}

// LocalChildLocations exports already resolved hints without reading or hashing.
// This is used when a parsed local node is admitted into a persistent cache.
func (m MaterializedNode) LocalChildLocations() (out [16]NodeLocation) {
	location := func(n node) NodeLocation {
		switch n := n.(type) {
		case *deferredNode:
			return n.ref.Location()
		case *locatedHashNode:
			return n.location
		}
		return NodeLocation{}
	}
	switch n := m.root.(type) {
	case *branchNode:
		for i, c := range n.children {
			out[i] = location(c)
		}
	case *extensionNode:
		out[0] = location(n.child)
	}
	return
}
