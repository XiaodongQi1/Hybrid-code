package mpt

import "sync/atomic"

// cachedDigest retains only a digest and canonical byte length. RLP is
// operation-local: hashing, proofs and staging never attach an encoding buffer
// to a logical node. Length distinguishes inline references without encoding
// an unchanged independently addressed child.
type cachedDigest struct {
	hash Hash
	size uint32
}

// A nil digest cache is hashDirty=true. A non-nil cache atomically publishes
// hashDirty=false together with the matching hash and canonical byte length.
// This avoids a separate boolean racing with cache initialization. Logical
// nodes are immutable: COW replacements start with a new, nil cache.
func digestCache(n node) *atomic.Pointer[cachedDigest] {
	switch n := n.(type) {
	case *leafNode:
		return &n.cache
	case *extensionNode:
		return &n.cache
	case *branchNode:
		return &n.cache
	default:
		return nil
	}
}
func publishDigest(n node, hash Hash, size uint32) *cachedDigest {
	p := digestCache(n)
	value := &cachedDigest{hash: hash, size: size}
	if p == nil {
		return value
	}
	if p.CompareAndSwap(nil, value) {
		return value
	}
	return p.Load()
}

// HashWork measures one serial hash request; no global counters or hooks are
// enabled on the normal path. Concurrent first requests may do duplicate work
// before one immutable result is published, but cannot publish partial caches.
type HashWork struct {
	VisitedNodes uint64
	EncodedNodes uint64
	HashedNodes  uint64
	CacheHits    uint64
}

// HashWithStats is Hash with per-request work accounting. Like Hash, it does
// not write nodes or mark any pending nodes as durable. Concurrent immutable
// readers are supported; concurrent logical mutation is not.
func (t *Trie) HashWithStats() (Hash, HashWork, error) {
	var work HashWork
	h, _, e := hashCached(t.root, &work)
	return h, work, e
}
