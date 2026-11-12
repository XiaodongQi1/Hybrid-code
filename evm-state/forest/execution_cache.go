// Execution-only LRU shared by the baseline account and storage tries. An
// expected MPT digest identifies the same immutable fields across all owners.
package forest

import (
	"encoding/binary"
	"unsafe"

	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

const baselineNodeLimit = 532 // largest Ethereum branch with sixteen hash children

type cachedImage struct {
	image             mpt.NodeImage
	prev, next, chain int32 // one-based, zero is the end of a list
}

// The owning backend serializes execution and statistics under its store lock.
// Views, historical proofs, imports and audits never hold this cache.
type ExecutionNodeCache struct {
	slots      []cachedImage
	payload    []byte
	buckets    []int32
	head, tail int32
	stats      kvstore.ReadCacheStats
}

func PlanExecutionNodeCache(budget uint64) (kvstore.ReadCachePlan, error) {
	p := kvstore.ReadCachePlan{RequestedBytes: budget}
	if budget == 0 {
		return p, nil
	}
	p.FixedBytes = uint64(unsafe.Sizeof(ExecutionNodeCache{}))
	p.SlotBytes = uint64(unsafe.Sizeof(cachedImage{})) + uint64(mpt.NodeImageMaxBytes(baselineNodeLimit)) + 4
	if budget < p.FixedBytes+p.SlotBytes || budget > 1<<30 {
		return p, kvstore.ErrLimit
	}
	p.Slots = (budget - p.FixedBytes) / p.SlotBytes
	// Spend sub-slot remainder on additional usable hash buckets.
	p.SlackBytes = (budget - p.FixedBytes - p.Slots*p.SlotBytes) % 4
	p.AllocatedBytes = budget - p.SlackBytes
	return p, nil
}

func NewExecutionNodeCache(budget uint64, readonly bool) (*ExecutionNodeCache, error) {
	p, err := PlanExecutionNodeCache(budget)
	if err != nil || budget == 0 || readonly {
		return nil, err
	}
	buckets := p.Slots + (p.AllocatedBytes-p.FixedBytes-p.Slots*p.SlotBytes)/4
	return &ExecutionNodeCache{
		slots:   make([]cachedImage, p.Slots),
		payload: make([]byte, p.Slots*uint64(mpt.NodeImageMaxBytes(baselineNodeLimit))),
		buckets: make([]int32, buckets),
		stats:   kvstore.ReadCacheStats{ReadCachePlan: p},
	}, nil
}

func (c *ExecutionNodeCache) Report(s *kvstore.Statistics) {
	if c != nil {
		s.ReadCachePolicy = "materialized-node-lru"
		s.ReadCache = c.stats
	}
}

func (c *ExecutionNodeCache) bucket(h mpt.Hash) uint64 {
	return binary.LittleEndian.Uint64(h[:8]) % uint64(len(c.buckets))
}

func (c *ExecutionNodeCache) unlink(n int32) {
	s := &c.slots[n-1]
	if s.prev == 0 {
		c.head = s.next
	} else {
		c.slots[s.prev-1].next = s.next
	}
	if s.next == 0 {
		c.tail = s.prev
	} else {
		c.slots[s.next-1].prev = s.prev
	}
}

func (c *ExecutionNodeCache) front(n int32) {
	s := &c.slots[n-1]
	s.prev, s.next = 0, c.head
	if c.head != 0 {
		c.slots[c.head-1].prev = n
	} else {
		c.tail = n
	}
	c.head = n
}

type executionStore struct {
	Store
	cache *ExecutionNodeCache
}

// WithExecutionNodeCache wraps a local execution reader whose node reads
// already validate the returned bytes against the expected digest.
func WithExecutionNodeCache(s Store, c *ExecutionNodeCache) Store {
	if c == nil {
		return s
	}
	return executionStore{s, c}
}

func (s executionStore) WithOwner(owner []byte) Store {
	return executionStore{ownedStore(s.Store, owner), s.cache}
}

func (s executionStore) GetNode(path []byte, h mpt.Hash) ([]byte, error) {
	return mpt.ReadNode(s.Store, path, h)
}

func (s executionStore) GetMaterializedNode(path []byte, h mpt.Hash) (mpt.MaterializedNode, error) {
	c := s.cache
	bucket := c.bucket(h)
	for n := c.buckets[bucket]; n != 0; n = c.slots[n-1].chain {
		entry := &c.slots[n-1]
		if entry.image.Hash() == h {
			c.stats.Hits++
			if n != c.head {
				c.unlink(n)
				c.front(n)
			}
			return entry.image.Materialize()
		}
	}
	c.stats.Misses++
	// Both baseline readers bind the physical bytes to the expected digest.
	raw, err := s.GetNode(path, h)
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	if len(raw) > baselineNodeLimit {
		c.stats.Bypasses++
		return mpt.DecodeTrustedLocalNode(raw, h)
	}
	// Validate and parse before changing any live slot.
	buf := make([]byte, mpt.NodeImageMaxBytes(baselineNodeLimit))
	image, err := mpt.CaptureTrustedLocalNodeImage(buf, raw, h)
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	var n int32
	if c.stats.Entries < uint64(len(c.slots)) {
		c.stats.Entries++
		n = int32(c.stats.Entries)
	} else {
		n = c.tail
		old := c.slots[n-1]
		link := &c.buckets[c.bucket(old.image.Hash())]
		for *link != n {
			link = &c.slots[*link-1].chain
		}
		*link = old.chain
		c.unlink(n)
		c.stats.LiveValueBytes -= uint64(old.image.DataBytes())
		c.stats.Evictions++
	}
	stride := int(mpt.NodeImageMaxBytes(baselineNodeLimit))
	start := int(n-1) * stride
	entry := &c.slots[n-1]
	entry.image, err = image.CloneTo(c.payload[start : start+stride])
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	entry.chain, c.buckets[bucket] = c.buckets[bucket], n
	c.front(n)
	c.stats.LiveValueBytes += uint64(entry.image.DataBytes())
	return entry.image.Materialize()
}
