package kvstore

import (
	"bytes"
	"hash/maphash"
	"sync"
	"unsafe"
)

// Fixed slots bound actual backing arrays, including keys, hash-chain links,
// LRU metadata and unused payload capacity. Large values bypass this cache.
const ReadCacheMaxKeyBytes = 128
const ReadCacheMaxValueBytes = 4096

type ReadCachePlan struct{ RequestedBytes, AllocatedBytes, SlackBytes, SlotBytes, FixedBytes, Slots uint64 }
type ReadCacheStats struct {
	ReadCachePlan
	Entries, LiveValueBytes, Hits, Misses, Evictions, Bypasses, Invalidations uint64
}
type pointEntry struct {
	key                    [ReadCacheMaxKeyBytes]byte
	column                 [63]byte
	hash                   uint64
	previous, next, chain  int32 // index+1, zero is absent
	keyLength, valueLength uint16
	columnLength           uint8
}
type pointCache struct {
	mu               sync.Mutex
	seed             maphash.Seed
	entries          []pointEntry
	values           []byte
	buckets          []int32
	head, tail, used int32
	stats            ReadCacheStats
}

func PlanReadCache(budget uint64) (ReadCachePlan, error) {
	p := ReadCachePlan{RequestedBytes: budget}
	if budget == 0 {
		return p, nil
	}
	p.FixedBytes = uint64(unsafe.Sizeof(pointCache{}))
	p.SlotBytes = uint64(unsafe.Sizeof(pointEntry{})) + ReadCacheMaxValueBytes + 8 // two bucket heads per slot
	if budget < p.FixedBytes+p.SlotBytes || budget > 1<<30 {
		return p, ErrLimit
	}
	p.Slots = (budget - p.FixedBytes) / p.SlotBytes
	p.AllocatedBytes = p.FixedBytes + p.Slots*p.SlotBytes
	p.SlackBytes = budget - p.AllocatedBytes
	return p, nil
}
func newPointCache(budget uint64) *pointCache {
	if budget == 0 {
		return nil
	}
	p, _ := PlanReadCache(budget)
	return &pointCache{seed: maphash.MakeSeed(), entries: make([]pointEntry, p.Slots), values: make([]byte, p.Slots*ReadCacheMaxValueBytes), buckets: make([]int32, 2*p.Slots), stats: ReadCacheStats{ReadCachePlan: p}}
}
func (c *pointCache) digest(column string, key []byte) uint64 {
	var h maphash.Hash
	h.SetSeed(c.seed)
	h.WriteString(column)
	h.WriteByte(0)
	h.Write(key)
	return h.Sum64()
}
func (c *pointCache) find(column string, key []byte, hash uint64) int32 {
	for n := c.buckets[hash%uint64(len(c.buckets))]; n != 0; n = c.entries[n-1].chain {
		e := &c.entries[n-1]
		if e.hash == hash && int(e.columnLength) == len(column) && int(e.keyLength) == len(key) && string(e.column[:e.columnLength]) == column && bytes.Equal(e.key[:e.keyLength], key) {
			return n
		}
	}
	return 0
}
func (c *pointCache) unlink(n int32) {
	e := &c.entries[n-1]
	if e.previous != 0 {
		c.entries[e.previous-1].next = e.next
	} else {
		c.head = e.next
	}
	if e.next != 0 {
		c.entries[e.next-1].previous = e.previous
	} else {
		c.tail = e.previous
	}
}
func (c *pointCache) front(n int32) {
	e := &c.entries[n-1]
	e.previous = 0
	e.next = c.head
	if c.head != 0 {
		c.entries[c.head-1].previous = n
	} else {
		c.tail = n
	}
	c.head = n
}
func (c *pointCache) removeHash(n int32) {
	e := &c.entries[n-1]
	bucket := e.hash % uint64(len(c.buckets))
	link := &c.buckets[bucket]
	for *link != n {
		link = &c.entries[*link-1].chain
	}
	*link = e.chain
}
func (c *pointCache) get(column string, key []byte) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(key) > ReadCacheMaxKeyBytes {
		c.stats.Bypasses++
		return nil, false
	}
	n := c.find(column, key, c.digest(column, key))
	if n == 0 {
		c.stats.Misses++
		return nil, false
	}
	c.stats.Hits++
	c.unlink(n)
	c.front(n)
	e := c.entries[n-1]
	offset := (n - 1) * ReadCacheMaxValueBytes
	return bytes.Clone(c.values[int(offset) : int(offset)+int(e.valueLength)]), true
}
func (c *pointCache) put(column string, key, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(key) > ReadCacheMaxKeyBytes {
		return
	}
	if len(value) > ReadCacheMaxValueBytes {
		c.stats.Bypasses++
		return
	}
	hash := c.digest(column, key)
	n := c.find(column, key, hash)
	fresh := n == 0
	if !fresh {
		c.unlink(n)
		c.stats.LiveValueBytes -= uint64(c.entries[n-1].valueLength)
	} else if c.tail != 0 && c.entries[c.tail-1].columnLength == 0 {
		n = c.tail
		c.unlink(n)
	} else if c.used < int32(len(c.entries)) {
		c.used++
		n = c.used
	} else {
		n = c.tail
		c.unlink(n)
		c.removeHash(n)
		c.stats.LiveValueBytes -= uint64(c.entries[n-1].valueLength)
		c.stats.Evictions++
		c.stats.Entries--
	}
	if fresh {
		e := pointEntry{hash: hash, keyLength: uint16(len(key)), valueLength: uint16(len(value)), columnLength: uint8(len(column))}
		copy(e.key[:], key)
		copy(e.column[:], column)
		bucket := hash % uint64(len(c.buckets))
		e.chain = c.buckets[bucket]
		c.entries[n-1] = e
		c.buckets[bucket] = n
		c.stats.Entries++
	} else {
		c.entries[n-1].valueLength = uint16(len(value))
	}
	offset := (n - 1) * ReadCacheMaxValueBytes
	copy(c.values[int(offset):], value)
	c.stats.LiveValueBytes += uint64(len(value))
	c.front(n)
}
func (c *pointCache) invalidate(column string, key []byte) {
	if len(key) > ReadCacheMaxKeyBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.find(column, key, c.digest(column, key))
	if n == 0 {
		return
	}
	c.removeHash(n)
	c.stats.LiveValueBytes -= uint64(c.entries[n-1].valueLength)
	c.stats.Invalidations++
	c.stats.Entries--
	e := &c.entries[n-1]
	e.columnLength = 0
	e.valueLength = 0
	e.chain = 0
	c.unlink(n)
	// Put vacant slots at the tail, so the next miss reuses one first.
	e.previous = c.tail
	e.next = 0
	if c.tail != 0 {
		c.entries[c.tail-1].next = n
	} else {
		c.head = n
	}
	c.tail = n
}
func (d *DB) read(column string, key []byte, load func() ([]byte, error)) ([]byte, error) {
	if d.cache != nil {
		if v, ok := d.cache.get(column, key); ok {
			return v, nil
		}
	}
	v, e := load()
	if e == nil && d.cache != nil {
		d.cache.put(column, key, v)
	}
	return v, e
}
