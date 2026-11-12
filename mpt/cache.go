package mpt

import (
	"container/list"
	"errors"
	"io"
	"sync"
)

var ErrInvalidCacheCapacity = errors.New("mpt: cache capacity must be positive")

// CachedStore adds a bounded, in-memory LRU cache to any Store. The capacity
// is expressed in entries. Cache values are copied at the API boundary so a
// caller cannot mutate cached node data.
type CachedStore struct {
	store Store
	cache *lruCache
}

// CacheStats is a point-in-time snapshot of LRU activity.
type CacheStats struct {
	Entries   int
	Capacity  int
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

func NewCachedStore(store Store, capacity int) (*CachedStore, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if capacity <= 0 {
		return nil, ErrInvalidCacheCapacity
	}
	return &CachedStore{store: store, cache: newLRUCache(capacity)}, nil
}

func (s *CachedStore) Get(key []byte) ([]byte, error) {
	if value, ok := s.cache.get(string(key)); ok {
		return value, nil
	}
	value, err := s.store.Get(key)
	if err != nil {
		return nil, err
	}
	s.cache.put(string(key), value)
	return cloneBytes(value), nil
}

func (s *CachedStore) NewBatch() Batch {
	return &cachedBatch{cache: s.cache, batch: s.store.NewBatch()}
}

// Stats returns cache activity without resetting counters.
func (s *CachedStore) Stats() CacheStats {
	return s.cache.stats()
}

// Close forwards Close to the wrapped store when it owns a closeable resource,
// such as LevelDB. It is safe to call for a MemoryStore-backed cache.
func (s *CachedStore) Close() error {
	if closer, ok := s.store.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type cachedBatch struct {
	cache  *lruCache
	batch  Batch
	writes []cacheWrite
}

type cacheWrite struct {
	key   string
	value []byte
}

func (b *cachedBatch) Put(key, value []byte) error {
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.writes = append(b.writes, cacheWrite{key: string(key), value: cloneBytes(value)})
	return nil
}

func (b *cachedBatch) Write() error {
	if err := b.batch.Write(); err != nil {
		return err
	}
	for _, write := range b.writes {
		b.cache.put(write.key, write.value)
	}
	b.writes = nil
	return nil
}

type lruCache struct {
	mu        sync.Mutex
	capacity  int
	items     map[string]*list.Element
	order     *list.List
	hits      uint64
	misses    uint64
	evictions uint64
}

type lruEntry struct {
	key   string
	value []byte
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

func (c *lruCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		c.misses++
		return nil, false
	}
	c.hits++
	c.order.MoveToFront(element)
	return cloneBytes(element.Value.(lruEntry).value), true
}

func (c *lruCache) put(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		element.Value = lruEntry{key: key, value: cloneBytes(value)}
		c.order.MoveToFront(element)
		return
	}
	element := c.order.PushFront(lruEntry{key: key, value: cloneBytes(value)})
	c.items[key] = element
	if c.order.Len() <= c.capacity {
		return
	}
	oldest := c.order.Back()
	entry := oldest.Value.(lruEntry)
	delete(c.items, entry.key)
	c.order.Remove(oldest)
	c.evictions++
}

func (c *lruCache) stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Entries:   c.order.Len(),
		Capacity:  c.capacity,
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
	}
}
