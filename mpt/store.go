package mpt

import (
	"errors"
	"sync"
)

var ErrNotFound = errors.New("mpt: key not found")

// Store is the minimal persistent storage required by the trie. Implementations
// must apply a batch atomically.
type Store interface {
	Get(key []byte) ([]byte, error)
	NewBatch() Batch
}

type Batch interface {
	Put(key, value []byte) error
	Write() error
}

// MemoryStore is a small in-memory Store useful for tests and ephemeral tries.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

func (s *MemoryStore) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[string(key)]
	if !ok {
		return nil, ErrNotFound
	}
	return cloneBytes(value), nil
}

func (s *MemoryStore) NewBatch() Batch {
	return &memoryBatch{store: s}
}

type memoryBatch struct {
	store  *MemoryStore
	writes map[string][]byte
}

func (b *memoryBatch) Put(key, value []byte) error {
	if b.writes == nil {
		b.writes = make(map[string][]byte)
	}
	b.writes[string(cloneBytes(key))] = cloneBytes(value)
	return nil
}

func (b *memoryBatch) Write() error {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	for key, value := range b.writes {
		b.store.data[key] = cloneBytes(value)
	}
	b.writes = nil
	return nil
}
