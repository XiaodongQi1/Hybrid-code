package mpt

import (
	"io"
	"sync"
)

// WriteStats records logical writes accepted by an MPT Store. It deliberately
// counts keys and values before the storage engine applies compression or
// compaction, making interval strategies comparable across store backends.
type WriteStats struct {
	Batches    uint64
	Writes     uint64
	Puts       uint64
	KeyBytes   uint64
	ValueBytes uint64
}

// CountingStore decorates a Store with concurrency-safe logical write
// accounting. It is useful for benchmark and operational instrumentation.
type CountingStore struct {
	store Store
	mu    sync.Mutex
	stats WriteStats
}

func NewCountingStore(store Store) *CountingStore {
	return &CountingStore{store: store}
}

func (s *CountingStore) Get(key []byte) ([]byte, error) { return s.store.Get(key) }

func (s *CountingStore) NewBatch() Batch {
	s.mu.Lock()
	s.stats.Batches++
	s.mu.Unlock()
	return &instrumentedBatch{owner: s, batch: s.store.NewBatch()}
}

func (s *CountingStore) Stats() WriteStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

type instrumentedBatch struct {
	owner *CountingStore
	batch Batch
	stats WriteStats
}

func (b *instrumentedBatch) Put(key, value []byte) error {
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.stats.Puts++
	b.stats.KeyBytes += uint64(len(key))
	b.stats.ValueBytes += uint64(len(value))
	return nil
}

func (b *instrumentedBatch) Write() error {
	if err := b.batch.Write(); err != nil {
		return err
	}
	b.owner.mu.Lock()
	b.owner.stats.Writes++
	b.owner.stats.Puts += b.stats.Puts
	b.owner.stats.KeyBytes += b.stats.KeyBytes
	b.owner.stats.ValueBytes += b.stats.ValueBytes
	b.owner.mu.Unlock()
	return nil
}

func (s *CountingStore) Close() error {
	if closer, ok := s.store.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
