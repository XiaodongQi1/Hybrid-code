package mpt

import "testing"

func TestCountingStoreRecordsCommittedLogicalWrites(t *testing.T) {
	store := NewCountingStore(NewMemoryStore())
	batch := store.NewBatch()
	if err := batch.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	stats := store.Stats()
	if stats.Batches != 1 || stats.Writes != 1 || stats.Puts != 1 || stats.KeyBytes != 3 || stats.ValueBytes != 5 {
		t.Fatalf("write stats = %+v", stats)
	}
}

type rejectedWriteStore struct{ Store }

func (s rejectedWriteStore) NewBatch() Batch { return rejectedWriteBatch{s.Store.NewBatch()} }

type rejectedWriteBatch struct{ Batch }

func (rejectedWriteBatch) Write() error { return ErrNotFound }
func TestCountingStoreExcludesFailedBatches(t *testing.T) {
	store := NewCountingStore(rejectedWriteStore{NewMemoryStore()})
	batch := store.NewBatch()
	if err := batch.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err == nil {
		t.Fatal("expected write failure")
	}
	if stats := store.Stats(); stats.Puts != 0 || stats.KeyBytes != 0 || stats.ValueBytes != 0 || stats.Writes != 0 {
		t.Fatalf("failed writes reported as durable: %+v", stats)
	}
}
