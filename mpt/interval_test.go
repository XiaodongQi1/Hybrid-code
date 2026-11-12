package mpt

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestIntervalStateDefersWritesUntilBoundary(t *testing.T) {
	store := &countingStore{store: NewMemoryStore()}
	state, err := OpenIntervalState(store, 3)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot, err := state.ApplyBlock(1, []Change{{Key: []byte("alice"), Value: []byte("10")}}); err != nil || snapshot != nil {
		t.Fatalf("block 1 = %v, %v", snapshot, err)
	}
	if snapshot, err := state.ApplyBlock(2, []Change{{Key: []byte("alice"), Value: []byte("20")}}); err != nil || snapshot != nil {
		t.Fatalf("block 2 = %v, %v", snapshot, err)
	}
	if store.batches != 0 || store.writes != 0 || store.puts != 0 {
		t.Fatalf("non-boundary blocks wrote to storage: %+v", store)
	}
	if len(state.updates) != 1 {
		t.Fatalf("pending update count = %d; want 1", len(state.updates))
	}
	if got, err := state.Get([]byte("alice")); err != nil || string(got) != "20" {
		t.Fatalf("in-memory value = %q, %v", got, err)
	}
	root, err := state.Hash()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := state.Prove([]byte("alice"))
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := VerifyProof(root, []byte("alice"), proof)
	if err != nil || !exists || string(value) != "20" {
		t.Fatalf("cached update proof = %q, %v, %v", value, exists, err)
	}

	snapshot, err := state.ApplyBlock(3, []Change{{Key: []byte("bob"), Value: []byte("30")}})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == nil || snapshot.Height != 3 {
		t.Fatalf("boundary snapshot = %+v", snapshot)
	}
	if store.batches != 1 || store.writes != 1 || store.puts < 2 {
		t.Fatalf("boundary persistence = %+v", store)
	}
	if len(state.updates) != 0 {
		t.Fatalf("boundary did not clear update cache: %d entries", len(state.updates))
	}
	if latest := state.LatestSnapshot(); latest != *snapshot {
		t.Fatalf("latest snapshot = %+v; want %+v", latest, *snapshot)
	}
	if got, err := state.Hash(); err != nil || got != snapshot.Root {
		t.Fatalf("current root = %x, %v; want %x", got, err, snapshot.Root)
	}
	stored, found, err := state.SnapshotAt(3)
	if err != nil || !found || stored != *snapshot {
		t.Fatalf("SnapshotAt(3) = %+v, %v, %v", stored, found, err)
	}
	if _, found, err := state.SnapshotAt(1); err != nil || found {
		t.Fatalf("SnapshotAt(1) found = %v, %v", found, err)
	}

	if snapshot, err := state.ApplyBlock(4, []Change{{Key: []byte("alice"), Value: []byte("40")}}); err != nil || snapshot != nil {
		t.Fatalf("block 4 = %v, %v", snapshot, err)
	}
	if store.writes != 1 {
		t.Fatalf("block 4 unexpectedly wrote: %+v", store)
	}
	if _, err := state.ApplyBlock(5, []Change{{Key: []byte("bob"), Delete: true}}); err != nil {
		t.Fatal(err)
	}
	if len(state.updates) != 2 {
		t.Fatalf("pending update count = %d; want 2", len(state.updates))
	}
	second, err := state.ApplyBlock(6, nil)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.Height != 6 || store.writes != 2 {
		t.Fatalf("second boundary = %+v, store=%+v", second, store)
	}
	if got, err := state.Get([]byte("alice")); err != nil || string(got) != "40" {
		t.Fatalf("alice = %q, %v", got, err)
	}
	if got, err := state.Get([]byte("bob")); err != nil || got != nil {
		t.Fatalf("bob = %q, %v", got, err)
	}
}

func TestIntervalStatePersistsAndRecoversFromLevelDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store, err := OpenLevelDBCached(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	state, err := OpenIntervalState(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyBlock(1, []Change{{Key: []byte("alice"), Value: []byte("100")}}); err != nil {
		t.Fatal(err)
	}
	first, err := state.ApplyBlock(2, []Change{{Key: []byte("bob"), Value: []byte("80")}})
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("missing first snapshot")
	}
	if stats := store.Stats(); stats.Entries == 0 {
		t.Fatalf("persisted snapshot did not populate KV cache: %+v", stats)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenLevelDBCached(path, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	recovered, err := OpenIntervalState(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.CurrentHeight() != 2 || recovered.LatestSnapshot() != *first {
		t.Fatalf("recovered state = height %d, snapshot %+v", recovered.CurrentHeight(), recovered.LatestSnapshot())
	}
	for key, want := range map[string]string{"alice": "100", "bob": "80"} {
		got, err := recovered.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("recovered %s = %q, %v; want %q", key, got, err, want)
		}
	}

	proof, err := recovered.Prove([]byte("alice"))
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := VerifyProof(first.Root, []byte("alice"), proof)
	if err != nil || !exists || string(value) != "100" {
		t.Fatalf("recovered proof = %q, %v, %v", value, exists, err)
	}

	if _, err := recovered.ApplyBlock(3, []Change{{Key: []byte("alice"), Value: []byte("75")}}); err != nil {
		t.Fatal(err)
	}
	second, err := recovered.ApplyBlock(4, []Change{{Key: []byte("bob"), Delete: true}})
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.Height != 4 {
		t.Fatalf("second snapshot = %+v", second)
	}
	old, found, err := recovered.SnapshotAt(2)
	if err != nil || !found || old != *first {
		t.Fatalf("historical snapshot = %+v, %v, %v", old, found, err)
	}
	historical, err := Open(old.Root, store)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := historical.Get([]byte("alice")); err != nil || string(got) != "100" {
		t.Fatalf("historical alice = %q, %v", got, err)
	}
}

func TestIntervalStateRejectsInvalidMetadataAndHeights(t *testing.T) {
	store := NewMemoryStore()
	batch := store.NewBatch()
	if err := batch.Put(latestSnapshotKey, []byte{snapshotMetadataVersion}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIntervalState(store, 1); !errors.Is(err, ErrCorruptMetadata) {
		t.Fatalf("corrupt metadata error = %v", err)
	}
	store = NewMemoryStore()
	batch = store.NewBatch()
	if err := batch.Put(latestSnapshotKey, encodeLatestSnapshot(Snapshot{Height: 1, Root: EmptyRootHash})); err != nil {
		t.Fatal(err)
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenIntervalState(store, 1); !errors.Is(err, ErrCorruptMetadata) {
		t.Fatalf("missing latest index error = %v", err)
	}
	if _, err := OpenIntervalState(NewMemoryStore(), 0); !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("invalid interval error = %v", err)
	}

	state, err := OpenIntervalState(NewMemoryStore(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyBlock(2, nil); !errors.Is(err, ErrUnexpectedHeight) {
		t.Fatalf("gap error = %v", err)
	}
	if _, err := state.ApplyBlock(1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyBlock(1, nil); !errors.Is(err, ErrUnexpectedHeight) {
		t.Fatalf("duplicate height error = %v", err)
	}
}

func TestIntervalStateScheduleOffsetAndPreview(t *testing.T) {
	state, err := OpenIntervalStateWithSchedule(NewMemoryStore(), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if interval, offset := state.Schedule(); interval != 3 || offset != 1 {
		t.Fatalf("schedule = %d, %d", interval, offset)
	}
	preview, err := state.Preview([]Change{{Key: []byte("preview"), Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := state.Get([]byte("preview")); got != nil {
		t.Fatalf("preview changed state: %q", got)
	}
	if snapshot, err := state.ApplyBlock(1, []Change{{Key: []byte("preview"), Value: []byte("value")}}); err != nil || snapshot == nil || snapshot.Root != preview {
		t.Fatalf("first offset snapshot = %+v, %v", snapshot, err)
	}
	if snapshot, err := state.ApplyBlock(2, nil); err != nil || snapshot != nil {
		t.Fatalf("height 2 snapshot = %+v, %v", snapshot, err)
	}
	if snapshot, err := state.ApplyBlock(3, nil); err != nil || snapshot != nil {
		t.Fatalf("height 3 snapshot = %+v, %v", snapshot, err)
	}
	if snapshot, err := state.ApplyBlock(4, nil); err != nil || snapshot == nil || snapshot.Height != 4 {
		t.Fatalf("height 4 snapshot = %+v, %v", snapshot, err)
	}
	if _, err := OpenIntervalStateWithSchedule(NewMemoryStore(), 3, 3); !errors.Is(err, ErrInvalidOffset) {
		t.Fatalf("invalid offset error = %v", err)
	}
}

func TestIntervalStateRetriesFailedBoundarySnapshot(t *testing.T) {
	base := NewMemoryStore()
	store := &failOnceStore{store: base, failures: 1}
	state, err := OpenIntervalState(store, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyBlock(1, []Change{{Key: []byte("alice"), Value: []byte("10")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.ApplyBlock(2, []Change{{Key: []byte("alice"), Value: []byte("20")}}); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("boundary write error = %v", err)
	}
	if len(state.updates) != 1 {
		t.Fatalf("failed boundary cleared update cache: %d entries", len(state.updates))
	}
	if _, err := base.Get(latestSnapshotKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("latest metadata persisted after failed batch: %v", err)
	}
	if _, err := state.ApplyBlock(3, nil); !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("next block error = %v", err)
	}
	snapshot, err := state.ForceSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Height != 2 {
		t.Fatalf("retried snapshot = %+v", snapshot)
	}
	if len(state.updates) != 0 {
		t.Fatalf("retried snapshot did not clear update cache: %d entries", len(state.updates))
	}
	if got, err := state.Get([]byte("alice")); err != nil || !bytes.Equal(got, []byte("20")) {
		t.Fatalf("retried state value = %q, %v", got, err)
	}
	if _, err := state.ApplyBlock(3, nil); err != nil {
		t.Fatalf("block after retry = %v", err)
	}
}

type countingStore struct {
	store   Store
	batches int
	puts    int
	writes  int
}

func (s *countingStore) Get(key []byte) ([]byte, error) {
	return s.store.Get(key)
}

func (s *countingStore) NewBatch() Batch {
	s.batches++
	return &countingBatch{store: s, batch: s.store.NewBatch()}
}

type countingBatch struct {
	store *countingStore
	batch Batch
}

func (b *countingBatch) Put(key, value []byte) error {
	if err := b.batch.Put(key, value); err != nil {
		return err
	}
	b.store.puts++
	return nil
}

func (b *countingBatch) Write() error {
	if err := b.batch.Write(); err != nil {
		return err
	}
	b.store.writes++
	return nil
}

var errInjectedWrite = errors.New("injected batch write failure")

type failOnceStore struct {
	store    Store
	failures int
}

func (s *failOnceStore) Get(key []byte) ([]byte, error) {
	return s.store.Get(key)
}

func (s *failOnceStore) NewBatch() Batch {
	return &failOnceBatch{store: s, batch: s.store.NewBatch()}
}

type failOnceBatch struct {
	store *failOnceStore
	batch Batch
}

func (b *failOnceBatch) Put(key, value []byte) error {
	return b.batch.Put(key, value)
}

func (b *failOnceBatch) Write() error {
	if b.store.failures > 0 {
		b.store.failures--
		return errInjectedWrite
	}
	return b.batch.Write()
}
