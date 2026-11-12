package mpt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var (
	ErrInvalidInterval  = errors.New("mpt: interval must be greater than zero")
	ErrInvalidOffset    = errors.New("mpt: snapshot offset must be less than interval")
	ErrUnexpectedHeight = errors.New("mpt: block height is not contiguous")
	ErrCorruptMetadata  = errors.New("mpt: corrupt snapshot metadata")
	ErrSnapshotRequired = errors.New("mpt: previous interval snapshot must be persisted first")
)

const snapshotMetadataVersion byte = 1

var (
	latestSnapshotKey = []byte("statera/meta/latest")
	snapshotKeyPrefix = []byte("statera/meta/snapshot/")
)

// Change is one state change in a block. Delete takes precedence over Value.
// A non-delete change with an empty Value follows Trie.Update semantics and
// deletes the key.
type Change struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Snapshot identifies one durable MPT root at a block height.
type Snapshot struct {
	Height uint64
	Root   Hash
}

// IntervalState keeps the last durable trie separate from an in-memory update
// cache. It materializes cached changes only at fixed block-height boundaries.
// It accepts one mutating caller. After a restart, the caller must replay
// blocks following LatestSnapshot.
type IntervalState struct {
	store          Store
	trie           *Trie
	updates        map[string]cachedChange
	interval       uint64
	offset         uint64
	snapshotHeight uint64
	snapshotRoot   Hash
	currentHeight  uint64
}

type cachedChange struct {
	key    []byte
	value  []byte
	delete bool
}

// OpenIntervalState opens the latest durable snapshot. When no snapshot has
// been persisted, it starts at the empty state at height zero.
func OpenIntervalState(store Store, interval uint64) (*IntervalState, error) {
	return OpenIntervalStateWithSchedule(store, interval, 0)
}

// OpenIntervalStateWithSchedule opens the latest durable snapshot using a
// schedule that persists height offset, offset+interval, and so on.
func OpenIntervalStateWithSchedule(store Store, interval, offset uint64) (*IntervalState, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if interval == 0 {
		return nil, ErrInvalidInterval
	}
	if offset >= interval {
		return nil, ErrInvalidOffset
	}

	snapshot, found, err := loadLatestSnapshot(store)
	if err != nil {
		return nil, err
	}
	if !found {
		snapshot = Snapshot{Root: EmptyRootHash}
	} else if err := verifySnapshotIndex(store, snapshot); err != nil {
		return nil, err
	}
	trie, err := Open(snapshot.Root, store)
	if err != nil {
		return nil, fmt.Errorf("mpt: open latest snapshot: %w", err)
	}
	return &IntervalState{
		store:          store,
		trie:           trie,
		updates:        make(map[string]cachedChange),
		interval:       interval,
		offset:         offset,
		snapshotHeight: snapshot.Height,
		snapshotRoot:   snapshot.Root,
		currentHeight:  snapshot.Height,
	}, nil
}

// ApplyBlock stores one contiguous block's final key changes in the independent
// update cache. It returns a non-nil snapshot only at an interval boundary.
func (s *IntervalState) ApplyBlock(height uint64, changes []Change) (*Snapshot, error) {
	if s.shouldPersist(s.currentHeight) && s.currentHeight != s.snapshotHeight {
		return nil, ErrSnapshotRequired
	}
	if height != s.currentHeight+1 {
		return nil, ErrUnexpectedHeight
	}

	updates := s.copyUpdates()
	applyChanges(updates, changes)

	s.updates = updates
	s.currentHeight = height
	if !s.shouldPersist(height) {
		return nil, nil
	}
	snapshot, err := s.persistSnapshot()
	if err != nil {
		return nil, err
	}
	return &snapshot, nil
}

func applyChanges(updates map[string]cachedChange, changes []Change) {
	for _, change := range changes {
		updates[string(change.Key)] = cachedChange{
			key:    cloneBytes(change.Key),
			value:  cloneBytes(change.Value),
			delete: change.Delete || len(change.Value) == 0,
		}
	}
}

// Schedule returns the immutable snapshot schedule selected at open time.
func (s *IntervalState) Schedule() (interval, offset uint64) {
	return s.interval, s.offset
}

// Preview calculates the root after applying changes without changing the
// update cache or writing to the backing store.
func (s *IntervalState) Preview(changes []Change) (Hash, error) {
	updates := s.copyUpdates()
	applyChanges(updates, changes)
	trie, err := s.materialize(updates)
	if err != nil {
		return Hash{}, err
	}
	return trie.Hash()
}

// Get returns the value from the current in-memory state.
func (s *IntervalState) Get(key []byte) ([]byte, error) {
	if change, ok := s.updates[string(key)]; ok {
		if change.delete {
			return nil, nil
		}
		return cloneBytes(change.value), nil
	}
	return s.trie.Get(key)
}

// Hash computes the current root without making it durable.
func (s *IntervalState) Hash() (Hash, error) {
	trie, err := s.materializeUpdates()
	if err != nil {
		return Hash{}, err
	}
	return trie.Hash()
}

// Prove creates a proof for the current in-memory state.
func (s *IntervalState) Prove(key []byte) (Proof, error) {
	trie, err := s.materializeUpdates()
	if err != nil {
		return nil, err
	}
	return trie.Prove(key)
}

// CurrentHeight returns the height represented by the in-memory trie.
func (s *IntervalState) CurrentHeight() uint64 {
	return s.currentHeight
}

// LatestSnapshot returns the most recently durable state root.
func (s *IntervalState) LatestSnapshot() Snapshot {
	return Snapshot{Height: s.snapshotHeight, Root: s.snapshotRoot}
}

// SnapshotAt returns a durable snapshot for height. Height zero always names
// the empty genesis state. Intermediate, non-materialized heights return false.
func (s *IntervalState) SnapshotAt(height uint64) (Snapshot, bool, error) {
	if height == 0 {
		return Snapshot{Root: EmptyRootHash}, true, nil
	}
	if height == s.snapshotHeight {
		return s.LatestSnapshot(), true, nil
	}
	value, err := s.store.Get(snapshotMetadataKey(height))
	if errors.Is(err, ErrNotFound) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("mpt: read snapshot metadata: %w", err)
	}
	snapshot, err := decodeSnapshotIndex(height, value)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, true, nil
}

// ReadSnapshotIndex reads metadata only; it never opens the referenced trie.
// Archive views must check their schedule before calling this function.
func ReadSnapshotIndex(store Store, height uint64) (Snapshot, error) {
	if height == 0 {
		return Snapshot{Root: EmptyRootHash}, nil
	}
	value, err := store.Get(snapshotMetadataKey(height))
	if err != nil {
		return Snapshot{}, err
	}
	return decodeSnapshotIndex(height, value)
}

// ForceSnapshot materializes the current state even away from an interval
// boundary. It is intended for explicit checkpointing and retrying a failed
// boundary commit.
func (s *IntervalState) ForceSnapshot() (Snapshot, error) {
	if s.currentHeight == s.snapshotHeight {
		return s.LatestSnapshot(), nil
	}
	return s.persistSnapshot()
}

func (s *IntervalState) persistSnapshot() (Snapshot, error) {
	trie, err := s.materializeUpdates()
	if err != nil {
		return Snapshot{}, err
	}
	prepared, err := trie.prepareCommit()
	if err != nil {
		return Snapshot{}, err
	}
	batch := s.store.NewBatch()
	if err := prepared.putNodes(batch); err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{Height: s.currentHeight, Root: prepared.root}
	if err := batch.Put(snapshotMetadataKey(snapshot.Height), encodeSnapshotIndex(snapshot)); err != nil {
		return Snapshot{}, err
	}
	if err := batch.Put(latestSnapshotKey, encodeLatestSnapshot(snapshot)); err != nil {
		return Snapshot{}, err
	}
	if err := batch.Write(); err != nil {
		return Snapshot{}, err
	}

	trie.acceptCommit(snapshot.Root)
	s.trie = trie
	s.updates = make(map[string]cachedChange)
	s.snapshotHeight = snapshot.Height
	s.snapshotRoot = snapshot.Root
	return snapshot, nil
}

func (s *IntervalState) copyUpdates() map[string]cachedChange {
	updates := make(map[string]cachedChange, len(s.updates))
	for key, change := range s.updates {
		updates[key] = cachedChange{
			key:    cloneBytes(change.key),
			value:  cloneBytes(change.value),
			delete: change.delete,
		}
	}
	return updates
}

func (s *IntervalState) materializeUpdates() (*Trie, error) {
	return s.materialize(s.updates)
}

func (s *IntervalState) materialize(updates map[string]cachedChange) (*Trie, error) {
	trie := s.trie.Copy()
	keys := make([]string, 0, len(updates))
	for key := range updates {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		change := updates[key]
		var err error
		if change.delete {
			err = trie.Delete(change.key)
		} else {
			err = trie.Update(change.key, change.value)
		}
		if err != nil {
			return nil, err
		}
	}
	return trie, nil
}

func (s *IntervalState) shouldPersist(height uint64) bool {
	return height > 0 && height >= s.offset && (height-s.offset)%s.interval == 0
}

func loadLatestSnapshot(store Store) (Snapshot, bool, error) {
	value, err := store.Get(latestSnapshotKey)
	if errors.Is(err, ErrNotFound) {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("mpt: read latest snapshot: %w", err)
	}
	snapshot, err := decodeLatestSnapshot(value)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, true, nil
}

func verifySnapshotIndex(store Store, latest Snapshot) error {
	value, err := store.Get(snapshotMetadataKey(latest.Height))
	if errors.Is(err, ErrNotFound) {
		return ErrCorruptMetadata
	}
	if err != nil {
		return fmt.Errorf("mpt: read latest snapshot index: %w", err)
	}
	indexed, err := decodeSnapshotIndex(latest.Height, value)
	if err != nil {
		return err
	}
	if indexed.Root != latest.Root {
		return ErrCorruptMetadata
	}
	return nil
}

func snapshotMetadataKey(height uint64) []byte {
	key := make([]byte, len(snapshotKeyPrefix)+8)
	copy(key, snapshotKeyPrefix)
	binary.BigEndian.PutUint64(key[len(snapshotKeyPrefix):], height)
	return key
}

func encodeLatestSnapshot(snapshot Snapshot) []byte {
	value := make([]byte, 1+8+len(snapshot.Root))
	value[0] = snapshotMetadataVersion
	binary.BigEndian.PutUint64(value[1:], snapshot.Height)
	copy(value[9:], snapshot.Root[:])
	return value
}

func decodeLatestSnapshot(value []byte) (Snapshot, error) {
	if len(value) != 1+8+len(Hash{}) || value[0] != snapshotMetadataVersion {
		return Snapshot{}, ErrCorruptMetadata
	}
	var root Hash
	copy(root[:], value[9:])
	return Snapshot{Height: binary.BigEndian.Uint64(value[1:]), Root: root}, nil
}

func encodeSnapshotIndex(snapshot Snapshot) []byte {
	value := make([]byte, 1+len(snapshot.Root))
	value[0] = snapshotMetadataVersion
	copy(value[1:], snapshot.Root[:])
	return value
}

func decodeSnapshotIndex(height uint64, value []byte) (Snapshot, error) {
	if len(value) != 1+len(Hash{}) || value[0] != snapshotMetadataVersion {
		return Snapshot{}, ErrCorruptMetadata
	}
	var root Hash
	copy(root[:], value[1:])
	return Snapshot{Height: height, Root: root}, nil
}
