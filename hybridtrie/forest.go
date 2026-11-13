package hybridtrie

import (
	"bytes"
	"errors"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// StorageLifecycle is local physical metadata. AccountTrieID is implicit and
// never appears as an active storage mapping. Inactive records are tombstones.
type StorageLifecycle struct {
	Active bool
	TrieID uint64
}

func encodeStorageLifecycle(l StorageLifecycle) ([]byte, error) {
	if !l.Active {
		if l.TrieID != 0 {
			return nil, ErrCorrupt
		}
		return []byte{0}, nil
	}
	if l.TrieID == 0 || l.TrieID > MaxTrieID {
		return nil, ErrLimit
	}
	raw := make([]byte, 1+TrieIDBytes)
	raw[0] = 1
	put48(raw[1:], l.TrieID)
	return raw, nil
}
func decodeStorageLifecycle(raw []byte) (StorageLifecycle, error) {
	if len(raw) == 1 && raw[0] == 0 {
		return StorageLifecycle{}, nil
	}
	if len(raw) == 1+TrieIDBytes && raw[0] == 1 {
		id := read48(raw[1:])
		if id != 0 {
			return StorageLifecycle{true, id}, nil
		}
	}
	return StorageLifecycle{}, ErrCorrupt
}
func lifecycleKey(address [20]byte, h uint64) []byte {
	key := make([]byte, 1+20+HeightBytes)
	key[0] = 'L'
	copy(key[1:], address[:])
	put48(key[21:], h)
	return key
}
func storageOwnerKey(id uint64) []byte {
	key := make([]byte, 1+TrieIDBytes)
	key[0] = 'O'
	put48(key[1:], id)
	return key
}
func readStorageLifecycle(snap diskSnapshot, address [20]byte, h uint64) (uint64, StorageLifecycle, error) {
	if h > MaxHeight {
		return 0, StorageLifecycle{}, ErrLimit
	}
	target := lifecycleKey(address, h)
	found, raw, err := snap.prev(metaColumn, target)
	if err != nil {
		return 0, StorageLifecycle{}, err
	}
	if len(found) != len(target) || !bytes.Equal(found[:21], target[:21]) || read48(found[21:]) > h {
		return 0, StorageLifecycle{}, ErrNotFound
	}
	life, err := decodeStorageLifecycle(raw)
	return read48(found[21:]), life, err
}
func readStorageOwner(snap diskSnapshot, id uint64) ([20]byte, bool, error) {
	var address [20]byte
	raw, err := snap.get(metaColumn, storageOwnerKey(id))
	if errors.Is(err, ErrNotFound) {
		return address, false, nil
	}
	if err != nil {
		return address, false, err
	}
	if len(raw) != 20 {
		return address, false, ErrCorrupt
	}
	copy(address[:], raw)
	return address, true, nil
}

// StorageLifecycle reads the mapping at this View's fixed height. Missing data
// is distinct from an explicit inactive row; neither maps to account namespace 0.
func (v *View) StorageLifecycle(address [20]byte) (uint64, StorageLifecycle, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return 0, StorageLifecycle{}, ErrClosed
	}
	return readStorageLifecycle(v.snapshot, address, v.height)
}

// StorageTrieID returns active=false for absent or deleted storage. The zero ID
// in that result must not be used as a redirect to the implicit account trie.
func (v *View) StorageTrieID(address [20]byte) (uint64, bool, error) {
	_, life, err := v.StorageLifecycle(address)
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return life.TrieID, life.Active, nil
}

// ForestUpdate is a single-owner optimistic block plan. Allocation, lifecycle
// changes and final trie roots publish in the same WAL+Sync write as Head. It
// keeps no native snapshot between calls; an intervening commit causes conflict.
type ForestUpdate struct {
	// Operation-local dirty emissions and old child locators, never node copies.
	pendingChanges map[uint64][]preparedChange
	retainedRoutes map[forestPosition]projectionRoute
	// Both banks are borrowed during commit; neither is retained by the update.
	retainedPrefix   map[forestPosition]bool // removed cache positions retain Prefix for this commit
	cacheBoundary    *boundaryBank           // final resident frontier for deferred Hash placement
	baseNodes        *boundaryBank           // resident hints; projection requires an exact base version/hash match
	projectedEdges   func(NodeKey, [16]boundaryEdge)
	initialNodeCheck func(NodeKey, []byte) error
	store            *Store
	base             Head
	height, nextID   uint64
	updates          map[uint64]TrieUpdate
	lives            map[[20]byte]StorageLifecycle
	created          map[uint64][20]byte
	codes            map[mpt.Hash][]byte
	codeBytes        uint64
	blockID          [32]byte
	done             bool
}

// BeginForest requires the same continuous-height and parent CAS as Commit.
// The plan is single-owner, and its caller must finish it with Commit or Abort.
func (s *Store) BeginForest(height uint64, parent [32]byte) (*ForestUpdate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if s.readOnly {
		return nil, ErrReadOnly
	}
	if s.importPending || s.importActive {
		return nil, ErrSnapshotPending
	}
	if height > MaxHeight {
		return nil, ErrLimit
	}
	expected := s.cfg.InitialHeight
	if s.head.Initialized {
		if s.head.Height == MaxHeight {
			return nil, ErrLimit
		}
		expected = s.head.Height + 1
	}
	if height != expected || parent != s.head.CommitID {
		return nil, ErrConflict
	}
	return &ForestUpdate{store: s, base: s.head, height: height, nextID: s.head.NextTrieID, updates: map[uint64]TrieUpdate{}, lives: map[[20]byte]StorageLifecycle{}, created: map[uint64][20]byte{}, codes: map[mpt.Hash][]byte{}}, nil
}
func (u *ForestUpdate) checkLocked() error {
	if u.done {
		return ErrClosed
	}
	if err := u.store.check(); err != nil {
		return err
	}
	if u.store.readOnly {
		return ErrReadOnly
	}
	if u.store.head != u.base {
		return ErrConflict
	}
	return nil
}
func (u *ForestUpdate) room(extra int) error {
	// A staging guard, separate from exact persisted Key+Value charging. Working
	// trie allocations are caller-owned and are not a hard RSS bound.
	if uint64(len(u.updates)+len(u.lives)+len(u.created)+extra) > u.store.cfg.MaxBatchBytes/128 {
		return ErrLimit
	}
	return nil
}
func (u *ForestUpdate) StorageTrieID(address [20]byte) (uint64, bool, error) {
	s := u.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := u.checkLocked(); err != nil {
		return 0, false, err
	}
	if life, ok := u.lives[address]; ok {
		return life.TrieID, life.Active, nil
	}
	if !u.base.Initialized {
		return 0, false, nil
	}
	snap := s.db.snapshot()
	defer snap.close()
	_, life, err := readStorageLifecycle(snap, address, u.base.Height)
	if errors.Is(err, ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return life.TrieID, life.Active, nil
}

// CreateStorageTrie allocates a new lifetime, not a new ID on every update.
// SetTrie must supply its final tree before commit, even if storage is empty.
func (u *ForestUpdate) CreateStorageTrie(address [20]byte) (uint64, error) {
	_, active, err := u.StorageTrieID(address)
	if err != nil {
		return 0, err
	}
	if active {
		return 0, ErrConflict
	}
	if u.nextID == 0 {
		return 0, ErrCorrupt
	}
	if u.nextID > MaxTrieID {
		return 0, ErrLimit
	}
	if err = u.room(2); err != nil {
		return 0, err
	}
	id := u.nextID
	u.nextID++
	u.created[id] = address
	u.lives[address] = StorageLifecycle{true, id}
	return id, nil
}

// DeleteStorageTrie ends a lifetime while retaining its historical roots.
// The EVM adapter decides when deletion occurs; an empty trie is not deletion.
func (u *ForestUpdate) DeleteStorageTrie(address [20]byte) error {
	id, active, err := u.StorageTrieID(address)
	if err != nil {
		return err
	}
	if !active {
		return nil
	}
	if err = u.room(1); err != nil {
		return err
	}
	u.lives[address] = StorageLifecycle{}
	if _, newID := u.created[id]; newID {
		delete(u.updates, id)
	}
	return nil
}

// SetTrie stages a final logical tree. Trees remain caller-owned and must stay
// readable and unmodified during Commit. AccountTrieID needs no mapping.
func (u *ForestUpdate) SetTrie(id uint64, tree *mpt.Trie, prefixMask uint16) error {
	s := u.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := u.checkLocked(); err != nil {
		return err
	}
	if id > MaxTrieID {
		return ErrLimit
	}
	if tree == nil {
		return ErrConflict
	}
	if _, exists := u.updates[id]; !exists {
		if err := u.room(1); err != nil {
			return err
		}
	}
	u.updates[id] = TrieUpdate{id, tree, prefixMask}
	return nil
}
func (u *ForestUpdate) Commit() (Head, CommitStats, error) { return u.CommitWithLayout(nil) }
func (u *ForestUpdate) CommitWithLayout(selector LayoutSelector) (Head, CommitStats, error) {
	return u.commitWithMode(selector, false, false)
}

// CommitWithSoftLayout is CommitWithLayout with Store.CommitWithSoftLayout
// semantics: selector-excluded children are cold, never forced to Hash.
func (u *ForestUpdate) CommitWithSoftLayout(selector LayoutSelector) (Head, CommitStats, error) {
	return u.commitWithMode(selector, false, true)
}

// CommitInitialHash publishes allocation/lifecycle/code metadata with a pure
// Hash initial forest. It is valid only before the first Head is initialized.
func (u *ForestUpdate) CommitInitialHash() (Head, CommitStats, error) {
	return u.commitWithMode(nil, true, false)
}
func (u *ForestUpdate) commitWithMode(selector LayoutSelector, initialHash, softLayout bool) (Head, CommitStats, error) {
	s := u.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := u.checkLocked(); err != nil {
		return s.head, CommitStats{}, err
	}
	updates := make([]TrieUpdate, 0, len(u.updates))
	for _, update := range u.updates {
		updates = append(updates, update)
	}
	head, stats, err := s.commitLocked(u.height, u.base.CommitID, updates, selector, u, initialHash, softLayout)
	if err == nil || errors.Is(err, ErrUncertain) {
		u.Abort()
	}
	return head, stats, err
}

// Abort discards unpublished allocations and releases staged tree references.
func (u *ForestUpdate) Abort() {
	u.done = true
	u.updates = nil
	u.pendingChanges = nil
	u.retainedRoutes = nil
	u.lives = nil
	u.created = nil
	u.codes = nil
	u.initialNodeCheck = nil
	u.cacheBoundary = nil
	u.baseNodes = nil
}

// validateStorageUpdate also guards generic commits/caches against updating a
// retired managed ID. Raw, explicitly supplied namespaces remain usable by the
// low-level Store API and advance the shared high-water mark.
func (s *Store) validateStorageUpdate(snap diskSnapshot, id uint64, forest *ForestUpdate) error {
	if id == AccountTrieID {
		return nil
	}
	if forest != nil {
		if owner, created := forest.created[id]; created {
			life := forest.lives[owner]
			if !life.Active || life.TrieID != id {
				return ErrConflict
			}
			if _, exists, err := readStorageOwner(snap, id); err != nil {
				return err
			} else if exists {
				return ErrConflict
			}
			if s.head.Initialized {
				if _, err := readRoot(snap, id, s.head.Height); err == nil {
					return ErrConflict
				} else if !errors.Is(err, ErrNotFound) {
					return err
				}
			}
			return nil
		}
	}
	owner, managed, err := readStorageOwner(snap, id)
	if err != nil {
		return err
	}
	if !managed {
		if forest != nil {
			return ErrConflict
		}
		return nil
	}
	var life StorageLifecycle
	if forest != nil {
		if pending, ok := forest.lives[owner]; ok {
			life = pending
		} else {
			_, life, err = readStorageLifecycle(snap, owner, s.head.Height)
		}
	} else {
		_, life, err = readStorageLifecycle(snap, owner, s.head.Height)
	}
	if err != nil {
		return err
	}
	if !life.Active || life.TrieID != id {
		return ErrConflict
	}
	return nil
}
func (u *ForestUpdate) stageMetadata(snap diskSnapshot, p *writePlan) ([]entry, error) {
	addresses := make([][20]byte, 0, len(u.lives))
	for address := range u.lives {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
	var metadata []entry
	stage := func(key, value []byte) error {
		if err := p.add(metaColumn, key, value); err != nil {
			return err
		}
		metadata = append(metadata, entry{metaColumn, key, value})
		return nil
	}
	for _, address := range addresses {
		life := u.lives[address]
		var before StorageLifecycle
		if u.base.Initialized {
			_, old, err := readStorageLifecycle(snap, address, u.base.Height)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return nil, err
			}
			before = old
		}
		if before == life {
			continue
		}
		if life.Active {
			if owner, ok := u.created[life.TrieID]; !ok || owner != address {
				return nil, ErrConflict
			}
			if _, ok := u.updates[life.TrieID]; !ok {
				return nil, ErrConflict
			}
			if err := stage(storageOwnerKey(life.TrieID), append([]byte(nil), address[:]...)); err != nil {
				return nil, err
			}
		}
		value, err := encodeStorageLifecycle(life)
		if err != nil {
			return nil, err
		}
		if err = stage(lifecycleKey(address, u.height), value); err != nil {
			return nil, err
		}
	}
	evm, err := u.stageEVM(snap, p)
	if err != nil {
		return nil, err
	}
	metadata = append(metadata, evm...)
	sort.Slice(metadata, func(i, j int) bool { return bytes.Compare(metadata[i].key, metadata[j].key) < 0 })
	return metadata, nil
}
