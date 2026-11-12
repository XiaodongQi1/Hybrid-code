//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func forestTree(t *testing.T, tag byte) *mpt.Trie {
	t.Helper()
	tree, err := mpt.NewEmpty(mpt.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if tag != 0 {
		if err = tree.Update([]byte{0x12}, bytes.Repeat([]byte{tag}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	return tree
}
func forestSet(t *testing.T, u *ForestUpdate, id uint64, tree *mpt.Trie) {
	t.Helper()
	if err := u.SetTrie(id, tree, 0xffff); err != nil {
		t.Fatal(err)
	}
}
func forestBegin(t *testing.T, s *Store, h uint64, parent [32]byte) *ForestUpdate {
	t.Helper()
	u, err := s.BeginForest(h, parent)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
func forestCommit(t *testing.T, u *ForestUpdate) (Head, CommitStats) {
	t.Helper()
	h, stats, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	return h, stats
}
func forestID(t *testing.T, u *ForestUpdate, address [20]byte, want uint64) {
	t.Helper()
	id, err := u.CreateStorageTrie(address)
	if err != nil || id != want {
		t.Fatal("allocation", id, want, err)
	}
}
func forestMapping(t *testing.T, s *Store, height uint64, address [20]byte, wantID uint64, wantActive bool) {
	t.Helper()
	v, err := s.ViewAt(height)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	id, active, err := v.StorageTrieID(address)
	if err != nil || id != wantID || active != wantActive {
		t.Fatal("historical mapping", height, id, active, err)
	}
}
func forestProof(t *testing.T, s *Store, height, id uint64, tree *mpt.Trie, tag byte) {
	t.Helper()
	v, err := s.ViewAt(height)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	root, err := tree.Hash()
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Root(id)
	if err != nil || got != root {
		t.Fatal("root", height, id, got, root, err)
	}
	proof, err := v.Prove(id, root, []byte{0x12})
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := mpt.VerifyProof(root, []byte{0x12}, proof)
	var want []byte
	if tag != 0 {
		want = bytes.Repeat([]byte{tag}, 64)
	}
	if err != nil || exists != (tag != 0) || !bytes.Equal(value, want) {
		t.Fatal("namespace proof", height, id, exists, err)
	}
}

func TestForestImplicitAccountAndHistoricalLifecycles(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "forest-lifecycles")
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, cfg, true, false)
	defer func() { s.Close() }()
	account, old, fresh := forestTree(t, 1), forestTree(t, 2), forestTree(t, 3)
	a, b := [20]byte{1}, [20]byte{2}
	u := forestBegin(t, s, 0, [32]byte{})
	forestSet(t, u, AccountTrieID, account)
	h, stats := forestCommit(t, u)
	if h.NextTrieID != 1 || stats.LifecycleRecords != 0 || stats.OwnerRecords != 0 {
		t.Fatal("account allocation or mapping", h, stats)
	}
	if err := s.db.(*rocksEngine).exportFamily(metaColumn, func(key, value []byte) error {
		if len(key) > 0 && (key[0] == 'L' || key[0] == 'O') {
			t.Fatal("account trie has mapping", key)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	forestMapping(t, s, 0, a, 0, false)
	v, _ := s.ViewAt(0)
	if _, _, err := v.StorageLifecycle(a); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing mapping", err)
	}
	v.Close()
	u = forestBegin(t, s, 1, h.CommitID)
	forestID(t, u, a, 1)
	forestID(t, u, b, 2)
	if _, err := u.CreateStorageTrie(a); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate deployment", err)
	}
	forestSet(t, u, 1, old)
	forestSet(t, u, 2, forestTree(t, 0))
	h, stats = forestCommit(t, u)
	if h.NextTrieID != 3 || stats.LifecycleRecords != 2 || stats.OwnerRecords != 2 || stats.LifecycleBytes != 68 || stats.OwnerBytes != 54 {
		t.Fatal("mapping charge", h, stats)
	}
	u = forestBegin(t, s, 2, h.CommitID)
	forestSet(t, u, 1, old)
	h, stats = forestCommit(t, u)
	if stats.LifecycleRecords != 0 || stats.OwnerRecords != 0 {
		t.Fatal("ordinary block rewrote lifecycle", stats)
	}
	u = forestBegin(t, s, 3, h.CommitID)
	if err := u.DeleteStorageTrie(a); err != nil {
		t.Fatal(err)
	}
	h, stats = forestCommit(t, u)
	if stats.LifecycleRecords != 1 || stats.LifecycleBytes != 28 || stats.OwnerRecords != 0 {
		t.Fatal("tombstone charge", stats)
	}
	if _, _, err := s.Commit(4, h.CommitID, []TrieUpdate{{1, old, 0}}); !errors.Is(err, ErrConflict) {
		t.Fatal("retired namespace accepted", err)
	}
	u = forestBegin(t, s, 4, h.CommitID)
	forestID(t, u, a, 3)
	forestSet(t, u, 3, fresh)
	h, _ = forestCommit(t, u)
	u = forestBegin(t, s, 5, h.CommitID)
	if err := u.DeleteStorageTrie(a); err != nil {
		t.Fatal(err)
	}
	forestID(t, u, a, 4) // same-block replacement uses a fresh ID as well.
	forestSet(t, u, 4, forestTree(t, 0))
	h, _ = forestCommit(t, u)
	u = forestBegin(t, s, 6, h.CommitID)
	forestSet(t, u, 1, old)
	if _, _, err := u.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("forest wrote retired namespace", err)
	}
	u.Abort()
	// Address zero is a possible local owner; it is not the implicit account ID.
	u = forestBegin(t, s, 6, h.CommitID)
	forestID(t, u, [20]byte{}, 5)
	forestSet(t, u, 5, forestTree(t, 0))
	h, _ = forestCommit(t, u)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, cfg, false, true)
	if got, err := s.Head(); err != nil || got != h || got.NextTrieID != 6 {
		t.Fatal("recovered allocator", got, err)
	}
	for height, id := range []uint64{0, 1, 1, 0, 3, 4, 4} {
		forestMapping(t, s, uint64(height), a, id, id != 0)
		forestProof(t, s, uint64(height), 0, account, 1)
	}
	forestMapping(t, s, 6, [20]byte{}, 5, true)
	forestProof(t, s, 1, 1, old, 2)
	forestProof(t, s, 4, 3, fresh, 3)
	forestProof(t, s, 5, 4, forestTree(t, 0), 0)
	// Retired roots remain queryable explicitly but address lookup selects the new ID.
	forestProof(t, s, 6, 1, old, 2)
	v, _ = s.ViewAt(3)
	trueHeight, life, err := v.StorageLifecycle(a)
	if err != nil || trueHeight != 3 || life.Active {
		t.Fatal("explicit deletion", trueHeight, life, err)
	}
	v.Close()
	if _, err = s.BeginForest(7, h.CommitID); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
}

func TestForestAbortStaleAndPrewriteLimits(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "forest-abort")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	a, b := [20]byte{1}, [20]byte{2}
	u := forestBegin(t, s, 0, [32]byte{})
	forestID(t, u, a, 1)
	u.Abort()
	first, stale := forestBegin(t, s, 0, [32]byte{}), forestBegin(t, s, 0, [32]byte{})
	forestID(t, first, a, 1)
	forestID(t, stale, b, 1)
	if _, _, err := first.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("published mapping without storage root", err)
	}
	if head, _ := s.Head(); head.Initialized || head.NextTrieID != 1 {
		t.Fatal("failed plan published counter", head)
	}
	large := forestTree(t, 1)
	if err := large.Update([]byte{0x13}, make([]byte, 8192)); err != nil {
		t.Fatal(err)
	}
	forestSet(t, first, 1, large)
	budget := s.cfg.MaxBatchBytes
	s.cfg.MaxBatchBytes = 4096
	if _, _, err := first.Commit(); !errors.Is(err, ErrLimit) {
		t.Fatal("batch limit", err)
	}
	if head, _ := s.Head(); head.Initialized || head.NextTrieID != 1 {
		t.Fatal("limited plan published", head)
	}
	s.cfg.MaxBatchBytes = budget
	h, _ := forestCommit(t, first)
	if _, _, err := stale.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("stale allocation published", err)
	}
	stale.Abort()
	forestMapping(t, s, 0, a, 1, true)
	forestMapping(t, s, 0, b, 0, false)
	u = forestBegin(t, s, 1, h.CommitID)
	forestID(t, u, b, 2)
	forestSet(t, u, 2, forestTree(t, 2))
	if err := u.DeleteStorageTrie(b); err != nil {
		t.Fatal(err)
	}
	h, stats := forestCommit(t, u)
	if h.NextTrieID != 3 || stats.LifecycleRecords != 0 || stats.OwnerRecords != 0 {
		t.Fatal("transient deployment", h, stats)
	}
	forestMapping(t, s, 1, b, 0, false)
	v, _ := s.ViewAt(1)
	if _, err := v.Root(2); !errors.Is(err, ErrNotFound) {
		t.Fatal("deleted staged tree persisted", err)
	}
	v.Close()
	u = forestBegin(t, s, 2, h.CommitID)
	forestID(t, u, b, 3)
	u.Abort()
}

func TestForestFull48BitIDsAndExhaustion(t *testing.T) {
	for _, high := range []uint64{1<<40 + 7, MaxTrieID - 1} {
		t.Run(fmt.Sprint(high), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "forest-wide-id")
			cfg.InitialHeight = 256
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			defer func() { s.Close() }()
			low, wide := forestTree(t, 1), forestTree(t, 2)
			h, _, err := s.Commit(256, [32]byte{}, []TrieUpdate{{0, low, 0xffff}, {7, low, 0xffff}, {high, wide, 0xffff}})
			if err != nil || h.NextTrieID != high+1 {
				t.Fatal("high-water mark", h, err)
			}
			u := forestBegin(t, s, 257, h.CommitID)
			forestID(t, u, [20]byte{1}, high+1)
			forestSet(t, u, high+1, forestTree(t, 0))
			h, _ = forestCommit(t, u)
			forestProof(t, s, 257, 0, low, 1) // no namespace may interleave account versions.
			forestProof(t, s, 257, 7, low, 1)
			forestProof(t, s, 257, high, wide, 2)
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openTestStore(t, path, cfg, false, false)
			got, err := s.Head()
			if err != nil || got != h {
				t.Fatal("wide counter recovery", got, err)
			}
			forestMapping(t, s, 257, [20]byte{1}, high+1, true)
			u = forestBegin(t, s, 258, h.CommitID)
			if high == MaxTrieID-1 {
				if _, err = u.CreateStorageTrie([20]byte{2}); !errors.Is(err, ErrLimit) {
					t.Fatal("allocator wrapped", err)
				}
				forestSet(t, u, 0, low) // exhaustion does not stop account updates.
				forestCommit(t, u)
			} else {
				forestID(t, u, [20]byte{2}, high+2)
				u.Abort()
			}
		})
	}
}

func TestForestAtomicAllocatorMappingAndRoots(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "forest-write-fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			old, fresh := forestTree(t, 1), forestTree(t, 2)
			u := forestBegin(t, s, 0, [32]byte{})
			forestSet(t, u, 0, old)
			h, _ := forestCommit(t, u)
			u = forestBegin(t, s, 1, h.CommitID)
			address := [20]byte{1}
			forestID(t, u, address, 1)
			forestSet(t, u, 0, fresh)
			forestSet(t, u, 1, old)
			realWrite := s.write
			s.write = func(entries []entry) error {
				seen := map[byte]int{}
				for _, e := range entries {
					if e.cf == metaColumn {
						seen[e.key[0]]++
						if bytes.Equal(e.key, headKey) {
							var next Head
							if err := json.Unmarshal(e.value, &next); err != nil || next.NextTrieID != 2 || next.Height != 1 {
								t.Fatal("allocator missing from batch", next, err)
							}
						}
					}
				}
				if seen['L'] != 1 || seen['O'] != 1 || seen['R'] != 2 || seen['B'] != 1 || seen['M'] != 1 {
					t.Fatal("not one complete atomic batch", seen)
				}
				if after {
					if err := realWrite(entries); err != nil {
						return err
					}
				}
				return errors.New("injected forest write outcome")
			}
			if _, _, err := u.Commit(); !errors.Is(err, ErrUncertain) || !u.done {
				t.Fatal("uncertain plan reused", err)
			}
			if _, err := s.Head(); !errors.Is(err, ErrUncertain) {
				t.Fatal("uncertain writer reused", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openTestStore(t, path, cfg, false, false)
			defer s.Close()
			head, err := s.Head()
			if err != nil {
				t.Fatal(err)
			}
			if after {
				if head.Height != 1 || head.NextTrieID != 2 {
					t.Fatal(head)
				}
				forestMapping(t, s, 1, address, 1, true)
				forestProof(t, s, 1, 0, fresh, 2)
				forestProof(t, s, 1, 1, old, 1)
			} else {
				if head != h || head.NextTrieID != 1 {
					t.Fatal(head)
				}
				forestMapping(t, s, 0, address, 0, false)
				forestProof(t, s, 0, 0, old, 1)
			}
			u = forestBegin(t, s, head.Height+1, head.CommitID)
			forestID(t, u, [20]byte{2}, head.NextTrieID)
			u.Abort()
		})
	}
}

func TestForestCachedHighIDAndRetiredWriteGuard(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "forest-cache-namespace")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	high := uint64(1<<40 + 7)
	low, wide := forestTree(t, 1), forestTree(t, 2)
	h, _, err := s.Commit(0, [32]byte{}, []TrieUpdate{{7, low, 0xffff}, {high, wide, 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	u := forestBegin(t, s, 1, h.CommitID)
	address := [20]byte{1}
	id := high + 1
	forestID(t, u, address, id)
	forestSet(t, u, id, wide)
	forestCommit(t, u)
	root, _ := wide.Hash()
	c, err := OpenCachedTrie(s, id, root, DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.id != id || c.cache.trieID != id {
		t.Fatal("cache discarded high namespace bits")
	}
	b, err := c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := b.Get([]byte{0x12})
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{2}, 64)) {
		t.Fatal("cache wrong namespace", got, err)
	}
	if err = b.Update([]byte{0x12}, bytes.Repeat([]byte{3}, 64)); err != nil {
		t.Fatal(err)
	}
	h, _, err = b.Commit()
	if err != nil || h.NextTrieID != id+1 {
		t.Fatal("cached managed commit", h, err)
	}
	forestProof(t, s, 2, 7, low, 1)
	forestProof(t, s, 2, id, forestTree(t, 3), 3)
	forestMapping(t, s, 2, address, id, true)
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
	u = forestBegin(t, s, 3, h.CommitID)
	if err = u.DeleteStorageTrie(address); err != nil {
		t.Fatal(err)
	}
	h, _ = forestCommit(t, u)
	// Even a fresh cache handle cannot publish a retired namespace through the
	// generic commit path. Historical explicit-ID reads remain valid.
	root, _ = forestTree(t, 3).Hash()
	c, err = OpenCachedTrie(s, id, root, DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err = c.BeginBlock(4)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("cache published retired storage", err)
	}
	if err = b.Abort(); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Head(); got != h {
		t.Fatal("retired cache changed head", got)
	}
}

func TestForestRejectsV5ManifestAndMalformedLifecycle(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "forest-format")
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, cfg, true, false)
	u := forestBegin(t, s, 0, [32]byte{})
	forestSet(t, u, 0, forestTree(t, 1))
	forestCommit(t, u)
	address := [20]byte{1}
	if err := s.write([]entry{{metaColumn, lifecycleKey(address, 0), []byte{1, 0, 0, 0, 0, 0, 0}}}); err != nil {
		t.Fatal(err)
	}
	v, _ := s.ViewAt(0)
	if _, _, err := v.StorageTrieID(address); !errors.Is(err, ErrCorrupt) {
		t.Fatal("storage alias to account accepted", err)
	}
	v.Close()
	snap := s.db.snapshot()
	raw, err := snap.get(metaColumn, manifestKey)
	snap.close()
	if err != nil {
		t.Fatal(err)
	}
	old := bytes.Replace(raw, []byte(StoreFormat), []byte("hybrid-mpt-dualref/v5"), 1)
	if bytes.Equal(old, raw) {
		t.Fatal("manifest replacement")
	}
	if err = s.write([]entry{{metaColumn, manifestKey, old}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if reopened, err := Open(path, cfg, false, true); !errors.Is(err, ErrCorrupt) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatal("v5 namespace layout accepted", err)
	}
	// Opening an incompatible store must not rewrite its manifest.
	engine, err := openEngine(path, false, true)
	if err != nil {
		t.Fatal(err)
	}
	snap = engine.snapshot()
	got, err := snap.get(metaColumn, manifestKey)
	if err != nil || !bytes.Equal(got, old) {
		t.Fatal("old manifest overwritten", err)
	}
	snap.close()
	engine.close()
}
