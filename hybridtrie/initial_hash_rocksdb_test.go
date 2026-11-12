//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func initialHashTree(t *testing.T, n int) *mpt.Trie {
	t.Helper()
	tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < n; i++ {
		if e := tr.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i + 1)}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	return tr
}
func countInitialFamily(t *testing.T, s *Store, cf column) int {
	t.Helper()
	n := 0
	if e := s.db.(*rocksEngine).exportFamily(cf, func(_, _ []byte) error { n++; return nil }); e != nil {
		t.Fatal(e)
	}
	return n
}
func initialHashProof(t *testing.T, s *Store, h, id uint64, tr *mpt.Trie) ProofStats {
	t.Helper()
	v, e := s.ViewAt(h)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	root, _ := tr.Hash()
	var last ProofStats
	for _, key := range [][]byte{{0}, {0x70}, {0xf0}, {0x12}} {
		proof, stats, e := v.ProveWithStats(id, root, key)
		if e != nil {
			t.Fatal(h, id, e)
		}
		got, exists, e := mpt.VerifyProof(root, key, proof)
		want, wantErr := tr.Get(key)
		if wantErr != nil {
			t.Fatal("reference read", wantErr)
		}
		if e != nil || exists != (len(want) > 0) || !bytes.Equal(got, want) {
			t.Fatal("proof", h, id, e)
		}
		last = stats
	}
	return last
}
func TestInitialHashForestAtomicColdAndMixedHistory(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "hash-initial")
	cfg.InitialHeight = 83
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, cfg, true, false)
	defer func() { s.Close() }()
	f, e := OpenCachedForest(s, DefaultPrefixBoundaryForestCacheConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer func() { f.Close() }()
	b, e := f.BeginBlock(83, [32]byte{})
	if e != nil {
		t.Fatal(e)
	}
	ids := []uint64{0}
	for i := byte(1); i <= 2; i++ {
		id, err := b.CreateStorageTrie([20]byte{i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	original := initialHashTree(t, 8)
	root, _ := original.Hash()
	for _, id := range ids {
		if e = b.SetTrie(id, original); e != nil {
			t.Fatal(e)
		}
	}
	writes := 0
	write := s.write
	s.write = func(entries []entry) error { writes++; return write(entries) }
	head, stats, e := b.CommitInitialHash()
	if e != nil {
		t.Fatal(e)
	}
	if writes != 1 || !stats.InitialHashCommit || stats.PrefixRecords != 0 || stats.HashRecords == 0 || stats.HashRecords != uint64(countInitialFamily(t, s, hashColumn)) || stats.ClosureBackfillRecords != 0 || countInitialFamily(t, s, prefixColumn) != 0 {
		t.Fatal("not one pure Hash commit", writes, stats)
	}
	// Identical account/storage forests share their content-addressed node records.
	unique := map[mpt.Hash]bool{}
	empty, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	original.DiffLogicalNodes(empty, 1000, func(c mpt.LogicalChange) error {
		if len(c.Path) == 0 || len(c.RLP) >= 32 {
			unique[c.Hash] = true
		}
		return nil
	})
	if stats.HashRecords != uint64(len(unique)) || stats.LifecycleRecords != 2 || stats.OwnerRecords != 2 {
		t.Fatal("dedup/metadata", stats, len(unique))
	}
	cs, _ := f.Stats()
	if cs.Boundary.NodeEntries != 0 || cs.Boundary.Admissions != 0 || cs.Boundary.HeatUpdates != 0 || cs.Boundary.HeatRotations != 0 || f.boundary.heat.Started {
		t.Fatal("initialization prewarmed execution", cs.Boundary)
	}
	type saved struct {
		h, id uint64
		tree  *mpt.Trie
	}
	var history []saved
	trees := map[uint64]*mpt.Trie{}
	refs := map[uint64]*mpt.Trie{}
	for _, id := range ids {
		trees[id] = original.Copy()
		refs[id] = original.Copy()
		history = append(history, saved{83, id, original.Copy()})
		snap := s.db.snapshot()
		loc, err := readRootLocation(snap, id, 83)
		snap.close()
		if err != nil || loc.Hash != root || loc.Source != Hash || loc.Height != 83 {
			t.Fatal("Hash root directory", loc, err)
		}
		ps := initialHashProof(t, s, 83, id, original)
		if ps.PrefixReads != 0 || ps.HashReads == 0 || ps.PrefixIteratorCreates != 0 {
			t.Fatal("Hash cold proof used Prefix", ps)
		}
	}
	// Empty blocks retain the root's declared Hash source.
	b, e = f.BeginBlock(84, head.CommitID)
	if e != nil {
		t.Fatal(e)
	}
	head, _, e = b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	for step := 0; step < 5; step++ {
		h := uint64(85 + step)
		b, e = f.BeginBlock(h, head.CommitID)
		if e != nil {
			t.Fatal(e)
		}
		for _, id := range ids {
			oldRoot, _ := trees[id].Hash()
			reader, err := b.TrieReader(id, oldRoot)
			if err != nil {
				t.Fatal(err)
			}
			tr, err := mpt.Open(oldRoot, reader)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tr.Get([]byte{0}); err != nil {
				t.Fatal(err)
			}
			if id == 0 {
				for _, target := range []*mpt.Trie{tr, refs[id]} {
					switch step {
					case 0:
						target.Update([]byte{0}, bytes.Repeat([]byte{99}, 64))
						target.Update([]byte{0}, bytes.Repeat([]byte{1}, 64)) // net zero, retain Hash source
					case 1:
						target.Update([]byte{0}, bytes.Repeat([]byte{99}, 64))
					case 2:
						target.Update([]byte{0}, bytes.Repeat([]byte{1}, 64))
						target.Update([]byte{0x10}, bytes.Repeat([]byte{88}, 64))
					case 3:
						target.Update([]byte{0x10}, bytes.Repeat([]byte{2}, 64)) // return to original root across blocks
					case 4:
						target.Delete([]byte{0x20})
					}
				}
			}
			trees[id] = tr
			if e = b.SetTrie(id, tr); e != nil {
				t.Fatal(e)
			}
		}
		head, stats, e = b.Commit()
		if e != nil {
			t.Fatal("mixed commit", h, e)
		}
		if stats.InitialHashCommit || stats.HashRecords != 0 {
			t.Fatal("unchanged initial Hash subtree was recopied", h, stats)
		}
		if step == 1 && stats.RetainedHashEdges == 0 {
			t.Fatal("first update lost Hash edges")
		}
		assertBoundary(t, f)
		if step == 0 {
			// A materialized Hash root records Hash edges, without invented heights.
			n := f.boundary.current.find(position(2, nil))
			if n < 0 || f.boundary.current.Nodes[n].Version.Source != Hash || f.boundary.current.Nodes[n].Version.Known {
				t.Fatal("Hash N root provenance")
			}
			if f.boundary.current.Nodes[n].Edges[7].Kind != boundaryHash {
				t.Fatal("Hash edge invented Prefix height")
			}
		}
		for _, id := range ids {
			snap := s.db.snapshot()
			loc, err := readRootLocation(snap, id, h)
			snap.close()
			if err != nil {
				t.Fatal(err)
			}
			want := Hash
			if id == 0 && step > 0 {
				want = Prefix
			}
			if loc.Source != want {
				t.Fatal("root source lost", h, id, loc.Source, want)
			}
			history = append(history, saved{h, id, refs[id].Copy()})
		}
	}
	// Clear the in-memory forest and reopen the database read-only.
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTestStore(t, path, cfg, false, true)
	for _, v := range history {
		initialHashProof(t, s, v.h, v.id, v.tree)
	}
	t.Logf("one initial write, %d unique Hash records, %d historical namespace states verified", len(unique), len(history))
}

func TestInitialHashInlineBoundaryAndShortRoot(t *testing.T) {
	seen31, seen32, shortRoot := false, false, false
	for _, size := range []int{1, 28, 29, 64} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "hash-inline")
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			for _, key := range []byte{0x10, 0x20} {
				if e := tr.Update([]byte{key}, bytes.Repeat([]byte{1}, size)); e != nil {
					t.Fatal(e)
				}
			}
			root, raw, _, _ := tr.CanonicalNodeAtPath(nil)
			if len(raw) < 32 {
				shortRoot = true
			}
			_, child, _, _ := tr.CanonicalNodeAtPath([]byte{1})
			seen31 = seen31 || len(child) == 31
			seen32 = seen32 || len(child) == 32
			h, stats, e := s.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{0, tr, 0xffff}})
			if e != nil {
				t.Fatal(e)
			}
			if stats.PrefixRecords != 0 || !h.Initialized || countInitialFamily(t, s, prefixColumn) != 0 {
				t.Fatal(stats)
			}
			snap := s.db.snapshot()
			stored, e := snap.get(hashColumn, root[:])
			snap.close()
			if e != nil || !bytes.Equal(stored, raw) {
				t.Fatal("short root missing", e)
			}
			v, e := s.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			defer v.Close()
			for _, key := range []byte{0x10, 0x20, 0x30} {
				proof, _, e := v.ProveWithStats(0, root, []byte{key})
				if e != nil {
					t.Fatal(e)
				}
				got, exists, e := mpt.VerifyProof(root, []byte{key}, proof)
				want, _ := tr.Get([]byte{key})
				if e != nil || exists != (len(want) > 0) || !bytes.Equal(got, want) {
					t.Fatal(e)
				}
			}
		})
	}
	if !seen31 || !seen32 || !shortRoot {
		t.Fatal("threshold coverage", seen31, seen32, shortRoot)
	}
}

func TestInitialHashLimitsUnknownWriteAndRepeat(t *testing.T) {
	for _, outcome := range []string{"limit", "write-before", "write-after", "success"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "hash-fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			defer func() { s.Close() }()
			f, e := OpenCachedForest(s, DefaultPrefixBoundaryForestCacheConfig())
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			tr := initialHashTree(t, 16)
			if outcome == "limit" {
				if e = tr.Update([]byte{0}, bytes.Repeat([]byte{1}, 8192)); e != nil {
					t.Fatal(e)
				}
			}
			b, e := f.BeginBlock(0, [32]byte{})
			if e != nil {
				t.Fatal(e)
			}
			if e = b.SetTrie(0, tr); e != nil {
				t.Fatal(e)
			}
			write := s.write
			calls := 0
			s.write = func(entries []entry) error {
				calls++
				if outcome == "write-before" {
					return errors.New("injected before Sync")
				}
				if err := write(entries); err != nil {
					return err
				}
				if outcome == "write-after" {
					return errors.New("injected after Sync")
				}
				return nil
			}
			if outcome == "limit" {
				s.cfg.MaxBatchBytes = 4096
			}
			_, stats, e := b.CommitInitialHash()
			if outcome == "limit" {
				if !errors.Is(e, ErrLimit) || calls != 0 || s.head.Initialized || countInitialFamily(t, s, hashColumn) != 0 {
					t.Fatal("limit published partial initialization", e, calls)
				}
				s.cfg.MaxBatchBytes = cfg.MaxBatchBytes
				if _, _, e = b.CommitInitialHash(); e != nil {
					t.Fatal("valid retry", e)
				}
			} else if outcome == "success" {
				if e != nil || !stats.InitialHashCommit || calls != 1 {
					t.Fatal(stats, e, calls)
				}
				before := calls
				if _, _, e = s.CommitInitialHash(1, s.head.CommitID, []TrieUpdate{{0, tr, 0}}); !errors.Is(e, ErrConflict) || calls != before {
					t.Fatal("initialization repeated", e)
				}
			} else {
				if !errors.Is(e, ErrUncertain) || !f.boundary.uncertain || calls != 1 {
					t.Fatal("write did not fail stop", e, calls)
				}
				if _, e = s.Head(); !errors.Is(e, ErrUncertain) {
					t.Fatal(e)
				}
				f.Close()
				s.Close()
				s = openTestStore(t, path, cfg, false, false)
				head, e := s.Head()
				if e != nil || head.Initialized != (outcome == "write-after") {
					t.Fatal("reopen publication", head, e)
				}
				if head.Initialized {
					initialHashProof(t, s, 0, 0, tr)
				} else if countInitialFamily(t, s, hashColumn) != 0 {
					t.Fatal("partial data")
				}
			}
		})
	}
}

func TestInitialHashUnchangedSourceIgnoresOrphanPrefix(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "hash-orphan")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	tr := initialHashTree(t, 4)
	head, _, e := s.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{0, tr, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	for h := uint64(1); h <= 4; h++ {
		switch h {
		case 1:
			tr.Update([]byte{0}, bytes.Repeat([]byte{99}, 64))
		case 2:
			tr.Update([]byte{0}, bytes.Repeat([]byte{1}, 64))
		case 3:
			tr.Update([]byte{0x10}, bytes.Repeat([]byte{88}, 64))
		case 4:
			// The old root declares Hash for child zero. Its older Prefix copy is now
			// an orphan; corruption there must not cause a read or implicit promotion.
			key, _ := EncodeNodeKey(NodeKey{TrieID: 0, Path: []byte{0}, Height: 2})
			if e = s.db.write([]entry{{prefixColumn, key, []byte{0}}}); e != nil {
				t.Fatal(e)
			}
			tr.Update([]byte{0x20}, bytes.Repeat([]byte{77}, 64))
		}
		mask := uint16(0xffff)
		if h == 3 {
			mask = 0
		}
		var stats CommitStats
		head, stats, e = s.Commit(h, head.CommitID, []TrieUpdate{{0, tr, mask}})
		if e != nil {
			t.Fatal(h, e)
		}
		if h == 4 {
			snap := s.db.snapshot()
			rec, err := readPrefix(snap, NodeKey{Height: h})
			snap.close()
			if err != nil || rec.Children[0].Kind != Hash || stats.RetainedHashEdges == 0 {
				t.Fatal("orphan promoted", rec.Children[0], stats, err)
			}
		}
	}
	initialHashProof(t, s, 4, 0, tr)
}

func TestInitialHashMixedReferenceBudgetIsFinalSize(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "hash-budget")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	tr := initialHashTree(t, 16)
	head, _, e := s.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{0, tr, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	original := tr.Copy()
	tr.Update([]byte{0}, bytes.Repeat([]byte{99}, 64))
	calls := 0
	write := s.write
	s.write = func(entries []entry) error { calls++; return write(entries) }
	s.cfg.MaxBatchBytes = 600 // provisional BP records fit; final 15 BH digests do not
	if _, _, e = s.Commit(1, head.CommitID, []TrieUpdate{{0, tr, 0xffff}}); !errors.Is(e, ErrLimit) || calls != 0 || s.head != head {
		t.Fatal("reference growth bypassed budget", e, calls)
	}
	initialHashProof(t, s, 0, 0, original)
	s.cfg.MaxBatchBytes = cfg.MaxBatchBytes
	if _, _, e = s.Commit(1, head.CommitID, []TrieUpdate{{0, tr, 0xffff}}); e != nil {
		t.Fatal("valid retry", e)
	}
}

func TestInitialHashNativePresetValidationStillApplies(t *testing.T) {
	cfg := DefaultConfig("ethereum-mpt", "hash-native-limit")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, DefaultEthereumPrefixBoundaryForestCacheConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := f.BeginBlock(0, [32]byte{})
	if e != nil {
		t.Fatal(e)
	}
	// The Ethereum cache preset requires 32-byte secure keys. A generic MPT may
	// accept this short key, but initialization must not bypass that contract.
	tr := initialHashTree(t, 1)
	if e = b.SetTrie(0, tr); e != nil {
		t.Fatal(e)
	}
	if _, _, e = b.CommitInitialHash(); !errors.Is(e, ErrCorrupt) {
		t.Fatal("incompatible native snapshot published", e)
	}
	if s.head.Initialized || countInitialFamily(t, s, hashColumn) != 0 {
		t.Fatal("invalid native data durable")
	}
	b.Abort()
}

type initialRootFaultSnapshot struct {
	diskSnapshot
	hashValue   []byte
	hashErr     error
	rootTag     bool
	prefixReads int
}

func (s *initialRootFaultSnapshot) get(cf column, key []byte) ([]byte, error) {
	if cf == hashColumn {
		if s.hashErr != nil {
			return nil, s.hashErr
		}
		if s.hashValue != nil {
			return s.hashValue, nil
		}
	}
	return s.diskSnapshot.get(cf, key)
}
func (s *initialRootFaultSnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	if cf == prefixColumn {
		s.prefixReads++
	}
	k, v, e := s.diskSnapshot.prev(cf, key)
	if s.rootTag && cf == metaColumn && len(k) > 0 && k[0] == 'R' && e == nil {
		v = append([]byte(nil), v...)
		v[0] = byte(Prefix)
	}
	return k, v, e
}
func TestInitialHashDeclaredRootDoesNotFallback(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "hash-source-fault")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	tr := initialHashTree(t, 4)
	root, _ := tr.Hash()
	if _, _, e := s.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{0, tr, 0}}); e != nil {
		t.Fatal(e)
	}
	for _, fault := range []string{"missing", "wrong", "tag"} {
		t.Run(fault, func(t *testing.T) {
			v, e := s.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			defer v.Close()
			wrapped := &initialRootFaultSnapshot{diskSnapshot: v.snapshot}
			v.snapshot = wrapped
			switch fault {
			case "missing":
				wrapped.hashErr = ErrNotFound
			case "wrong":
				other := initialHashTree(t, 5)
				_, wrapped.hashValue, _, _ = other.CanonicalNodeAtPath(nil)
			case "tag":
				wrapped.rootTag = true
			}
			if _, _, e = v.ProveWithStats(0, root, []byte{0}); e == nil {
				t.Fatal("invalid Hash source accepted")
			}
			if wrapped.prefixReads != 0 {
				t.Fatal("declared Hash source fell back to Prefix")
			}
		})
	}
}
