package hybridtrie

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type deltaRun struct {
	roots                     []mpt.Hash
	versions                  [][][]byte
	prefixBytes, deltaBytes   uint64
	deltaRecords, baseReads   uint64
	prefixColumnBytes         uint64
	storedDeltas, maxDepth    int
	hashRecords, prefixRecord uint64
}

// runDeltaWorkload applies the same random updates (inserts, overwrites and
// deletes) through a cache that evicts almost everything, so nodes move between
// Prefix and Hash forms, and returns what was written and read back.
func runDeltaWorkload(t *testing.T, interval uint8, hashInit bool, blocks int) deltaRun {
	t.Helper()
	cfg := DefaultConfig("raw-bytes/v1", "delta-workload")
	cfg.DeltaInterval = interval
	opt := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opt
	db, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(err)
	defer db.Close()
	tree, err := mpt.NewEmpty(mpt.NewMemoryStore())
	check(err)
	const keyCount = 400
	keyOf := func(i int) []byte { return []byte{byte(i * 7), byte(i * 13 >> 3), byte(i), byte(i >> 8)} }
	values := make([][]byte, keyCount)
	snapshotValues := func() [][]byte { return append([][]byte(nil), values...) }
	for i := 0; i < keyCount; i++ {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 40)
		check(tree.Update(keyOf(i), values[i]))
	}
	var head Head
	if hashInit {
		head, _, err = db.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{Tree: tree}})
	} else {
		head, _, err = db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	}
	check(err)
	root, err := tree.Hash()
	check(err)
	var out deltaRun
	out.roots = []mpt.Hash{root}
	out.versions = [][][]byte{snapshotValues()}
	cache := DefaultPrefixBoundaryForestCacheConfig()
	allocation, err := BoundaryCacheAllocation(cache.Boundary)
	check(err)
	cache.Boundary.NodeBudget = ClassBudget{TotalBytes: allocation.Boundary.SlotBytes}
	cache.Boundary.PolicyIntervalBlocks = 1
	forest, err := OpenCachedForest(db, cache)
	check(err)
	defer forest.Close()
	seed := uint32(99)
	next := func(n int) int { seed = seed*1664525 + 1013904223; return int(seed>>8) % n }
	for h := uint64(1); h <= uint64(blocks); h++ {
		block, err := forest.BeginBlock(h, head.CommitID)
		check(err)
		reader, err := block.TrieReader(0, root)
		check(err)
		working, err := mpt.Open(root, reader)
		check(err)
		for n := 0; n < 4; n++ {
			i := next(keyCount)
			switch {
			case next(7) == 0 && values[i] != nil:
				values[i] = nil
				check(working.Delete(keyOf(i)))
			default:
				values[i] = bytes.Repeat([]byte{byte(h), byte(n), 1}, 14)
				check(working.Update(keyOf(i), values[i]))
			}
		}
		check(block.SetTrie(0, working))
		var st CommitStats
		head, st, err = block.Commit()
		check(err)
		out.prefixBytes += st.PrefixBytes
		out.deltaBytes += st.DeltaBytes
		out.deltaRecords += st.DeltaRecords
		out.baseReads += st.DeltaBaseReads
		out.hashRecords += st.HashRecords
		out.prefixRecord += st.PrefixRecords
		root, err = working.Hash()
		check(err)
		out.roots = append(out.roots, root)
		out.versions = append(out.versions, snapshotValues())
	}
	// Every historical value must prove against the root of its own height.
	for h, root := range out.roots {
		view, err := db.ViewAt(uint64(h))
		check(err)
		for i := 0; i < keyCount; i += 3 {
			proof, err := view.Prove(0, root, keyOf(i))
			check(err)
			got, found, err := mpt.VerifyProof(root, keyOf(i), proof)
			check(err)
			want := out.versions[h][i]
			if want == nil && found || want != nil && (!found || !bytes.Equal(got, want)) {
				t.Fatalf("interval %d hashInit %v height %d key %d: found=%v", interval, hashInit, h, i, found)
			}
		}
		check(view.Close())
	}
	snap := db.db.snapshot().(*kvSnapshot)
	check(snap.scan(prefixColumn, nil, nil, func(k, v []byte) error {
		out.prefixColumnBytes += uint64(len(k) + len(v))
		if isDelta(v) {
			out.storedDeltas++
			if d := int(storedDepth(v)); d > out.maxDepth {
				out.maxDepth = d
			}
		}
		return nil
	}))
	snap.close()
	return out
}

func TestDeltaVersionsReadBackAndShrink(t *testing.T) {
	for _, hashInit := range []bool{false, true} {
		base := runDeltaWorkload(t, 0, hashInit, 40)
		if base.storedDeltas != 0 || base.deltaRecords != 0 {
			t.Fatal("deltas written with the feature off")
		}
		for _, k := range []uint8{1, 3, 8} {
			got := runDeltaWorkload(t, k, hashInit, 40)
			for h := range base.roots {
				if got.roots[h] != base.roots[h] {
					t.Fatalf("hashInit %v k %d: root differs at height %d", hashInit, k, h)
				}
			}
			if got.deltaRecords == 0 || got.storedDeltas == 0 {
				t.Fatalf("hashInit %v k %d: no deltas written (%+v)", hashInit, k, got)
			}
			if got.maxDepth > int(k) {
				t.Fatalf("hashInit %v k %d: chain depth %d exceeds the interval", hashInit, k, got.maxDepth)
			}
			if got.prefixColumnBytes >= base.prefixColumnBytes {
				t.Fatalf("hashInit %v k %d: deltas did not shrink the Prefix column (%d >= %d)", hashInit, k, got.prefixColumnBytes, base.prefixColumnBytes)
			}
			t.Logf("hashInit=%v k=%d: prefix column %d -> %d B, %d deltas (max depth %d), %d base reads for %d Prefix records",
				hashInit, k, base.prefixColumnBytes, got.prefixColumnBytes, got.storedDeltas, got.maxDepth, got.baseReads, got.prefixRecord)
		}
	}
}

// The plain Store commit path (no cache forest, every child Prefix) writes
// deltas too, reads the old version through a non-local reader, and survives
// deletes and re-created paths.
func TestDeltaPlainStoreCommit(t *testing.T) {
	for _, hashInit := range []bool{false, true} {
		cfg := DefaultConfig("raw-bytes/v1", "delta-plain")
		cfg.DeltaInterval = 5
		opt := kvstore.DefaultOptions(kvstore.LevelDB)
		cfg.Storage = &opt
		db, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		if err != nil {
			t.Fatal(err)
		}
		tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
		const n = 200
		keyOf := func(i int) []byte { return []byte{byte(i * 11), byte(i), byte(i >> 8)} }
		values := make([][]byte, n)
		for i := 0; i < n; i++ {
			values[i] = bytes.Repeat([]byte{byte(i + 1)}, 40)
			if err := tree.Update(keyOf(i), values[i]); err != nil {
				t.Fatal(err)
			}
		}
		var head Head
		if hashInit {
			head, _, err = db.CommitInitialHash(0, [32]byte{}, []TrieUpdate{{Tree: tree}})
		} else {
			head, _, err = db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
		}
		if err != nil {
			t.Fatal(err)
		}
		roots := []mpt.Hash{}
		root, _ := tree.Hash()
		roots = append(roots, root)
		history := [][][]byte{append([][]byte(nil), values...)}
		var deltas uint64
		for h := uint64(1); h <= 40; h++ {
			for j := 0; j < 3; j++ {
				i := int(h*13+uint64(j)*29) % n
				switch {
				case h%5 == 0 && j == 0:
					values[i] = nil
					if err := tree.Delete(keyOf(i)); err != nil {
						t.Fatal(err)
					}
				default:
					values[i] = bytes.Repeat([]byte{byte(h), byte(j)}, 20)
					if err := tree.Update(keyOf(i), values[i]); err != nil {
						t.Fatal(err)
					}
				}
			}
			var st CommitStats
			head, st, err = db.Commit(h, head.CommitID, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
			if err != nil {
				t.Fatal(err)
			}
			deltas += st.DeltaRecords
			root, _ = tree.Hash()
			roots = append(roots, root)
			history = append(history, append([][]byte(nil), values...))
		}
		if deltas == 0 {
			t.Fatalf("hashInit %v: no deltas on the plain commit path", hashInit)
		}
		for h, root := range roots {
			view, err := db.ViewAt(uint64(h))
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < n; i++ {
				proof, err := view.Prove(0, root, keyOf(i))
				if err != nil {
					t.Fatal(err)
				}
				got, found, err := mpt.VerifyProof(root, keyOf(i), proof)
				want := history[h][i]
				if err != nil || want == nil && found || want != nil && (!found || !bytes.Equal(got, want)) {
					t.Fatalf("hashInit %v height %d key %d: %v", hashInit, h, i, err)
				}
			}
			view.Close()
		}
		db.Close()
	}
}

// A delta whose depth does not follow its chain, or whose base is gone, is
// corrupt rather than silently decoded against the wrong version.
func TestDeltaChainCorruptionIsDetected(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "delta-corrupt")
	cfg.DeltaInterval = 6
	opt := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opt
	db, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	keyOf := func(i int) []byte { return []byte{byte(i * 17), byte(i)} }
	for i := 0; i < 60; i++ {
		if err := tree.Update(keyOf(i), bytes.Repeat([]byte{byte(i + 1)}, 40)); err != nil {
			t.Fatal(err)
		}
	}
	head, _, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	for h := uint64(1); h <= 12; h++ {
		if err := tree.Update(keyOf(int(h)), bytes.Repeat([]byte{byte(h), 7}, 20)); err != nil {
			t.Fatal(err)
		}
		if head, _, err = db.Commit(h, head.CommitID, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}}); err != nil {
			t.Fatal(err)
		}
	}
	snap := db.db.snapshot()
	defer snap.close()
	var foundKey NodeKey
	var foundRaw []byte
	if err := snap.(*kvSnapshot).scan(prefixColumn, nil, nil, func(k, v []byte) error {
		if isDelta(v) && storedDepth(v) >= 2 && foundRaw == nil {
			nk, e := DecodeNodeKey(k)
			if e != nil {
				return e
			}
			foundKey, foundRaw = nk, append([]byte(nil), v...)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if foundRaw == nil {
		t.Fatal("no delta of depth two or more")
	}
	if _, err := resolveDelta(snap, foundKey, foundRaw, nil); err != nil {
		t.Fatal("valid chain rejected", err)
	}
	for _, depth := range []byte{foundRaw[recordHeaderBytes] + 1, foundRaw[recordHeaderBytes] - 1, 0} {
		bad := append([]byte(nil), foundRaw...)
		bad[recordHeaderBytes] = depth
		if _, err := resolveDelta(snap, foundKey, bad, nil); err == nil {
			t.Fatal("wrong depth accepted", depth)
		}
	}
	// A base that is not the previous version of this path is not found.
	moved := foundKey
	moved.Path = append(append([]byte(nil), moved.Path...), 0)
	if _, err := resolveDelta(snap, moved, foundRaw, nil); err == nil {
		t.Fatal("base resolved for a different path")
	}
	// The read budget is charged for every base.
	var charged int
	if _, err := resolveDelta(snap, foundKey, foundRaw, func(n int) error { charged += n; return nil }); err != nil || charged == 0 {
		t.Fatal("base reads not charged", charged, err)
	}
}

// A commit that creates a node asks for the previous version of a path the old
// tree does not have; that is "no previous version", never corruption.
func TestOldVersionOfAbsentPathIsNotFound(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "old-version")
	opt := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opt
	db, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for _, k := range []byte{0x00, 0x10, 0x20} {
		if err := tree.Update([]byte{k}, bytes.Repeat([]byte{k + 1}, 40)); err != nil {
			t.Fatal(err)
		}
	}
	head, _, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Hash()
	snap := db.db.snapshot()
	defer snap.close()
	var cursor projectionCursor
	defer cursor.close()
	reader := &projectionReader{trieReader: &trieReader{snapshot: snap, cfg: db.cfg, id: 0, height: head.Height, root: root, rootSource: Prefix}, routes: map[string]projectionRoute{}, cursor: &cursor}
	if n, err := reader.oldVersion(nil); err != nil || n.record.Kind != mpt.CanonicalBranch || n.route.source != Prefix {
		t.Fatal("root", n.route, err)
	}
	if n, err := reader.oldVersion([]byte{0}); err != nil || n.record.Kind != mpt.CanonicalLeaf {
		t.Fatal("leaf", err)
	}
	for _, path := range [][]byte{{5}, {0, 1}, {1, 0, 0}} {
		if _, err := reader.oldVersion(path); !errors.Is(err, ErrNotFound) {
			t.Fatalf("path %v: want ErrNotFound, got %v", path, err)
		}
	}
}
