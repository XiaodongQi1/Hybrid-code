package hybridtrie

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// The root and one branch fill the cache. A hotter sibling displaces the
// updated branch without conversion. Only a later modification of its child
// changes that child to Hash; its untouched sibling retains the initial Prefix key.
func TestPrefixSnapshotCapacityPlacement(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "victim-placement")
	opt := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opt
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, cfg, true, false)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(err)
	defer func() { db.Close() }()
	tree, err := mpt.NewEmpty(mpt.NewMemoryStore())
	check(err)
	keys := []byte{0x10, 0x11, 0x20, 0x30}
	put := func(k, value byte) { check(tree.Update([]byte{k}, bytes.Repeat([]byte{value}, 48))) }
	for _, k := range keys {
		put(k, k)
	}
	head, stats, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	check(err)
	if stats.HashRecords != 0 {
		t.Fatal(stats)
	}
	c := DefaultPrefixBoundaryForestCacheConfig()
	c.Boundary.HashUpdatedVictims = true
	c.Boundary.PolicyIntervalBlocks = 2
	sizes, err := BoundaryCacheAllocation(c.Boundary)
	check(err)
	c.Boundary.NodeBudget = ClassBudget{TotalBytes: 2 * sizes.Boundary.SlotBytes}
	f, err := OpenCachedForest(db, c)
	check(err)
	root, err := tree.Hash()
	check(err)
	roots := []mpt.Hash{root}
	for h := uint64(1); h <= 5; h++ {
		b, err := f.BeginBlock(h, head.CommitID)
		check(err)
		if h == 2 {
			reader, err := b.TrieReader(0, root)
			check(err)
			working, err := mpt.Open(root, reader)
			check(err)
			for i := 0; i < 20; i++ {
				_, err = working.Get([]byte{0x20})
				check(err)
			}
		}
		if h == 5 {
			before := f.boundary.stats.HashGets
			reader, err := b.TrieReader(0, root)
			check(err)
			live, err := mpt.Open(root, reader)
			check(err)
			got, err := live.Get([]byte{0x10})
			check(err)
			if !bytes.Equal(got, bytes.Repeat([]byte{4}, 48)) || f.boundary.stats.HashGets <= before {
				t.Fatal("changed cold child did not use Hash", got)
			}
		}
		if h == 3 {
			// The live parent must retain the evicted child hash and exact Prefix height.
			parent := f.boundary.current.find(position(0, nil))
			edge := f.boundary.current.Nodes[parent].Edges[1]
			if edge.Kind != boundaryPrefix || edge.height() != 2 {
				t.Fatal("lost Prefix boundary", edge)
			}
			if _, ok := f.boundary.current.Nodes[parent].Image.ChildHash(1); !ok {
				t.Fatal("lost child hash")
			}
			before := f.boundary.stats.PrefixExactGets
			reader, err := b.TrieReader(0, root)
			check(err)
			live, err := mpt.Open(root, reader)
			check(err)
			got, err := live.Get([]byte{0x10})
			check(err)
			if !bytes.Equal(got, bytes.Repeat([]byte{2}, 48)) {
				t.Fatal("stale live route", got)
			}
			if f.boundary.stats.PrefixExactGets <= before {
				t.Fatal("evicted node did not use exact Prefix Get")
			}
		}
		if h <= 2 || h == 4 {
			put(0x10, byte(h))
		}
		if h >= 2 {
			put(0x20, byte(h))
		}
		check(b.SetTrie(0, tree))
		head, stats, err = b.Commit()
		check(err)
		if h == 2 && (f.boundary.stats.PolicyEvictions != 1 || stats.HashRecords != 0 || stats.ClosureBackfillRecords != 0) {
			t.Fatalf("expected Prefix eviction without Hash writes or closure backfill: %+v evictions=%d", stats, f.boundary.stats.PolicyEvictions)
		}
		if h == 4 {
			if stats.HashRecords != 1 || stats.ClosureBackfillRecords != 0 {
				t.Fatal("expected only modified cold leaf by Hash", stats)
			}
		} else if stats.HashRecords != 0 {
			t.Fatal(h, stats)
		}
		root, err = tree.Hash()
		check(err)
		roots = append(roots, root)
	}
	f.Close()
	check(db.Close())
	db, err = Open(path, cfg, false, true)
	check(err)
	disk := db.db.snapshot()
	defer disk.close()
	for _, p := range [][]byte{{1}, {1, 0}} {
		k, err := EncodeNodeKey(NodeKey{0, p, 2})
		check(err)
		_, err = disk.get(prefixColumn, k)
		check(err)
	}
	for _, p := range [][]byte{{1, 1}, {3}} {
		k, err := EncodeNodeKey(NodeKey{0, p, 2})
		check(err)
		if _, err = disk.get(prefixColumn, k); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unexpected Prefix write at %v: %v", p, err)
		}
	}
	key, err := EncodeNodeKey(NodeKey{0, []byte{1}, 4})
	check(err)
	raw, err := disk.get(prefixColumn, key)
	check(err)
	rec, err := DecodeRecord(raw)
	check(err)
	if rec.Children[0].Kind != Hash || rec.Children[1].Kind != Prefix {
		t.Fatal("expected mixed updated Hash / unchanged Prefix children", rec)
	}
	key, err = EncodeNodeKey(NodeKey{0, []byte{1, 0}, 4})
	check(err)
	if _, err = disk.get(prefixColumn, key); !errors.Is(err, ErrNotFound) {
		t.Fatal("duplicate Prefix copy of changed Hash child", err)
	}
	for h, root := range roots {
		v, err := db.ViewAt(uint64(h))
		check(err)
		for _, k := range keys {
			proof, err := v.Prove(0, root, []byte{k})
			check(err)
			value, found, err := mpt.VerifyProof(root, []byte{k}, proof)
			check(err)
			want := k
			if k == 0x10 && h >= 1 {
				want = byte(h)
				if h == 3 {
					want = 2
				}
				if h >= 4 {
					want = 4
				}
			}
			if k == 0x20 && h >= 2 {
				want = byte(h)
			}
			if !found || !bytes.Equal(value, bytes.Repeat([]byte{want}, 48)) {
				t.Fatal(h, k, value)
			}
		}
		check(v.Close())
	}
}

// A node modified below a nonresident parent switches to canonical Hash storage
// only when every non-inline child is already Hash-resolvable. A branch with an
// unchanged Prefix-only child keeps a mixed-reference Prefix record: its modified
// child is Hash, its untouched child remains Prefix, and no Hash copy of the
// untouched subtree is written (no closure backfill, no duplicate storage).
func TestColdBranchModificationKeepsMixedReferencesAndHistory(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "cold-branch-modification")
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
	keys := [][]byte{{0x10, 0x00}, {0x10, 0x10}, {0x11, 0x00}, {0x20, 0x00}}
	for i, key := range keys {
		check(tree.Update(key, bytes.Repeat([]byte{byte(i + 1)}, 48)))
	}
	head, _, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	check(err)
	root, err := tree.Hash()
	check(err)
	roots := []mpt.Hash{root}
	cache := DefaultPrefixBoundaryForestCacheConfig()
	allocation, err := BoundaryCacheAllocation(cache.Boundary)
	check(err)
	cache.Boundary.NodeBudget = ClassBudget{TotalBytes: allocation.Boundary.SlotBytes}
	forest, err := OpenCachedForest(db, cache)
	check(err)
	defer forest.Close()
	for height := uint64(1); height <= 3; height++ {
		block, err := forest.BeginBlock(height, head.CommitID)
		check(err)
		reader, err := block.TrieReader(0, root)
		check(err)
		working, err := mpt.Open(root, reader)
		check(err)
		_, err = working.Get(keys[0])
		check(err)
		if height == 2 {
			check(working.Update(keys[0], bytes.Repeat([]byte{9}, 48)))
		}
		if height == 3 {
			// A reverted update does not convert or duplicate additional records.
			check(working.Update(keys[1], bytes.Repeat([]byte{8}, 48)))
			check(working.Update(keys[1], bytes.Repeat([]byte{2}, 48)))
		}
		check(block.SetTrie(0, working))
		var stats CommitStats
		head, stats, err = block.Commit()
		check(err)
		if height == 2 {
			if stats.HashRecords != 1 || stats.PrefixRecords != 3 || stats.ClosureBackfillRecords != 0 || stats.ClosureBackfillBytes != 0 {
				t.Fatalf("only the modified leaf uses Hash; branches stay Prefix without backfill: %+v", stats)
			}
		} else if stats.HashRecords != 0 || stats.PrefixRecords != 0 {
			t.Fatalf("read/reverted update rewrote nodes: %+v", stats)
		}
		if forest.boundary.statsSnapshot().Boundary.NodeEntries != 1 {
			t.Fatal("fixture frontier")
		}
		root, err = working.Hash()
		check(err)
		roots = append(roots, root)
	}
	snapshot := db.db.snapshot()
	defer snapshot.close()
	key, err := EncodeNodeKey(NodeKey{0, []byte{1}, 2})
	check(err)
	raw, err := snapshot.get(prefixColumn, key)
	check(err)
	// The stored value may be a delta; compare the complete record.
	raw, err = normalizePrefix(snapshot, NodeKey{0, []byte{1}, 2}, raw, nil)
	check(err)
	record, err := DecodeRecord(raw)
	check(err)
	if record.Children[0].Kind != Prefix || record.Children[1].Kind != Prefix {
		t.Fatal("cold parent must reference its Prefix-written child", record)
	}
	key, err = EncodeNodeKey(NodeKey{0, []byte{1, 0}, 2})
	check(err)
	raw, err = snapshot.get(prefixColumn, key)
	check(err)
	raw, err = normalizePrefix(snapshot, NodeKey{0, []byte{1, 0}, 2}, raw, nil)
	check(err)
	record, err = DecodeRecord(raw)
	check(err)
	if record.Children[0].Kind != Hash || record.Children[1].Kind != Prefix {
		t.Fatal("modified child must be Hash while the untouched sibling keeps Prefix", record)
	}
	for _, path := range [][]byte{{1, 0, 0}, {1, 0, 1}, {1, 1}} {
		key, err := EncodeNodeKey(NodeKey{0, path, 2})
		check(err)
		if _, err = snapshot.get(prefixColumn, key); !errors.Is(err, ErrNotFound) {
			t.Fatal("unexpected rewritten/duplicate Prefix", path, err)
		}
	}
	for h, root := range roots {
		view, err := db.ViewAt(uint64(h))
		check(err)
		for i, key := range keys {
			proof, err := view.Prove(0, root, key)
			check(err)
			got, found, err := mpt.VerifyProof(root, key, proof)
			check(err)
			want := byte(i + 1)
			if h >= 2 && i == 0 {
				want = 9
			}
			if !found || !bytes.Equal(got, bytes.Repeat([]byte{want}, 48)) {
				t.Fatal("historical value", h, i, got)
			}
		}
		check(view.Close())
	}
}

// Randomized cold updates over many blocks never copy unchanged descendants to
// the Hash column, yet every historical value and proof still verifies.
func TestColdUpdatesNeverBackfillAndRemainVerifiable(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "cold-no-backfill")
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
	const keyCount = 300
	keyOf := func(i int) []byte { return []byte{byte(i * 7), byte(i * 13 >> 3), byte(i)} }
	values := make([][]byte, keyCount)
	versions := make([][][]byte, 0, 32)
	snapshotValues := func() [][]byte { return append([][]byte(nil), values...) }
	for i := 0; i < keyCount; i++ {
		values[i] = bytes.Repeat([]byte{byte(i)}, 40)
		check(tree.Update(keyOf(i), values[i]))
	}
	head, _, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	check(err)
	root, err := tree.Hash()
	check(err)
	roots := []mpt.Hash{root}
	versions = append(versions, snapshotValues())
	cache := DefaultPrefixBoundaryForestCacheConfig()
	allocation, err := BoundaryCacheAllocation(cache.Boundary)
	check(err)
	cache.Boundary.NodeBudget = ClassBudget{TotalBytes: allocation.Boundary.SlotBytes}
	cache.Boundary.PolicyIntervalBlocks = 1
	forest, err := OpenCachedForest(db, cache)
	check(err)
	defer forest.Close()
	seed := uint32(12345)
	next := func(n int) int { seed = seed*1664525 + 1013904223; return int(seed>>8) % n }
	var hashRecords, prefixRecords uint64
	for height := uint64(1); height <= 40; height++ {
		block, err := forest.BeginBlock(height, head.CommitID)
		check(err)
		reader, err := block.TrieReader(0, root)
		check(err)
		working, err := mpt.Open(root, reader)
		check(err)
		for n := 0; n < 3; n++ {
			i := next(keyCount)
			values[i] = bytes.Repeat([]byte{byte(height), byte(n)}, 20)
			check(working.Update(keyOf(i), values[i]))
		}
		check(block.SetTrie(0, working))
		var stats CommitStats
		head, stats, err = block.Commit()
		check(err)
		if stats.ClosureBackfillRecords != 0 || stats.ClosureBackfillBytes != 0 {
			t.Fatalf("height %d copied unchanged descendants to Hash: %+v", height, stats)
		}
		hashRecords += stats.HashRecords
		prefixRecords += stats.PrefixRecords
		root, err = working.Hash()
		check(err)
		roots = append(roots, root)
		versions = append(versions, snapshotValues())
	}
	if hashRecords == 0 {
		t.Fatal("cold modification never used Hash; fixture does not exercise conversion", prefixRecords)
	}
	for h, root := range roots {
		view, err := db.ViewAt(uint64(h))
		check(err)
		for i := 0; i < keyCount; i += 7 {
			proof, err := view.Prove(0, root, keyOf(i))
			check(err)
			got, found, err := mpt.VerifyProof(root, keyOf(i), proof)
			check(err)
			if !found || !bytes.Equal(got, versions[h][i]) {
				t.Fatal("historical value", h, i)
			}
		}
		check(view.Close())
	}
}

// A layout that leaves every child out of the Prefix set never backfills: the
// excluded children are cold, only modified ones may become Hash, and every
// value stays provable.
func TestLayoutSelectorNeverBackfillsAndStaysVerifiable(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "no-backfill-layout")
	opt := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opt
	db, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	const n = 300
	keyOf := func(i int) []byte { return []byte{byte(i * 7), byte(i * 13 >> 3), byte(i)} }
	for i := 0; i < n; i++ {
		if err := tree.Update(keyOf(i), bytes.Repeat([]byte{byte(i)}, 40)); err != nil {
			t.Fatal(err)
		}
	}
	head, _, err := db.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	allHash := func(NodeKey, uint16) (uint16, error) { return 0, nil }
	want := map[int][]byte{}
	var hashes uint64
	for h := uint64(1); h <= 20; h++ {
		i := int(h*37) % n
		want[i] = bytes.Repeat([]byte{byte(h), 1}, 20)
		if err := tree.Update(keyOf(i), want[i]); err != nil {
			t.Fatal(err)
		}
		var stats CommitStats
		head, stats, err = db.CommitWithLayout(h, head.CommitID, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}}, allHash)
		if err != nil {
			t.Fatal(err)
		}
		if stats.ClosureBackfillRecords != 0 || stats.ClosureBackfillBytes != 0 {
			t.Fatalf("height %d backfilled: %+v", h, stats)
		}
		hashes += stats.HashRecords
	}
	if hashes == 0 {
		t.Fatal("modified children never became Hash")
	}
	root, _ := tree.Hash()
	view, err := db.ViewAt(head.Height)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	for i, v := range want {
		proof, err := view.Prove(0, root, keyOf(i))
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := mpt.VerifyProof(root, keyOf(i), proof)
		if err != nil || !found || !bytes.Equal(got, v) {
			t.Fatal("value", i, err)
		}
	}
}
