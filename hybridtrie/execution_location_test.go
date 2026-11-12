package hybridtrie

import (
	"bytes"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type locationReadEngine struct {
	diskEngine
	roots, opens int
	count        bool
}
type locationReadSnapshot struct {
	diskSnapshot
	owner *locationReadEngine
}
type locationReadCursor struct {
	diskPredecessor
	owner *locationReadEngine
	cf    column
}

func (e *locationReadEngine) snapshot() diskSnapshot {
	return &locationReadSnapshot{e.diskEngine.snapshot(), e}
}
func (e *locationReadEngine) record(cf column, key []byte) {
	if e.count && cf == prefixColumn {
		if at, err := DecodeNodeKey(key); err == nil && len(at.Path) == 0 {
			e.roots++
		}
	}
}
func (s *locationReadSnapshot) get(cf column, key []byte) ([]byte, error) {
	s.owner.record(cf, key)
	return s.diskSnapshot.get(cf, key)
}
func (s *locationReadSnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	s.owner.record(cf, key)
	return s.diskSnapshot.prev(cf, key)
}
func (s *locationReadSnapshot) newPredecessor(cf column) (diskPredecessor, error) {
	c, e := s.diskSnapshot.(predecessorSource).newPredecessor(cf)
	if e != nil {
		return nil, e
	}
	if s.owner.count && cf == prefixColumn {
		s.owner.opens++
	}
	return &locationReadCursor{c, s.owner, cf}, nil
}
func (c *locationReadCursor) prev(key []byte) ([]byte, []byte, error) {
	c.owner.record(c.cf, key)
	return c.diskPredecessor.prev(key)
}

func TestExecutionLocationsContinueOutsideResidentFrontier(t *testing.T) {
	s := pendingStore(t)
	original, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	keys := [][]byte{{0x10, 0x10}, {0x10, 0x11}, {0x10, 0x20}, {0x20}, {0x30}}
	for _, key := range keys {
		if e := original.Update(key, bytes.Repeat(key, 32)); e != nil {
			t.Fatal(e)
		}
	}
	head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: original, PrefixMask: 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	root, _ := original.Hash()
	expected := original.Copy()
	tracked := &locationReadEngine{diskEngine: s.db}
	s.db = tracked
	cfg := DefaultPrefixBoundaryForestCacheConfig()
	allocation, e := BoundaryCacheAllocation(cfg.Boundary)
	if e != nil {
		t.Fatal(e)
	}
	cfg.Boundary.NodeBudget = ClassBudget{TotalBytes: allocation.Boundary.SlotBytes}
	f, e := OpenCachedForest(s, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := f.BeginBlock(1, head.CommitID)
	if e != nil {
		t.Fatal(e)
	}
	reader, e := b.TrieReader(0, root)
	if e != nil {
		t.Fatal(e)
	}
	tr, e := mpt.Open(root, reader)
	if e != nil {
		t.Fatal(e)
	}
	tracked.count = true
	for _, tree := range []*mpt.Trie{tr, expected} {
		if e = tree.Update(keys[0], bytes.Repeat([]byte{99}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	if tracked.roots != 1 {
		t.Fatalf("first descent read root %d times", tracked.roots)
	}
	// The live COW root now carries child locators. Neither another branch
	// read nor a deletion may restart from the old root, even with a one-slot pool.
	got, e := tr.Get(keys[2])
	want, _ := expected.Get(keys[2])
	if e != nil || !bytes.Equal(got, want) {
		t.Fatal(e)
	}
	for _, tree := range []*mpt.Trie{tr, expected} {
		if e = tree.Delete(keys[1]); e != nil {
			t.Fatal(e)
		}
	}
	if tracked.roots != 1 {
		t.Fatalf("restarted from root: %d", tracked.roots)
	}
	if f.boundary.stats.ColdResolutions != 0 {
		t.Fatal("lost a locator during normal descent")
	}
	// Each loaded parent reuses one cursor for all Prefix children.
	if tracked.opens > 6 {
		t.Fatalf("per-child iterator creation: %d", tracked.opens)
	}
	// An explicit Prefix locator without a known version seeks at that child,
	// never at the root. This also tests the cold physical-version discovery path.
	child, _, ok, e := original.CanonicalNodeAtPath([]byte{1})
	if e != nil || !ok {
		t.Fatal(e)
	}
	_, e = reader.(*boundaryReader).GetLocatedMaterializedNode([]byte{1}, child, mpt.NodeLocation{Kind: uint8(Prefix)})
	if e != nil || tracked.roots != 1 {
		t.Fatal("unknown version restarted at root", e)
	}
	tracked.count = false
	newRoot, _ := tr.Hash()
	wantRoot, _ := expected.Hash()
	if newRoot != wantRoot {
		t.Fatal("root changed")
	}
	if e = b.SetTrie(0, tr); e != nil {
		t.Fatal(e)
	}
	_, stats, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if stats.PrefixRecords != stats.PrefixEncodes {
		t.Fatal("prepared commit encoded a Prefix record twice", stats)
	}
	for h := uint64(0); h <= 1; h++ {
		v, e := s.ViewAt(h)
		if e != nil {
			t.Fatal(e)
		}
		rt, tree := root, original
		if h == 1 {
			rt, tree = newRoot, expected
		}
		for _, key := range keys {
			proof, _, e := v.ProveWithStats(0, rt, key)
			if e != nil {
				t.Fatal(e)
			}
			value, exists, e := mpt.VerifyProof(rt, key, proof)
			want, _ := tree.Get(key)
			if e != nil || exists != (want != nil) || !bytes.Equal(value, want) {
				t.Fatal("history", h, e)
			}
		}
		v.Close()
	}
	if _, e = tr.Get(keys[2]); e == nil {
		t.Fatal("locator outlived pinned block reader")
	}
}
