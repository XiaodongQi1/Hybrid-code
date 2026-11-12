package hybridtrie

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type pendingReadEngine struct {
	diskEngine
	count     bool
	nodeReads uint64
}
type pendingReadSnapshot struct {
	diskSnapshot
	owner *pendingReadEngine
}

func (e *pendingReadEngine) snapshot() diskSnapshot {
	return &pendingReadSnapshot{e.diskEngine.snapshot(), e}
}
func (s *pendingReadSnapshot) get(c column, k []byte) ([]byte, error) {
	if s.owner.count && c != metaColumn {
		s.owner.nodeReads++
	}
	return s.diskSnapshot.get(c, k)
}
func (s *pendingReadSnapshot) prev(c column, k []byte) ([]byte, []byte, error) {
	if s.owner.count && c != metaColumn {
		s.owner.nodeReads++
	}
	return s.diskSnapshot.prev(c, k)
}

func pendingStore(t *testing.T) *Store {
	t.Helper()
	c := DefaultConfig("raw-bytes/v1", "pending-test")
	o := kvstore.DefaultOptions(kvstore.LevelDB)
	c.Storage = &o
	s, e := Open(filepath.Join(t.TempDir(), "db"), c, true, false)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// A fully resident Prefix tree must not reopen its old logical nodes merely
// to generate a new physical version. Root/lifecycle metadata reads are allowed.
func TestPendingCommitReusesResidentChildLocators(t *testing.T) {
	s := pendingStore(t)
	f, e := OpenCachedForest(s, DefaultPrefixBoundaryForestCacheConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		if e = tr.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 64)); e != nil {
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
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	root, _ := tr.Hash()
	counted := &pendingReadEngine{diskEngine: s.db}
	s.db = counted
	b, e = f.BeginBlock(1, head.CommitID)
	if e != nil {
		t.Fatal(e)
	}
	r, e := b.TrieReader(0, root)
	if e != nil {
		t.Fatal(e)
	}
	tr, e = mpt.Open(root, r)
	if e != nil {
		t.Fatal(e)
	}
	if e = tr.Update([]byte{0x20}, bytes.Repeat([]byte{99}, 64)); e != nil {
		t.Fatal(e)
	}
	newRoot, _ := tr.Hash()
	if e = b.SetTrie(0, tr); e != nil {
		t.Fatal(e)
	}
	counted.count = true
	_, stats, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	counted.count = false
	if counted.nodeReads != 0 {
		t.Fatalf("commit reread %d old node records", counted.nodeReads)
	}
	if stats.PrefixRecords != 2 {
		t.Fatalf("wrote clean siblings: %+v", stats)
	}
	_, e = tr.PendingLogicalNodes(newRoot, 100, func(mpt.LogicalChange) error { t.Fatal("successful commit remained dirty"); return nil })
	if e != nil {
		t.Fatal(e)
	}
	for h := uint64(0); h <= 1; h++ {
		v, e := s.ViewAt(h)
		if e != nil {
			t.Fatal(e)
		}
		rt := root
		if h == 1 {
			rt = newRoot
		}
		for i := 0; i < 16; i++ {
			proof, _, e := v.ProveWithStats(0, rt, []byte{byte(i << 4)})
			if e != nil {
				t.Fatal(e)
			}
			value, exists, e := mpt.VerifyProof(rt, []byte{byte(i << 4)}, proof)
			want := byte(i)
			if h == 1 && i == 2 {
				want = 99
			}
			if e != nil || !exists || !bytes.Equal(value, bytes.Repeat([]byte{want}, 64)) {
				t.Fatal("history", h, i, e)
			}
		}
		v.Close()
	}
}

func TestPendingCommitFailureKeepsWrites(t *testing.T) {
	for _, writeFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "staging", true: "uncertain-write"}[writeFailure], func(t *testing.T) {
			s := pendingStore(t)
			tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			tr.Update([]byte{0x10}, bytes.Repeat([]byte{1}, 64))
			head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tr, PrefixMask: 0xffff}})
			if e != nil {
				t.Fatal(e)
			}
			base, _ := tr.Hash()
			tr.Update([]byte{0x20}, bytes.Repeat([]byte{2}, 64))
			tr.Hash()
			fault := errors.New("injected pending failure")
			selector := LayoutSelector(func(NodeKey, uint16) (uint16, error) { return 0, fault })
			if writeFailure {
				s.write = func([]entry) error { return fault }
				selector = nil
			}
			_, _, e = s.CommitWithLayout(1, head.CommitID, []TrieUpdate{{Tree: tr, PrefixMask: 0xffff}}, selector)
			if e == nil {
				t.Fatal("expected failure")
			}
			st, e := tr.PendingLogicalNodes(base, 100, func(mpt.LogicalChange) error { return nil })
			if e != nil || st.Emitted != 3 {
				t.Fatal("failure cleared dirty nodes", st, e)
			}
			if !writeFailure {
				if _, _, e = s.Commit(1, head.CommitID, []TrieUpdate{{Tree: tr, PrefixMask: 0xffff}}); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}
