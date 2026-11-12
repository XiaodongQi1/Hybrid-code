package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

type closureSource struct {
	mpt.Store
	reads map[string]int
}

func (s *closureSource) Get(k []byte) ([]byte, error) { s.reads[string(k)]++; return s.Store.Get(k) }
func TestHashClosureContinuesFromLoadedAncestors(t *testing.T) {
	s := pendingStore(t)
	db := mpt.NewMemoryStore()
	original, _ := mpt.NewEmpty(db)
	for i := 0; i < 64; i++ {
		if e := original.Update([]byte{byte(i), 0}, bytes.Repeat([]byte{byte(i)}, 40)); e != nil {
			t.Fatal(e)
		}
	}
	root, e := original.Commit()
	if e != nil {
		t.Fatal(e)
	}
	source := &closureSource{Store: db, reads: map[string]int{}}
	tree, e := mpt.Open(root, source)
	if e != nil {
		t.Fatal(e)
	}
	snapshot := s.db.snapshot()
	defer snapshot.close()
	plan := &writePlan{entries: map[string]entry{}, limit: s.cfg.MaxBatchBytes}
	if e = s.ensureHash(plan, snapshot, tree, nil, root, 0); e != nil {
		t.Fatal(e)
	}
	for k, n := range source.reads {
		if n != 1 {
			t.Fatalf("source node %x loaded %d times", k, n)
		}
	}
	if len(plan.entries) != len(source.reads) {
		t.Fatalf("incomplete closure: %d records, %d nodes", len(plan.entries), len(source.reads))
	}
	for _, item := range plan.entries {
		raw, e := db.Get(item.key)
		if e != nil || !bytes.Equal(raw, item.value) {
			t.Fatal("changed closure bytes", e)
		}
	}
}
