package forest

import (
	"bytes"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type cacheTestStore struct{ *mpt.MemoryStore }

func (cacheTestStore) Code(common.Hash) ([]byte, error) { panic("unexpected code read") }

func TestExecutionNodeCacheEvictionAndDetachedValues(t *testing.T) {
	plan, err := PlanExecutionNodeCache(8192)
	if err != nil {
		t.Fatal(err)
	}
	budget := plan.FixedBytes + 2*plan.SlotBytes
	cache, err := NewExecutionNodeCache(budget, false)
	if err != nil {
		t.Fatal(err)
	}
	store := cacheTestStore{mpt.NewMemoryStore()}
	var roots []mpt.Hash
	for i := 0; i < 3; i++ {
		tree, err := mpt.NewEmpty(store)
		if err != nil {
			t.Fatal(err)
		}
		if err = tree.Update([]byte{1}, bytes.Repeat([]byte{byte(i + 1)}, 64)); err != nil {
			t.Fatal(err)
		}
		root, err := tree.Commit()
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}
	exec := WithExecutionNodeCache(store, cache)
	// Touch 0 again before adding 2: 1 must be evicted even though 0 came first.
	var first *mpt.Trie
	for i, id := range []int{0, 1, 0, 2, 0, 1} {
		tree, err := mpt.Open(roots[id], exec)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = tree
		}
	}
	s := kvstore.Statistics{}
	cache.Report(&s)
	if s.ReadCache.Hits != 2 || s.ReadCache.Misses != 4 || s.ReadCache.Evictions != 2 || s.ReadCache.AllocatedBytes != budget {
		t.Fatal(s.ReadCache)
	}
	got, err := first.Get([]byte{1})
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{1}, 64)) {
		t.Fatal("slot reuse altered an open trie", err)
	}
}
