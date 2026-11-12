package mpt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func cachedFixture(t *testing.T, count int, store Store) *Trie {
	t.Helper()
	tree, e := NewEmpty(store)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < count; i++ {
		if e = tree.Update([]byte{byte(i >> 16), byte(i >> 8), byte(i)}, bytes.Repeat([]byte{byte(i + 1)}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	return tree
}

func TestCacheHashWorkScalesWithChangedPath(t *testing.T) {
	for _, size := range []int{256, 4096, 16384} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			tree := cachedFixture(t, size, NewMemoryStore())
			old, first, e := tree.HashWithStats()
			if e != nil || first.HashedNodes < uint64(size) {
				t.Fatal(first, e)
			}
			again, repeat, e := tree.HashWithStats()
			if e != nil || again != old || repeat.HashedNodes != 0 || repeat.EncodedNodes != 0 || repeat.VisitedNodes != 1 {
				t.Fatal("repeat must stop at root", repeat, e)
			}
			previous := tree.Copy()
			if e = tree.Update([]byte{0, 0, 1}, bytes.Repeat([]byte{0xab}, 64)); e != nil {
				t.Fatal(e)
			}
			next, changed, e := tree.HashWithStats()
			if e != nil || next == old || changed.HashedNodes == 0 || changed.HashedNodes > 7 {
				t.Fatal("must hash only one 6-nibble path", changed, e)
			}
			saved, work, e := previous.HashWithStats()
			if e != nil || saved != old || work.HashedNodes != 0 {
				t.Fatal("COW cache isolation", work, e)
			}
			t.Logf("nodes=%d first_hashes=%d repeat_hashes=%d changed_path_hashes=%d changed_visits=%d", size, first.HashedNodes, repeat.HashedNodes, changed.HashedNodes, changed.VisitedNodes)
		})
	}
}

func TestCacheHashThenStageRetryAndDiskReopen(t *testing.T) {
	path := t.TempDir() + "/db"
	store, e := OpenLevelDB(path)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	tree := cachedFixture(t, 128, store)
	root, _, e := tree.HashWithStats()
	if e != nil {
		t.Fatal(e)
	}
	// Prove and parallel Hash may also initialize caches, but cannot consume writes.
	if _, e = tree.Prove([]byte{0, 0, 1}); e != nil {
		t.Fatal(e)
	}
	h, e := tree.HashParallel(context.Background(), 4)
	if e != nil || h != root {
		t.Fatal(e)
	}
	abandoned := store.NewBatch()
	if _, e = tree.Stage(abandoned); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(root, store); e == nil {
		t.Fatal("abandoned stage became durable")
	}
	retry := store.NewBatch()
	staged, e := tree.Stage(retry)
	if e != nil || staged != root {
		t.Fatal(e)
	}
	if e = retry.Write(); e != nil {
		t.Fatal(e)
	}
	retained := tree.Copy() // fully expanded clean nodes, all cached before commit
	if committed, e := tree.Commit(); e != nil || committed != root {
		t.Fatal(e)
	}
	if e = retained.Update([]byte{0, 0, 1}, []byte("new value")); e != nil {
		t.Fatal(e)
	}
	updated, _, e := retained.HashWithStats()
	if e != nil || updated == root {
		t.Fatal(e)
	}
	if _, e = retained.Commit(); e != nil {
		t.Fatal(e)
	}
	if e = store.Close(); e != nil {
		t.Fatal(e)
	}
	store = nil
	reopened, e := OpenLevelDB(path)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	for _, want := range []struct {
		root  Hash
		value []byte
	}{{root, bytes.Repeat([]byte{2}, 64)}, {updated, []byte("new value")}} {
		tr, e := Open(want.root, reopened)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 128; i++ {
			key := []byte{0, 0, byte(i)}
			p, e := tr.Prove(key)
			if e != nil {
				t.Fatal(e)
			}
			v, exists, e := VerifyProof(want.root, key, p)
			expected := bytes.Repeat([]byte{byte(i + 1)}, 64)
			if i == 1 {
				expected = want.value
			}
			if e != nil || !exists || !bytes.Equal(v, expected) {
				t.Fatal("missing descendant after cached stage", i, e)
			}
		}
	}
}

type cacheFaultStore struct {
	Store
	mode string
}
type cacheFaultBatch struct {
	Batch
	owner *cacheFaultStore
}

func (s *cacheFaultStore) NewBatch() Batch { return &cacheFaultBatch{s.Store.NewBatch(), s} }
func (b *cacheFaultBatch) Write() error {
	if b.owner.mode == "before" {
		return errors.New("write failed")
	}
	if e := b.Batch.Write(); e != nil {
		return e
	}
	if b.owner.mode == "after" {
		return errors.New("write outcome unknown")
	}
	return nil
}
func TestCacheCommitFailuresDoNotConsumePendingNodes(t *testing.T) {
	for _, failure := range []string{"before", "after"} {
		t.Run(failure, func(t *testing.T) {
			memory := NewMemoryStore()
			store := &cacheFaultStore{Store: memory}
			tree := cachedFixture(t, 32, store)
			old, e := tree.Commit()
			if e != nil {
				t.Fatal(e)
			}
			if e = tree.Update([]byte{0, 0, 1}, []byte("changed")); e != nil {
				t.Fatal(e)
			}
			root, _, e := tree.HashWithStats()
			if e != nil {
				t.Fatal(e)
			}
			store.mode = failure
			if _, e = tree.Commit(); e == nil {
				t.Fatal("injected failure ignored")
			}
			if _, ok := tree.root.(hashNode); ok {
				t.Fatal("failed commit accepted root")
			}
			if _, e = Open(old, memory); e != nil {
				t.Fatal("old root lost", e)
			}
			_, readErr := Open(root, memory)
			if (failure == "before") != (readErr != nil) {
				t.Fatal("unexpected durable state", readErr)
			}
			// Low-level MPT retries identical immutable nodes. A chain coordinator must
			// first reconcile an unknown write outcome before deciding whether to retry.
			store.mode = ""
			if h, e := tree.Commit(); e != nil || h != root {
				t.Fatal(e)
			}
			reopened, e := Open(root, memory)
			if e != nil {
				t.Fatal(e)
			}
			v, e := reopened.Get([]byte{0, 0, 1})
			if e != nil || string(v) != "changed" {
				t.Fatal("clean pending node lost", e)
			}
		})
	}
}

func TestCacheInlineThresholdAndRollback(t *testing.T) {
	store := NewMemoryStore()
	tr, _ := NewEmpty(store)
	tr.Update([]byte{0x10}, bytes.Repeat([]byte{1}, 28))
	tr.Update([]byte{0x11}, []byte{2})
	old, e := tr.Hash()
	if e != nil {
		t.Fatal(e)
	}
	prior := tr.Copy()
	branch := tr.root.(*extensionNode).child.(*branchNode)
	short := branch.children[0].(*leafNode)
	if int(short.cache.Load().size) != 31 {
		t.Fatal("expected inline boundary")
	}
	tr.Update([]byte{0x10}, bytes.Repeat([]byte{1}, 29))
	next, e := tr.Hash()
	if e != nil || next == old {
		t.Fatal(e)
	}
	branch = tr.root.(*extensionNode).child.(*branchNode)
	if int(branch.children[0].(*leafNode).cache.Load().size) != 32 {
		t.Fatal("expected hash reference boundary")
	}
	tr.Delete([]byte{0x11})
	tr.Hash()
	tr.Update([]byte{0x11}, []byte{2})
	tr.Update([]byte{0x10}, bytes.Repeat([]byte{1}, 28))
	restored, e := tr.Hash()
	if e != nil || restored != old {
		t.Fatal("split/collapse/net-zero changed root", e)
	}
	tr = prior // explicit private-root rollback
	if h, e := tr.Commit(); e != nil || h != old {
		t.Fatal("rollback commit", e)
	}
	p, e := tr.Prove([]byte{0x10})
	if e != nil {
		t.Fatal(e)
	}
	v, ok, e := VerifyProof(old, []byte{0x10}, p)
	if e != nil || !ok || len(v) != 28 {
		t.Fatal(e)
	}
}

func TestCacheColdConcurrentHashProofAndSharedCopies(t *testing.T) {
	tree := cachedFixture(t, 256, NewMemoryStore())
	old := tree.Copy()
	candidate := tree.Copy()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				roots, e := HashMany(context.Background(), []*Trie{tree, old, tree.Copy()}, 4)
				if e != nil {
					t.Error(e)
					return
				}
				if roots[0] != roots[1] || roots[0] != roots[2] {
					t.Error("shared roots diverged")
					return
				}
				p, e := old.Prove([]byte{0, 0, 1})
				if e != nil {
					t.Error(e)
					return
				}
				if _, ok, e := VerifyProof(roots[0], []byte{0, 0, 1}, p); e != nil || !ok {
					t.Error("concurrent proof", e)
					return
				}
			}
		}()
	}
	// Mutate only the private root; cached shared descendants remain immutable.
	for i := 0; i < 10; i++ {
		if e := candidate.Update([]byte{0, 0, byte(i)}, []byte("candidate")); e != nil {
			t.Fatal(e)
		}
		if _, e := candidate.Hash(); e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
	root, work, e := old.HashWithStats()
	if e != nil || work.HashedNodes != 0 {
		t.Fatal(work, e)
	}
	if committed, e := old.Commit(); e != nil || committed != root {
		t.Fatal("concurrent hashing consumed writes", e)
	}
}
