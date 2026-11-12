package versionedtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
	"testing"
)

func TestConcurrentReconstructedProofsDuringCommits(t *testing.T) {
	s, _, _ := testStore(t)
	full := mpt.NewMemoryStore()
	tr := canonicalTree(t, full, map[string][]byte{"abc": []byte("old"), "abcd": []byte("child")})
	root, _ := tr.Hash()
	u := beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	old := view(t, s, 0)
	reader, _ := old.TrieReader(0)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				tree, e := mpt.Open(root, reader)
				if e != nil {
					t.Error(e)
					return
				}
				p, e := tree.Prove([]byte("abc"))
				if e != nil {
					t.Error(e)
					return
				}
				value, exists, e := mpt.VerifyProof(root, []byte("abc"), p)
				if e != nil || !exists || !bytes.Equal(value, []byte("old")) {
					t.Error("old proof changed", e)
					return
				}
			}
		}()
	}
	prior := root
	for h := uint64(1); h <= 5; h++ {
		tr = canonicalTree(t, full, map[string][]byte{"abc": bytes.Repeat([]byte{byte(h)}, 40), "abcd": []byte("child")})
		next, _ := tr.Hash()
		u = beginWithRoot(t, s, h, next)
		if _, e := u.ProjectTrie(0, prior, tr); e != nil {
			t.Fatal(e)
		}
		commit(t, u)
		prior = next
	}
	wg.Wait()
	old.Close()
}
