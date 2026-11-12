package mpt

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func newTestTrie(t *testing.T) (*Trie, *MemoryStore) {
	t.Helper()
	store := NewMemoryStore()
	trie, err := NewEmpty(store)
	if err != nil {
		t.Fatal(err)
	}
	return trie, store
}

func TestCRUDCommitAndReopen(t *testing.T) {
	trie, store := newTestTrie(t)
	values := map[string]string{
		"do":    "verb",
		"dog":   "puppy",
		"doge":  "coin",
		"horse": "stallion",
	}
	for key, value := range values {
		if err := trie.Update([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	for key, want := range values {
		got, err := trie.Get([]byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, want)
		}
	}
	if err := trie.Delete([]byte("dog")); err != nil {
		t.Fatal(err)
	}
	if got, err := trie.Get([]byte("dog")); err != nil || got != nil {
		t.Fatalf("deleted value = %q, %v", got, err)
	}
	if got, _ := trie.Get([]byte("doge")); string(got) != "coin" {
		t.Fatalf("sibling was damaged: %q", got)
	}

	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(root, store)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"do", "doge", "horse"} {
		got, err := reopened.Get([]byte(key))
		if err != nil || string(got) != values[key] {
			t.Fatalf("reopened Get(%q) = %q, %v", key, got, err)
		}
	}
}

func TestCopyOnWritePreservesOldRoot(t *testing.T) {
	trie, store := newTestTrie(t)
	if err := trie.Update([]byte("account"), []byte("100")); err != nil {
		t.Fatal(err)
	}
	oldRoot, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := trie.Update([]byte("account"), []byte("75")); err != nil {
		t.Fatal(err)
	}
	newRoot, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if oldRoot == newRoot {
		t.Fatal("state update did not change root")
	}
	oldTrie, err := Open(oldRoot, store)
	if err != nil {
		t.Fatal(err)
	}
	oldValue, err := oldTrie.Get([]byte("account"))
	if err != nil || string(oldValue) != "100" {
		t.Fatalf("old version = %q, %v", oldValue, err)
	}
	newTrie, err := Open(newRoot, store)
	if err != nil {
		t.Fatal(err)
	}
	newValue, err := newTrie.Get([]byte("account"))
	if err != nil || string(newValue) != "75" {
		t.Fatalf("new version = %q, %v", newValue, err)
	}
}

func TestDeterministicRootAndEmptyRoot(t *testing.T) {
	a, _ := newTestTrie(t)
	b, _ := newTestTrie(t)
	root, err := a.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if root != EmptyRootHash {
		t.Fatalf("empty root = %x", root)
	}
	entries := [][2]string{{"alpha", "1"}, {"beta", "2"}, {"gamma", "3"}}
	for _, entry := range entries {
		if err := a.Update([]byte(entry[0]), []byte(entry[1])); err != nil {
			t.Fatal(err)
		}
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if err := b.Update([]byte(entries[i][0]), []byte(entries[i][1])); err != nil {
			t.Fatal(err)
		}
	}
	rootA, _ := a.Hash()
	rootB, _ := b.Hash()
	if rootA != rootB {
		t.Fatalf("roots differ: %x != %x", rootA, rootB)
	}
}

func TestProofOfInclusionAndAbsence(t *testing.T) {
	trie, _ := newTestTrie(t)
	for _, entry := range [][2]string{{"alice", "100"}, {"bob", "80"}, {"carol", "50"}} {
		if err := trie.Update([]byte(entry[0]), []byte(entry[1])); err != nil {
			t.Fatal(err)
		}
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := trie.Prove([]byte("bob"))
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err := VerifyProof(root, []byte("bob"), proof)
	if err != nil || !exists || string(value) != "80" {
		t.Fatalf("inclusion proof = %q, %v, %v", value, exists, err)
	}

	absenceProof, err := trie.Prove([]byte("dave"))
	if err != nil {
		t.Fatal(err)
	}
	value, exists, err = VerifyProof(root, []byte("dave"), absenceProof)
	if err != nil || exists || value != nil {
		t.Fatalf("absence proof = %q, %v, %v", value, exists, err)
	}

	tampered := make(Proof, len(proof))
	for i := range proof {
		tampered[i] = cloneBytes(proof[i])
	}
	tampered[0][len(tampered[0])-1] ^= 1
	if _, _, err := VerifyProof(root, []byte("bob"), tampered); err == nil {
		t.Fatal("tampered proof was accepted")
	}
}

func TestMissingRootAndDefensiveCopies(t *testing.T) {
	store := NewMemoryStore()
	if _, err := Open(Hash{1}, store); !errors.Is(err, ErrMissingNode) {
		t.Fatalf("Open error = %v", err)
	}
	trie, _ := NewEmpty(store)
	key, value := []byte("key"), []byte("value")
	if err := trie.Update(key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, err := trie.Get(key)
	if err != nil || !bytes.Equal(got, []byte("value")) {
		t.Fatalf("stored value = %q, %v", got, err)
	}
	got[0] = 'Y'
	again, _ := trie.Get(key)
	if !bytes.Equal(again, []byte("value")) {
		t.Fatalf("Get leaked internal memory: %q", again)
	}
}

func TestRandomizedVersionsAndProofs(t *testing.T) {
	trie, store := newTestTrie(t)
	rng := rand.New(rand.NewSource(1))
	want := make(map[string][]byte)
	for round := 0; round < 100; round++ {
		key := fmt.Sprintf("key-%02d", rng.Intn(30))
		if rng.Intn(4) == 0 {
			if err := trie.Delete([]byte(key)); err != nil {
				t.Fatal(err)
			}
			delete(want, key)
		} else {
			value := []byte(fmt.Sprintf("value-%03d", rng.Intn(1000)))
			if err := trie.Update([]byte(key), value); err != nil {
				t.Fatal(err)
			}
			want[key] = cloneBytes(value)
		}
		if round%10 != 9 {
			continue
		}
		root, err := trie.Commit()
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := Open(root, store)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 30; i++ {
			candidate := fmt.Sprintf("key-%02d", i)
			value, err := snapshot.Get([]byte(candidate))
			if err != nil || !bytes.Equal(value, want[candidate]) {
				t.Fatalf("round %d, %s = %q, %v; want %q", round, candidate, value, err, want[candidate])
			}
			proof, err := snapshot.Prove([]byte(candidate))
			if err != nil {
				t.Fatal(err)
			}
			proved, exists, err := VerifyProof(root, []byte(candidate), proof)
			if err != nil {
				t.Fatalf("round %d, proof %s: %v", round, candidate, err)
			}
			_, expected := want[candidate]
			if exists != expected || !bytes.Equal(proved, want[candidate]) {
				t.Fatalf("round %d, proof %s = %q, %v; want %q, %v", round, candidate, proved, exists, want[candidate], expected)
			}
		}
	}
}

func TestLevelDBPersistence(t *testing.T) {
	path := t.TempDir()
	store, err := OpenLevelDBCached(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	trie, err := NewEmpty(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := trie.Update([]byte("alice"), []byte("100")); err != nil {
		t.Fatal(err)
	}
	firstRoot, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := trie.Update([]byte("alice"), []byte("75")); err != nil {
		t.Fatal(err)
	}
	secondRoot, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenLevelDBCached(path, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for root, want := range map[Hash]string{firstRoot: "100", secondRoot: "75"} {
		snapshot, err := Open(root, store)
		if err != nil {
			t.Fatal(err)
		}
		value, err := snapshot.Get([]byte("alice"))
		if err != nil || string(value) != want {
			t.Fatalf("root %x value = %q, %v; want %q", root, value, err, want)
		}
		proof, err := snapshot.Prove([]byte("alice"))
		if err != nil {
			t.Fatal(err)
		}
		value, exists, err := VerifyProof(root, []byte("alice"), proof)
		if err != nil || !exists || string(value) != want {
			t.Fatalf("root %x proof = %q, %v, %v", root, value, exists, err)
		}
	}
}

func TestCachedStoreLRUEviction(t *testing.T) {
	base := NewMemoryStore()
	batch := base.NewBatch()
	for _, key := range []string{"a", "b", "c"} {
		if err := batch.Put([]byte(key), []byte(key+"-value")); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}
	store, err := NewCachedStore(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b", "a", "c", "b"} {
		value, err := store.Get([]byte(key))
		if err != nil || string(value) != key+"-value" {
			t.Fatalf("Get(%q) = %q, %v", key, value, err)
		}
	}
	stats := store.Stats()
	if stats.Entries != 2 || stats.Hits != 1 || stats.Misses != 4 || stats.Evictions != 2 {
		t.Fatalf("unexpected cache stats: %+v", stats)
	}
}
