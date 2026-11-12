package mpt

import (
	"bytes"
	"testing"
)

func TestStageMultipleTreesDoesNotPublishBeforeAtomicWrite(t *testing.T) {
	store := NewMemoryStore()
	a, _ := NewEmpty(store)
	b, _ := NewEmpty(store)
	if err := a.Update([]byte("account"), bytes.Repeat([]byte{1}, 64)); err != nil {
		t.Fatal(err)
	}
	if err := b.Update([]byte("slot"), bytes.Repeat([]byte{2}, 64)); err != nil {
		t.Fatal(err)
	}
	batch := store.NewBatch()
	ar, err := a.Stage(batch)
	if err != nil {
		t.Fatal(err)
	}
	br, err := b.Stage(batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ar, store); err == nil {
		t.Fatal("Stage wrote account nodes")
	}
	if _, err = Open(br, store); err == nil {
		t.Fatal("Stage wrote storage nodes")
	}
	batch.Put([]byte("marker"), ar[:])
	if err = batch.Write(); err != nil {
		t.Fatal(err)
	}
	for _, root := range []Hash{ar, br} {
		if _, err = Open(root, store); err != nil {
			t.Fatal(err)
		}
	}
	// Staging must not mutate the trie into a committed root: independent copies
	// and subsequent updates remain usable; abandoning a batch is harmless.
	prior := a.Copy()
	a.Update([]byte("other"), []byte{9})
	retry := store.NewBatch()
	next, err := a.Stage(retry)
	if err != nil || next == ar {
		t.Fatal("updated root", err)
	}
	pr, err := prior.Hash()
	if err != nil || pr != ar {
		t.Fatal("copy root changed")
	}
	abandoned := store.NewBatch()
	if _, err = a.Stage(abandoned); err != nil {
		t.Fatal(err)
	}
	if err = retry.Write(); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(next, store); err != nil {
		t.Fatal(err)
	}
}
