package mpt

import (
	"bytes"
	"errors"
	"testing"
)

func pendingForTest(t *testing.T, tr *Trie, base Hash) map[string]LogicalChange {
	t.Helper()
	out := map[string]LogicalChange{}
	_, err := tr.PendingLogicalNodes(base, 10000, func(c LogicalChange) error { out[string(c.Path)] = c; return nil })
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPersistenceDirtyIndependentOfHashAndCopy(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	for _, k := range []byte{0x10, 0x20, 0x30} {
		if err := tr.Update([]byte{k}, bytes.Repeat([]byte{k}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	base, err := tr.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingForTest(t, tr, EmptyRootHash)) != 4 {
		t.Fatal("Hash consumed pending writes")
	}
	// Acknowledgment belongs to one copy, not the shared node objects.
	copyBefore := tr.Copy()
	tr.AcceptLogicalPersistence(base)
	if len(pendingForTest(t, tr, base)) != 0 {
		t.Fatal("successful publication stayed dirty")
	}
	if err = tr.Update([]byte{0x20}, bytes.Repeat([]byte{0x20}, 64)); err != nil {
		t.Fatal(err)
	}
	if len(pendingForTest(t, tr, base)) != 0 {
		t.Fatal("writing the same value dirtied the path")
	}
	if len(pendingForTest(t, copyBefore, EmptyRootHash)) != 4 {
		t.Fatal("acknowledgment cleaned another copy")
	}
	if err = tr.Update([]byte{0x20}, bytes.Repeat([]byte{9}, 64)); err != nil {
		t.Fatal(err)
	}
	_, work, err := tr.HashWithStats()
	if err != nil || work.HashedNodes != 2 {
		t.Fatal(work, err)
	}
	changes := pendingForTest(t, tr, base)
	if len(changes) != 2 || changes[string([]byte{2})].RLP == nil {
		t.Fatal("clean siblings were emitted", changes)
	}
	// Callback failure is staging failure; even hashing and a previous partial
	// walk must leave all writes available to the next attempt.
	fault := errors.New("stop staging")
	_, err = tr.PendingLogicalNodes(base, 1000, func(LogicalChange) error { return fault })
	if !errors.Is(err, fault) || len(pendingForTest(t, tr, base)) != 2 {
		t.Fatal("failure consumed dirty state", err)
	}
	// Native hash staging remains usable after versioned publication.
	h, _ := tr.Hash()
	tr.AcceptLogicalPersistence(h)
	if _, err = tr.Commit(); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(h, tr.store)
	if err != nil {
		t.Fatal(err)
	}
	v, err := opened.Get([]byte{0x20})
	if err != nil || !bytes.Equal(v, bytes.Repeat([]byte{9}, 64)) {
		t.Fatal(v, err)
	}
}

func TestPersistenceIndependentForkWatermarks(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	tr.Update([]byte{0x10}, bytes.Repeat([]byte{1}, 64))
	base, _ := tr.Hash()
	tr.AcceptLogicalPersistence(base)
	tr.Update([]byte{0x20}, bytes.Repeat([]byte{2}, 64))
	fork := tr.Copy()
	tr.Update([]byte{0x30}, bytes.Repeat([]byte{3}, 64))
	h, _ := tr.Hash()
	tr.AcceptLogicalPersistence(h)
	if len(pendingForTest(t, fork, base)) != 3 {
		t.Fatal("shared pending nodes incorrectly cleaned")
	}
	// Different destination base requires a complete import, even for a clean
	// materialized tree, and must not change its pending-state watermark.
	if len(pendingForTest(t, tr, EmptyRootHash)) != 4 {
		t.Fatal("foreign-base import omitted clean nodes")
	}
	if len(pendingForTest(t, tr, h)) != 0 {
		t.Fatal("import changed pending state")
	}
}

func TestPersistenceStructuralChangesCoverLogicalDiff(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	keys := [][]byte{{0x10, 0x10}, {0x10, 0x20}, {0x10, 0x21}, {0x20}, {0x30}}
	for i, k := range keys {
		tr.Update(k, bytes.Repeat([]byte{byte(i + 1)}, 64))
	}
	for step := 0; step < 60; step++ {
		before := tr.Copy()
		base, _ := tr.Hash()
		tr.AcceptLogicalPersistence(base)
		k := keys[step%len(keys)]
		var err error
		if step%3 == 0 {
			err = tr.Delete(k)
		} else {
			err = tr.Update(k, bytes.Repeat([]byte{byte(step + 10)}, 1+(step%3)*32))
		}
		if err != nil {
			t.Fatal(err)
		}
		tr.Hash() // cleared hash dirtiness must not hide pending nodes
		pending := pendingForTest(t, tr, base)
		_, err = tr.DiffLogicalNodes(before, 10000, func(c LogicalChange) error {
			got, ok := pending[string(c.Path)]
			if !ok || got.Hash != c.Hash || !bytes.Equal(got.RLP, c.RLP) {
				t.Fatalf("step %d missing changed path %x", step, c.Path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
