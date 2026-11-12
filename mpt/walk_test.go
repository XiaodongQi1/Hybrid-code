package mpt

import (
	"bytes"
	"testing"
)

func TestWalkAuthenticatedReachable(t *testing.T) {
	s := NewMemoryStore()
	tr, _ := NewEmpty(s)
	entries := map[string][]byte{"": {1}, "a": {2}, "ab": bytes.Repeat([]byte{3}, 60), "z": {4}}
	for k, v := range entries {
		if e := tr.Update([]byte(k), v); e != nil {
			t.Fatal(e)
		}
	}
	root, e := tr.Commit()
	if e != nil {
		t.Fatal(e)
	}
	dst := NewMemoryStore()
	batch := dst.NewBatch()
	seen := 0
	e = Walk(root, s, func(h Hash, b []byte) error { return batch.Put(h[:], b) }, func(k, v []byte) error {
		seen++
		if !bytes.Equal(entries[string(k)], v) {
			t.Fatal("leaf mismatch")
		}
		return nil
	})
	if e != nil || seen != len(entries) {
		t.Fatal(e, seen)
	}
	batch.Write()
	if _, e = Open(root, dst); e != nil {
		t.Fatal(e)
	}
	bad := s.NewBatch()
	bad.Put(root[:], []byte("corrupt"))
	bad.Write()
	if e = Walk(root, s, nil, nil); e == nil {
		t.Fatal("corruption accepted")
	}
}

func TestWalkPathsCanonicalAndPruning(t *testing.T) {
	s := NewMemoryStore()
	tr, _ := NewEmpty(s)
	for _, k := range []byte{0x10, 0x11, 0x20, 0x21} {
		if err := tr.Update([]byte{k}, bytes.Repeat([]byte{k}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := tr.Commit()
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]bool{}
	var leaves []byte
	err = WalkPaths(root, s, func(p []byte, _ Hash, _ []byte) (bool, error) {
		if paths[string(p)] {
			t.Fatal("duplicate node path")
		}
		paths[string(p)] = true
		return !bytes.Equal(p, []byte{1}), nil
	}, func(k, v []byte) error { leaves = append(leaves, k[0]); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !paths[""] || !paths[string([]byte{1})] || !paths[string([]byte{2})] || paths[string([]byte{1, 0})] || !bytes.Equal(leaves, []byte{0x20, 0x21}) {
		t.Fatalf("paths=%v leaves=%x", paths, leaves)
	}
	// Even a pruned root is authenticated before callbacks can skip it.
	b := s.NewBatch()
	b.Put(root[:], []byte("corrupt"))
	b.Write()
	if WalkPaths(root, s, func([]byte, Hash, []byte) (bool, error) { return false, nil }, nil) == nil {
		t.Fatal("pruning bypassed authentication")
	}
}
