package mpt

import (
	"bytes"
	"testing"
)

func TestCompactProofInlineBoundariesAndStrictPath(t *testing.T) {
	for _, valueSize := range []int{1, 28, 29, 64} {
		store := NewMemoryStore()
		tree, _ := NewEmpty(store)
		for _, key := range [][]byte{{0x10}, {0x11}, {0x20}} {
			if e := tree.Update(key, bytes.Repeat([]byte{key[0]}, valueSize)); e != nil {
				t.Fatal(e)
			}
		}
		root, e := tree.Commit()
		if e != nil {
			t.Fatal(e)
		}
		for _, key := range [][]byte{{0x10}, {0x11}, {0x12}, {0x30}, {}} {
			expanded, e := tree.Prove(key)
			if e != nil {
				t.Fatal(e)
			}
			compact, e := tree.ProveCompact(key)
			if e != nil {
				t.Fatal(e)
			}
			for i, raw := range compact {
				if i > 0 && len(raw) < 32 {
					t.Fatal("inline child emitted separately")
				}
			}
			want, exists, e := VerifyProof(root, key, expanded)
			if e != nil {
				t.Fatal(e)
			}
			got, found, e := VerifyCompactProof(root, key, compact)
			if e != nil || found != exists || !bytes.Equal(got, want) {
				t.Fatalf("size=%d key=%x: %t %v", valueSize, key, found, e)
			}
			bad := append(append(Proof(nil), compact...), expanded[len(expanded)-1])
			if _, _, e = VerifyCompactProof(root, key, bad); e == nil {
				t.Fatal("surplus node accepted")
			}
			if _, _, e = VerifyCompactProof(root, key, compact[:len(compact)-1]); e == nil {
				t.Fatal("missing node accepted")
			}
			if len(compact) > 1 {
				bad = append(Proof(nil), compact...)
				bad[0], bad[1] = bad[1], bad[0]
				if _, _, e = VerifyCompactProof(root, key, bad); e == nil {
					t.Fatal("reordered nodes accepted")
				}
			}
		}
	}
}

func TestCompactProofShortAndEmptyRoots(t *testing.T) {
	tree, _ := NewEmpty(NewMemoryStore())
	if e := tree.Update([]byte{1}, []byte{1}); e != nil {
		t.Fatal(e)
	}
	root, _ := tree.Hash()
	p, _ := tree.ProveCompact([]byte{1})
	if len(p) != 1 || len(p[0]) >= 32 {
		t.Fatal("short root dropped")
	}
	if v, ok, e := VerifyCompactProof(root, []byte{1}, p); e != nil || !ok || !bytes.Equal(v, []byte{1}) {
		t.Fatal("short-root proof", e)
	}
	if _, ok, e := VerifyCompactProof(EmptyRootHash, []byte{1}, nil); e != nil || ok {
		t.Fatal("empty root", e)
	}
	if _, _, e := VerifyCompactProof(EmptyRootHash, []byte{1}, p); e == nil {
		t.Fatal("empty-root junk accepted")
	}
	copy := CompactProof(p)
	copy[0][0] ^= 1
	if bytes.Equal(copy[0], p[0]) {
		t.Fatal("conversion aliases source bytes")
	}
}
