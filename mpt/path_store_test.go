package mpt

import (
	"bytes"
	"errors"
	"testing"
)

type pathOnlyStore struct {
	nodes   map[string][]byte
	backing Store
}

func (s *pathOnlyStore) Get([]byte) ([]byte, error) { return nil, errors.New("hash reads forbidden") }
func (s *pathOnlyStore) GetNode(p []byte, _ Hash) ([]byte, error) {
	b, ok := s.nodes[string(p)]
	if !ok {
		return nil, ErrMissingNode
	}
	return cloneBytes(b), nil
}
func (s *pathOnlyStore) NewBatch() Batch { return s.backing.NewBatch() }
func TestPathStoreMutationProofAndEquality(t *testing.T) {
	mem := NewMemoryStore()
	base, _ := NewEmpty(mem)
	keys := [][]byte{{0x12, 0x34}, {0x12, 0x35}, {0x12, 0xff}, {0x99, 0x00}}
	for i, k := range keys {
		if e := base.Update(k, bytes.Repeat([]byte{byte(i + 1)}, 80)); e != nil {
			t.Fatal(e)
		}
	}
	root, e := base.Commit()
	if e != nil {
		t.Fatal(e)
	}
	ps := &pathOnlyStore{nodes: map[string][]byte{}, backing: mem}
	if e := WalkPaths(root, mem, func(p []byte, _ Hash, b []byte) (bool, error) { ps.nodes[string(p)] = b; return true, nil }, nil); e != nil {
		t.Fatal(e)
	}
	got, e := Open(root, ps)
	if e != nil {
		t.Fatal(e)
	}
	ref, e := Open(root, mem)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range keys {
		a, e := got.Get(k)
		if e != nil {
			t.Fatal(e)
		}
		b, _ := ref.Get(k)
		if !bytes.Equal(a, b) {
			t.Fatal("read mismatch")
		}
	}
	// Mutate without publishing; proof must still traverse in-memory dirty nodes.
	for i, k := range keys {
		if i%2 == 0 {
			e = got.Delete(k)
			_ = ref.Delete(k)
		} else {
			e = got.Update(k, bytes.Repeat([]byte{9}, 90))
			_ = ref.Update(k, bytes.Repeat([]byte{9}, 90))
		}
		if e != nil {
			t.Fatal(e)
		}
		a, _ := got.Hash()
		b, _ := ref.Hash()
		if a != b {
			t.Fatal("root mismatch")
		}
		same, e := got.Equal(ref)
		if e != nil || !same {
			t.Fatalf("equal %v %v", same, e)
		}
		for _, key := range keys {
			proof, e := got.Prove(key)
			if e != nil {
				t.Fatal(e)
			}
			value, _, e := VerifyProof(a, key, proof)
			if e != nil {
				t.Fatal(e)
			}
			want, _ := ref.Get(key)
			if !bytes.Equal(value, want) {
				t.Fatal("proof mismatch")
			}
		}
	}
	// Optional path stores do not weaken Merkle authentication.
	ps.nodes[""] = []byte{0x80}
	if _, e := Open(root, ps); e == nil {
		t.Fatal("bad root accepted")
	}
}
