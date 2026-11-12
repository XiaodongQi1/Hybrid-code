package mpt

import (
	"bytes"
	"errors"
	"math/rand"
	"testing"
)

func checkNodeImage(t *testing.T, raw []byte) NodeImage {
	t.Helper()
	h := hashBytes(raw)
	arena := make([]byte, NodeImageMaxBytes(uint32(len(raw))))
	image, e := CaptureNodeImage(arena, raw, h)
	if e != nil {
		t.Fatal(e, len(raw))
	}
	again, e := image.RLP()
	if e != nil || !bytes.Equal(again, raw) {
		t.Fatal("field image round trip", e)
	}
	if image.Hash() != h || image.EncodedSize() != uint32(len(raw)) || image.DataBytes() > len(arena) {
		t.Fatal("bad image bounds")
	}
	identity, e := image.Identity()
	want, e2 := InspectNodeIdentity(raw)
	if e != nil || e2 != nil || identity != want {
		t.Fatal("identity changed", identity, want, e, e2)
	}
	desc, e := InspectCanonicalNode(raw)
	if e != nil {
		t.Fatal(e)
	}
	for i, ref := range desc.Children {
		got, ok := image.ChildHash(i)
		expected := ref.Present && len(ref.Inline) == 0
		if ok != expected || ok && got != ref.Hash {
			t.Fatal("child digest changed", i)
		}
	}
	return image
}

func TestNodeImageFieldsIncludingInlineAndLargeValues(t *testing.T) {
	rng := rand.New(rand.NewSource(31))
	store := NewMemoryStore()
	tr, _ := NewEmpty(store)
	for i := 0; i < 500; i++ {
		key := []byte{byte(rng.Intn(32)), byte(rng.Intn(256))}
		value := make([]byte, 1+rng.Intn(90))
		rng.Read(value)
		if e := tr.Update(key, value); e != nil {
			t.Fatal(e)
		}
	}
	empty, _ := NewEmpty(store)
	if _, e := tr.DiffLogicalNodes(empty, 4096, func(ch LogicalChange) error { checkNodeImage(t, ch.RLP); return nil }); e != nil {
		t.Fatal(e)
	}
	// Field-header expansion is largest for very short nested inline nodes.
	var short node = &leafNode{path: []byte{1}, value: []byte{1}}
	for i := 0; i < 14; i++ {
		short = &extensionNode{path: []byte{2}, child: short}
	}
	raw, _, e := encodeNode(short, nil)
	if e != nil || len(raw) != 31 {
		t.Fatal(len(raw), e)
	}
	root := new(branchNode)
	for i := range root.children {
		root.children[i] = short
	}
	raw, _, e = encodeNode(root, nil)
	if e != nil {
		t.Fatal(e)
	}
	dense := checkNodeImage(t, raw)
	if dense.DataBytes() <= len(raw)+256 {
		t.Fatal("fixture must exercise the inline header margin")
	}
	large := &leafNode{path: bytes.Repeat([]byte{15}, 64), value: bytes.Repeat([]byte{0xf0}, MaxCanonicalNodeBytes-128)}
	raw, _, e = encodeNode(large, nil)
	if e != nil {
		t.Fatal(e)
	}
	checkNodeImage(t, raw)
}

type imageStore struct {
	images     map[Hash]NodeImage
	byteCalls  int
	batchCalls int
}

func (s *imageStore) Get([]byte) ([]byte, error) {
	s.byteCalls++
	return nil, errors.New("byte interface forbidden during execution")
}
func (s *imageStore) NewBatch() Batch { s.batchCalls++; return NewMemoryStore().NewBatch() }
func (s *imageStore) GetMaterializedNode(_ []byte, h Hash) (MaterializedNode, error) {
	image, ok := s.images[h]
	if !ok {
		return MaterializedNode{}, ErrMissingNode
	}
	return image.Materialize()
}
func TestMaterializedExecutionNeverUsesRLPByteInterface(t *testing.T) {
	memory := NewMemoryStore()
	tr := cachedFixture(t, 128, memory)
	root, e := tr.Commit()
	if e != nil {
		t.Fatal(e)
	}
	source := &imageStore{images: map[Hash]NodeImage{}}
	for key, raw := range memory.data {
		var h Hash
		copy(h[:], key)
		image, e := CaptureNodeImage(make([]byte, NodeImageMaxBytes(uint32(len(raw)))), raw, h)
		if e != nil {
			t.Fatal(e)
		}
		source.images[h] = image
	}
	live, e := Open(root, source)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 128; i++ {
		got, e := live.Get([]byte{0, 0, byte(i)})
		if e != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(i + 1)}, 64)) {
			t.Fatal(i, e)
		}
	}
	if h, e := live.Commit(); e != nil || h != root || source.batchCalls != 0 {
		t.Fatal("clean source root became a write", source.batchCalls, e)
	}
	old := live.Copy()
	if e = live.Update([]byte{0, 0, 1}, []byte("changed")); e != nil {
		t.Fatal(e)
	}
	if e = live.Delete([]byte{0, 0, 2}); e != nil {
		t.Fatal(e)
	}
	if h, _, e := live.HashWithStats(); e != nil || h == root {
		t.Fatal(h, e)
	}
	h, work, e := old.HashWithStats()
	if e != nil || h != root || work.EncodedNodes != 0 || work.HashedNodes != 0 {
		t.Fatal(work, e)
	}
	if source.byteCalls != 0 {
		t.Fatal("execution requested RLP", source.byteCalls)
	}
}
func TestNodeImageDetachedFromArenaAndCowInvalidation(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	tr.Update([]byte{1}, []byte("old"))
	raw, h, _ := encodeNode(tr.root, nil)
	arena := make([]byte, NodeImageMaxBytes(1024))
	image, e := CaptureNodeImage(arena, raw, h)
	if e != nil {
		t.Fatal(e)
	}
	materialized, e := image.Materialize()
	if e != nil {
		t.Fatal(e)
	}
	live := &Trie{root: materialized.root, store: NewMemoryStore()}
	old := live.Copy()
	tr.Update([]byte{1}, []byte("arena replacement"))
	raw, next, _ := encodeNode(tr.root, nil)
	if _, e = CaptureNodeImage(arena, raw, next); e != nil {
		t.Fatal(e)
	}
	if got, e := live.Get([]byte{1}); e != nil || string(got) != "old" {
		t.Fatal("arena alias", got, e)
	}
	if e = live.Update([]byte{1}, []byte("new")); e != nil {
		t.Fatal(e)
	}
	if h, work, e := live.HashWithStats(); e != nil || h == image.Hash() || work.HashedNodes != 1 {
		t.Fatal(work, e)
	}
	if h, work, e := old.HashWithStats(); e != nil || h != image.Hash() || work.EncodedNodes != 0 || work.HashedNodes != 0 {
		t.Fatal("COW lost digest", work, e)
	}
}
func TestNodeImageRejectsUnauthenticatedAndSmallArenas(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	tr.Update([]byte{1}, []byte("value"))
	raw, h, _ := encodeNode(tr.root, nil)
	if _, e := CaptureNodeImage(make([]byte, 1024), raw, Hash{}); e == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, e := CaptureNodeImage(make([]byte, 1), raw, h); e == nil {
		t.Fatal("arena limit ignored")
	}
	if _, e := CaptureNodeImage(make([]byte, 1024), []byte{0xc0}, hashBytes([]byte{0xc0})); e == nil {
		t.Fatal("invalid node accepted")
	}
}
func TestMaterializedNodeRejectsOverflowingRLP(t *testing.T) {
	for _, prefix := range []byte{0xbf, 0xff} {
		for _, length := range [][]byte{{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, {0x80, 0, 0, 0, 0, 0, 0, 0}} {
			raw := append([]byte{prefix}, length...)
			if _, e := DecodeMaterializedNode(raw, hashBytes(raw)); e == nil {
				t.Fatal("overflowing length accepted")
			}
			if _, _, e := VerifyProof(hashBytes(raw), nil, Proof{raw}); e == nil {
				t.Fatal("malformed proof accepted")
			}
		}
	}
}
func FuzzNodeImageRoundtrip(f *testing.F) {
	f.Add([]byte{0xff, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	for _, d := range []CanonicalNode{{Kind: CanonicalLeaf, Path: []byte{0x20}, Value: []byte{1}}, {Kind: CanonicalBranch, Value: []byte{2}}} {
		raw, _, _ := RebuildCanonicalNode(d)
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxCanonicalNodeBytes {
			return
		}
		if _, e := InspectCanonicalNode(raw); e != nil {
			return
		}
		checkNodeImage(t, raw)
	})
}
