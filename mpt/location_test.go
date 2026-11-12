package mpt

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

type locationTestStore struct {
	Store
	root              Hash
	locations, points int
}

var testLocation = NodeLocation{Kind: 7, Height: 123, Exact: true}

func (s *locationTestStore) load(h Hash) (MaterializedNode, error) {
	raw, e := s.Store.Get(h[:])
	if e != nil {
		return MaterializedNode{}, e
	}
	n, e := DecodeTrustedLocalNode(raw, h)
	if e != nil {
		return n, e
	}
	var locations [16]NodeLocation
	for i := range locations {
		locations[i] = testLocation
	}
	return n.WithChildLocations(locations), nil
}
func (s *locationTestStore) GetMaterializedNode(path []byte, h Hash) (MaterializedNode, error) {
	if len(path) != 0 || h != s.root {
		return MaterializedNode{}, fmt.Errorf("lost child location at %x", path)
	}
	return s.load(h)
}
func (s *locationTestStore) GetLocatedMaterializedNode(_ []byte, h Hash, loc NodeLocation) (MaterializedNode, error) {
	if loc != testLocation {
		return MaterializedNode{}, fmt.Errorf("wrong location: %+v", loc)
	}
	s.locations++
	return s.load(h)
}
func (s *locationTestStore) LocatedLocalValue(_ []byte, _ Hash, _ []byte, loc NodeLocation, _ TrieAccessObserver) ([]byte, bool, error) {
	if loc != testLocation {
		return nil, true, ErrInvalidPath
	}
	s.points++
	return nil, false, nil
}

func TestChildLocationsSurviveCOWWithoutChangingEncoding(t *testing.T) {
	db := NewMemoryStore()
	original, _ := NewEmpty(db)
	for _, key := range [][]byte{{0x10, 0x10}, {0x10, 0x11}, {0x10, 0x20}, {0x20}, {0x30}} {
		if e := original.Update(key, bytes.Repeat(key, 64)); e != nil {
			t.Fatal(e)
		}
	}
	root, e := original.Commit()
	if e != nil {
		t.Fatal(e)
	}
	s := &locationTestStore{Store: db, root: root}
	tr, e := Open(root, s)
	if e != nil {
		t.Fatal(e)
	}
	before := tr.Copy()
	plain, e := Open(root, db)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range [][]byte{{0x10, 0x10}, {0x20}} {
		got, e := tr.Get(key)
		want, _ := plain.Get(key)
		if e != nil || !bytes.Equal(got, want) {
			t.Fatal(e)
		}
	}
	if s.points == 0 || s.locations == 0 {
		t.Fatal("located interfaces were bypassed")
	}
	for _, tree := range []*Trie{tr, plain} {
		if e = tree.Update([]byte{0x10, 0x10}, []byte{99}); e != nil {
			t.Fatal(e)
		}
		if e = tree.Delete([]byte{0x10, 0x11}); e != nil {
			t.Fatal(e)
		}
		if e = tree.Update([]byte{0x10, 0x21}, bytes.Repeat([]byte{8}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	h, e := tr.HashParallel(context.Background(), 4)
	want, _ := plain.Hash()
	if e != nil || h != want {
		t.Fatal("locator entered hash", e)
	}
	if h, _ = before.Hash(); h != root {
		t.Fatal("modified COW copy")
	}
	for _, key := range [][]byte{{0x10, 0x10}, {0x10, 0x11}, {0x20}, {0x40}} {
		proof, e := tr.Prove(key)
		if e != nil {
			t.Fatal(e)
		}
		v, exists, e := VerifyProof(want, key, proof)
		value, _ := plain.Get(key)
		if e != nil || exists != (value != nil) || !bytes.Equal(v, value) {
			t.Fatal("proof differs", e)
		}
	}
	p, e := tr.prepareCommit()
	if e != nil {
		t.Fatal(e)
	}
	q, e := plain.prepareCommit()
	if e != nil {
		t.Fatal(e)
	}
	if p.root != q.root || len(p.nodes) != len(q.nodes) {
		t.Fatal("staging changed")
	}
	for h, raw := range p.nodes {
		if !bytes.Equal(raw, q.nodes[h]) {
			t.Fatal("location serialized")
		}
	}
	// Parsed cache images likewise retain only canonical fields.
	m, e := s.load(root)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := db.Get(root[:])
	plainNode, _ := DecodeTrustedLocalNode(raw, root)
	a, e := m.capture(make([]byte, NodeImageMaxBytes(uint32(len(raw)))), len(raw))
	if e != nil {
		t.Fatal(e)
	}
	b, e := plainNode.capture(make([]byte, NodeImageMaxBytes(uint32(len(raw)))), len(raw))
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(a.data, b.data) {
		t.Fatal("location entered cache image")
	}
}
