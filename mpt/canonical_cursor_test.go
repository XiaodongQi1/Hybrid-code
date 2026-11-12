package mpt

import (
	"bytes"
	"testing"
)

type cursorReadStore struct {
	Store
	reads map[string]int
}

func (s *cursorReadStore) Get(k []byte) ([]byte, error) { s.reads[string(k)]++; return s.Store.Get(k) }

func TestCanonicalCursorReusesAncestors(t *testing.T) {
	db := NewMemoryStore()
	source, _ := NewEmpty(db)
	for i := 0; i < 256; i++ {
		if e := source.Update([]byte{byte(i), 0}, bytes.Repeat([]byte{byte(i)}, 40)); e != nil {
			t.Fatal(e)
		}
	}
	var paths [][]byte
	if e := source.WalkMaterialized(1000, func(c LogicalChange) error { paths = append(paths, cloneBytes(c.Path)); return nil }); e != nil {
		t.Fatal(e)
	}
	root, e := source.Commit()
	if e != nil {
		t.Fatal(e)
	}
	counted := &cursorReadStore{Store: db, reads: map[string]int{}}
	live, e := Open(root, counted)
	if e != nil {
		t.Fatal(e)
	}
	cursor := live.CanonicalCursor()
	for _, p := range paths {
		h, raw, ok, e := cursor.AtPath(p)
		want, wr, wok, we := source.CanonicalNodeAtPath(p)
		if e != nil || we != nil || ok != wok || h != want || !bytes.Equal(raw, wr) {
			t.Fatalf("path %x: %v %v", p, e, we)
		}
	}
	for k, n := range counted.reads {
		if n != 1 {
			t.Fatalf("node %x read %d times", k, n)
		}
	}
	// Changing the source tree does not change the cursor's pinned COW root.
	if e := live.Update([]byte{1, 0}, []byte{99}); e != nil {
		t.Fatal(e)
	}
	h, _, _, e := cursor.AtPath(nil)
	if e != nil || h != root {
		t.Fatal("cursor root changed", e)
	}
	for i := len(paths) - 1; i >= 0; i-- {
		h, raw, ok, e := cursor.AtPath(paths[i])
		want, wr, wok, we := source.CanonicalNodeAtPath(paths[i])
		if e != nil || we != nil || ok != wok || h != want || !bytes.Equal(raw, wr) {
			t.Fatal("reverse lookup", e)
		}
	}
	if _, _, _, e := cursor.AtPath([]byte{16}); e == nil {
		t.Fatal("invalid nibble")
	}
	if _, _, ok, e := cursor.AtPath([]byte{1, 1, 0}); e != nil || ok {
		t.Fatal("non-node position", e)
	}
}

func TestMaterializeLocalFieldsPreservesEncoding(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	for _, k := range [][]byte{{0x10}, {0x11}, {0x20}, {0x30, 0x10}, {0x30, 0x11}} {
		if e := tr.Update(k, []byte{1, 2, 3}); e != nil {
			t.Fatal(e)
		}
	}
	if e := tr.WalkMaterialized(100, func(c LogicalChange) error {
		fields, e := DecodeLocalNodeFields(c.RLP)
		if e != nil {
			return e
		}
		n, e := MaterializeLocalFields(fields, c.Hash, uint32(len(c.RLP)))
		if e != nil {
			return e
		}
		raw, h, e := encodeNode(n.root, nil)
		if e != nil || h != c.Hash || !bytes.Equal(raw, c.RLP) {
			t.Fatal("changed canonical bytes", e)
		}
		var work HashWork
		if _, _, e := hashCached(n.root, &work); e != nil || work.HashedNodes != 0 {
			t.Fatal("rehashed parsed host", e, work)
		}
		// Parsed fields may point into a temporary read buffer.
		for i := range fields.Value {
			fields.Value[i] ^= 0xff
		}
		again, _, e := encodeNode(n.root, nil)
		if e != nil || !bytes.Equal(again, raw) {
			t.Fatal("aliased fields", e)
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
}
