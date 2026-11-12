package mpt

import (
	"bytes"
	"testing"
)

func TestNodeAtPathExactAndEmbedded(t *testing.T) {
	store := NewMemoryStore()
	tr, _ := NewEmpty(store)
	for i := 0; i < 80; i++ {
		if e := tr.Update([]byte{byte(i), 0x42}, bytes.Repeat([]byte{byte(i)}, 100)); e != nil {
			t.Fatal(e)
		}
	}
	root, e := tr.Commit()
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	e = WalkPaths(root, store, func(p []byte, h Hash, b []byte) (bool, error) {
		got, blob, ok, e := NodeAtPath(root, p, store)
		if e != nil || !ok || got != h || !bytes.Equal(blob, b) {
			t.Fatalf("path %x: %v", p, e)
		}
		count++
		return true, nil
	}, nil)
	if e != nil || count < 80 {
		t.Fatal(e, count)
	}
	if _, _, _, e = NodeAtPath(root, []byte{16}, store); e == nil {
		t.Fatal("invalid nibble")
	}
	if _, _, ok, e := NodeAtPath(root, []byte{15, 15, 15, 15}, store); e != nil || ok {
		t.Fatal("absent path")
	}
}
