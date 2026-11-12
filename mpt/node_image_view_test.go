package mpt

import (
	"bytes"
	"testing"
)

func TestBorrowedImageMaterializationDetaches(t *testing.T) {
	d := CanonicalNode{Kind: CanonicalLeaf, Value: bytes.Repeat([]byte{19}, 110)}
	d.Path, _ = EncodeCompactPath(bytes.Repeat([]byte{7}, 64), true)
	raw, h, e := RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	var buf [256]byte
	image, e := CaptureLocalFieldsImage(buf[:], d, h, uint32(len(raw)))
	if e != nil {
		t.Fatal(e)
	}
	view := ViewNodeImage(buf[:image.DataBytes()], h, uint32(len(raw)))
	detached, e := view.Materialize()
	if e != nil {
		t.Fatal(e)
	}
	for i := range buf {
		buf[i] = 0xff
	}
	got, hash, e := encodeNode(detached.root, nil)
	if e != nil || hash != h || !bytes.Equal(raw, got) {
		t.Fatal("materialized node retained mutable arena", e)
	}
}
func TestImageIndependentChildMask(t *testing.T) {
	d := CanonicalNode{Kind: CanonicalBranch}
	leaf := CanonicalNode{Kind: CanonicalLeaf, Value: []byte{1}}
	leaf.Path, _ = EncodeCompactPath(nil, true)
	inline, _, e := RebuildCanonicalNode(leaf)
	if e != nil {
		t.Fatal(e)
	}
	d.Children[1] = CanonicalReference{Present: true, Hash: Hash{17}}
	d.Children[3] = CanonicalReference{Present: true, Inline: inline}
	d.Children[15] = CanonicalReference{Present: true, Hash: Hash{31}}
	raw, h, e := RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	var buf [804]byte
	image, e := CaptureLocalFieldsImage(buf[:], d, h, uint32(len(raw)))
	if e != nil {
		t.Fatal(e)
	}
	if image.IndependentChildren() != (1<<1 | 1<<15) {
		t.Fatal(image.IndependentChildren())
	}
}
