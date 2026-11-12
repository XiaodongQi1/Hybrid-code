package mpt

import (
	"bytes"
	"testing"
)

func TestLocalRLPSizeMatchesEncoding(t *testing.T) {
	for _, n := range []int{0, 1, 31, 32, 55, 56, 255, 256, 4096} {
		for _, kind := range []CanonicalKind{CanonicalBranch, CanonicalExtension, CanonicalLeaf} {
			d := CanonicalNode{Kind: kind, Path: []byte{0x20}, Value: bytes.Repeat([]byte{0x81}, n)}
			if kind == CanonicalExtension {
				d.Value = nil
			}
			d.Children[0] = CanonicalReference{Present: true, Hash: Hash{1}}
			d.Children[7] = CanonicalReference{Present: true, Inline: []byte{0xc2, 0x20, 1}}
			raw, e := d.LocalRLP()
			if e != nil || int(d.LocalRLPSize()) != len(raw) {
				t.Fatal(kind, n, len(raw), d.LocalRLPSize(), e)
			}
		}
	}
}
