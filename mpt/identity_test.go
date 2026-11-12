package mpt

import (
	"bytes"
	"testing"
)

func TestNodeIdentitySeparatesStructureFromContent(t *testing.T) {
	leaf := func(path []byte, value byte) CanonicalNode {
		hp, err := EncodeCompactPath(path, true)
		if err != nil {
			t.Fatal(err)
		}
		return CanonicalNode{Kind: CanonicalLeaf, Path: hp, Value: bytes.Repeat([]byte{value}, 64)}
	}
	extension := func(path []byte, h byte) CanonicalNode {
		hp, err := EncodeCompactPath(path, false)
		if err != nil {
			t.Fatal(err)
		}
		d := CanonicalNode{Kind: CanonicalExtension, Path: hp}
		d.Children[0] = CanonicalReference{Present: true, Hash: Hash{h}}
		return d
	}
	branch := CanonicalNode{Kind: CanonicalBranch}
	branch.Children[1] = CanonicalReference{Present: true, Hash: Hash{1}}
	moreChildren := branch
	moreChildren.Children[2] = CanonicalReference{Present: true, Hash: Hash{2}}
	for _, tc := range []struct {
		name string
		a, b CanonicalNode
		same bool
	}{
		{"leaf value", leaf([]byte{1, 2}, 1), leaf([]byte{1, 2}, 2), true},
		{"leaf suffix", leaf([]byte{1, 2}, 1), leaf([]byte{1, 3}, 1), false},
		{"extension digest", extension([]byte{1, 2}, 1), extension([]byte{1, 2}, 2), true},
		{"extension fragment", extension([]byte{1, 2}, 1), extension([]byte{1}, 1), false},
		{"branch occupancy", branch, moreChildren, true},
		{"kind", leaf([]byte{1, 2}, 1), extension([]byte{1, 2}, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _, err := RebuildCanonicalNode(tc.a)
			if err != nil {
				t.Fatal(err)
			}
			b, _, err := RebuildCanonicalNode(tc.b)
			if err != nil {
				t.Fatal(err)
			}
			x, err := InspectNodeIdentity(a)
			if err != nil {
				t.Fatal(err)
			}
			y, err := InspectNodeIdentity(b)
			if err != nil {
				t.Fatal(err)
			}
			if (x == y) != tc.same {
				t.Fatal(x, y)
			}
		})
	}
	for _, raw := range [][]byte{nil, {0x80}, {0xc0}, make([]byte, MaxCanonicalNodeBytes+1)} {
		if _, err := InspectNodeIdentity(raw); err == nil {
			t.Fatal("invalid identity input accepted")
		}
	}
}
