package mpt

import (
	"bytes"
	"testing"
)

func TestCanonicalViewRoundtripIncludingInlineNodes(t *testing.T) {
	store := NewMemoryStore()
	tr, _ := NewEmpty(store)
	for _, kv := range [][2]string{{"abc", "123"}, {"abcd", "abcd"}, {"abc", "abc"}, {"horse", "stallion"}} {
		tr.Update([]byte(kv[0]), []byte(kv[1]))
	}
	empty, _ := NewEmpty(store)
	inline := 0
	count := 0
	stats, e := tr.DiffLogicalNodes(empty, 100, func(ch LogicalChange) error {
		desc, e := InspectCanonicalNode(ch.RLP)
		if e != nil {
			return e
		}
		raw, h, e := RebuildCanonicalNode(desc)
		if e != nil {
			return e
		}
		if !bytes.Equal(raw, ch.RLP) || h != ch.Hash {
			t.Fatal("canonical roundtrip")
		}
		local, err := DecodeLocalNodeFields(raw)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := local.LocalRLP()
		if err != nil || !bytes.Equal(parsed, raw) {
			t.Fatal("local codec disagrees with canonical RLP", err)
		}
		if len(raw) < 32 {
			inline++
		}
		count++
		return nil
	})
	if e != nil || inline == 0 || stats.Emitted != uint64(count) {
		t.Fatal(stats, inline, e)
	}
	if _, e = tr.Commit(); e != nil {
		t.Fatal(e)
	}
}
func TestCanonicalViewRejectsInvalidMetadata(t *testing.T) {
	for _, d := range []CanonicalNode{{Kind: 3}, {Kind: CanonicalExtension, Path: []byte{0x20}}, {Kind: CanonicalLeaf, Path: []byte{0}}, {Kind: CanonicalBranch, Path: []byte{0x20}}} {
		if _, _, e := RebuildCanonicalNode(d); e == nil {
			t.Fatal("accepted invalid metadata", d)
		}
	}
	d := CanonicalNode{Kind: CanonicalBranch}
	d.Children[0] = CanonicalReference{Present: true, Inline: make([]byte, 32)}
	if _, _, e := RebuildCanonicalNode(d); e == nil {
		t.Fatal("oversize inline")
	}
	d.Children[0] = CanonicalReference{Hash: Hash{1}}
	if _, _, e := RebuildCanonicalNode(d); e == nil {
		t.Fatal("absent child contains hash")
	}
	for _, raw := range [][]byte{nil, {0x80}, {0xc0}, make([]byte, MaxCanonicalNodeBytes+1)} {
		if _, e := InspectCanonicalNode(raw); e == nil {
			t.Fatal("accepted invalid RLP")
		}
	}
}
func TestLogicalDiffPrunesOnlyReachableSamePosition(t *testing.T) {
	store := NewMemoryStore()
	old, _ := NewEmpty(store)
	old.Update([]byte{0x10}, []byte{1})
	old.Update([]byte{0x11}, []byte{2})
	oldRoot, _ := old.Commit()
	a, _ := Open(oldRoot, store)
	same, e := a.DiffLogicalNodes(old, 100, func(LogicalChange) error { t.Fatal("unchanged emitted"); return nil })
	if e != nil || same.Pruned != 1 || same.Visited != 1 {
		t.Fatal(same, e)
	}
	a.Update([]byte{0x10}, []byte{3})
	a.Delete([]byte{0x11})
	intermediate, _ := a.Commit()
	before, _ := Open(intermediate, store)
	after, _ := Open(oldRoot, store)
	found := false
	stats, e := after.DiffLogicalNodes(before, 100, func(ch LogicalChange) error {
		if equalPath(ch.Path, []byte{1, 0}) {
			found = true
		}
		return nil
	})
	if e != nil || !found || stats.Emitted < 3 {
		t.Fatal("reappearing paths omitted", stats, found, e)
	}
	if _, e = after.DiffLogicalNodes(before, 1, func(LogicalChange) error { return nil }); e == nil {
		t.Fatal("budget ignored")
	}
}

func TestLogicalDiffDoesNotScanUnchangedSubtrees(t *testing.T) {
	before := cachedFixture(t, 4096, NewMemoryStore())
	before.Hash()
	after := before.Copy()
	after.Update([]byte{0, 0, 1}, bytes.Repeat([]byte{7}, 64))
	stats, e := after.DiffLogicalNodes(before, 100, func(LogicalChange) error { return nil })
	if e != nil || stats.Emitted > 7 || stats.Visited > 100 || stats.Pruned == 0 {
		t.Fatal(stats, e)
	}
	t.Logf("leaves=4096 visited=%d emitted=%d pruned=%d old_position_lookups=%d", stats.Visited, stats.Emitted, stats.Pruned, stats.OldLookups)
}

func FuzzCanonicalNodeRoundtrip(f *testing.F) {
	for _, d := range []CanonicalNode{{Kind: CanonicalLeaf, Path: []byte{0x20}, Value: []byte{1}}, {Kind: CanonicalBranch, Value: []byte{2}}} {
		raw, _, _ := RebuildCanonicalNode(d)
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		d, e := InspectCanonicalNode(raw)
		if e != nil {
			return
		}
		rebuilt, h, e := RebuildCanonicalNode(d)
		if e != nil || !bytes.Equal(raw, rebuilt) || hashBytes(raw) != h {
			t.Fatal("canonical roundtrip mismatch")
		}
	})
}

func TestCompactPathAdapterBoundaries(t *testing.T) {
	for n := 0; n <= 64; n++ {
		for _, leaf := range []bool{false, true} {
			path := make([]byte, n)
			for i := range path {
				path[i] = byte(i % 16)
			}
			raw, e := EncodeCompactPath(path, leaf)
			if n == 0 && !leaf {
				if e == nil {
					t.Fatal("empty extension accepted")
				}
				continue
			}
			if e != nil {
				t.Fatal(e)
			}
			got, isLeaf, e := DecodeCompactPath(raw)
			if e != nil || isLeaf != leaf || !bytes.Equal(got, path) {
				t.Fatal(n, leaf, e)
			}
		}
	}
	for _, path := range [][]byte{{16}, make([]byte, 65)} {
		if _, e := EncodeCompactPath(path, true); e == nil {
			t.Fatal("invalid path accepted")
		}
	}
}

func TestCanonicalNodeAtPathInspectsUncommittedTree(t *testing.T) {
	tree, _ := NewEmpty(NewMemoryStore())
	tree.Update([]byte{0x10}, []byte{1})
	tree.Update([]byte{0x11}, []byte{2})
	root, _ := tree.Hash()
	h, raw, found, e := tree.CanonicalNodeAtPath(nil)
	if e != nil || !found || h != root {
		t.Fatal(h, found, e)
	}
	raw[0] ^= 0xff
	_, fresh, found, e := tree.CanonicalNodeAtPath(nil)
	if e != nil || !found || hashBytes(fresh) != root {
		t.Fatal("canonical bytes alias hash cache", e)
	}
	_, inline, found, e := tree.CanonicalNodeAtPath([]byte{1, 0})
	if e != nil || !found || len(inline) >= 32 {
		t.Fatal("inline logical position", found, e)
	}
	if _, _, found, e := tree.CanonicalNodeAtPath([]byte{1, 2}); e != nil || found {
		t.Fatal("unreachable position", found, e)
	}
	if _, _, _, e := tree.CanonicalNodeAtPath([]byte{16}); e == nil {
		t.Fatal("invalid position")
	}
}
