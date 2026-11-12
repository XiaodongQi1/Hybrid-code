package hybridtrie

import (
	"bytes"
	"errors"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func leafRLP(t *testing.T, fragment []byte, valueLen int) []byte {
	t.Helper()
	hp, e := mpt.EncodeCompactPath(fragment, true)
	if e != nil {
		t.Fatal(e)
	}
	raw, _, e := mpt.RebuildCanonicalNode(mpt.CanonicalNode{Kind: mpt.CanonicalLeaf, Path: hp, Value: bytes.Repeat([]byte{1}, valueLen)})
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func roundtrip(t *testing.T, r Record) Record {
	t.Helper()
	b, e := EncodeRecord(r)
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeRecord(b)
	if e != nil {
		t.Fatal(e)
	}
	again, e := EncodeRecord(got)
	if e != nil || !bytes.Equal(b, again) {
		t.Fatal("wire roundtrip", e)
	}
	return got
}

func TestBranchWireGoldenOffsets(t *testing.T) {
	r := Record{Kind: mpt.CanonicalBranch, Hash: mpt.Hash{9}}
	for _, i := range []int{2, 7, 12} {
		r.Children[i] = Reference{Kind: Hash, Hash: mpt.Hash{byte(i)}}
	}
	b, e := EncodeRecord(r)
	if e != nil || len(b) != 133 {
		t.Fatal(len(b), e)
	}
	if b[0] != 0 || b[1] != 9 || !bytes.Equal(b[33:37], []byte{0, 0, 0x10, 0x84}) {
		t.Fatal("header", b[:37])
	}
	for j, i := range []int{2, 7, 12} {
		off, e := ExplicitHashOffset(0x1084, i)
		if e != nil || off != 37+j*32 || b[off] != byte(i) {
			t.Fatal(i, off, e)
		}
	}
	for _, i := range []int{-1, 0, 16} {
		if _, e := ExplicitHashOffset(0x1084, i); e == nil {
			t.Fatal(i)
		}
	}
	roundtrip(t, r)
}

func TestRecordTypesAndOwnership(t *testing.T) {
	inline := leafRLP(t, nil, 1)
	cases := []Record{
		{Kind: mpt.CanonicalLeaf, Fragment: []byte{10, 11, 12}, Value: []byte{7}},
		{Kind: mpt.CanonicalExtension, Fragment: []byte{10, 11, 12}, Children: [16]Reference{{Kind: Prefix}}},
		{Kind: mpt.CanonicalExtension, Fragment: []byte{10}, Children: [16]Reference{{Kind: Hash, Hash: mpt.Hash{3}}}},
		{Kind: mpt.CanonicalExtension, Fragment: []byte{10}, Children: [16]Reference{{Kind: Embed, Inline: inline}}},
		{Kind: mpt.CanonicalBranch, Value: []byte{4}, Children: [16]Reference{{Kind: Prefix}, {Kind: Hash, Hash: mpt.Hash{2}}, {Kind: Embed, Inline: inline}}},
		{Kind: mpt.CanonicalBranch, Value: []byte{5}},
	}
	for _, r := range cases {
		b, e := EncodeRecord(r)
		if e != nil {
			t.Fatal(e)
		}
		got := roundtrip(t, r)
		for i := range b {
			b[i] = 0xff
		}
		if _, e = EncodeRecord(got); e != nil {
			t.Fatal("decoder aliases input", e)
		}
	}
	b, _ := EncodeRecord(cases[0])
	if len(b) != 41 || !bytes.Equal(b[33:], []byte{3, 0xab, 0xc0, 0, 0, 0, 1, 7}) {
		t.Fatal("leaf golden", b)
	}
	b, _ = EncodeRecord(cases[1])
	if !bytes.Equal(b[33:], []byte{3, 0xab, 0xc0}) || b[0] != 5 {
		t.Fatal("extension golden", b)
	}
}

func TestRecordRejectsMalformedInput(t *testing.T) {
	r := Record{Kind: mpt.CanonicalBranch, Children: [16]Reference{{Kind: Prefix}, {Kind: Hash, Hash: mpt.Hash{3}}, {Kind: Embed, Inline: leafRLP(t, nil, 1)}}}
	b, _ := EncodeRecord(r)
	for n := 0; n < len(b); n++ {
		if _, e := DecodeRecord(b[:n]); e == nil {
			t.Fatal("truncation accepted", n)
		}
	}
	mutations := []func([]byte){
		func(b []byte) { b[0] |= 0x80 }, func(b []byte) { b[0] |= 4 }, func(b []byte) { b[34] |= 2 },
		func(b []byte) { b[70] |= 1 }, func(b []byte) { b[71] = 32 },
	}
	for i, mut := range mutations {
		bad := append([]byte(nil), b...)
		mut(bad)
		if _, e := DecodeRecord(bad); e == nil {
			t.Fatal("mutation accepted", i)
		}
	}
	if _, e := DecodeRecord(append(b, 0)); e == nil {
		t.Fatal("trailing byte accepted")
	}
	leaf := Record{Kind: mpt.CanonicalLeaf, Fragment: []byte{1}, Value: []byte{1}}
	bad, _ := EncodeRecord(leaf)
	bad[34] |= 1
	if _, e := DecodeRecord(bad); e == nil {
		t.Fatal("padding accepted")
	}
	for _, invalid := range []Record{
		{Kind: 3}, {Kind: mpt.CanonicalLeaf}, {Kind: mpt.CanonicalExtension},
		{Kind: mpt.CanonicalBranch, Fragment: []byte{1}},
		{Kind: mpt.CanonicalBranch, Children: [16]Reference{{Hash: mpt.Hash{1}}}},
		{Kind: mpt.CanonicalBranch, Children: [16]Reference{{Kind: Prefix, Hash: mpt.Hash{1}}}},
		{Kind: mpt.CanonicalBranch, Children: [16]Reference{{Kind: Embed, Inline: make([]byte, 32)}}},
		{Kind: mpt.CanonicalLeaf, Value: []byte{1}, Children: [16]Reference{{Kind: Hash}}},
	} {
		if _, e := EncodeRecord(invalid); e == nil {
			t.Fatal("invalid record", invalid)
		}
	}
	if _, e := DecodeRecord(make([]byte, MaxRecordBytes+1)); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
}

func TestCanonicalLayoutsAndAuthentication(t *testing.T) {
	desc := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
	desc.Children[1] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{1}}
	desc.Children[7] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{7}}
	desc.Children[12] = mpt.CanonicalReference{Present: true, Inline: leafRLP(t, nil, 1)}
	raw, hash, _ := mpt.RebuildCanonicalNode(desc)
	ctx := NodeKey{7, []byte{10}, 123}
	for _, mask := range []uint16{0, 2, 128, 130} {
		r, e := FromCanonical(ctx, raw, mask)
		if e != nil {
			t.Fatal(e)
		}
		r = roundtrip(t, r)
		calls := 0
		rebuilt, h, e := RebuildCanonical(ctx, r, func(loc ChildLocator) (mpt.Hash, error) {
			calls++
			i := loc.Target.Path[len(loc.Target.Path)-1]
			if loc.Kind != Prefix || loc.Target.TrieID != 7 || loc.Target.Height != 123 {
				t.Fatal(loc)
			}
			return desc.Children[i].Hash, nil
		})
		if e != nil || h != hash || !bytes.Equal(raw, rebuilt) {
			t.Fatal(mask, e)
		}
		want := 0
		for _, i := range []int{1, 7} {
			if mask&(1<<i) != 0 {
				want++
			}
		}
		if calls != want {
			t.Fatal(calls, want)
		}
	}
	if _, e := FromCanonical(ctx, raw, 1<<12); e == nil {
		t.Fatal("embedded selected as prefix")
	}
	r, _ := FromCanonical(ctx, raw, 2)
	if _, _, e := RebuildCanonical(ctx, r, nil); !errors.Is(e, ErrUnresolvedPrefix) {
		t.Fatal(e)
	}
	sentinel := errors.New("missing referenced child")
	if _, _, e := RebuildCanonical(ctx, r, func(ChildLocator) (mpt.Hash, error) { return mpt.Hash{}, sentinel }); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if _, _, e := RebuildCanonical(ctx, r, func(ChildLocator) (mpt.Hash, error) { return mpt.Hash{99}, nil }); !errors.Is(e, ErrHashMismatch) {
		t.Fatal(e)
	}
	r, _ = FromCanonical(ctx, raw, 0)
	r.Hash[0] ^= 1
	if _, _, e := RebuildCanonical(ctx, r, nil); !errors.Is(e, ErrHashMismatch) {
		t.Fatal(e)
	}
}

func TestInlineThresholdAndShortRoot(t *testing.T) {
	for _, size := range []int{31, 32, 33} {
		raw := leafRLP(t, nil, size-3)
		if len(raw) != size {
			t.Fatal(len(raw), size)
		}
		desc := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
		_, h, _ := mpt.RebuildCanonicalNode(mustInspect(t, raw))
		ref := mpt.CanonicalReference{Present: true, Hash: h}
		if size < 32 {
			ref = mpt.CanonicalReference{Present: true, Inline: raw}
		}
		desc.Children[0] = ref
		parent, _, e := mpt.RebuildCanonicalNode(desc)
		if e != nil {
			t.Fatal(e)
		}
		r, e := FromCanonical(NodeKey{}, parent, 0)
		if e != nil {
			t.Fatal(e)
		}
		want := Hash
		if size < 32 {
			want = Embed
		}
		if r.Children[0].Kind != want {
			t.Fatal(size, r.Children[0])
		}
		_, e = FromCanonical(NodeKey{Path: []byte{0}}, raw, 0)
		if size < 32 && !errors.Is(e, ErrEmbeddedNode) || size >= 32 && e != nil {
			t.Fatal(size, e)
		}
	}
	short := leafRLP(t, nil, 1)
	r, e := FromCanonical(NodeKey{}, short, 0)
	if e != nil {
		t.Fatal(e)
	}
	out, _, e := RebuildCanonical(NodeKey{}, r, nil)
	if e != nil || !bytes.Equal(out, short) {
		t.Fatal(e)
	}
}
func mustInspect(t *testing.T, raw []byte) mpt.CanonicalNode {
	t.Helper()
	d, e := mpt.InspectCanonicalNode(raw)
	if e != nil {
		t.Fatal(e)
	}
	return d
}

func TestLocatorExtensionAndInlineDepth(t *testing.T) {
	ctx := NodeKey{2, []byte{10, 11}, 999}
	r := Record{Kind: mpt.CanonicalExtension, Fragment: []byte{12, 13}, Children: [16]Reference{{Kind: Prefix}}}
	loc, e := LocateChild(ctx, r, 0)
	if e != nil || !bytes.Equal(loc.Target.Path, []byte{10, 11, 12, 13}) || loc.Target.Height != 999 {
		t.Fatal(loc, e)
	}
	loc.Target.Path[0] = 0
	if ctx.Path[0] != 10 {
		t.Fatal("alias")
	}
	if _, e := LocateChild(ctx, r, 1); e == nil {
		t.Fatal("extension slot")
	}
	r.Fragment = bytes.Repeat([]byte{1}, 63)
	if _, e := LocateChild(ctx, r, 0); e == nil {
		t.Fatal("depth overflow")
	}
	r = Record{Kind: mpt.CanonicalBranch, Children: [16]Reference{{Kind: Embed, Inline: leafRLP(t, []byte{1}, 1)}}}
	if e = r.ValidateAt(NodeKey{Path: make([]byte, 63)}); e == nil {
		t.Fatal("inline descendant exceeds key depth")
	}
}

func FuzzRecord(f *testing.F) {
	for _, r := range []Record{{Kind: mpt.CanonicalLeaf, Value: []byte{1}}, {Kind: mpt.CanonicalBranch}, {Kind: mpt.CanonicalExtension, Fragment: []byte{1}, Children: [16]Reference{{Kind: Hash}}}} {
		b, _ := EncodeRecord(r)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r, e := DecodeRecord(b)
		if e != nil {
			return
		}
		out, e := EncodeRecord(r)
		if e != nil || !bytes.Equal(b, out) {
			t.Fatal("noncanonical record")
		}
	})
}
