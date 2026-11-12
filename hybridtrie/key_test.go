package hybridtrie

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"sort"
	"testing"
)

func TestNodeKeyGoldenAndLimits(t *testing.T) {
	raw, err := EncodeNodeKey(NodeKey{0x010203040506, []byte{10, 11, 12}, 0xab0203040506})
	if err != nil || hex.EncodeToString(raw) != "01020304050603abc0ab0203040506" {
		t.Fatal(hex.EncodeToString(raw), err)
	}
	for depth := 0; depth <= 64; depth++ {
		path := make([]byte, depth)
		for i := range path {
			path[i] = byte(i % 16)
		}
		for _, h := range []uint64{0, 1, math.MaxUint32, math.MaxUint32 + 1, (1 << 44) - 1, 1 << 44, MaxHeight - 1, MaxHeight} {
			k := NodeKey{math.MaxUint32, path, h}
			raw, e := EncodeNodeKey(k)
			if e != nil {
				t.Fatal(e)
			}
			got, e := DecodeNodeKey(raw)
			if e != nil || got.TrieID != k.TrieID || got.Height != h || !bytes.Equal(got.Path, path) || len(raw) != 13+(depth+1)/2 {
				t.Fatal(depth, h, got, e)
			}
		}
	}
	for _, path := range [][]byte{{16}, make([]byte, 65)} {
		if _, e := EncodeNodeKey(NodeKey{Path: path}); e == nil {
			t.Fatal("invalid path accepted")
		}
	}
	bad := append([]byte(nil), raw...)
	bad[8] |= 1
	for _, b := range [][]byte{nil, raw[:8], append(raw, 0), bad} {
		if _, e := DecodeNodeKey(b); e == nil {
			t.Fatalf("invalid key accepted: %x", b)
		}
	}
}

func TestPredecessorPositionIsolation(t *testing.T) {
	var keys [][]byte
	paths := [][]byte{nil, {10, 11, 12}, {10, 11, 12, 0}, {10, 11, 13}}
	for _, id := range []uint64{0, 1, 2, 1<<32 + 1, MaxTrieID} {
		for _, p := range paths {
			for _, h := range []uint64{0, 100, 200, 400, math.MaxUint32 + 1, MaxHeight} {
				k, _ := EncodeNodeKey(NodeKey{id, p, h})
				keys = append(keys, k)
			}
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	target := NodeKey{1, []byte{10, 11, 12}, 220}
	raw, _ := EncodeNodeKey(target)
	i := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], raw) > 0 }) - 1
	got, e := MatchPredecessor(keys[i], target)
	if e != nil || got.Height != 200 {
		t.Fatal(got, e)
	}
	for _, wrong := range []NodeKey{{2, target.Path, 200}, {1, []byte{10, 11, 12, 0}, 200}, {1, target.Path, 400}, {1, nil, 200}} {
		b, _ := EncodeNodeKey(wrong)
		if _, e := MatchPredecessor(b, target); e == nil {
			t.Fatal("neighbor accepted", wrong)
		}
	}
}

func FuzzNodeKey(f *testing.F) {
	b, _ := EncodeNodeKey(NodeKey{1, []byte{1, 2, 3}, MaxHeight})
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		k, e := DecodeNodeKey(b)
		if e != nil {
			return
		}
		out, e := EncodeNodeKey(k)
		if e != nil || !bytes.Equal(b, out) {
			t.Fatal("noncanonical key")
		}
	})
}

func TestExternalHeightConversion(t *testing.T) {
	path := []byte{1, 2, 3}
	key, err := NewNodeKey(7, path, MaxHeight)
	if err != nil || key.Height != MaxHeight {
		t.Fatal(key, err)
	}
	path[0] = 9
	if key.Path[0] != 1 {
		t.Fatal("constructor aliases caller path")
	}
	for _, height := range []uint64{MaxHeight + 1, 1 << 63, math.MaxUint64} {
		if _, err := NewNodeKey(7, nil, height); !errors.Is(err, ErrLimit) {
			t.Fatal(height, err)
		}
		if _, err := EncodeNodeKey(NodeKey{Height: height}); !errors.Is(err, ErrLimit) {
			t.Fatal("unchecked height encoding", height, err)
		}
		if _, err := MatchPredecessor(nil, NodeKey{Height: height}); !errors.Is(err, ErrLimit) {
			t.Fatal("unchecked seek target", height, err)
		}
	}
	if _, err := NewNodeKey(7, []byte{16}, 1); err == nil {
		t.Fatal("invalid path accepted")
	}
	for _, oldHex := range []string{"0102030403abc001020304", "0102030403abc0ab02030405060708"} {
		old, _ := hex.DecodeString(oldHex)
		if _, err := DecodeNodeKey(old); err == nil {
			t.Fatal("old height layout accepted", oldHex)
		}
	}
}

func TestFull48BitHeight(t *testing.T) {
	raw, err := EncodeNodeKey(NodeKey{Height: MaxHeight})
	if err != nil || !bytes.Equal(raw[len(raw)-HeightBytes:], []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff}) {
		t.Fatal(raw, err)
	}
	for nibble := uint64(0); nibble < 16; nibble++ {
		height := nibble<<44 | (1 << 44) - 1
		raw, err := EncodeNodeKey(NodeKey{Height: height})
		if err != nil {
			t.Fatal(height, err)
		}
		got, err := DecodeNodeKey(raw)
		if err != nil || got.Height != height {
			t.Fatal("high nibble not preserved", nibble, got.Height, err)
		}
		if _, err = MatchPredecessor(raw, NodeKey{Height: height}); err != nil {
			t.Fatal("high nibble rejected by seek", nibble, err)
		}
	}
	heights := []uint64{0, 1, 255, 256, math.MaxUint32, math.MaxUint32 + 1, (1 << 44) - 1, 1 << 44, MaxHeight - 1, MaxHeight}
	var previous []byte
	for _, h := range heights {
		raw, e := EncodeNodeKey(NodeKey{Height: h})
		if e != nil {
			t.Fatal(e)
		}
		if previous != nil && bytes.Compare(previous, raw) >= 0 {
			t.Fatal("height byte order", h)
		}
		previous = raw
	}
}

func TestFull48BitTrieNamespacesAndAccountPadding(t *testing.T) {
	for _, id := range []uint64{0, 1, 1<<32 - 1, 1 << 32, 1 << 44, MaxTrieID} {
		raw, err := EncodeNodeKey(NodeKey{TrieID: id, Path: make([]byte, 64), Height: MaxHeight})
		if err != nil || len(raw) != 45 || read48(raw[:TrieIDBytes]) != id {
			t.Fatal(id, len(raw), err)
		}
		decoded, err := DecodeNodeKey(raw)
		if err != nil || decoded.TrieID != id {
			t.Fatal(decoded, err)
		}
	}
	for _, id := range []uint64{MaxTrieID + 1, 1 << 63, math.MaxUint64} {
		if _, err := NewNodeKey(id, nil, 0); !errors.Is(err, ErrLimit) {
			t.Fatal("constructor truncated ID", id, err)
		}
		if _, err := EncodeNodeKey(NodeKey{TrieID: id}); !errors.Is(err, ErrLimit) {
			t.Fatal("encoder truncated ID", id, err)
		}
		if _, err := MatchPredecessor(nil, NodeKey{TrieID: id}); !errors.Is(err, ErrLimit) {
			t.Fatal("seek target truncated ID", id, err)
		}
	}
	account, _ := EncodeNodeKey(NodeKey{Height: 256})
	storage, _ := EncodeNodeKey(NodeKey{TrieID: 1, Height: 1})
	if len(account) != 13 || !bytes.Equal(account[:TrieIDBytes], make([]byte, TrieIDBytes)) || bytes.Equal(account, storage) {
		t.Fatal("implicit namespace field missing")
	}
	var keys [][]byte
	for _, id := range []uint64{0, 1} {
		for _, height := range []uint64{1, 256, 258, MaxHeight} {
			raw, _ := EncodeNodeKey(NodeKey{TrieID: id, Height: height})
			keys = append(keys, raw)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
	target := NodeKey{Height: 257}
	seek, _ := EncodeNodeKey(target)
	i := sort.Search(len(keys), func(i int) bool { return bytes.Compare(keys[i], seek) > 0 }) - 1
	got, err := MatchPredecessor(keys[i], target)
	if err != nil || got.TrieID != 0 || got.Height != 256 {
		t.Fatal("storage namespace interrupted account versions", got, err)
	}
}
