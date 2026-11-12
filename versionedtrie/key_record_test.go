package versionedtrie

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestNodeKeyCanonicalBoundaries(t *testing.T) {
	for depth := 0; depth <= 64; depth++ {
		p := make([]byte, depth)
		for i := range p {
			p[i] = byte(i % 16)
		}
		for _, id := range []uint64{0, 1, math.MaxUint32} {
			for _, h := range []uint64{0, 1, 255, 256, 65535, 65536, math.MaxUint32} {
				raw, e := EncodeNodeKey(id, p, h)
				if e != nil {
					t.Fatal(e)
				}
				k, e := DecodeNodeKey(raw)
				if e != nil || k.TrieID != uint32(id) || k.Height != uint32(h) || !bytes.Equal(k.Path, p) {
					t.Fatal(k, e)
				}
				if len(raw) != 10+(depth+1)/2 {
					t.Fatal("key width")
				}
			}
		}
	}
	for _, spec := range []struct {
		id uint64
		p  []byte
		h  uint64
	}{{1 << 32, nil, 0}, {0, nil, 1 << 32}, {0, make([]byte, 65), 0}, {0, []byte{16}, 0}} {
		if _, e := EncodeNodeKey(spec.id, spec.p, spec.h); e == nil {
			t.Fatal("accepted bad key")
		}
	}
	key, _ := EncodeNodeKey(0, []byte{1}, 0)
	key[6] |= 1
	if _, e := DecodeNodeKey(key); e == nil {
		t.Fatal("noncanonical padding")
	}
	key, _ = EncodeNodeKey(0, nil, 0)
	for n := 0; n < len(key); n++ {
		if _, e := DecodeNodeKey(key[:n]); e == nil {
			t.Fatal("truncated key")
		}
	}
}
func TestRecordStrictLayout(t *testing.T) {
	records := []Record{{Kind: Branch, Bitmap: 7}, {Kind: Branch, Value: []byte("branch value")}, {Kind: Extension, Path: []byte{0x11}}, {Kind: Leaf, Path: []byte{0x20}, Value: []byte{1}, Inline: true}, {Kind: Leaf, Path: []byte{0x31}, Value: bytes.Repeat([]byte{2}, 1024)}}
	for _, r := range records {
		raw, e := EncodeRecord(r)
		if e != nil {
			t.Fatal(e)
		}
		back, e := DecodeRecord(raw)
		if e != nil || !reflect.DeepEqual(back, r) {
			t.Fatal("roundtrip", back, r, e)
		}
		raw[34] |= 0x80
		if _, e = DecodeRecord(raw); e == nil {
			t.Fatal("reserved flag accepted")
		}
	}
	branch, _ := EncodeRecord(records[0])
	if len(branch) != 35 {
		t.Fatal("public header changed")
	}
	key, _ := EncodeNodeKey(1, []byte{1, 2, 3, 4, 5}, 0)
	if len(key)+len(branch) != 48 {
		t.Fatal("record accounting")
	}
	for _, r := range []Record{{Kind: Kind(3)}, {Kind: Leaf, Path: []byte{0x20}}, {Kind: Leaf, Path: []byte{0}, Value: []byte{1}}, {Kind: Leaf, Bitmap: 1, Path: []byte{0x20}, Value: []byte{1}}, {Kind: Extension, Path: []byte{0}}, {Kind: Extension, Path: []byte{0x11}, Value: []byte{1}}, {Kind: Branch, Path: []byte{0x20}}} {
		if _, e := EncodeRecord(r); e == nil {
			t.Fatal("invalid record", r)
		}
	}
	leaf, _ := EncodeRecord(records[3])
	for i := 0; i < len(leaf); i++ {
		if _, e := DecodeRecord(leaf[:i]); e == nil {
			t.Fatal("truncation", i)
		}
	}
	for _, raw := range [][]byte{append(append([]byte(nil), branch...), 0), append(append([]byte(nil), leaf...), 0), bytes.Repeat([]byte{0}, MaxRecordBytes+1)} {
		if _, e := DecodeRecord(raw); e == nil {
			t.Fatal("bad record length")
		}
	}
	bad := append([]byte(nil), branch...)
	bad[34] = 8
	if _, e := DecodeRecord(bad); e == nil {
		t.Fatal("missing branch value")
	}
	for _, life := range []Lifecycle{{}, {true, 1}, {true, math.MaxUint32}} {
		b, e := encodeLife(life)
		back, e2 := decodeLife(b)
		if e != nil || e2 != nil || back != life {
			t.Fatal(life, e, e2)
		}
	}
	for _, b := range [][]byte{nil, {0, 0}, {1}, {1, 0, 0, 0, 0}, {2}, {1, 0, 0, 0, 1, 0}} {
		if _, e := decodeLife(b); e == nil {
			t.Fatal("bad lifecycle")
		}
	}
}
func FuzzDecodeNodeKey(f *testing.F) {
	for _, p := range [][]byte{nil, {1}, {1, 2, 3}, make([]byte, 64)} {
		k, _ := EncodeNodeKey(1, p, 1)
		f.Add(k)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		k, e := DecodeNodeKey(b)
		if e != nil {
			return
		}
		wire, e := EncodeNodeKey(uint64(k.TrieID), k.Path, uint64(k.Height))
		if e != nil || !bytes.Equal(b, wire) {
			t.Fatal("noncanonical accepted")
		}
	})
}
func FuzzDecodeRecord(f *testing.F) {
	for _, r := range []Record{{Kind: Branch}, {Kind: Leaf, Path: []byte{0x20}, Value: []byte{1}}} {
		b, _ := EncodeRecord(r)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r, e := DecodeRecord(b)
		if e != nil {
			return
		}
		wire, e := EncodeRecord(r)
		if e != nil || !bytes.Equal(wire, b) {
			t.Fatal("noncanonical accepted")
		}
	})
}
func TestPredecessorAgainstReference(t *testing.T) {
	cfg := DefaultConfig("legacy-raw105", "random-seek")
	s, e := Open(t.TempDir()+"/db", cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	rng := rand.New(rand.NewSource(20261031))
	paths := [][]byte{nil, {0}, {0, 0}, {0, 0, 0}, {1}, {1, 0}, {15}, make([]byte, 64)}
	for d := 0; d < 65; d++ {
		p := make([]byte, d)
		for i := range p {
			p[i] = byte(rng.Intn(16))
		}
		paths = append(paths, p)
	}
	// Use direct physical writes to cover random histories independently of the
	// sequential block coordinator. No logical MPT validity is asserted here.
	histories := map[string][]uint32{}
	batch := new(leveldb.Batch)
	for id := uint64(0); id < 3; id++ {
		for _, p := range paths {
			head, _ := nodeHeader(id, p)
			heights := []uint32{10, 256, 65536, math.MaxUint32}
			histories[string(head)] = heights
			for _, h := range heights {
				k, _ := EncodeNodeKey(id, p, uint64(h))
				batch.Put(k, be32(h))
			}
		}
	}
	if e = s.db.Write(batch, nil); e != nil {
		t.Fatal(e)
	}
	count := 0
	for header, heights := range histories {
		for _, target := range []uint32{0, 9, 10, 11, 255, 256, 257, 65535, 65536, math.MaxUint32} {
			var want uint32
			found := false
			for _, h := range heights {
				if h <= target {
					want = h
					found = true
				}
			}
			got, value, e := predecessor(s.db, []byte(header), target)
			if !found {
				if !errors.Is(e, ErrNotFound) {
					t.Fatal(e)
				}
			} else if e != nil || got != want || !bytes.Equal(value, be32(want)) {
				t.Fatal("incorrect predecessor", got, want, e)
			}
			count++
		}
	}
	t.Logf("checked %d predecessor queries across depths, prefixes, IDs and height boundaries", count)
}
