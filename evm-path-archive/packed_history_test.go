package patharchive

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func packedRows(kind byte) []historyEntry {
	r := reader{format: PackedFormat}
	var rows []historyEntry
	owners := 1
	if kind%2 == 1 {
		owners = 3
	}
	for owner := 0; owner < owners; owner++ {
		o := []byte{kind % 2}
		if kind%2 == 1 {
			o = append(o, bytes.Repeat([]byte{byte(0x11 + owner)}, 32)...)
		}
		for i := 0; i < 49; i++ {
			path := []byte{byte(i / 16), byte(i % 16)}
			tag := byte('P')
			if kind < 2 {
				tag = 'F'
				path = make([]byte, 32)
				path[31] = byte(i)
			}
			// Include empty values, >256-byte values and a >chunk-target value.
			size := []int{0, 1, 700, 9000}[i%4]
			rows = append(rows, historyEntry{r.key(tag, o, path), bytes.Repeat([]byte{0xab}, size)})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	return rows
}
func TestV7OwnerHeadersOffsetsAndPointLookup(t *testing.T) {
	for kind := byte(0); kind < 4; kind++ {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			l := DefaultHistoryLayout().ForFormat(PackedFormat)
			rows := packedRows(kind)
			for _, restart := range []uint64{1, 16, 256} {
				l.RestartEntries = restart
				wire := encodePackedChunk(rows, l)
				decoded, e := decodeChunk(wire, l)
				must(t, e)
				equal(t, decoded, rows)
				groups, _, e := parsePackedChunk(wire, l)
				must(t, e)
				for _, g := range groups {
					if uint64(len(g.keyOffsets)) != (g.count+restart-1)/restart {
						t.Fatal("payload affects restart count")
					}
					if kind%2 == 1 && bytes.Count(wire, g.owner[2:]) != 1 {
						t.Fatal("owner duplicated outside group header")
					}
				}
				for _, row := range rows {
					v, e := findChunk(wire, row.key, l)
					must(t, e)
					equal(t, v, row.value)
				}
				for _, bad := range [][]byte{nil, {0}, wire[:len(wire)-1], append(bytes.Clone(wire), 0)} {
					if _, e := decodeChunk(bad, l); e == nil {
						t.Fatal("malformed chunk accepted")
					}
				}
			}
			l.RestartEntries = 16
			l.ChunkBytes = 512
			cfg := indexedConfigForFormat("codec", PackedFormat)
			cfg.HistoryLayout = &l
			store := Store{cfg: cfg}
			pending := map[string][]byte{}
			for _, row := range rows {
				pending[string(row.key)] = row.value
			}
			kv := map[string][]byte{}
			stats := Changes{}
			must(t, store.writeSections(pending, 7, func(k, v []byte) error { kv[string(k)] = bytes.Clone(v); return nil }, &stats))
			r := reader{format: PackedFormat, layout: l, ctx: context.Background(), get: func(k []byte) ([]byte, error) {
				v, ok := kv[string(k)]
				if !ok {
					return nil, ErrCorrupt
				}
				return v, nil
			}}
			for _, row := range rows {
				v, e := r.historyEntry(row.key, 7)
				must(t, e)
				equal(t, v, row.value)
			}
			bounds, e := decodePackedBounds(kv[string(sectionKey('R', 7, kind))], l)
			must(t, e)
			if len(bounds) < 2 {
				t.Fatal("multi-chunk case not exercised")
			}
			for _, b := range bounds {
				if b.key != nil {
					t.Fatal("R repeated a boundary key")
				}
			}
			key := chunkKey(7, kind, 0)
			saved := kv[string(key)]
			delete(kv, string(key))
			if _, e := r.historyEntry(rows[0].key, 7); e == nil {
				t.Fatal("missing chunk accepted")
			}
			kv[string(key)] = saved[:len(saved)-1]
			if _, e := r.historyEntry(rows[0].key, 7); e == nil {
				t.Fatal("truncated chunk accepted")
			}
		})
	}
}
func TestV7MalformedOffsetsNeverPanic(t *testing.T) {
	l := DefaultHistoryLayout().ForFormat(PackedFormat)
	seed := encodePackedChunk(packedRows(3), l)
	rng := rand.New(rand.NewSource(7007))
	for i := 0; i < 2000; i++ {
		var b []byte
		if i%2 == 0 {
			b = bytes.Clone(seed)
			b[rng.Intn(len(b))] ^= byte(1 + rng.Intn(255))
		} else {
			b = make([]byte, rng.Intn(128))
			rng.Read(b)
			if len(b) > 0 {
				b[0] = 0
			}
		}
		rows, e := decodeChunk(b, l)
		if e == nil {
			// Any accepted mutation must still be a canonical, internally consistent
			// key/offset encoding; payload mutation is rejected by the proof layer.
			if !bytes.Equal(encodePackedChunk(rows, l), b) {
				t.Fatal("accepted noncanonical offsets")
			}
		}
		decodePackedBounds(b, l)
	}
}
