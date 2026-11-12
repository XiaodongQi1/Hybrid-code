package patharchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

func TestH4GroupCodecAndIndexBoundaries(t *testing.T) {
	owner := []byte{'P', 0}
	var rows []historyEntry
	r := reader{format: Format}
	for i := 0; i < 40; i++ {
		k := r.key('P', []byte{0}, []byte{1, 2, byte(i / 16), byte(i % 16)})
		rows = append(rows, historyEntry{k, []byte{0}})
	}
	wire := encodeGroup(rows, len(owner))
	decoded, e := decodeGroup(owner, wire, 40)
	must(t, e)
	equal(t, decoded, rows)
	if _, e = decodeGroup(owner, wire, 39); e == nil {
		t.Fatal("group bound ignored")
	}
	for _, bad := range [][]byte{nil, {1, 1, 1, 0, 0}, {0x80, 0, 0, 0}, {0, 0, 99}, append(encodeGroup(rows[:1], 2), encodeGroup(rows[:1], 2)...)} {
		if _, e = decodeGroup(owner, bad, 40); e == nil {
			t.Fatalf("malformed group accepted %x", bad)
		}
	}
	gk := groupKey(owner, 256, 17)
	o, h, c, e := parseGroupKey(gk)
	must(t, e)
	equal(t, o, owner)
	if h != 256 || c != 17 {
		t.Fatal("key parse")
	}
	for _, bad := range [][]byte{nil, {'G'}, {'G', 'P', 0, 0}, {'G', 'P', 0, 1, 0, 0}, append(gk, 0)} {
		if _, _, _, e = parseGroupKey(bad); e == nil {
			t.Fatal("bad key accepted")
		}
	}
	b := binary.AppendUvarint(nil, 1)
	b = binary.AppendUvarint(b, 0)
	b = binary.AppendUvarint(b, 63)
	b = binary.AppendUvarint(b, 17)
	loc, e := decodeLocations(b, 4, 64)
	must(t, e)
	equal(t, loc, []historyLocation{{256, 0}, {319, 17}})
	for _, bad := range [][]byte{nil, {0, 0}, {65, 0}, {1}, {0x81, 0, 0}, {1, 0, 64, 0}} {
		if _, e = decodeLocations(bad, 0, 64); e == nil {
			t.Fatal("bad index accepted")
		}
	}
}
func TestH4HistoryRestartAndGroups(t *testing.T) {
	w, addresses, slots := dataset(t)
	roots, outcomes := runOracle(t, w, addresses, slots, DefaultConfig("h4").Chain)
	for _, opts := range [][3]uint64{{1, 2, 1}, {4, 4, 2}, {16, 64, 8}} {
		t.Run(fmt.Sprint(opts), func(t *testing.T) {
			cfg := DefaultConfig("h4")
			cfg.Format = Format
			cfg.HistoryGroupEntries = opts[0]
			cfg.HistoryIndexSpan = opts[1]
			cfg.HistoryNodeCheckpointInterval = opts[2]
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			var pinned diskSnapshot
			for i, b := range w.Blocks {
				rec, e := a.ExecuteBlock(context.Background(), b)
				must(t, e)
				equal(t, rec.Root, roots[i+1])
				equal(t, rec.Outcomes, outcomes[i])
				if i == 4 {
					must(t, a.Close())
					a, e = Open(path, cfg, false, false)
					must(t, e)
					pinned, e = a.s.db.GetSnapshot()
					must(t, e)
				}
			}
			pr := snapshotReader(context.Background(), pinned, Format, 3)
			pr.limit = cfg.HistoryNodeCheckpointInterval
			pr.groupEntries = cfg.HistoryGroupEntries
			pr.indexSpan = cfg.HistoryIndexSpan
			_, e = pr.GetNode(nil, roots[3])
			must(t, e)
			pinned.Release()
			verifyHistory(t, a, roots, addresses, slots)
			must(t, a.Audit(context.Background()))
			it := a.s.db.NewIterator(prefixRange([]byte{'D'}))
			if it.First() {
				t.Fatal("v5 retained old D records")
			}
			must(t, it.Error())
			it.Release()
			must(t, a.Close())
			a, e = Open(path, cfg, false, true)
			must(t, e)
			verifyHistory(t, a, roots, addresses, slots)
			must(t, a.Audit(context.Background()))
			must(t, a.Close())
		})
	}
}
func TestH4MissingGroupAndIndexRejected(t *testing.T) {
	w, _, _ := dataset(t)
	cfg := DefaultConfig("corrupt-group")
	cfg.Format = Format
	a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	must(t, e)
	defer a.Close()
	for _, b := range w.Blocks {
		_, e = a.ExecuteBlock(context.Background(), b)
		must(t, e)
	}
	for _, tag := range []byte{'G', 'J'} {
		it := a.s.db.NewIterator(prefixRange([]byte{tag}))
		if !it.First() {
			t.Fatal("missing records")
		}
		k, v := bytes.Clone(it.Key()), bytes.Clone(it.Value())
		it.Release()
		batch := newDiskBatch()
		batch.Delete(k)
		must(t, a.s.write(batch))
		if e = a.Audit(context.Background()); e == nil {
			t.Fatalf("missing %c accepted", tag)
		}
		batch.Reset()
		batch.Put(k, []byte{0xff})
		must(t, a.s.write(batch))
		if e = a.Audit(context.Background()); e == nil {
			t.Fatal("corrupt record accepted")
		}
		batch.Reset()
		batch.Put(k, v)
		must(t, a.s.write(batch))
		must(t, a.Audit(context.Background()))
	}
}
