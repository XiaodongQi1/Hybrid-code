package patharchive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/rlp"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func branchFixture(t *testing.T) []byte {
	t.Helper()
	v := make([][]byte, 17)
	for i := 0; i < 16; i++ {
		v[i] = bytes.Repeat([]byte{byte(i + 1)}, 32)
	}
	b, e := rlp.EncodeToBytes(v)
	must(t, e)
	return b
}
func replaceElement(t *testing.T, b []byte, i int, v []byte) []byte {
	t.Helper()
	a, e := nodeElements(b)
	must(t, e)
	a[i], e = rlp.EncodeToBytes(v)
	must(t, e)
	b, e = rlp.EncodeToBytes(a)
	must(t, e)
	return b
}
func TestH0NodeHistoryCodec(t *testing.T) {
	b := branchFixture(t)
	rng := rand.New(rand.NewSource(23))
	for i := 0; i < 200; i++ {
		next := bytes.Clone(b)
		n := 1 + rng.Intn(16)
		for j := 0; j < n; j++ {
			v := make([]byte, 32)
			rng.Read(v)
			next = replaceElement(t, next, rng.Intn(16), v)
		}
		rec, _, e := encodeNodeHistory(b, next, false)
		must(t, e)
		back, e := restoreNodeHistory(rec, next)
		must(t, e)
		if !bytes.Equal(back, b) {
			t.Fatal("round trip")
		}
		b = next
	}
	for _, pair := range [][2][]byte{{nil, b}, {b, nil}} {
		rec, _, e := encodeNodeHistory(pair[0], pair[1], false)
		must(t, e)
		back, e := restoreNodeHistory(rec, pair[1])
		must(t, e)
		if !bytes.Equal(back, pair[0]) {
			t.Fatal("absence")
		}
	}
	// A long local leaf value requires varint lengths, not upstream one-byte sizes.
	leaf, e := rlp.EncodeToBytes([][]byte{{0x20}, bytes.Repeat([]byte{7}, 400)})
	must(t, e)
	newer := replaceElement(t, leaf, 1, bytes.Repeat([]byte{8}, 400))
	rec, _, e := encodeNodeHistory(leaf, newer, false)
	must(t, e)
	back, e := restoreNodeHistory(rec, newer)
	must(t, e)
	equal(t, back, leaf)
	rec, _, e = encodeNodeHistory(b, replaceElement(t, b, 0, bytes.Repeat([]byte{99}, 32)), false)
	must(t, e)
	if rec[0]&3 != deltaNode {
		t.Fatal("expected compression")
	}
	for i := 0; i < len(rec); i++ {
		if _, e = restoreNodeHistory(rec[:i], b); e == nil {
			t.Fatalf("truncation %d", i)
		}
	}
	for _, bad := range [][]byte{{3}, {4}, {1}, {2}, {0, 0}, {0xff}, append(bytes.Clone(rec), 0), {6, 1, 0, 0x81, 0, 0x80}} {
		if _, e = restoreNodeHistory(bad, b); e == nil {
			t.Fatalf("invalid accepted %x", bad)
		}
	}
	// Branch slot 16 and embedded RLP children are preserved as raw elements.
	old := replaceElement(t, b, 16, []byte{7})
	newv := replaceElement(t, old, 16, []byte{8})
	rec, _, e = encodeNodeHistory(old, newv, false)
	must(t, e)
	back, e = restoreNodeHistory(rec, newv)
	must(t, e)
	equal(t, back, old)
	a, _ := nodeElements(b)
	a[0] = leafSmall(t)
	old, e = rlp.EncodeToBytes(a)
	must(t, e)
	a[0], _ = rlp.EncodeToBytes(bytes.Repeat([]byte{2}, 32))
	newv, e = rlp.EncodeToBytes(a)
	must(t, e)
	rec, _, e = encodeNodeHistory(old, newv, false)
	must(t, e)
	back, e = restoreNodeHistory(rec, newv)
	must(t, e)
	equal(t, back, old)
}
func leafSmall(t *testing.T) []byte {
	b, e := rlp.EncodeToBytes([][]byte{{0x20}, {1}})
	must(t, e)
	return b
}
func TestH1H2BoundedHistory(t *testing.T) {
	for _, interval := range []uint64{1, 2, 8} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			cfg := DefaultConfig("chain")
			cfg.Format = DifferentialFormat
			cfg.HistoryNodeCheckpointInterval = interval
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			r := a.s.live(context.Background())
			k := r.key('P', []byte{0}, []byte{1, 2})
			versions := [][]byte{branchFixture(t)}
			b := newDiskBatch()
			b.Put(k, versions[0])
			b.Put(counterKey(k), []byte{0})
			must(t, a.s.write(b))
			for i := 1; i <= 25; i++ {
				old := versions[len(versions)-1]
				v := replaceElement(t, old, i%16, bytes.Repeat([]byte{byte(40 + i)}, 32))
				batch := newDiskBatch()
				stats := Changes{}
				rec, e := a.s.historyWrite(func(k, v []byte) error { batch.Put(k, v); return nil }, func(k []byte) error { batch.Delete(k); return nil }, k, old, v, &stats)
				must(t, e)
				batch.Put(k, v)
				batch.Put(append(r.historyPrefix(k), orderedHeight(uint64(i))...), rec)
				must(t, a.s.write(batch))
				versions = append(versions, v)
				if i == 12 {
					must(t, a.Close())
					a, e = Open(path, cfg, false, false)
					must(t, e)
					r = a.s.live(context.Background())
				}
			}
			for h, v := range versions {
				r.current = false
				r.height = uint64(h)
				got, e := r.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(v)))
				must(t, e)
				equal(t, got, v)
			}
			metrics := a.RecoveryMetrics()
			if metrics.MaxDeltas >= interval {
				t.Fatal("unbounded chain")
			}
			r.byteLimit = 1
			if _, e = r.value(k, 0); !errors.Is(e, ErrHistoryLimit) {
				t.Fatal("byte limit", e)
			}
			r.byteLimit = cfg.MaxHistoryDecodeBytes
			// Missing historical records must fail the canonical expected hash.
			hk := append(r.historyPrefix(k), orderedHeight(1)...)
			saved, e := a.s.db.Get(hk)
			must(t, e)
			missing := newDiskBatch()
			missing.Delete(hk)
			must(t, a.s.write(missing))
			r.height = 0
			if _, e = r.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(versions[0]))); e == nil {
				t.Fatal("missing history accepted")
			}
			missing.Reset()
			missing.Put(hk, saved)
			must(t, a.s.write(missing))
			// A pinned snapshot must keep its own current anchor across subsequent writes.
			snap, e := a.s.db.GetSnapshot()
			must(t, e)
			pinned := snapshotReader(context.Background(), snap, DifferentialFormat, 24)
			pinned.limit = interval
			// Missing anchor bytes must fail trusted-hash verification.
			batch := newDiskBatch()
			batch.Put(k, []byte{0xff})
			must(t, a.s.write(batch))
			r.height = 25
			if _, e = r.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(versions[25]))); e == nil {
				t.Fatal("corruption accepted")
			}
			got, e := pinned.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(versions[24])))
			must(t, e)
			equal(t, got, versions[24])
			snap.Release()
			must(t, a.Close())
		})
	}
}
func TestH1CounterMissingRejects(t *testing.T) {
	a, e := Open(filepath.Join(t.TempDir(), "db"), DefaultConfig("missing"), true, false)
	must(t, e)
	defer a.Close()
	k := a.s.live(context.Background()).key('P', []byte{0}, nil)
	_, e = a.s.historyWrite(func([]byte, []byte) error { return nil }, func([]byte) error { return nil }, k, branchFixture(t), branchFixture(t), &Changes{})
	if !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
}
