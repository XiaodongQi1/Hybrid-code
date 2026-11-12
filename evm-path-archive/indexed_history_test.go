package patharchive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func indexedConfig(id string) Config {
	c := DefaultConfig(id)
	c.Format = IndexedFormat
	l := DefaultHistoryLayout()
	c.HistoryLayout = &l
	return c
}
func TestG0VersionAndSectionCodecs(t *testing.T) {
	l := DefaultHistoryLayout()
	l.RestartBytes = 16
	ids := []uint64{1, 2, 127, 128, 255, 256, 16383, 16384, ^uint64(0) - 1, ^uint64(0)}
	wire := encodeIDs(ids, l.RestartBytes)
	out, e := decodeIDs(wire, l.IndexBlockBytes)
	must(t, e)
	equal(t, out, ids)
	for _, target := range []uint64{0, 1, 126, 127, 128, 255, 256, 16383, 16384, ^uint64(0) - 1, ^uint64(0)} {
		id, found, e := seekIDs(wire, target, l.IndexBlockBytes)
		must(t, e)
		var expected uint64
		var ok bool
		for _, n := range ids {
			if n > target {
				expected = n
				ok = true
				break
			}
		}
		if found != ok || id != expected {
			t.Fatal(target, id, found)
		}
	}
	for _, bad := range [][]byte{nil, {0}, {1, 1, 1, 1, 0}, {0x81, 0}, {1, 2, 1, 1, 1}, append(bytes.Clone(wire), 0)} {
		if _, e := decodeIDs(bad, l.IndexBlockBytes); e == nil {
			t.Fatalf("invalid ID block %x", bad)
		}
	}
	r := reader{format: IndexedFormat}
	var rows []historyEntry
	for i := 0; i < 80; i++ {
		k := r.key('P', []byte{0}, []byte{byte(i / 16), byte(i % 16)})
		rows = append(rows, historyEntry{k, bytes.Repeat([]byte{byte(i)}, 1+i%19)})
	}
	wire = encodeChunk(rows, l)
	decoded, e := decodeChunk(wire, l)
	must(t, e)
	equal(t, decoded, rows)
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
	if _, e := findChunk(wire, r.key('P', []byte{0}, []byte{15, 15}), l); e == nil {
		t.Fatal("missing key accepted")
	}
}
func TestG2IndexRotationInlineAndPointReads(t *testing.T) {
	cfg := indexedConfig("g2")
	cfg.HistoryLayout.IndexBlockBytes = 64
	cfg.HistoryLayout.RestartBytes = 16
	cfg.HistoryLayout.InlineIndexBytes = 24
	a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	must(t, e)
	defer a.Close()
	k := a.s.live(context.Background()).key('P', []byte{0}, []byte{1, 2})
	for i := uint64(1); i <= 300; i++ {
		b := newDiskBatch()
		ordinal, e := a.s.live(context.Background()).appendVersion(k, i*7, func(k, v []byte) error { b.Put(k, v); return nil })
		must(t, e)
		if ordinal != i {
			t.Fatal(ordinal, i)
		}
		must(t, a.s.write(b))
	}
	r := a.s.live(context.Background())
	r.iter = func(*kvstore.Bounds) iterator { t.Fatal("index used KV iterator"); return nil }
	x, ok, e := r.readIndex(k)
	must(t, e)
	if !ok || x.inline != nil || len(x.desc) < 2 {
		t.Fatal("blocks did not rotate")
	}
	for i := uint64(0); i < 2100; i++ {
		id, found, e := r.nextVersion(k, i)
		must(t, e)
		if !found || id != (i/7+1)*7 {
			t.Fatal(i, id, found)
		}
	}
	_, found, e := r.nextVersion(k, 2100)
	must(t, e)
	if found {
		t.Fatal("future version")
	}
	for i := range x.desc {
		b, e := r.get(indexBlockKey(k, uint64(i)))
		must(t, e)
		if len(b) > 64 {
			t.Fatal("oversized index")
		}
	}
}
func TestG3HistoryProofRestart(t *testing.T) { runHistoryProofRestart(t, IndexedFormat) }
func runHistoryProofRestart(t *testing.T, format string) {
	w, addresses, slots := dataset(t)
	roots, outcomes := runOracle(t, w, addresses, slots, DefaultConfig("oracle").Chain)
	for _, opts := range [][3]uint64{{128, 64, 1}, {512, 128, 2}, {32768, 4096, 8}} {
		t.Run(fmt.Sprint(opts), func(t *testing.T) {
			cfg := indexedConfigForFormat("g3", format)
			cfg.HistoryLayout.ChunkBytes = opts[0]
			cfg.HistoryLayout.IndexBlockBytes = opts[1]
			cfg.HistoryLayout.RestartBytes = 32
			cfg.HistoryLayout.InlineIndexBytes = 24
			cfg.HistoryNodeCheckpointInterval = opts[2]
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			var pinned diskSnapshot
			var lastID uint64
			for i, b := range w.Blocks {
				rec, e := a.ExecuteBlock(context.Background(), b)
				must(t, e)
				equal(t, rec.Root, roots[i+1])
				equal(t, rec.Outcomes, outcomes[i])
				if i == 0 && rec.StateID != 0 {
					t.Fatal("init history")
				}
				if i > 0 && rec.Root == roots[i] && rec.StateID != lastID {
					t.Fatal("unchanged state allocated ID")
				}
				lastID = rec.StateID
				if i == 4 {
					must(t, a.Close())
					a, e = Open(path, cfg, false, false)
					must(t, e)
					pinned, e = a.s.db.GetSnapshot()
					must(t, e)
				}
			}
			rec, e := a.Result(9)
			must(t, e)
			empty, e := a.Result(10)
			must(t, e)
			if rec.StateID != empty.StateID {
				t.Fatal("empty block created history")
			}
			pr := snapshotReader(context.Background(), pinned, format, 3)
			pr.layout = *cfg.HistoryLayout
			pr.limit = cfg.HistoryNodeCheckpointInterval
			pr.iter = func(*kvstore.Bounds) iterator { t.Fatal("historical read used KV iterator"); return nil }
			_, e = pr.GetNode(nil, roots[3])
			must(t, e)
			pinned.Release()
			verifyHistory(t, a, roots, addresses, slots)
			inventory, e := a.HistoryInventory()
			must(t, e)
			encoding, e := a.EncodingMetrics()
			must(t, e)
			var values, keys, records uint64
			for _, c := range inventory {
				values += c.ValueBytes
				keys += c.KeyBytes
				records += c.Records
			}
			if values != encoding.ValueBytes || keys != encoding.KeyBytes || records != encoding.Full+encoding.Absent+encoding.Delta {
				t.Fatal("persisted history inventory disagrees with commit ledger")
			}
			must(t, a.Audit(context.Background()))
			for _, tag := range []byte{'D', 'G', 'J', 'Q', 'I'} {
				it := a.s.db.NewIterator(prefixRange([]byte{tag}))
				if it.First() {
					t.Fatalf("retained obsolete %c", tag)
				}
				must(t, it.Error())
				it.Release()
			}
			must(t, a.Close())
			for _, ro := range []bool{true, false} {
				a, e = Open(path, cfg, false, ro)
				must(t, e)
				verifyHistory(t, a, roots, addresses, slots)
				must(t, a.Audit(context.Background()))
				must(t, a.Close())
			}
		})
	}
}
func TestG3MissingCorruptIndexAndHistory(t *testing.T) {
	runMissingCorruptIndexAndHistory(t, IndexedFormat)
}
func runMissingCorruptIndexAndHistory(t *testing.T, format string) {
	w, _, _ := dataset(t)
	cfg := indexedConfigForFormat("g3-corruption", format)
	cfg.HistoryLayout.InlineIndexBytes = 0
	a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	must(t, e)
	defer a.Close()
	for _, b := range w.Blocks {
		_, e = a.ExecuteBlock(context.Background(), b)
		must(t, e)
	}
	for _, tag := range []byte{'H', 'R', 'X', 'Y'} {
		it := a.s.db.NewIterator(prefixRange([]byte{tag}))
		if !it.First() {
			t.Fatalf("no %c", tag)
		}
		k, v := bytes.Clone(it.Key()), bytes.Clone(it.Value())
		it.Release()
		for _, bad := range [][]byte{nil, {255}} {
			b := newDiskBatch()
			if bad == nil {
				b.Delete(k)
			} else {
				b.Put(k, bad)
			}
			must(t, a.s.write(b))
			if a.Audit(context.Background()) == nil {
				t.Fatalf("bad %c accepted", tag)
			}
			b.Reset()
			b.Put(k, v)
			must(t, a.s.write(b))
		}
		must(t, a.Audit(context.Background()))
	}
}
func TestG3AtomicFailureAndRetry(t *testing.T) { runAtomicFailureAndRetry(t, IndexedFormat) }
func runAtomicFailureAndRetry(t *testing.T, format string) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			w, addresses, slots := dataset(t)
			cfg := indexedConfigForFormat("g3-fault", format)
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			_, e = a.ExecuteBlock(context.Background(), w.Blocks[0])
			must(t, e)
			original := a.s.write
			a.s.write = func(b *diskBatch) error {
				if after {
					if e := original(b); e != nil {
						return e
					}
				}
				return errors.New("injected write failure")
			}
			_, e = a.ExecuteBlock(context.Background(), w.Blocks[1])
			if !errors.Is(e, ErrUnknown) {
				t.Fatal(e)
			}
			must(t, a.Close())
			a, e = Open(path, cfg, false, false)
			must(t, e)
			if after && a.s.head.Height != 2 || !after && a.s.head.Height != 1 {
				t.Fatal("torn head")
			}
			_, e = a.ExecuteBlock(context.Background(), w.Blocks[1])
			must(t, e)
			roots := make([]mpt.Hash, len(w.Blocks)+1)
			for h := uint64(1); h <= 2; h++ {
				r, e := a.Result(h)
				must(t, e)
				roots[h] = r.Root
			}
			for _, b := range w.Blocks[2:] {
				rec, e := a.ExecuteBlock(context.Background(), b)
				must(t, e)
				roots[b.Height] = rec.Root
			}
			verifyHistory(t, a, roots, addresses, slots)
			must(t, a.Audit(context.Background()))
			must(t, a.Close())
		})
	}
}

func TestG3OrdinalCheckpointsAndLimits(t *testing.T) {
	runOrdinalCheckpointsAndLimits(t, IndexedFormat)
}
func runOrdinalCheckpointsAndLimits(t *testing.T, format string) {
	for _, interval := range []uint64{1, 2, 8} {
		t.Run(fmt.Sprint(interval), func(t *testing.T) {
			cfg := indexedConfigForFormat("g3-long-chain", format)
			cfg.HistoryNodeCheckpointInterval = interval
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			defer func() { a.Close() }()
			r := a.s.live(context.Background())
			k := r.key('P', []byte{0}, []byte{1, 2})
			versions := [][]byte{branchFixture(t)}
			batch := newDiskBatch()
			batch.Put(k, versions[0])
			wire, _ := json.Marshal(Record{Height: 0, StateID: 0})
			batch.Put(heightKey('B', 0), wire)
			must(t, a.s.write(batch))
			for i := uint64(1); i <= 32; i++ {
				old := versions[len(versions)-1]
				next := replaceElement(t, old, int(i%16), bytes.Repeat([]byte{byte(40 + i)}, 32))
				batch.Reset()
				put := func(k, v []byte) error { batch.Put(k, v); return nil }
				ordinal, e := r.appendVersion(k, i, put)
				must(t, e)
				if ordinal != i {
					t.Fatal("ordinal")
				}
				stats := Changes{}
				undo, e := indexedEncoding(old, next, ordinal, interval, &stats)
				must(t, e)
				if i%interval == 0 && undo[0]&3 != fullNode {
					t.Fatal("missing checkpoint")
				}
				must(t, a.s.writeSections(map[string][]byte{string(k): undo}, i, put, &stats))
				batch.Put(k, next)
				wire, _ := json.Marshal(Record{Height: i, StateID: i})
				batch.Put(heightKey('B', i), wire)
				must(t, a.s.write(batch))
				versions = append(versions, next)
				if i == 16 {
					must(t, a.Close())
					a, e = Open(path, cfg, false, false)
					must(t, e)
					r = a.s.live(context.Background())
				}
			}
			r.current = false
			r.iter = func(*kvstore.Bounds) iterator { t.Fatal("range query used"); return nil }
			for h, v := range versions {
				r.height = uint64(h)
				got, e := r.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(v)))
				must(t, e)
				equal(t, got, v)
			}
			if a.RecoveryMetrics().MaxDeltas >= interval {
				t.Fatal("unbounded recovery")
			}
			r.byteLimit = 1
			if _, e = r.value(k, 0); !errors.Is(e, ErrHistoryLimit) {
				t.Fatal("read budget", e)
			}
			r.byteLimit = cfg.MaxHistoryDecodeBytes
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r.ctx = ctx
			if _, e = r.value(k, 0); !errors.Is(e, context.Canceled) {
				t.Fatal("cancel", e)
			}
			r.ctx = context.Background()
			batch.Reset()
			batch.Delete(indexKey(k))
			must(t, a.s.write(batch))
			r.height = 0
			if _, e = r.GetNode([]byte{1, 2}, mpt.Hash(crypto.Keccak256Hash(versions[0]))); e == nil {
				t.Fatal("missing index bypassed root")
			}
		})
	}
}
func TestG0MalformedCodecsNeverPanic(t *testing.T) {
	l := DefaultHistoryLayout()
	rng := rand.New(rand.NewSource(77))
	for i := 0; i < 2000; i++ {
		b := make([]byte, rng.Intn(128))
		rng.Read(b)
		decodeIndex(b, l)
		decodeIDs(b, l.IndexBlockBytes)
		decodeBounds(b, l)
		decodeChunk(b, l)
	}
}

func TestG3EmptyInitializationAndNoOpBlocks(t *testing.T) {
	runEmptyInitializationAndNoOpBlocks(t, IndexedFormat)
}
func runEmptyInitializationAndNoOpBlocks(t *testing.T, format string) {
	w, addresses, slots := dataset(t)
	for i := range w.Blocks {
		w.Blocks[i].Allocations = nil
		w.Blocks[i].Transactions = nil
	}
	cfg := indexedConfigForFormat("empty-conversions", format)
	path := filepath.Join(t.TempDir(), "db")
	a, e := Open(path, cfg, true, false)
	must(t, e)
	roots := make([]mpt.Hash, len(w.Blocks)+1)
	for i, b := range w.Blocks {
		rec, e := a.ExecuteBlock(context.Background(), b)
		must(t, e)
		roots[i+1] = rec.Root
		if rec.StateID != 0 {
			t.Fatal("no-op created state ID")
		}
	}
	verifyHistory(t, a, roots, addresses, slots)
	must(t, a.Audit(context.Background()))
	for _, tag := range []byte{'H', 'R', 'X', 'Y'} {
		it := a.s.db.NewIterator(prefixRange([]byte{tag}))
		if it.First() {
			t.Fatalf("empty state wrote %c", tag)
		}
		it.Release()
	}
	must(t, a.Close())
	a, e = Open(path, cfg, false, true)
	must(t, e)
	defer a.Close()
	verifyHistory(t, a, roots, addresses, slots)
}
func TestG0LayoutValidationAndIdentity(t *testing.T) {
	for _, field := range []string{"ChunkBytes", "RestartBytes", "IndexBlockBytes", "MaxIndexMetaBytes", "MaxRecordBytes"} {
		c := indexedConfig("invalid")
		switch field {
		case "ChunkBytes":
			c.HistoryLayout.ChunkBytes = 0
		case "RestartBytes":
			c.HistoryLayout.RestartBytes = 0
		case "IndexBlockBytes":
			c.HistoryLayout.IndexBlockBytes = 0
		case "MaxIndexMetaBytes":
			c.HistoryLayout.MaxIndexMetaBytes = 0
		case "MaxRecordBytes":
			c.HistoryLayout.MaxRecordBytes = 0
		}
		if a, e := Open(filepath.Join(t.TempDir(), "db"), c, true, false); e == nil {
			a.Close()
			t.Fatal("invalid", field)
		}
	}
	c := indexedConfig("identity")
	path := filepath.Join(t.TempDir(), "db")
	a, e := Open(path, c, true, false)
	must(t, e)
	must(t, a.Close())
	c.HistoryLayout.IndexBlockBytes *= 2
	if a, e = Open(path, c, false, true); e == nil {
		a.Close()
		t.Fatal("layout identity ignored")
	}
}

func TestG2InlineBoundaryConversionAndCursorReuse(t *testing.T) {
	l := DefaultHistoryLayout()
	l.IndexBlockBytes = 64
	l.InlineIndexBytes = 64
	l.RestartBytes = 16
	db := map[string][]byte{}
	reads := map[string]int{}
	r := reader{ctx: context.Background(), layout: l, get: func(k []byte) ([]byte, error) {
		reads[string(k)]++
		v, ok := db[string(k)]
		if !ok {
			return nil, kvstore.ErrNotFound
		}
		return bytes.Clone(v), nil
	}}
	key := []byte{'P', 0, 0}
	put := func(k, v []byte) error { db[string(k)] = bytes.Clone(v); return nil }
	for i := uint64(1); i <= 200; i++ {
		n, e := r.appendVersion(key, i, put)
		must(t, e)
		if n != i {
			t.Fatal(n)
		}
	}
	x, ok, e := r.readIndex(key)
	must(t, e)
	if !ok || x.inline != nil || len(x.desc) < 2 {
		t.Fatal("inline did not convert")
	}
	if _, e = r.appendVersion(key, 200, put); e == nil {
		t.Fatal("duplicate mutation accepted")
	}
	reads = map[string]int{}
	c, e := r.versionCursor(key)
	must(t, e)
	for i := uint64(0); i < 200; i++ {
		n, found, e := c.seekGT(i)
		must(t, e)
		if !found || n != i+1 {
			t.Fatal("cursor lookup")
		}
	}
	if reads[string(indexKey(key))] != 1 {
		t.Fatal("repeated metadata read")
	}
	for i := range x.desc {
		if reads[string(indexBlockKey(key, uint64(i)))] != 1 {
			t.Fatal("repeated block read")
		}
	}
}

func indexedConfigForFormat(id, format string) Config {
	c := indexedConfig(id)
	c.Format = format
	l := c.HistoryLayout.ForFormat(format)
	c.HistoryLayout = &l
	return c
}
func TestV7HistoryIntegration(t *testing.T) {
	for name, run := range map[string]func(*testing.T, string){
		"proof-restart":     runHistoryProofRestart,
		"missing-corrupt":   runMissingCorruptIndexAndHistory,
		"atomic-failure":    runAtomicFailureAndRetry,
		"checkpoint-limits": runOrdinalCheckpointsAndLimits,
		"empty-blocks":      runEmptyInitializationAndNoOpBlocks,
	} {
		t.Run(name, func(t *testing.T) { run(t, PackedFormat) })
	}
}
