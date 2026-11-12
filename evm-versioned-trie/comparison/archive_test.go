package comparison

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pa "github.com/XiaodongQi1/Hybrid-code/evm-path-archive"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestArchiveLevelDBLiveEVM(t *testing.T) { runArchiveLiveEVM(t, kvstore.LevelDB) }
func runArchiveLiveEVM(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	ctx := context.Background()
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 64, Blocks: 6, TransactionsPerBlock: 12, Contracts: 4, StorageSlots: 8, Seed: 73})
	if e != nil {
		t.Fatal(e)
	}
	rootDir := t.TempDir()
	sourcePath := filepath.Join(rootDir, "artifacts")
	ac := DefaultArchiveConfig("common-live-fixture", engine)
	ac.Storage.BlockCacheBytes, ac.Storage.WriteBufferBytes = 1<<20, 1<<20
	source, e := artifact.Open(sourcePath, artifact.Config{WorkloadID: ac.WorkloadID, Codec: codec.Ethereum}, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	ap := filepath.Join(rootDir, "archive")
	a, e := OpenArchive(ap, ac, true, false, source)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { a.Close() }()
	pc := pa.DefaultConfig(ac.WorkloadID)
	pc.Codec, pc.ZeroGenesis, pc.Format, pc.StateOnly = codec.Ethereum, true, pa.PackedFormat, true
	pc.MaxBatchBytes, pc.MaxStagingBytes = ac.MaxBatchBytes, ac.MaxStagingBytes
	options := ac.Storage
	pc.Storage = &options
	pp := filepath.Join(rootDir, "path-import")
	p, e := pa.OpenWithArtifacts(pp, pc, true, false, source)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { p.Close() }()
	pl, e := pa.OpenWithArtifacts(filepath.Join(rootDir, "path-live"), pc, true, false, source)
	if e != nil {
		t.Fatal(e)
	}
	defer pl.Close()
	hc := backend.DefaultHybridConfig(ac.WorkloadID)
	hc.Store.Storage = &options
	hc.Initialization = backend.InitializationHashCold
	hp := filepath.Join(rootDir, "hybrid")
	h, e := backend.OpenHybrid(hp, hc, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { h.Close() }()
	var roots []mpt.Hash
	var pinned *ArchiveView
	for i, b := range w.Blocks {
		var r ArchiveResult
		var trace block.LogicalBlock
		if i == 0 {
			r, e = a.Execute(ctx, b)
		} else {
			r, trace, e = a.ExecuteWithTrace(ctx, b)
		}
		if e != nil {
			t.Fatal("Archive execute", i, e)
		}
		again, e := a.Execute(ctx, b)
		if e != nil || !sameJSON(r, again) {
			t.Fatal("Archive retry", i, e)
		}
		if i > 0 {
			_, again, e := a.ExecuteWithTrace(ctx, b)
			if e != nil || !sameJSON(trace, again) {
				t.Fatal("trace retry", i, e)
			}
		}
		var pr pa.Record
		if i == 0 {
			pr, e = p.ExecuteBlock(ctx, b)
		} else {
			pr, e = p.ApplyLogicalBlock(ctx, b, trace)
		}
		if e != nil || pr.Root != r.Root || !sameJSON(pr.Outcomes, r.OutcomesData) {
			t.Fatal("Path import", i, e)
		}
		if pr.Artifacts.Input != r.Input || pr.Artifacts.Outcomes != r.Outcomes || pr.Artifacts.Logical != r.Logical {
			t.Fatal("methods duplicated artifacts", i)
		}
		live, e := pl.ExecuteBlock(ctx, b)
		if e != nil || live.Root != r.Root || !sameJSON(live.Outcomes, r.OutcomesData) {
			t.Fatal("independent Path EVM execution", i, e)
		}
		var hr *backend.Result
		if i == 0 {
			hr, e = h.Apply(b, nil)
		} else {
			hr, e = h.ApplyLogicalBlock(b, trace)
		}
		if e != nil || hr.Root != r.Root {
			t.Fatal("Hybrid import", i, e)
		}
		if i == 0 {
			pinned, e = a.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
		}
		roots = append(roots, r.Root)
	}
	if pinned.Root() != roots[0] {
		t.Fatal("pinned view changed")
	}
	if e = a.Close(); !errors.Is(e, kvstore.ErrBusy) {
		t.Fatal("closed with pinned view", e)
	}
	if e = pinned.Close(); e != nil {
		t.Fatal(e)
	}
	if e = a.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = p.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = pl.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	for kind, want := range map[string]int{"block-input": len(w.Blocks), "logical-block": len(w.Blocks) - 1} {
		entries, e := os.ReadDir(filepath.Join(sourcePath, kind))
		if e != nil || len(entries) != want {
			t.Fatal("shared object count", kind, len(entries), want, e)
		}
	}
	// Archive nodes are keyed by their canonical hash, and the state namespace
	// has only code, compact block headers and coordination/config metadata.
	it, e := a.db.NewIterator("default", kvstore.Bounds{})
	if e != nil {
		t.Fatal(e)
	}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) == 0 || (k[0] != 'M' && k[0] != 'B' && k[0] != 'C') {
			t.Fatal("artifact in Archive state namespace", k)
		}
	}
	if e = it.Error(); e != nil {
		t.Fatal(e)
	}
	it.Close()
	if a.LogicalWrites().Total().Puts == 0 {
		t.Fatal("missing logical writes")
	}
	if e = a.Close(); e != nil {
		t.Fatal(e)
	}
	if e = p.Close(); e != nil {
		t.Fatal(e)
	}
	if e = pl.Close(); e != nil {
		t.Fatal(e)
	}
	if e = h.Close(); e != nil {
		t.Fatal(e)
	}
	// Offline artifacts prove that ordinary historical queries use state only.
	if e = source.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(sourcePath, sourcePath+"-offline"); e != nil {
		t.Fatal(e)
	}
	a, e = OpenArchive(ap, ac, false, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	p, e = pa.Open(pp, pc, false, true)
	if e != nil {
		t.Fatal(e)
	}
	h, e = backend.OpenHybrid(hp, hc, false, true)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for i, root := range roots {
		for _, address := range []common.Address{{18: 0x10}, {18: 0x20}, {18: 0x20, 19: 1}, {18: 0xde, 19: 0xad}} {
			requests := []q.Request{{Height: uint64(i), Kind: q.Account, Address: address}, {Height: uint64(i), Kind: q.Code, Address: address}}
			for slot := byte(0); slot < 8; slot++ {
				requests = append(requests, q.Request{Height: uint64(i), Kind: q.Storage, Address: address, Slot: common.Hash{31: slot}})
			}
			for _, req := range requests {
				x, e := a.Query(ctx, req)
				if e != nil {
					t.Fatal(e)
				}
				y, e := p.Query(ctx, req)
				if e != nil {
					t.Fatal(e)
				}
				v, e := h.ViewAt(req.Height)
				if e != nil {
					t.Fatal(e)
				}
				z, e := q.Read(ctx, req, v)
				v.Close()
				if e != nil {
					t.Fatal(e)
				}
				for _, got := range []q.Response{x, y, z} {
					if _, e = q.VerifyWithCodec(ctx, root, req, got, q.DefaultLimits(), codec.Ethereum); e != nil {
						t.Fatal(e)
					}
					if !sameJSON(got, x) {
						t.Fatal("query wire differs", req)
					}
					count++
				}
			}
		}
	}
	if _, e = a.Execute(ctx, w.Blocks[0]); !errors.Is(e, kvstore.ErrReadOnly) {
		t.Fatal(e)
	}
	if _, e = a.Result(0); e == nil {
		t.Fatal("Result silently omitted external receipts")
	}
	t.Logf("engine=%s blocks=%d EVM_transactions=%d verified_responses=%d", engine, len(w.Blocks), 6*12, count)
}

func TestArchiveRejectsWithoutPublishingState(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	c := DefaultArchiveConfig("reject-fixture", kvstore.LevelDB)
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 64, Blocks: 1, TransactionsPerBlock: 1, Contracts: 1})
	if e != nil {
		t.Fatal(e)
	}
	sourcePath := filepath.Join(path, "artifacts")
	s, e := artifact.Open(sourcePath, artifact.Config{WorkloadID: c.WorkloadID, Codec: codec.Ethereum}, true, false)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e = artifact.Open(sourcePath, artifact.Config{WorkloadID: c.WorkloadID, Codec: codec.Ethereum}, false, true)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { s.Close() }()
	ap := filepath.Join(path, "archive")
	a, e := OpenArchive(ap, c, true, false, s)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { a.Close() }()
	if _, e = a.Execute(ctx, w.Blocks[0]); !errors.Is(e, artifact.ErrReadOnly) {
		t.Fatal(e)
	}
	if _, e = a.Header(0); !errors.Is(e, kvstore.ErrNotFound) {
		t.Fatal("failed artifact commit published Head", e)
	}
	it, e := a.db.NewIterator("hash", kvstore.Bounds{})
	if e != nil {
		t.Fatal(e)
	}
	if it.First() {
		t.Fatal("failed artifact commit published nodes")
	}
	it.Close()
	a.Close()
	s.Close()
	s, e = artifact.Open(sourcePath, artifact.Config{WorkloadID: c.WorkloadID, Codec: codec.Ethereum}, false, false)
	if e != nil {
		t.Fatal(e)
	}
	a, e = OpenArchive(ap, c, false, false, s)
	if e != nil {
		t.Fatal(e)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e = a.Execute(canceled, w.Blocks[0]); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e = a.Execute(ctx, w.Blocks[0]); e != nil {
		t.Fatal(e)
	}
	bad := w.Blocks[0]
	bad.Cursor = "conflicting retry"
	if _, e = a.Execute(ctx, bad); e == nil {
		t.Fatal("conflicting retry accepted")
	}
	if _, _, e = a.ExecuteWithTrace(ctx, w.Blocks[0]); e == nil {
		t.Fatal("invented initial trace")
	}
	if e = a.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	a.Close()
	wrong := c
	wrong.Storage.Engine = kvstore.RocksDB
	if bad, e := OpenArchive(ap, wrong, false, true, nil); e == nil {
		bad.Close()
		t.Fatal("wrong engine opened")
	}
}
