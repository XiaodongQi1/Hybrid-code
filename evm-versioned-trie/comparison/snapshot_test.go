package comparison

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	pa "github.com/XiaodongQi1/Hybrid-code/evm-path-archive"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	snap "github.com/XiaodongQi1/Hybrid-code/evm-state/snapshot"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestCommonBulkSnapshotLevelDB(t *testing.T) { runCommonBulkSnapshot(t, kvstore.LevelDB) }
func runCommonBulkSnapshot(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	id := "common-bulk-snapshot"
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 300, Blocks: 3, TransactionsPerBlock: 6, Contracts: 4, Seed: 29})
	if e != nil {
		t.Fatal(e)
	}
	for i := range w.Blocks[0].Allocations {
		a := &w.Blocks[0].Allocations[i]
		if len(a.Code) > 0 {
			a.Storage = map[common.Hash]common.Hash{{31: 1}: {31: 9}, {31: 3}: {31: 7}}
		}
	}
	header := w.Blocks[0]
	rows := append([]block.Allocation(nil), header.Allocations...)
	header.Allocations = nil
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Address[:], rows[j].Address[:]) < 0 })
	o := kvstore.DefaultOptions(engine)
	o.BlockCacheBytes, o.WriteBufferBytes = 1<<20, 1<<20
	index := 0
	builder, e := snap.Build(ctx, filepath.Join(dir, "source"), snap.Config{SourceID: id, Header: header, BatchBytes: 4096, ChunkEntries: 8, CheckpointAccounts: 8, Storage: &o}, func() (block.Allocation, error) {
		if index == len(rows) {
			return block.Allocation{}, io.EOF
		}
		a := rows[index]
		index++
		return a, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer builder.Close()
	source, e := builder.OpenSource()
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	manifest, e := snap.Verify(ctx, source, header)
	if e != nil {
		t.Fatal("source verification", e)
	}
	art, e := artifact.Open(filepath.Join(dir, "artifacts"), artifact.Config{WorkloadID: id, Codec: codec.Ethereum}, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer art.Close()
	ac := DefaultArchiveConfig(id, engine)
	ac.Storage = o
	ref, e := OpenArchive(filepath.Join(dir, "reference"), ac, true, false, art)
	if e != nil {
		t.Fatal(e)
	}
	defer ref.Close()
	initial, e := ref.Execute(ctx, w.Blocks[0])
	if e != nil {
		t.Fatal(e)
	}
	if initial.Root != manifest.Root {
		t.Fatal("streaming root differs from one-shot EVM initialization")
	}
	ap := filepath.Join(dir, "archive")
	a, e := OpenArchive(ap, ac, true, false, art)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { a.Close() }()
	pc := pa.DefaultConfig(id)
	pc.Codec, pc.Format, pc.ZeroGenesis, pc.StateOnly = codec.Ethereum, pa.PackedFormat, true, true
	pc.Storage = &o
	pp := filepath.Join(dir, "path")
	p, e := pa.OpenWithArtifacts(pp, pc, true, false, art)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { p.Close() }()
	hc := backend.DefaultHybridConfig(id)
	hc.Store.Storage = &o
	hc.Initialization = backend.InitializationHashColdBulk
	hc.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	hp := filepath.Join(dir, "hybrid")
	h, e := backend.OpenHybrid(hp, hc, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { h.Close() }()
	// An interrupted first batch must be detected on reopen, even before any
	// trie row has been copied. Normal operations remain blocked until resume.
	manifestRaw, _ := json.Marshal(manifest)
	manifestID, e := art.Ensure("snapshot-manifest", manifestRaw)
	if e != nil {
		t.Fatal(e)
	}
	headerRaw, _ := json.Marshal(header)
	inputID, e := art.Ensure("block-input", headerRaw)
	if e != nil {
		t.Fatal(e)
	}
	interrupt := func(db *kvstore.DB, identity string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, e := db.ImportInitial(ctx, kvstore.BulkPlan{Identity: identity, MarkerKey: []byte("M/initial-import"), BatchBytes: 4096, Final: kvstore.NewBatch(), Committed: func(*kvstore.Batch) { cancel() }}, func(kvstore.BulkEmit) error { return ctx.Err() })
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	}
	interrupt(a.db, manifestID.Hex()+":"+inputID.Hex())
	a.Close()
	a, e = OpenArchive(ap, ac, false, false, art)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = a.Header(0); !errors.Is(e, kvstore.ErrImportPending) {
		t.Fatal("Archive exposed partial import", e)
	}
	p.Close()
	po := o
	po.Columns = []string{"default"}
	pd, e := kvstore.Open(pp, po, false, false)
	if e != nil {
		t.Fatal(e)
	}
	interrupt(pd, manifestID.Hex()+":"+pc.Format)
	pd.Close()
	p, e = pa.OpenWithArtifacts(pp, pc, false, false, art)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.Query(ctx, q.Request{Height: 0, Kind: q.Account, Address: rows[0].Address}); !errors.Is(e, kvstore.ErrImportPending) {
		t.Fatal("Path exposed partial import", e)
	}
	ar, as, e := a.ImportSnapshot(ctx, header, source, &initial.Root)
	if e != nil {
		t.Fatal("Archive bulk", e)
	}
	pr, ps, e := p.ImportSnapshot(ctx, header, source, &initial.Root)
	if e != nil {
		t.Fatal("Path bulk", e)
	}
	hr, hs, e := h.ImportPreparedSnapshot(ctx, header, builder, &initial.Root)
	if e != nil {
		t.Fatal("Hybrid bulk", e)
	}
	if ar.Root != initial.Root || pr.Root != initial.Root || hr.Root != initial.Root || as.Batches < 2 || ps.Batches < 2 || hs.Import.Batches < 2 || as.PeakBatchBytes > 4096 || ps.PeakBatchBytes > 4096 || hs.Import.PeakBatchBytes > 4096 {
		t.Fatal("bulk publication/budget", as, ps, hs)
	}
	if !as.Resumed || !ps.Resumed {
		t.Fatal("reopened imports lost recovery status")
	}
	cache, e := h.CacheStats()
	if e != nil {
		t.Fatal(e)
	}
	if cache.Boundary.NodeEntries != 0 || cache.Boundary.AccessEvents != 0 || cache.Boundary.HeatCoverageBlocks != 0 {
		t.Fatal("initial snapshot warmed execution state", cache)
	}
	hi, e := h.MeasureStorage(ctx, false)
	if e != nil {
		t.Fatal(e)
	}
	if hi.Inventory.Columns["prefix"].Records != 0 || hi.Inventory.Columns["hash"].Records == 0 {
		t.Fatal("Hash bulk imported Prefix nodes", hi)
	}
	if ar.SnapshotManifest == "" || ar.SnapshotManifest != pr.Artifacts.SnapshotManifest {
		t.Fatal("methods did not reuse the source manifest")
	}
	// Imports are idempotent and preserve input provenance outside state.
	_, again, e := a.ImportSnapshot(ctx, header, source, &initial.Root)
	if e != nil || again.Batches != as.Batches {
		t.Fatal(e)
	}
	_, again, e = p.ImportSnapshot(ctx, header, source, &initial.Root)
	if e != nil || again.Batches != ps.Batches {
		t.Fatal(e)
	}
	_, repeat, e := h.ImportPreparedSnapshot(ctx, header, builder, &initial.Root)
	if e != nil || !repeat.AlreadyComplete {
		t.Fatal(e)
	}
	for _, b := range w.Blocks[1:] {
		r, trace, e := ref.ExecuteWithTrace(ctx, b)
		if e != nil {
			t.Fatal(e)
		}
		// Normal block ceilings are separate from the deliberately tiny import
		// ceiling: use a short block that fits that ceiling as well.
		ar, e := a.ApplyLogicalBlock(ctx, b, trace)
		if e != nil {
			t.Fatal("Archive after bulk", e)
		}
		pr, e := p.ApplyLogicalBlock(ctx, b, trace)
		if e != nil {
			t.Fatal("Path after bulk", e)
		}
		hr, e := h.ApplyLogicalBlock(b, trace)
		if e != nil {
			t.Fatal("Hybrid after bulk", e)
		}
		if ar.Root != r.Root || pr.Root != r.Root || hr.Root != r.Root {
			t.Fatal("post-snapshot root differs")
		}
	}
	if e = a.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = p.Audit(ctx); e != nil {
		t.Fatal(e)
	}
	a.Close()
	p.Close()
	h.Close()
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
	responses := 0
	var proofCases []exportedProof
	for _, b := range w.Blocks {
		root, e := ref.Header(b.Height)
		if e != nil {
			t.Fatal(e)
		}
		for _, address := range []common.Address{rows[0].Address, common.HexToAddress("0x2000"), common.HexToAddress("0xdead")} {
			for _, req := range []q.Request{{Height: b.Height, Kind: q.Account, Address: address}, {Height: b.Height, Kind: q.Code, Address: address}, {Height: b.Height, Kind: q.Storage, Address: address, Slot: common.Hash{31: 1}}, {Height: b.Height, Kind: q.Storage, Address: address, Slot: common.Hash{31: 99}}} {
				want, e := ref.Query(ctx, req)
				if e != nil {
					t.Fatal(e)
				}
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
					var accountValue []byte
					if got.Account != nil {
						accountValue, _ = codec.Ethereum.EncodeAccount(*got.Account)
					}
					proofCases = append(proofCases, exportProof(root.Root, state.AccountKey(req.Address), got.AccountProof, accountValue, got.Account != nil))
					if req.Kind == q.Storage && got.Account != nil {
						word, _ := codec.Ethereum.EncodeStorage(got.Value)
						proofCases = append(proofCases, exportProof(got.Account.StorageRoot, state.SlotKey(req.Slot), got.StorageProof, word, got.SlotExists))
					}
					raw, _ := json.Marshal(got)
					wire, _ := json.Marshal(want)
					if !bytes.Equal(raw, wire) {
						t.Fatal("bulk query wire differs", req)
					}
					if _, e = q.VerifyWithCodec(ctx, root.Root, req, got, q.DefaultLimits(), codec.Ethereum); e != nil {
						t.Fatal(e)
					}
					responses++
				}
			}
		}
	}
	if reportDir := os.Getenv("BULK_PROOF_REPORT_DIR"); reportDir != "" {
		raw, e := json.MarshalIndent(struct {
			Schema string
			Cases  []exportedProof
		}{"versioned-trie-proof-cases/v1", proofCases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.MkdirAll(reportDir, 0700); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(reportDir, "proofs-"+string(engine)+".json"), append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("engine=%s accounts=%d verified_responses=%d archive_batches=%d path_batches=%d hybrid_batches=%d", engine, len(rows), responses, as.Batches, ps.Batches, hs.Import.Batches)
}
