package patharchive

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestSharedLevelDBStateOnlyArtifacts(t *testing.T) { runStateOnlyArtifacts(t, kvstore.LevelDB) }
func runStateOnlyArtifacts(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	ctx := context.Background()
	w, addresses, slots := dataset(t)
	cfg := DefaultConfig("state-only-fixture")
	cfg.Codec, cfg.Format, cfg.StateOnly = codec.Ethereum, PackedFormat, true
	layout := DefaultHistoryLayout().ForFormat(PackedFormat)
	cfg.HistoryLayout = &layout
	o := kvstore.DefaultOptions(engine)
	o.BlockCacheBytes, o.WriteBufferBytes = 1<<20, 1<<20
	cfg.Storage = &o
	sourcePath := filepath.Join(t.TempDir(), "artifacts")
	source, e := artifact.Open(sourcePath, artifact.Config{WorkloadID: cfg.WorkloadID, Codec: codec.Ethereum}, true, false)
	must(t, e)
	defer source.Close()
	path := filepath.Join(t.TempDir(), "path")
	db, e := OpenWithArtifacts(path, cfg, true, false, source)
	must(t, e)
	defer func() { db.Close() }()
	roots, outcomes := runOracleWithCodec(t, w, addresses, slots, cfg.Chain, codec.Ethereum)
	for i, input := range w.Blocks {
		r, e := db.ExecuteBlock(ctx, input)
		must(t, e)
		if r.Root != roots[i+1] || r.Artifacts == nil {
			t.Fatal("state-only root/refs", input.Height)
		}
		equal(t, r.Outcomes, outcomes[i])
		again, e := db.ExecuteBlock(ctx, input)
		must(t, e)
		equal(t, r, again)
		raw, e := db.s.db.Get(heightKey('B', input.Height))
		must(t, e)
		var header map[string]json.RawMessage
		must(t, json.Unmarshal(raw, &header))
		if header["Outcomes"] != nil || header["Changes"] != nil || header["Input"] != nil {
			t.Fatal("execution artifact in state DB")
		}
		if _, e = db.s.db.Get(heightKey('L', input.Height)); !errors.Is(e, kvstore.ErrNotFound) {
			t.Fatal("input in measured state", e)
		}
	}
	info, e := db.KVInfo()
	must(t, e)
	if info.Engine != engine {
		t.Fatal(info)
	}
	inv, e := db.StorageInventory()
	must(t, e)
	if inv.StateBytes+inv.OtherBytes != inv.AllKVBytes || inv.Components["root-headers"] == 0 || inv.Components["coordination"] == 0 {
		t.Fatal(inv)
	}
	must(t, db.Audit(ctx))
	must(t, db.Close())
	db, e = OpenWithArtifacts(path, cfg, false, true, source)
	must(t, e)
	must(t, db.Audit(ctx))
	r, e := db.Result(w.Blocks[2].Height)
	must(t, e)
	equal(t, r.Outcomes, outcomes[2])
	must(t, db.Close())
	// State-only queries remain independent of every external input/receipt file.
	if e = os.Rename(sourcePath, sourcePath+"-offline"); e != nil {
		t.Fatal(e)
	}
	db, e = Open(path, cfg, false, true)
	must(t, e)
	must(t, db.PrepareQueryHeaders(ctx))
	for i, input := range w.Blocks {
		for _, address := range addresses {
			requests := []q.Request{{Height: input.Height, Kind: q.Account, Address: address}, {Height: input.Height, Kind: q.Code, Address: address}}
			for _, slot := range slots {
				requests = append(requests, q.Request{Height: input.Height, Kind: q.Storage, Address: address, Slot: slot})
			}
			for _, req := range requests {
				r, e := db.Query(ctx, req)
				must(t, e)
				_, e = q.VerifyWithCodec(ctx, roots[i+1], req, r, q.DefaultLimits(), codec.Ethereum)
				must(t, e)
			}
		}
	}
	if _, e = db.Result(w.Blocks[0].Height); !errors.Is(e, ErrArtifactsUnavailable) {
		t.Fatal(e)
	}
	if e = db.Audit(ctx); !errors.Is(e, ErrArtifactsUnavailable) {
		t.Fatal(e)
	}
	if _, e = db.ExecuteBlock(ctx, w.Blocks[0]); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
	must(t, db.Close())
	wrong := cfg
	alt := o
	if engine == kvstore.LevelDB {
		alt.Engine = kvstore.RocksDB
	} else {
		alt.Engine = kvstore.LevelDB
	}
	wrong.Storage = &alt
	if bad, e := Open(path, wrong, false, true); e == nil {
		bad.Close()
		t.Fatal("engine mismatch accepted")
	}
}
