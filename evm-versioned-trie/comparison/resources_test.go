package comparison

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	pa "github.com/XiaodongQi1/HybridTrie/evm-path-archive"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/artifact"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

func TestDefaultResourcePlanMatchesAllocatedMemory(t *testing.T) {
	b := DefaultResourceBudget()
	for _, engine := range []kvstore.Engine{kvstore.LevelDB, kvstore.RocksDB} {
		p, e := b.Resolve(engine, ht.DefaultEthereumPrefixBoundaryForestCacheConfig())
		if e != nil {
			t.Fatal(e)
		}
		if p.Hybrid.ApplicationAllocatedBytes != p.HybridAllocation.AllocatedBytes() || p.Hybrid.ApplicationAllocatedBytes != p.CommonApplicationTargetBytes {
			t.Fatal("hybrid allocation mismatch", p)
		}
		for _, m := range []MethodResources{p.Archive, p.Path, p.Hybrid} {
			if m.ApplicationAllocatedBytes > b.ApplicationCacheBytes || m.Storage.GlobalWriteBufferBytes != b.GlobalWriteBufferBytes || m.Storage.BlockCacheBytes != b.BlockCacheBytes {
				t.Fatal("component budget mismatch", m)
			}
		}
		if p.Archive.ApplicationAllocatedBytes != p.Path.ApplicationAllocatedBytes || p.CommonApplicationTargetBytes-p.Archive.ApplicationAllocatedBytes >= p.Archive.AllocationQuantumBytes {
			t.Fatal("baseline backing mismatch", p)
		}
		if p.Hybrid.Storage.ApplicationCacheBytes != 0 || p.Archive.Storage.ApplicationCacheBytes != 0 || p.Path.Storage.ApplicationCacheBytes != 0 || p.Archive.ExecutionNodeCacheBytes != p.CommonApplicationTargetBytes || p.Path.ExecutionNodeCacheBytes != p.CommonApplicationTargetBytes {
			t.Fatal("query cache policy mismatch")
		}
		t.Logf("engine=%s app_requested=%d common_target=%d archive/path_allocated=%d hybrid_allocated=%d unassigned=%d pair_difference=%d", engine, b.ApplicationCacheBytes, p.CommonApplicationTargetBytes, p.Archive.ApplicationAllocatedBytes, p.Hybrid.ApplicationAllocatedBytes, p.UnassignedApplicationBytes, p.CommonApplicationTargetBytes-p.Archive.ApplicationAllocatedBytes)
	}
	bad := b
	bad.ApplicationCacheBytes = 1 << 20
	if _, e := bad.Resolve(kvstore.LevelDB, ht.DefaultEthereumPrefixBoundaryForestCacheConfig()); e == nil {
		t.Fatal("undersized total accepted")
	}
}

func TestCommonResourcesLevelDB(t *testing.T) { runCommonResources(t, kvstore.LevelDB) }
func runCommonResources(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	ctx := context.Background()
	b := DefaultResourceBudget()
	b.ApplicationCacheBytes = 8 << 20
	b.BlockCacheBytes = 1 << 20
	b.GlobalWriteBufferBytes = 2 << 20
	plan, e := b.Resolve(engine, ht.DefaultEthereumPrefixBoundaryForestCacheConfig())
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	source, e := artifact.Open(filepath.Join(dir, "artifacts"), artifact.Config{WorkloadID: "resource-fixture", Codec: codec.Ethereum}, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	ac, pc, hc := plan.ArchiveConfig("resource-fixture"), plan.PathConfig("resource-fixture"), plan.HybridConfig("resource-fixture")
	ap, pp, hp := filepath.Join(dir, "archive"), filepath.Join(dir, "path"), filepath.Join(dir, "hybrid")
	a, e := OpenArchive(ap, ac, true, false, source)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { a.Close() }()
	p, e := pa.OpenWithArtifacts(pp, pc, true, false, source)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { p.Close() }()
	h, e := backend.OpenHybrid(hp, hc, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { h.Close() }()
	methods := []struct {
		name      string
		measure   func(context.Context, bool) (kvstore.Measurement, error)
		stats     func() (kvstore.Statistics, error)
		resources MethodResources
	}{
		{"Archive", func(c context.Context, s bool) (kvstore.Measurement, error) { return a.MeasureStorage(c, s) }, func() (kvstore.Statistics, error) { return a.EngineStats() }, plan.Archive},
		{"Path", func(c context.Context, s bool) (kvstore.Measurement, error) { return p.MeasureStorage(c, s) }, func() (kvstore.Statistics, error) { return p.EngineStats() }, plan.Path},
		{"Hybrid", func(c context.Context, s bool) (kvstore.Measurement, error) { return h.MeasureStorage(c, s) }, func() (kvstore.Statistics, error) { return h.EngineStats() }, plan.Hybrid},
	}
	for _, m := range methods {
		s, e := m.stats()
		if e != nil {
			t.Fatal(e)
		}
		allocated := s.ReadCache.AllocatedBytes
		if m.name == "Hybrid" {
			cache, e := h.CacheStats()
			if e != nil {
				t.Fatal(e)
			}
			allocated = cache.AllocatedBytes()
		}
		if allocated != m.resources.ApplicationAllocatedBytes || s.Info.Memory.GlobalWriteBufferBytes != b.GlobalWriteBufferBytes {
			t.Fatal(m.name, "actual resource allocation mismatch", allocated, s)
		}
		if engine == kvstore.RocksDB {
			if s.Engine.WriteBufferCapacityBytes == nil || *s.Engine.WriteBufferCapacityBytes != uint64(b.GlobalWriteBufferBytes) || s.Engine.BlockCacheCapacityBytes == nil || *s.Engine.BlockCacheCapacityBytes != uint64(b.BlockCacheBytes) {
				t.Fatal("native capacities differ", m.name, s)
			}
		}
	}
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 64, Blocks: 4, TransactionsPerBlock: 8, Contracts: 2, StorageSlots: 4, Seed: 81})
	if e != nil {
		t.Fatal(e)
	}
	initial, final := map[string]kvstore.Measurement{}, map[string]kvstore.Measurement{}
	var initialCache, finalCache ht.ForestCacheStats
	for i, input := range w.Blocks {
		var r ArchiveResult
		if i == 0 {
			r, e = a.Execute(ctx, input)
			if e != nil {
				t.Fatal(e)
			}
			pr, e := p.ExecuteBlock(ctx, input)
			if e != nil || pr.Root != r.Root {
				t.Fatal("Path initialization", e)
			}
			hr, e := h.Apply(input, nil)
			if e != nil || hr.Root != r.Root {
				t.Fatal("Hybrid initialization", e)
			}
		} else {
			result, trace, err := a.ExecuteWithTrace(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			r = result
			pr, e := p.ApplyLogicalBlock(ctx, input, trace)
			if e != nil || pr.Root != r.Root {
				t.Fatal("Path import", e)
			}
			hr, e := h.ApplyLogicalBlock(input, trace)
			if e != nil || hr.Root != r.Root {
				t.Fatal("Hybrid import", e)
			}
		}
		req := q.Request{Height: input.Height, Kind: q.Storage, Address: common.HexToAddress("0x2000"), Slot: common.Hash{}}
		x, e := a.Query(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		y, e := p.Query(ctx, req)
		if e != nil {
			t.Fatal(e)
		}
		v, e := h.ViewAt(input.Height)
		if e != nil {
			t.Fatal(e)
		}
		z, e := q.Read(ctx, req, v)
		v.Close()
		if e != nil {
			t.Fatal(e)
		}
		if !sameJSON(x, y) || !sameJSON(x, z) {
			t.Fatal("cache changed query response")
		}
		if _, e = q.VerifyWithCodec(ctx, r.Root, req, x, q.DefaultLimits(), codec.Ethereum); e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			for _, m := range methods {
				initial[m.name], e = m.measure(ctx, true)
				if e != nil {
					t.Fatal(m.name, "initial measurement", e)
				}
			}
			initialCache, e = h.CacheStats()
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	deltas := map[string]kvstore.InventoryDelta{}
	for _, m := range methods {
		final[m.name], e = m.measure(ctx, true)
		if e != nil {
			t.Fatal(m.name, "final measurement", e)
		}
		end := final[m.name]
		if m.name != "Hybrid" {
			stats := end.BeforeMaintenance
			if stats.ReadCachePolicy != "materialized-node-lru" || stats.ReadCache.Hits == 0 || stats.ReadCache.Misses == 0 ||
				stats.ReadCache.AllocatedBytes != plan.CommonApplicationTargetBytes {
				t.Fatalf("%s did not exercise its matched execution cache: %+v", m.name, stats.ReadCache)
			}
		}
		if end.Inventory.State.Bytes() == 0 || end.Inventory.ExcludedComponents["configuration"].Bytes() == 0 || end.Inventory.State.Bytes()+end.Inventory.Excluded.Bytes() != end.Inventory.All.Bytes() {
			t.Fatal("incomplete measured state", m.name, end)
		}
		deltas[m.name], e = kvstore.Delta(initial[m.name], end)
		if e != nil {
			t.Fatal(e)
		}
		writes, e := kvstore.WriteDelta(initial[m.name].AfterMaintenance, end.BeforeMaintenance)
		if e != nil || writes.Commits != uint64(len(w.Blocks)-1) {
			t.Fatal(m.name, "write counts include maintenance or miss blocks", writes, e)
		}
	}
	finalCache, e = h.CacheStats()
	if e != nil {
		t.Fatal(e)
	}
	if initialCache.AllocatedBytes() != plan.Hybrid.ApplicationAllocatedBytes || finalCache.AllocatedBytes() != initialCache.AllocatedBytes() {
		t.Fatal("execution changed cache backing allocation")
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
	for _, m := range methods {
		s, e := m.stats()
		if e != nil {
			t.Fatal(e)
		}
		if s.ReadCache.AllocatedBytes != 0 || s.Scope.CreatedWithDatabase {
			t.Fatal("read-only execution cache/counting scope", m.name, s)
		}
		readonly, e := m.measure(ctx, false)
		if e != nil {
			t.Fatal(e)
		}
		if readonly.Inventory.SHA256 != final[m.name].Inventory.SHA256 {
			t.Fatal("read-only state changed", m.name)
		}
		if _, e = kvstore.WriteDelta(final[m.name].AfterMaintenance, s); e == nil {
			t.Fatal("reopen lost counters without an explicit scope change")
		}
	}
	process, e := CaptureProcessMemory()
	if e != nil {
		t.Fatal(e)
	}
	if reportDir := os.Getenv("COMPARISON_RESOURCE_REPORT_DIR"); reportDir != "" {
		report := struct {
			Schema                               string
			Engine                               kvstore.Engine
			Plan                                 ResourcePlan
			Initial, Final                       map[string]kvstore.Measurement
			Deltas                               map[string]kvstore.InventoryDelta
			Process                              ProcessMemory
			HybridCacheInitial, HybridCacheFinal ht.ForestCacheStats
			Verified                             bool
		}{"comparison-resource-smoke/v1", engine, plan, initial, final, deltas, process, initialCache, finalCache, true}
		raw, e := json.MarshalIndent(report, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.MkdirAll(reportDir, 0700); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(reportDir, "resources-"+string(engine)+".json"), append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("engine=%s verified resource budgets, initial/final inventories, signed deltas and open-scoped write counts for three methods", engine)
}
