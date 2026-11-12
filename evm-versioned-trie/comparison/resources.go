package comparison

import (
	"fmt"

	pa "github.com/XiaodongQi1/Hybrid-code/evm-path-archive"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

type ResourceBudget struct {
	MaxBatchBytes, MaxStagingBytes          uint64
	ApplicationCacheBytes                   uint64
	BlockCacheBytes, GlobalWriteBufferBytes int64
	OpenFiles, BackgroundJobs               int
	Compression                             kvstore.Compression
}

func DefaultResourceBudget() ResourceBudget {
	return ResourceBudget{ApplicationCacheBytes: 64 << 20, BlockCacheBytes: 64 << 20, GlobalWriteBufferBytes: 32 << 20, OpenFiles: 128, BackgroundJobs: 2, Compression: kvstore.None, MaxBatchBytes: 8 << 20, MaxStagingBytes: 8 << 20}
}

type MethodResources struct {
	ProofContextBudgetBytes uint64 `json:",omitempty"`
	// Transient read-table limit, additional to the matched resident allocation.
	ExecutionReadBudgetBytes                                                 uint64 `json:",omitempty"`
	ExecutionNodeCacheBytes                                                  uint64 `json:",omitempty"`
	Storage                                                                  kvstore.Options
	ApplicationAllocatedBytes, ApplicationSlackBytes, AllocationQuantumBytes uint64
}

// ResourcePlan matches actual cache backing, not NodeBudget or resident values.
// Baselines use a fixed-allocation parsed-node LRU; Hybrid uses its existing
// parsed-node forest cache. Their lookup policies deliberately remain distinct.
type ResourcePlan struct {
	Requested                                                ResourceBudget
	CommonApplicationTargetBytes, UnassignedApplicationBytes uint64
	Archive, Path, Hybrid                                    MethodResources
	HybridCache                                              ht.ForestCacheConfig
	HybridAllocation                                         ht.ForestCacheStats
}

func (b ResourceBudget) Resolve(engine kvstore.Engine, template ht.ForestCacheConfig) (ResourcePlan, error) {
	p := ResourcePlan{Requested: b}
	if b.ApplicationCacheBytes == 0 || b.ApplicationCacheBytes > 1<<30 || b.BlockCacheBytes <= 0 || b.GlobalWriteBufferBytes < 128<<10 || b.GlobalWriteBufferBytes > 512<<20 || b.OpenFiles < 16 || b.BackgroundJobs < 1 || b.BackgroundJobs > 64 {
		return p, kvstore.ErrInvalid
	}
	if b.MaxBatchBytes < 4096 || b.MaxBatchBytes > 128<<20 || b.MaxStagingBytes < 1024 || b.MaxStagingBytes > 128<<20 {
		return p, kvstore.ErrInvalid
	}
	if template.Mode != ht.PrefixBoundaryCache || template.Slots != 0 || template.PerTrie != (ht.CacheConfig{}) || template.ReservedShared != (ht.ReservedSharedConfig{}) || template.Boundary.MaxNodeBytes != 532 {
		return p, fmt.Errorf("%w: comparison budgets require native prefix-boundary cache", kvstore.ErrInvalid)
	}
	boundary, allocation, e := ht.FitBoundaryCache(b.ApplicationCacheBytes, template.Boundary)
	if e != nil {
		return p, e
	}
	p.HybridCache = template
	p.HybridCache.Boundary = boundary
	p.HybridAllocation = allocation
	p.CommonApplicationTargetBytes = allocation.AllocatedBytes()
	p.UnassignedApplicationBytes = b.ApplicationCacheBytes - p.CommonApplicationTargetBytes
	lru, e := forest.PlanExecutionNodeCache(p.CommonApplicationTargetBytes)
	if e != nil {
		return p, e
	}
	options := kvstore.Options{Engine: engine, Compression: b.Compression, BlockCacheBytes: b.BlockCacheBytes, GlobalWriteBufferBytes: b.GlobalWriteBufferBytes, WriteBufferBytes: b.GlobalWriteBufferBytes / 2, OpenFiles: b.OpenFiles, EnableStatistics: true}
	if engine == kvstore.RocksDB {
		options.BackgroundJobs = b.BackgroundJobs
	}
	for _, item := range []struct {
		out                *MethodResources
		columns            []string
		cache              uint64
		allocated, quantum uint64
	}{
		{&p.Archive, []string{"default", "hash"}, p.CommonApplicationTargetBytes, lru.AllocatedBytes, lru.SlotBytes},
		{&p.Path, []string{"default"}, p.CommonApplicationTargetBytes, lru.AllocatedBytes, lru.SlotBytes},
		{&p.Hybrid, []string{"default", "prefix", "hash"}, 0, p.CommonApplicationTargetBytes, allocation.Boundary.AllocationQuantumBytes},
	} {
		o := options
		o.Columns = item.columns
		o, e = o.Resolved()
		if e != nil {
			return p, e
		}
		*item.out = MethodResources{Storage: o, ExecutionNodeCacheBytes: item.cache, ApplicationAllocatedBytes: item.allocated, ApplicationSlackBytes: b.ApplicationCacheBytes - item.allocated, AllocationQuantumBytes: item.quantum}
	}
	p.Hybrid.ExecutionReadBudgetBytes = allocation.ExecutionReadBudgetBytes
	return p, nil
}
func (p ResourcePlan) ArchiveConfig(workload string) ArchiveConfig {
	c := DefaultArchiveConfig(workload, p.Archive.Storage.Engine)
	c.ExecutionNodeCacheBytes = p.Archive.ExecutionNodeCacheBytes
	c.Storage = ownedOptions(p.Archive.Storage)
	c.MaxBatchBytes, c.MaxStagingBytes = p.Requested.MaxBatchBytes, p.Requested.MaxStagingBytes
	return c
}
func (p ResourcePlan) PathConfig(workload string) pa.Config {
	c := pa.DefaultConfig(workload)
	c.Codec, c.Format, c.ZeroGenesis, c.StateOnly = codec.Ethereum, pa.PackedFormat, true, true
	o := ownedOptions(p.Path.Storage)
	c.Storage = &o
	c.ExecutionNodeCacheBytes = p.Path.ExecutionNodeCacheBytes
	c.MaxBatchBytes, c.MaxStagingBytes = p.Requested.MaxBatchBytes, p.Requested.MaxStagingBytes
	return c
}
func (p ResourcePlan) HybridConfig(workload string) backend.HybridConfig {
	c := backend.DefaultHybridConfig(workload)
	o := ownedOptions(p.Hybrid.Storage)
	c.Store.Storage = &o
	c.Store.MaxBatchBytes = p.Requested.MaxBatchBytes
	c.Cache = p.HybridCache
	c.Initialization = backend.InitializationHashCold
	return c
}
func ownedOptions(o kvstore.Options) kvstore.Options {
	o.Columns = append([]string(nil), o.Columns...)
	return o
}
