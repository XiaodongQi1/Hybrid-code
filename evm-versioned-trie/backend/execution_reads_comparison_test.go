package backend

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
	"time"
)

// Opt-in experiment: same generated blocks, native engine and resident budget
// for all modes. The extra transient read budget is reported, not hidden in an
// equal-total-memory claim. Regular correctness coverage is in hybrid_*_test.go.
func TestHybridExecutionReadComparison(t *testing.T) {
	report := os.Getenv("HYBRID_EXECUTION_READ_REPORT")
	if report == "" {
		t.Skip("opt-in timing experiment")
	}
	engine := kvstore.Engine(os.Getenv("HYBRID_EXECUTION_READ_ENGINE"))
	if engine == "" {
		engine = kvstore.LevelDB
	}
	type row struct {
		Workload, Mode                                     string
		Trial, WarmupBlocks, MeasuredBlocks                int
		MeanBlockMS                                        float64
		BlockMS                                            []float64
		RetainedCacheBytes, ExtraReadBudgetBytes           uint64
		Before, After                                      ht.ForestCacheStats
		PrefixRecords, HashRecords, PrefixBytes, HashBytes uint64
	}
	var rows []row
	for _, scenario := range []struct {
		name, pattern      string
		warmup, blocks, tx int
		large              bool
	}{
		{"cold-prefix", "account-sweep", 0, 12, 64, false},
		{"warm-small-cache", "migrate", 1000, 20, 16, false},
		{"warm-resident", "account-sweep", 1000, 20, 8, true},
	} {
		if selected := os.Getenv("HYBRID_EXECUTION_READ_CASE"); selected != "" && selected != scenario.name {
			continue
		}
		w, err := workload.Synthetic(workload.Options{BackgroundAccounts: 2048, Blocks: scenario.warmup + scenario.blocks, TransactionsPerBlock: scenario.tx, Contracts: 8, StorageSlots: 32, Pattern: scenario.pattern, Seed: 71})
		if err != nil {
			t.Fatal(err)
		}
		expected := make([]mpt.Hash, len(w.Blocks))
		for trial := 0; trial < 2; trial++ {
			for mode := 0; mode < 4; mode++ {
				name := []string{"legacy", "lazy-sync", "async-1", "async-2"}[mode]
				cfg := DefaultHybridConfig("execution-read-comparison")
				cfg.Initialization = InitializationPrefix
				options := kvstore.DefaultOptions(engine)
				options.BlockCacheBytes = 2 << 20
				options.WriteBufferBytes = 2 << 20
				cfg.Store.Storage = &options
				cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
				cfg.Cache.Boundary.PolicyIntervalBlocks = 16
				a, err := ht.BoundaryCacheAllocation(cfg.Cache.Boundary)
				if err != nil {
					t.Fatal(err)
				}
				slots := uint64(32)
				if scenario.large {
					slots = 4096
					cfg.Cache.Boundary.PlanBytes = 8 << 20
				}
				cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: slots * a.Boundary.SlotBytes}
				if mode > 0 {
					cfg.Cache.Boundary.ExecutionReads = ht.DefaultExecutionReadConfig()
					cfg.Cache.Boundary.ExecutionReads.Workers = uint32(mode - 1)
				}
				b, err := OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
				if err != nil {
					t.Fatal(err)
				}
				r := row{Workload: scenario.name, Mode: name, Trial: trial, WarmupBlocks: scenario.warmup, MeasuredBlocks: scenario.blocks}
				for i, input := range w.Blocks {
					if i == scenario.warmup+1 {
						r.Before, err = b.CacheStats()
						if err != nil {
							t.Fatal(err)
						}
					}
					start := time.Now()
					out, e := b.Apply(input, nil)
					elapsed := time.Since(start)
					if e != nil {
						b.Close()
						t.Fatalf("%s %s block %d: %v", scenario.name, name, i, e)
					}
					if trial == 0 && mode == 0 {
						expected[i] = out.Root
					} else if expected[i] != out.Root {
						b.Close()
						t.Fatalf("root differs at %s %s %d", scenario.name, name, i)
					}
					if i > scenario.warmup {
						r.BlockMS = append(r.BlockMS, float64(elapsed)/float64(time.Millisecond))
						c := b.store.(*hybridStore).lastCommit
						r.PrefixRecords += c.PrefixRecords
						r.HashRecords += c.HashRecords
						r.PrefixBytes += c.PrefixBytes
						r.HashBytes += c.HashBytes
					}
				}
				r.After, err = b.CacheStats()
				if err != nil {
					t.Fatal(err)
				}
				r.RetainedCacheBytes = r.After.AllocatedBytes()
				r.ExtraReadBudgetBytes = r.After.ExecutionReadBudgetBytes
				for _, v := range r.BlockMS {
					r.MeanBlockMS += v
				}
				r.MeanBlockMS /= float64(len(r.BlockMS))
				if err = b.Close(); err != nil {
					t.Fatal(err)
				}
				rows = append(rows, r)
				t.Logf("%s %s trial%d: %.3f ms/block, %d reads", scenario.name, name, trial, r.MeanBlockMS, r.After.ExecutionReads.RecordReads-r.Before.ExecutionReads.RecordReads)
				data, _ := json.MarshalIndent(rows, "", "  ")
				if err = os.WriteFile(report, append(data, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	fmt.Fprintln(os.Stdout, "execution read comparison:", report)
}
