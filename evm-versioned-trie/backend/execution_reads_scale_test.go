package backend

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// This opt-in scale experiment changes no production path. Each completed
// group is saved atomically; unfinished groups are rerun from initialization.
type executionReadScaleConfig struct {
	StabilizeInitial                                             bool
	Output, SourceID                                             string
	Accounts, WarmupBlocks, MeasuredBlocks, Transactions, Trials int
	Seed                                                         int64
	NodeBudgets                                                  []uint64
}
type executionReadScaleTruth struct {
	Root     mpt.Hash
	Outcomes [32]byte
}
type executionReadScaleSample struct {
	Height                                                  uint64
	TotalNS, PrepareNS, ExecuteNS, MaterializeNS, PublishNS int64
	ProjectionNS, WriteNS                                   uint64
}
type executionReadScaleGroup struct {
	ConfigID, SourceID, WorkloadID, Mode               string
	Budget                                             uint64
	Trial, Order                                       int
	Verified                                           bool
	CacheConfig                                        ht.ForestCacheConfig
	Initial                                            *kvstore.Measurement `json:",omitempty"`
	Before, After                                      ht.ForestCacheStats
	EngineBefore, EngineAfter                          kvstore.Statistics
	Samples                                            []executionReadScaleSample
	PrefixRecords, HashRecords, PrefixBytes, HashBytes uint64
	TotalNS                                            int64
}

func scaleSave(t *testing.T, path string, v any) {
	t.Helper()
	data, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	tmp, e := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if e != nil {
		t.Fatal(e)
	}
	defer os.Remove(tmp.Name())
	if _, e = tmp.Write(append(data, '\n')); e != nil {
		tmp.Close()
		t.Fatal(e)
	}
	if e = tmp.Sync(); e != nil {
		tmp.Close()
		t.Fatal(e)
	}
	if e = tmp.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(tmp.Name(), path); e != nil {
		t.Fatal(e)
	}
}
func TestHybridExecutionReadScale(t *testing.T) {
	name := os.Getenv("HYBRID_EXECUTION_READ_SCALE_CONFIG")
	if name == "" {
		t.Skip("opt-in scale experiment")
	}
	raw, e := os.ReadFile(name)
	if e != nil {
		t.Fatal(e)
	}
	var c executionReadScaleConfig
	if e = json.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	if c.Output == "" || c.SourceID == "" || c.Trials < 1 || c.Trials > 4 || c.MeasuredBlocks < 1 || len(c.NodeBudgets) == 0 {
		t.Fatal("invalid scale configuration")
	}
	if e = os.MkdirAll(c.Output, 0700); e != nil {
		t.Fatal(e)
	}
	configDigest := sha256.Sum256(raw)
	configID := fmt.Sprintf("%x", configDigest)
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: c.Accounts, Blocks: c.WarmupBlocks + c.MeasuredBlocks, TransactionsPerBlock: c.Transactions, Contracts: 8, StorageSlots: 32, Pattern: "account-sweep", Seed: c.Seed})
	if e != nil {
		t.Fatal(e)
	}
	wire, e := json.Marshal(w)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(wire)
	wire = nil
	workloadID := fmt.Sprintf("%x", sum)
	truthPath := filepath.Join(c.Output, "truth.json")
	truth := make([]executionReadScaleTruth, len(w.Blocks))
	haveTruth := false
	if data, e := os.ReadFile(truthPath); e == nil {
		var saved struct {
			ConfigID, WorkloadID string
			Truth                []executionReadScaleTruth
		}
		if e = json.Unmarshal(data, &saved); e != nil || saved.ConfigID != configID || saved.WorkloadID != workloadID || len(saved.Truth) != len(truth) {
			t.Fatal("reference checkpoint mismatch", e)
		}
		truth, haveTruth = saved.Truth, true
	} else if !os.IsNotExist(e) {
		t.Fatal(e)
	}
	names := []string{"legacy", "lazy-sync", "async-1", "async-2"}
	for trial := 0; trial < c.Trials; trial++ {
		for _, budget := range c.NodeBudgets {
			for order := 0; order < 4; order++ {
				mode := (order + trial) % 4
				groupName := fmt.Sprintf("budget-%d-trial-%d-%s", budget, trial, names[mode])
				path := filepath.Join(c.Output, groupName+".json")
				if data, err := os.ReadFile(path); err == nil {
					var g executionReadScaleGroup
					if err = json.Unmarshal(data, &g); err != nil || !g.Verified || g.ConfigID != configID || g.SourceID != c.SourceID || g.WorkloadID != workloadID || !haveTruth || len(g.Samples) != c.MeasuredBlocks {
						t.Fatal("invalid completed group", groupName, err)
					}
					t.Log("resume: keeping", groupName)
					continue
				} else if !os.IsNotExist(err) {
					t.Fatal(err)
				}
				scaleSave(t, filepath.Join(c.Output, "status.json"), map[string]any{"Group": groupName, "Phase": "initialization", "Height": 0, "ConfigID": configID})
				cfg := DefaultHybridConfig(workloadID)
				cfg.Initialization = InitializationPrefix
				options := kvstore.DefaultOptions(kvstore.RocksDB)
				options.BlockCacheBytes = 2 << 20
				options.GlobalWriteBufferBytes = 2 << 20
				options.WriteBufferBytes = 1 << 20
				options.BackgroundJobs = 2
				options.EnableStatistics = true
				cfg.Store.Storage = &options
				cfg.Store.MaxBatchBytes = 32 << 20
				cfg.Cache = ht.DefaultAdaptiveEthereumForestCacheConfig()
				cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: budget, AccountReserveBytes: budget / 4, StorageReserveBytes: budget / 4}
				cfg.Cache.Boundary.PlanBytes = 128 << 20
				cfg.Cache.Boundary.MetadataBytes = 64 << 20
				cfg.Cache.Boundary.WorkUnits = 4 << 20
				if mode != 0 {
					cfg.Cache.Boundary.ExecutionReads = ht.DefaultExecutionReadConfig()
					cfg.Cache.Boundary.ExecutionReads.Workers = uint32(mode - 1)
				}
				runtime.GC() // outside timing, before allocating this group's independent DB/cache
				dbPath := filepath.Join(t.TempDir(), "state")
				b, err := OpenHybrid(dbPath, cfg, true, false)
				if err != nil {
					t.Fatal(err)
				}
				g := executionReadScaleGroup{ConfigID: configID, SourceID: c.SourceID, WorkloadID: workloadID, Mode: names[mode], Budget: budget, Trial: trial, Order: order, CacheConfig: cfg.Cache}
				for i, input := range w.Blocks {
					if i == c.WarmupBlocks+1 {
						g.Before, err = b.CacheStats()
						if err != nil {
							b.Close()
							t.Fatal(err)
						}
						g.EngineBefore, err = b.EngineStats()
						if err != nil {
							b.Close()
							t.Fatal(err)
						}
					}
					var timing block.ExecutionTiming
					start := time.Now()
					result, err := b.ApplyMeasured(input, nil, &timing)
					end := time.Now()
					if err != nil {
						b.Close()
						t.Fatalf("%s height %d: %v", groupName, i, err)
					}
					// Root/receipt checks and reports remain outside all timed intervals.
					receipts, err := json.Marshal(result.Outcomes)
					if err != nil {
						b.Close()
						t.Fatal(err)
					}
					got := executionReadScaleTruth{result.Root, sha256.Sum256(receipts)}
					if !haveTruth {
						truth[i] = got
					} else if truth[i] != got {
						b.Close()
						t.Fatalf("%s root/receipt mismatch at %d", groupName, i)
					}
					if i == 0 && c.StabilizeInitial {
						measure, err := b.MeasureStorage(context.Background(), true)
						if err != nil {
							b.Close()
							t.Fatal(err)
						}
						g.Initial = &measure
					}
					if i > c.WarmupBlocks {
						if timing.Transactions.Before(start) || timing.Materialize.Before(timing.Transactions) || timing.Publish.Before(timing.Materialize) || end.Before(timing.Publish) {
							b.Close()
							t.Fatal("invalid stage timings")
						}
						commit := b.store.(*hybridStore).lastCommit
						g.Samples = append(g.Samples, executionReadScaleSample{input.Height, end.Sub(start).Nanoseconds(), timing.Transactions.Sub(start).Nanoseconds(), timing.Materialize.Sub(timing.Transactions).Nanoseconds(), timing.Publish.Sub(timing.Materialize).Nanoseconds(), end.Sub(timing.Publish).Nanoseconds(), commit.ProjectionNanos, commit.WriteNanos})
						g.TotalNS += end.Sub(start).Nanoseconds()
						g.PrefixRecords += commit.PrefixRecords
						g.HashRecords += commit.HashRecords
						g.PrefixBytes += commit.PrefixBytes
						g.HashBytes += commit.HashBytes
					}
					if i%100 == 0 || i == len(w.Blocks)-1 {
						phase := "warmup"
						if i > c.WarmupBlocks {
							phase = "measurement"
						}
						scaleSave(t, filepath.Join(c.Output, "status.json"), map[string]any{"Group": groupName, "Phase": phase, "Height": i, "ConfigID": configID})
						t.Logf("%s %s height %d", groupName, phase, i)
					}
				}
				g.After, err = b.CacheStats()
				if err != nil {
					b.Close()
					t.Fatal(err)
				}
				g.EngineAfter, err = b.EngineStats()
				if err != nil {
					b.Close()
					t.Fatal(err)
				}
				if err = b.Close(); err != nil {
					t.Fatal(err)
				}
				if !haveTruth {
					scaleSave(t, truthPath, struct {
						ConfigID, WorkloadID string
						Truth                []executionReadScaleTruth
					}{configID, workloadID, truth})
					haveTruth = true
				}
				g.Verified = true
				scaleSave(t, path, g)
				if err = os.RemoveAll(dbPath); err != nil {
					t.Fatal(err)
				} // only this test's temporary, closed DB
				t.Logf("DONE %s: %.3f ms/block; resident=%d; reads=%d", groupName, float64(g.TotalNS)/float64(c.MeasuredBlocks)/1e6, g.After.Boundary.NodeEntries, g.After.ExecutionReads.RecordReads-g.Before.ExecutionReads.RecordReads)
			}
		}
	}
	scaleSave(t, filepath.Join(c.Output, "completion.json"), map[string]any{"Verified": true, "Groups": c.Trials * len(c.NodeBudgets) * 4, "ConfigID": configID, "WorkloadID": workloadID})
}
