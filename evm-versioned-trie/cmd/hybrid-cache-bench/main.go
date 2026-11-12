// hybrid-cache-bench compares real online EVM execution under three bounded
// forest cache policies. Generated databases/workloads stay outside source Git.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"strconv"
	"strings"
	"time"
)

type row struct {
	Profile, Mode, WorkloadID                              string
	Repetition, WarmupBlocks, Blocks, Transactions, Proofs int
	MeanMS, P50MS, P90MS, P95MS                            float64
	MeasuredBlockMS                                        []float64
	AllocatedBytes, ReferenceAllocatedBytes                uint64
	Verified                                               bool
	Cache                                                  ht.ForestCacheStats
	Config                                                 ht.ForestCacheConfig
	MeasuredBoundary                                       []blockPolicy          `json:",omitempty"`
	MeasuredWrites                                         *kvstore.WriteCounters `json:",omitempty"`
	FinalStorage                                           *kvstore.Measurement   `json:",omitempty"`
}
type blockPolicy struct {
	Height                                                                         int
	Due                                                                            bool
	EligibleSum, Attempts, Comparisons, Empty, HeatRejects, Evictions, Exhaustions uint64
	Hits, Misses, ExactGets, Seeks, HashGets                                       uint64
	IndexNanos, SamplingNanos, PolicyNanos                                         uint64
}

func measuredPolicy(i int, previous, current ht.BoundaryStats) blockPolicy {
	return blockPolicy{
		Height:        i,
		Due:           current.PolicyRuns > previous.PolicyRuns,
		EligibleSum:   current.EligibleSum - previous.EligibleSum,
		Attempts:      current.SampleAttempts - previous.SampleAttempts,
		Comparisons:   current.SampleComparisons - previous.SampleComparisons,
		Empty:         current.EmptySamples - previous.EmptySamples,
		HeatRejects:   current.HeatRejects - previous.HeatRejects,
		Evictions:     current.PolicyEvictions - previous.PolicyEvictions,
		Exhaustions:   current.WorkExhaustions - previous.WorkExhaustions,
		Hits:          current.NodeHits - previous.NodeHits,
		Misses:        current.NodeMisses - previous.NodeMisses,
		ExactGets:     current.PrefixExactGets - previous.PrefixExactGets,
		Seeks:         current.PrefixSeeks - previous.PrefixSeeks,
		HashGets:      current.HashGets - previous.HashGets,
		SamplingNanos: current.SamplingNanos - previous.SamplingNanos,
		IndexNanos:    current.SelectionIndexNanos - previous.SelectionIndexNanos,
		PolicyNanos:   current.PolicyNanos - previous.PolicyNanos,
	}
}

func save(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0600)
}
func allocation(s ht.ForestCacheStats) uint64 {
	return s.NodeAllocatedBytes + s.HashAllocatedBytes + s.CandidateAllocatedBytes + s.PlanAllocatedBytes + s.PolicyAllocatedBytes + s.InstanceBytes
}
func percentile(xs []float64, q float64) float64 {
	a := append([]float64(nil), xs...)
	sort.Float64s(a)
	return a[int(math.Ceil(q*float64(len(a))))-1]
}
func run() error {
	includeBoundary := flag.Bool("include-boundary", false, "also compare the no-H prefix-boundary policy at matched total backing allocation")
	studySlots := flag.Uint("boundary-study-slots", 0, "use a fixed prefix-boundary slot count for sample-size studies; partitioned supplies the correctness reference")
	sampleSize := flag.Uint("boundary-samples", 5, "distinct eligible eviction candidates per comparison (1..64)")
	sampleSeed := flag.Uint64("boundary-seed", 0, "reproducible boundary sampling seed")
	interval := flag.Uint64("boundary-interval", 16, "prefix-boundary policy interval in blocks")
	out := flag.String("out", "", "new output directory")
	accounts := flag.Int("accounts", 256, "background accounts")
	blocks := flag.Int("blocks", 2000, "execution blocks including warmup")
	warmup := flag.Int("warmup-blocks", workload.DefaultWarmupBlocks, "leading unmeasured execution blocks")
	txs := flag.Int("tx", 32, "transactions per block")
	contracts := flag.Int("contracts", 16, "storage tries")
	reps := flag.Int("repetitions", 3, "independent fresh databases per profile")
	profiles := flag.String("profiles", "account-hot,storage-hot,rotate,migrate,revisit", "comma separated profiles")
	work := flag.Uint("work-units", 262144, "shared optional work per block")
	half := flag.Uint64("half-life", 64, "heat half life for all policies")
	epsilon := flag.Float64("epsilon", .1, "shared admission hysteresis")
	aReserve := flag.Uint64("account-reserve-percent", 25, "shared mode account reservation")
	sReserve := flag.Uint64("storage-reserve-percent", 25, "shared mode storage reservation")
	trees := flag.Uint("trees-per-attempt", 4, "sampled tries per attempt")
	paths := flag.Uint("paths-per-trie", 2, "paths per sampled trie")
	flag.Parse()
	if *studySlots > 65536 {
		return fmt.Errorf("boundary-study-slots exceeds 65536")
	}
	if *sampleSize == 0 || *sampleSize > 64 {
		return fmt.Errorf("boundary-samples must be in [1,64]")
	}
	if *interval == 0 {
		return fmt.Errorf("boundary interval must be positive")
	}
	if *out == "" || *blocks < 3 || *warmup < 1 || *warmup >= *blocks || *reps < 1 || *reps > 10 {
		return fmt.Errorf("set -out, blocks>=3, 1<=warmup-blocks<blocks, and repetitions in [1,10]")
	}
	if *work > 1<<20 || *trees == 0 || *trees > 64 || *paths == 0 || *paths > 64 || *aReserve > 100 || *sReserve > 100-*aReserve {
		return fmt.Errorf("invalid sensitivity parameters")
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(*out, 0700); err != nil {
		return err
	}
	rows := []row{}
	for _, profile := range strings.Split(*profiles, ",") {
		for rep := 0; rep < *reps; rep++ {
			w, err := workload.Synthetic(workload.Options{BackgroundAccounts: *accounts, Blocks: *blocks, TransactionsPerBlock: *txs, Contracts: *contracts, StorageSlots: 64, Pattern: profile, Seed: int64(rep + 1)})
			if err != nil {
				return err
			}
			wid, wire, err := w.Freeze()
			if err != nil {
				return err
			}
			var expected [][32]byte
			var target uint64
			var referenceN, referenceH, referenceCandidates uint64
			modes := []string{"partitioned", "reserved-only", "reserved-shared"}
			if *studySlots > 0 {
				modes = []string{"partitioned", ht.PrefixBoundaryCache}
			} else if *includeBoundary {
				modes = append(modes, ht.PrefixBoundaryCache)
			}
			for _, mode := range modes {
				dir := filepath.Join(*out, fmt.Sprintf("%s-%d-%s", profile, rep+1, mode))
				if err := os.Mkdir(dir, 0700); err != nil {
					return err
				}
				if err := os.WriteFile(filepath.Join(dir, "workload.json"), wire, 0600); err != nil {
					return err
				}
				cfg := backend.DefaultHybridConfig(wid)
				cfg.Store.MaxBatchBytes = 16 << 20
				if mode == "partitioned" {
					cfg.Cache.Slots = 4
					cfg.Cache.PerTrie.NodeBytes = 16 << 10
					cfg.Cache.PerTrie.HashBytes = 8 << 10
					cfg.Cache.PerTrie.CandidateBytes = 64 << 10
					cfg.Cache.PerTrie.Policy.HalfLife = *half
				} else if mode == ht.PrefixBoundaryCache {
					cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
					cfg.Cache.Boundary.PolicyIntervalBlocks = *interval
					cfg.Cache.Boundary.SampleSize = uint32(*sampleSize)
					cfg.Cache.Boundary.SampleSeed = *sampleSeed
					cfg.Cache.Boundary.HalfLife = *half
					cfg.Cache.Boundary.WorkUnits = uint32(*work)
					if *studySlots > 0 {
						size, err := ht.BoundaryCacheAllocation(cfg.Cache.Boundary)
						if err != nil {
							return err
						}
						cfg.Cache.Boundary.NodeBudget.TotalBytes = uint64(*studySlots) * size.Boundary.SlotBytes
						cfg.Cache.Boundary.PlanBytes = 128 << 20
					}
					cfg.Cache.Boundary.NodeBudget.AccountReserveBytes = cfg.Cache.Boundary.NodeBudget.TotalBytes * *aReserve / 100
					cfg.Cache.Boundary.NodeBudget.StorageReserveBytes = cfg.Cache.Boundary.NodeBudget.TotalBytes * *sReserve / 100
					if *studySlots == 0 {
						if err := matchBoundaryAllocation(dir, &cfg, target, *aReserve, *sReserve); err != nil {
							return err
						}
					} else {
						plan, err := ht.BoundaryCacheAllocation(cfg.Cache.Boundary)
						if err != nil {
							return err
						}
						target = plan.AllocatedBytes()
					}
				} else {
					cfg.Cache = ht.DefaultReservedSharedForestCacheConfig()
					c := &cfg.Cache.ReservedShared
					c.NodeBudget = ht.ClassBudget{TotalBytes: referenceN}
					c.HashBudget = ht.ClassBudget{TotalBytes: referenceH}
					c.CandidateBytes = referenceCandidates
					c.MaxResidentTries = 64
					c.WorkUnits = uint32(*work)
					c.HalfLife = *half
					c.Epsilon = *epsilon
					c.TreesPerAttempt = uint32(*trees)
					c.PathsPerTrie = uint32(*paths)
					c.NodeBudget.AccountReserveBytes = c.NodeBudget.TotalBytes * *aReserve / 100
					c.NodeBudget.StorageReserveBytes = c.NodeBudget.TotalBytes * *sReserve / 100
					c.HashBudget.AccountReserveBytes = c.HashBudget.TotalBytes * *aReserve / 100
					c.HashBudget.StorageReserveBytes = c.HashBudget.TotalBytes * *sReserve / 100
					if mode == "reserved-only" {
						c.NodeBudget.AccountReserveBytes = referenceN / 2
						c.NodeBudget.StorageReserveBytes = referenceN - referenceN/2
						c.HashBudget.AccountReserveBytes = referenceH / 2
						c.HashBudget.StorageReserveBytes = referenceH - referenceH/2
					}
					// Measure global backing arrays before selecting PlanBytes. The
					// reference has already warmed four slots on this exact workload.
					probe, err := backend.OpenHybrid(filepath.Join(dir, "budget-probe"), cfg, true, false)
					if err != nil {
						return err
					}
					stats, _ := probe.CacheStats()
					if err = probe.Close(); err != nil {
						return err
					}
					other := allocation(stats) - stats.PlanAllocatedBytes
					if target <= other {
						return fmt.Errorf("reference budget too small for shared metadata")
					}
					c.PlanBytes = target - other
				}
				if err := save(filepath.Join(dir, "config.json"), cfg); err != nil {
					return err
				}
				b, err := backend.OpenHybrid(filepath.Join(dir, "db"), cfg, true, false)
				if err != nil {
					return err
				}
				r := row{WarmupBlocks: *warmup, Profile: profile, Mode: mode, Repetition: rep + 1, WorkloadID: wid, Config: cfg.Cache}
				var cacheStart ht.ForestCacheStats
				study := *studySlots > 0 && mode == ht.PrefixBoundaryCache
				var previous ht.BoundaryStats
				var writesStart kvstore.Statistics
				for i, input := range w.Blocks {
					start := time.Now()
					result, err := b.Apply(input, nil)
					elapsed := float64(time.Since(start)) / 1e6
					if err != nil {
						b.Close()
						return fmt.Errorf("%s block %d: %w", dir, i, err)
					}
					raw, _ := json.Marshal(result)
					digest := sha256.Sum256(raw)
					if mode == "partitioned" {
						expected = append(expected, digest)
					} else if expected[i] != digest {
						b.Close()
						return fmt.Errorf("root/receipt/VM outcome differs at %s block %d", dir, i)
					}
					if i == *warmup {
						cacheStart, _ = b.CacheStats()
						if mode == "partitioned" {
							target = allocation(cacheStart)
							referenceN, referenceH, referenceCandidates = cacheStart.NodeAllocatedBytes, cacheStart.HashAllocatedBytes, cacheStart.CandidateAllocatedBytes
						}
					}
					if study && i >= *warmup {
						stats, err := b.CacheStats()
						if err != nil {
							b.Close()
							return err
						}
						current := stats.Boundary
						if i == *warmup {
							writesStart, err = b.EngineStats()
							if err != nil {
								b.Close()
								return err
							}
						} else {
							r.MeasuredBoundary = append(r.MeasuredBoundary, measuredPolicy(i, previous, current))
						}
						previous = current
					}
					if i > *warmup {
						r.MeasuredBlockMS = append(r.MeasuredBlockMS, elapsed)
						r.Blocks++
						r.Transactions += len(input.Transactions)
					}
				}
				if study {
					end, err := b.EngineStats()
					if err != nil {
						b.Close()
						return err
					}
					writes, err := kvstore.WriteDelta(writesStart, end)
					if err != nil {
						b.Close()
						return err
					}
					r.MeasuredWrites = &writes
					storage, err := b.MeasureStorage(context.Background(), true)
					if err != nil {
						b.Close()
						return err
					}
					r.FinalStorage = &storage
				}
				r.Cache, err = b.CacheStats()
				if err != nil {
					b.Close()
					return err
				}
				r.AllocatedBytes = allocation(r.Cache)
				r.ReferenceAllocatedBytes = target
				if mode != "partitioned" && (r.AllocatedBytes > target || target-r.AllocatedBytes > 2048) {
					b.Close()
					return fmt.Errorf("backing allocation mismatch %d vs %d", r.AllocatedBytes, target)
				}
				if err = b.Close(); err != nil {
					return err
				}
				b, err = backend.OpenHybrid(filepath.Join(dir, "db"), cfg, false, true)
				if err != nil {
					return err
				}
				for _, height := range []uint64{0, uint64(*blocks / 2), uint64(*blocks)} {
					v, err := b.ViewAt(height)
					if err != nil {
						b.Close()
						return err
					}
					for k := 0; k < min(8, *contracts); k++ {
						a := common.BigToAddress(big.NewInt(int64(0x2000 + k)))
						proof, err := v.AccountProof(a)
						if err != nil {
							v.Close()
							b.Close()
							return err
						}
						if _, _, err = mpt.VerifyProof(v.Root(), state.AccountKey(a), proof); err != nil {
							v.Close()
							b.Close()
							return err
						}
						r.Proofs++
						root, proof, err := v.StorageProof(a, common.Hash{})
						if err != nil {
							v.Close()
							b.Close()
							return err
						}
						if _, _, err = mpt.VerifyProof(root, state.SlotKey(common.Hash{}), proof); err != nil {
							v.Close()
							b.Close()
							return err
						}
						r.Proofs++
					}
					v.Close()
				}
				if err = b.Close(); err != nil {
					return err
				}
				for _, x := range r.MeasuredBlockMS {
					r.MeanMS += x
				}
				r.MeanMS /= float64(len(r.MeasuredBlockMS))
				r.P50MS = percentile(r.MeasuredBlockMS, .5)
				r.P90MS = percentile(r.MeasuredBlockMS, .9)
				r.P95MS = percentile(r.MeasuredBlockMS, .95)
				r.Verified = true
				if err := save(filepath.Join(dir, "checkpoint.json"), r); err != nil {
					return err
				}
				if err := save(filepath.Join(dir, "warmup-cache.json"), cacheStart); err != nil {
					return err
				}
				rows = append(rows, r)
				if err := save(filepath.Join(*out, "results.json"), rows); err != nil {
					return err
				}
				fmt.Printf("%s repeat=%d %s mean=%.3f ms p95=%.3f ms bytes=%d verified\n", profile, rep+1, mode, r.MeanMS, r.P95MS, r.AllocatedBytes)
			}
		}
	}
	f, err := os.OpenFile(filepath.Join(*out, "results.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	csvw := csv.NewWriter(f)
	csvw.Write([]string{"profile", "repeat", "mode", "blocks", "transactions", "mean_ms", "p50_ms", "p90_ms", "p95_ms", "allocated_bytes", "reference_bytes", "verified"})
	for _, r := range rows {
		csvw.Write([]string{r.Profile, strconv.Itoa(r.Repetition), r.Mode, strconv.Itoa(r.Blocks), strconv.Itoa(r.Transactions), fmt.Sprint(r.MeanMS), fmt.Sprint(r.P50MS), fmt.Sprint(r.P90MS), fmt.Sprint(r.P95MS), fmt.Sprint(r.AllocatedBytes), fmt.Sprint(r.ReferenceAllocatedBytes), fmt.Sprint(r.Verified)})
	}
	csvw.Flush()
	if err := csvw.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Match owned buffers, including the second bank, indexes and metadata.
func matchBoundaryAllocation(dir string, cfg *backend.HybridConfig, target, accountReserve, storageReserve uint64) error {
	for attempt := 0; attempt < 8; attempt++ {
		b, e := backend.OpenHybrid(filepath.Join(dir, fmt.Sprintf("budget-probe-%d", attempt)), *cfg, true, false)
		if e != nil {
			return e
		}
		s, e := b.CacheStats()
		ce := b.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
		total := allocation(s)
		if total <= target && target-total <= 2048 {
			return nil
		}
		quantum := int64(2 * s.Boundary.SlotBytes)
		delta := int64(target) - int64(total)
		steps := delta / quantum
		if delta < 0 && delta%quantum != 0 {
			steps--
		}
		if steps == 0 {
			steps = -1
		}
		slots := int64(s.Boundary.Slots) + steps
		if slots < 1 {
			return fmt.Errorf("reference budget cannot fit boundary cache")
		}
		c := &cfg.Cache.Boundary
		c.NodeBudget.TotalBytes = uint64(slots) * s.Boundary.SlotBytes
		c.NodeBudget.AccountReserveBytes = c.NodeBudget.TotalBytes * accountReserve / 100
		c.NodeBudget.StorageReserveBytes = c.NodeBudget.TotalBytes * storageReserve / 100
		c.PlanBytes = 128 << 20
	}
	return fmt.Errorf("could not match boundary allocation within 2048 bytes")
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
