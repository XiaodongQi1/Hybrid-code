//go:build darwin || linux

package backend

import (
	"context"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type plannedProofMeasurement struct {
	Mode                                                                                                          string
	Policy                                                                                                        ht.ParallelProofConfig
	Trial, Clients, Queries                                                                                       int
	Verified                                                                                                      bool
	LatencyNS, VerificationNS                                                                                     []int64
	AccountNS, StorageNS                                                                                          []int64
	CPUTimeNS, AllocatedBytes                                                                                     uint64
	ThroughputElapsedNS                                                                                           int64
	TotalReads, PrefixReads, HashReads, SiblingHeaders, BodyDecodes, WorkerStarts, CursorCreates, BackgroundReads uint64
	PeakContextBytes, PeakTemporaryBytes, MaxReadBufferBytes                                                      uint64
	MainWalkNanos, WaitNanos, AssembleNanos                                                                       uint64
}

func proofCPU() uint64 {
	var r syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &r) != nil {
		return 0
	}
	return uint64(r.Utime.Sec+r.Stime.Sec)*1e9 + uint64(r.Utime.Usec+r.Stime.Usec)*1000
}

// Opt-in, warm-read, native end-to-end proof comparison. Verification is outside
// timing; each group saves immediately, and all readers use the same closed DB.
func TestParallelProofComparison(t *testing.T) {
	out := os.Getenv("HYBRID_PARALLEL_PROOF_REPORT")
	if out == "" {
		t.Skip("opt-in proof comparison")
	}
	engine := kvstore.Engine(os.Getenv("HYBRID_PARALLEL_PROOF_ENGINE"))
	if engine == "" {
		engine = kvstore.LevelDB
	}
	if e := os.MkdirAll(filepath.Dir(out), 0700); e != nil {
		t.Fatal(e)
	}
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 4096, Blocks: 64, TransactionsPerBlock: 64, Contracts: 16, StorageSlots: 128, Seed: 71})
	if e != nil {
		t.Fatal(e)
	}
	cfg := DefaultHybridConfig("parallel-proof-comparison")
	cfg.Initialization = InitializationPrefix
	op := kvstore.DefaultOptions(engine)
	op.BlockCacheBytes = 2 << 20
	op.WriteBufferBytes = 8 << 20
	op.GlobalWriteBufferBytes = 16 << 20
	if engine == kvstore.RocksDB {
		op.BackgroundJobs = 2
	}
	cfg.Store.Storage = &op
	cfg.Cache = ht.DefaultAdaptiveEthereumForestCacheConfig()
	cfg.Cache.Boundary, _, e = ht.FitBoundaryCache(8<<20, cfg.Cache.Boundary)
	if e != nil {
		t.Fatal(e)
	}
	db := filepath.Join(t.TempDir(), "state")
	b, e := OpenHybrid(db, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	var roots []mpt.Hash
	for _, input := range w.Blocks {
		r, e := b.Apply(input, nil)
		if e != nil {
			b.Close()
			t.Fatal(e)
		}
		roots = append(roots, r.Root)
	}
	before, e := b.MeasureStorage(context.Background(), true)
	if e != nil {
		b.Close()
		t.Fatal(e)
	}
	if e = b.Close(); e != nil {
		t.Fatal(e)
	}
	makeRequests := func(n int, seed int64, exclude map[query.Request]bool) []query.Request {
		requests := make([]query.Request, 0, n)
		rng := rand.New(rand.NewSource(seed))
		for len(requests) < n {
			r := query.Request{Height: uint64(32 + rng.Intn(33)), Kind: query.Account, Address: common.BigToAddress(big.NewInt(int64(0x100000 + rng.Intn(4096))))}
			if len(requests)%2 != 0 {
				r.Kind = query.Storage
				r.Address = common.BigToAddress(big.NewInt(int64(0x2000 + rng.Intn(16))))
				r.Slot = common.BigToHash(big.NewInt(int64(rng.Intn(128))))
			}
			if !exclude[r] {
				requests = append(requests, r)
			}
		}
		return requests
	}
	requests := makeRequests(512, 902, nil)
	measured := make(map[query.Request]bool, len(requests))
	for _, q := range requests {
		measured[q] = true
	}
	warmup := makeRequests(128, 903, measured)
	want := make([]query.Response, len(requests))
	b, e = OpenHybrid(db, cfg, false, true)
	if e != nil {
		t.Fatal(e)
	}
	for i, q := range requests {
		want[i], e = b.Query(context.Background(), q)
		if e != nil {
			b.Close()
			t.Fatal(e)
		}
		if _, e = query.VerifyWithCodec(context.Background(), roots[q.Height], q, want[i], query.DefaultLimits(), codec.Ethereum); e != nil {
			b.Close()
			t.Fatal(e)
		}
	}
	b.Close()
	var rows []plannedProofMeasurement
	names := []string{"legacy", "planned-sync", "parallel-1", "parallel-2", "parallel-4"}
	baseConfig := cfg
	report := func() {
		scaleSave(t, out, struct {
			Engine            kvstore.Engine
			Config            HybridConfig
			Requests          []query.Request
			WarmupRequests    []query.Request
			Rows              []plannedProofMeasurement
			ReadOnlyUnchanged bool
		}{engine, baseConfig, requests, warmup, rows, true})
	}
	for trial := 0; trial < 3; trial++ {
		for order := 0; order < 5; order++ {
			mode := (order + trial) % 5
			cfg.ParallelProof = ht.ParallelProofConfig{}
			workers := uint32(0)
			if mode > 0 {
				cfg.ParallelProof = ht.DefaultParallelProofConfig()
				workers = []uint32{0, 0, 1, 2, 4}[mode]
				cfg.ParallelProof.Workers = workers
			}
			b, e = OpenHybrid(db, cfg, false, true)
			if e != nil {
				t.Fatal(e)
			}
			for _, q := range warmup {
				if _, e = b.Query(context.Background(), q); e != nil {
					b.Close()
					t.Fatal(e)
				}
			}
			r := plannedProofMeasurement{Mode: names[mode], Policy: cfg.ParallelProof, Trial: trial, Queries: len(requests), Clients: 8 / int(workers+1), LatencyNS: make([]int64, len(requests)), VerificationNS: make([]int64, len(requests))}
			got := make([]query.Response, len(requests))
			stats := make([]ht.ParallelProofStats, len(requests))
			var m0, m1 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m0)
			cpu := proofCPU()
			for i, q := range requests {
				start := time.Now()
				got[i], stats[i], e = b.QueryWithProofStats(context.Background(), q)
				r.LatencyNS[i] = time.Since(start).Nanoseconds()
				if e != nil {
					b.Close()
					t.Fatal(e)
				}
			}
			r.CPUTimeNS = proofCPU() - cpu
			runtime.ReadMemStats(&m1)
			r.AllocatedBytes = m1.TotalAlloc - m0.TotalAlloc
			for i, q := range requests {
				if !reflect.DeepEqual(got[i], want[i]) {
					b.Close()
					t.Fatal("proof response differs", r.Mode, q)
				}
				verifyStart := time.Now()
				_, e = query.VerifyWithCodec(context.Background(), roots[q.Height], q, got[i], query.DefaultLimits(), codec.Ethereum)
				r.VerificationNS[i] = time.Since(verifyStart).Nanoseconds()
				if e != nil {
					b.Close()
					t.Fatal(e)
				}
				if q.Kind == query.Account {
					r.AccountNS = append(r.AccountNS, r.LatencyNS[i])
				} else {
					r.StorageNS = append(r.StorageNS, r.LatencyNS[i])
				}
				s := stats[i]
				r.MainWalkNanos += s.MainWalkNanos
				r.WaitNanos += s.WaitNanos
				r.AssembleNanos += s.AssembleNanos
				r.TotalReads += s.ReadNodes
				r.PrefixReads += s.PrefixReads
				r.HashReads += s.HashReads
				r.SiblingHeaders += s.SiblingHeaderReads
				r.BodyDecodes += s.BodyDecodes
				r.WorkerStarts += s.WorkerStarts
				r.CursorCreates += s.PrefixIteratorCreates
				r.BackgroundReads += s.BackgroundReads
				if s.PeakContextBytes > r.PeakContextBytes {
					r.PeakContextBytes = s.PeakContextBytes
				}
				if s.PeakTemporaryBytes > r.PeakTemporaryBytes {
					r.PeakTemporaryBytes = s.PeakTemporaryBytes
				}
				if s.MaxReadBufferBytes > r.MaxReadBufferBytes {
					r.MaxReadBufferBytes = s.MaxReadBufferBytes
				}
			}
			// Separate throughput pass, at most eight foreground+configured-worker
			// slots. 2/4-worker modes leave unused slots; report Clients explicitly.
			var wg sync.WaitGroup
			fail := make(chan error, r.Clients)
			start := time.Now()
			for c := 0; c < r.Clients; c++ {
				wg.Add(1)
				go func(c int) {
					defer wg.Done()
					for i := c; i < len(requests); i += r.Clients {
						result, e := b.Query(context.Background(), requests[i])
						if e != nil {
							fail <- e
							return
						}
						got[i] = result
					}
				}(c)
			}
			wg.Wait()
			r.ThroughputElapsedNS = time.Since(start).Nanoseconds()
			close(fail)
			for e := range fail {
				if e != nil {
					b.Close()
					t.Fatal(e)
				}
			}
			for i := range got {
				if !reflect.DeepEqual(got[i], want[i]) {
					b.Close()
					t.Fatal("concurrent response differs")
				}
			}
			after, e := b.MeasureStorage(context.Background(), false)
			if e != nil || after.Inventory.SHA256 != before.Inventory.SHA256 {
				b.Close()
				t.Fatal("read-only database changed", e)
			}
			if e = b.Close(); e != nil {
				t.Fatal(e)
			}
			r.Verified = true
			rows = append(rows, r)
			report()
			var total int64
			for _, n := range r.LatencyNS {
				total += n
			}
			t.Logf("trial %d %s %.2f us/query; reads=%d; background=%d; %.0f queries/s", trial, r.Mode, float64(total)/float64(r.Queries)/1e3, r.TotalReads, r.BackgroundReads, float64(r.Queries)*1e9/float64(r.ThroughputElapsedNS))
		}
	}
}
