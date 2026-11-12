package backend

import (
	"context"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestParallelProofEVMLevelDB(t *testing.T) { runParallelProofEVM(t, kvstore.LevelDB) }
func runParallelProofEVM(t *testing.T, engine kvstore.Engine) {
	for _, initial := range []string{InitializationPrefix, InitializationHashCold} {
		t.Run(initial, func(t *testing.T) {
			w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 256, Blocks: 4, TransactionsPerBlock: 48, Contracts: 8, StorageSlots: 32, Seed: 71})
			if e != nil {
				t.Fatal(e)
			}
			cfg := DefaultHybridConfig("parallel-proof-evm")
			cfg.Initialization = initial
			o := kvstore.DefaultOptions(engine)
			cfg.Store.Storage = &o
			cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
			cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: 2 << 20}
			path := filepath.Join(t.TempDir(), "db")
			b, e := OpenHybrid(path, cfg, true, false)
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
			if e = b.Close(); e != nil {
				t.Fatal(e)
			}
			b, e = OpenHybrid(path, cfg, false, true)
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close()
			hs := b.store.(*hybridStore)
			before, e := b.MeasureStorage(context.Background(), false)
			if e != nil {
				t.Fatal(e)
			}
			var requests []query.Request
			for h := range roots {
				for _, addr := range []common.Address{common.BigToAddress(big.NewInt(0x2000)), common.BigToAddress(big.NewInt(0x100000)), common.HexToAddress("0xffaa")} {
					for _, kind := range []query.Kind{query.Account, query.Storage, query.Code} {
						requests = append(requests, query.Request{Height: uint64(h), Address: addr, Kind: kind})
						if kind == query.Storage {
							requests = append(requests, query.Request{Height: uint64(h), Address: addr, Kind: kind, Slot: common.HexToHash("0x1000")})
						}
					}
				}
			}
			for _, req := range requests {
				hs.parallelProof = ht.ParallelProofConfig{}
				want, e := b.Query(context.Background(), req)
				if e != nil {
					t.Fatal(e)
				}
				for _, workers := range []uint32{0, 1, 2, 4} {
					c := ht.DefaultParallelProofConfig()
					c.Workers = workers
					hs.parallelProof = c
					got, st, e := b.QueryWithProofStats(context.Background(), req)
					if e != nil {
						t.Fatal(req, workers, e)
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("changed response %v workers %d\ngot %#v\nwant %#v", req, workers, got, want)
					}
					if _, e = query.VerifyWithCodec(context.Background(), roots[req.Height], req, got, query.DefaultLimits(), codec.Ethereum); e != nil {
						t.Fatal(e)
					}
					directories := uint64(1)
					if req.Kind == query.Storage && got.Account != nil && got.Account.StorageRoot != mpt.EmptyRootHash {
						directories = 2
					}
					if st.DirectoryReads != directories || st.LifecycleReads != directories-1 || st.WorkerStarts > uint64(workers) {
						t.Fatal("repeated metadata or excess workers", req, st)
					}
				}
			}
			// Each concurrent request owns its snapshot/cursors/results.
			hs.parallelProof = ht.DefaultParallelProofConfig()
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					req := requests[i*3]
					r, _, e := b.QueryWithProofStats(context.Background(), req)
					if e == nil {
						_, e = query.VerifyWithCodec(context.Background(), roots[req.Height], req, r, query.DefaultLimits(), codec.Ethereum)
					}
					errs <- e
				}(i)
			}
			wg.Wait()
			close(errs)
			for e := range errs {
				if e != nil {
					t.Fatal(e)
				}
			}
			after, e := b.MeasureStorage(context.Background(), false)
			if e != nil {
				t.Fatal(e)
			}
			if before.Inventory.SHA256 != after.Inventory.SHA256 {
				t.Fatal("proof query changed database")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, _, e = b.QueryWithProofStats(ctx, requests[0]); e != context.Canceled {
				t.Fatal(fmt.Sprint(e))
			}
		})
	}
}
