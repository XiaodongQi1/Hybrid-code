package patharchive

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"sync"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/vm"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/vm/program"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	archive "github.com/XiaodongQi1/HybridTrie/evm-state/block"
	dq "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	ds "github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func verifyHistory(t *testing.T, a *Archive, roots []mpt.Hash, addresses []common.Address, slots []common.Hash) {
	t.Helper()
	for h := a.s.cfg.initial(); h <= a.s.head.Height; h++ {
		for _, addr := range addresses {
			requests := []dq.Request{{Height: h, Address: addr, Kind: dq.Account}, {Height: h, Address: addr, Kind: dq.Code}}
			for _, slot := range slots {
				requests = append(requests, dq.Request{Height: h, Address: addr, Kind: dq.Storage, Slot: slot})
			}
			for _, q := range requests {
				out, e := a.Query(context.Background(), q)
				must(t, e)
				_, e = dq.Verify(context.Background(), roots[h], q, out, dq.DefaultLimits())
				must(t, e)
			}
		}
	}
}

func TestExecutionIndependentOracleHistoryRestart(t *testing.T) {
	w, addresses, slots := dataset(t)
	id, _, e := w.Freeze()
	must(t, e)
	cfg := DefaultConfig(id)
	roots, outcomes := runOracle(t, w, addresses, slots, cfg.Chain)
	path := filepath.Join(t.TempDir(), "db")
	a, e := Open(path, cfg, true, false)
	must(t, e)
	defer func() {
		if a != nil {
			a.Close()
		}
	}()
	for i, b := range w.Blocks {
		out, e := a.ExecuteBlock(context.Background(), b)
		must(t, e)
		equal(t, out.Root, roots[i+1])
		equal(t, out.Outcomes, outcomes[i])
		retry, e := a.ExecuteBlock(context.Background(), b)
		must(t, e)
		equal(t, retry, out)
		verifyHistory(t, a, roots, addresses, slots)
		must(t, a.Close())
		a, e = Open(path, cfg, false, false)
		must(t, e)
	}
	must(t, a.Audit(context.Background()))
	sizes, e := a.Breakdown()
	must(t, e)
	for _, name := range []string{"current-trie", "current-flat", "reverse-history"} {
		if sizes[name] == 0 {
			t.Fatalf("missing %s", name)
		}
	}
	if sizes["hash-location-index"] != 0 {
		t.Fatal("v2 wrote hash compatibility index")
	}
	must(t, a.Close())
	a, e = Open(path, cfg, false, true)
	must(t, e)
	verifyHistory(t, a, roots, addresses, slots)
	if _, e = a.ExecuteBlock(context.Background(), w.Blocks[9]); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
}

func TestCreate2DestroyRecreateAndContext(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			key, e := crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
			must(t, e)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			factory := common.HexToAddress("0xf000")
			env := common.HexToAddress("0xe000")
			coinbase := common.HexToAddress("0xcafe")
			runtime := program.New().Selfdestruct(sender).Bytes()
			init := program.New().Sstore(0, 42).ReturnViaCodeCopy(runtime).Bytes()
			fc := program.New().Create2(init, 7).Push(0).Op(vm.MSTORE).Return(0, 32).Bytes()
			deployed := crypto.CreateAddress2(factory, [32]byte(word(7)), crypto.Keccak256(init))
			w := archive.Workload{Schema: archive.WorkloadSchema}
			for h := uint64(1); h <= 5; h++ {
				b := archive.InputBlock{Height: h, ID: word(500 + h), Context: archive.Context{Time: 2000 + h, GasLimit: 10000000, Coinbase: coinbase, BaseFee: big.NewInt(1), BlobBaseFee: big.NewInt(1)}}
				if h > 1 {
					b.ParentID = w.Blocks[h-2].ID
				}
				if h == 1 {
					b.Allocations = []archive.Allocation{{Address: sender, Balance: big.NewInt(10000000000)}, {Address: factory, Balance: big.NewInt(0), Code: fc}, {Address: env, Balance: big.NewInt(0), Code: common.FromHex("436000524260205260014060405260606000f3")}}
				} else {
					to := factory
					if h == 3 {
						to = deployed
					}
					if h == 5 {
						to = env
					}
					tx, e := types.SignTx(types.NewTx(&types.LegacyTx{Nonce: h - 2, Gas: 500000, GasPrice: big.NewInt(2), To: &to}), types.LatestSignerForChainID(big.NewInt(1337)), key)
					must(t, e)
					wire, e := tx.MarshalBinary()
					must(t, e)
					b.Transactions = [][]byte{wire}
				}
				w.Blocks = append(w.Blocks, b)
			}
			id, _, e := w.Freeze()
			must(t, e)
			cfg := DefaultConfig(id)
			if legacy {
				cfg.Chain.ShanghaiTime = nil
				cfg.Chain.CancunTime = nil
			}
			addresses := []common.Address{sender, factory, deployed, env, coinbase}
			slots := []common.Hash{{}}
			roots, outcomes := runOracle(t, w, addresses, slots, cfg.Chain)
			a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
			must(t, e)
			defer a.Close()
			for i, b := range w.Blocks {
				out, e := a.ExecuteBlock(context.Background(), b)
				must(t, e)
				equal(t, out.Root, roots[i+1])
				equal(t, out.Outcomes, outcomes[i])
			}
			verifyHistory(t, a, roots, addresses, slots)
			must(t, a.Audit(context.Background()))
			q := dq.Request{Height: 3, Address: deployed, Kind: dq.Storage}
			out, e := a.Query(context.Background(), q)
			must(t, e)
			if (out.Account == nil) != legacy {
				t.Fatal("selfdestruct fork mismatch")
			}
		})
	}
}

func TestRejectCancelBudgetAndConcurrentReads(t *testing.T) {
	w, addresses, slots := dataset(t)
	cfg := DefaultConfig("reject")
	a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	must(t, e)
	defer a.Close()
	for _, b := range w.Blocks[:2] {
		_, e = a.ExecuteBlock(context.Background(), b)
		must(t, e)
	}
	before := a.s.head
	bad := w.Blocks[2]
	bad.Transactions = [][]byte{bad.Transactions[0], bad.Transactions[0]}
	if _, e = a.ExecuteBlock(context.Background(), bad); e == nil {
		t.Fatal("accepted duplicate nonce")
	}
	equal(t, before, a.s.head)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = a.ExecuteBlock(ctx, w.Blocks[2]); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	for _, field := range []*uint64{&a.s.cfg.MaxBatchBytes, &a.s.cfg.MaxWalkNodes, &a.s.cfg.MaxStagingBytes} {
		old := *field
		*field = 1
		if _, e = a.ExecuteBlock(context.Background(), w.Blocks[2]); e == nil {
			t.Fatal("budget ignored")
		}
		*field = old
		equal(t, before, a.s.head)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			q := dq.Request{Height: 2, Address: addresses[1], Kind: dq.Storage}
			out, e := a.Query(context.Background(), q)
			if e != nil {
				t.Error(e)
				return
			}
			_, e = dq.Verify(context.Background(), before.Root, q, out, dq.DefaultLimits())
			if e != nil {
				t.Error(e)
				return
			}
		}
	}()
	for _, b := range w.Blocks[2:] {
		_, e = a.ExecuteBlock(context.Background(), b)
		must(t, e)
	}
	wg.Wait()
	roots, _ := runOracle(t, w, addresses, slots, cfg.Chain)
	verifyHistory(t, a, roots, addresses, slots)
	bad = w.Blocks[9]
	bad.Cursor = "conflicting"
	if _, e = a.ExecuteBlock(context.Background(), bad); e == nil {
		t.Fatal("conflicting retry")
	}
	if _, e = a.Query(context.Background(), dq.Request{Height: 11, Kind: dq.Account}); e == nil {
		t.Fatal("future height")
	}
}

func TestAtomicCommitFailureRecovery(t *testing.T) {
	for _, format := range []string{DifferentialFormat, Format, IndexedFormat, PackedFormat} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprint(format, after), func(t *testing.T) {
				w, _, _ := dataset(t)
				cfg := DefaultConfig("atomic")
				cfg.Format = format
				path := filepath.Join(t.TempDir(), "db")
				a, e := Open(path, cfg, true, false)
				must(t, e)
				_, e = a.ExecuteBlock(context.Background(), w.Blocks[0])
				must(t, e)
				write := a.s.write
				a.s.write = func(b *diskBatch) error {
					if after {
						if e := write(b); e != nil {
							return e
						}
					}
					return errors.New("injected I/O failure")
				}
				_, e = a.ExecuteBlock(context.Background(), w.Blocks[1])
				if !errors.Is(e, ErrUnknown) {
					t.Fatal(e)
				}
				if _, e = a.Result(1); !errors.Is(e, ErrUnknown) {
					t.Fatal("uncertain read allowed")
				}
				must(t, a.Close())
				a, e = Open(path, cfg, false, false)
				must(t, e)
				defer a.Close()
				want := uint64(1)
				if after {
					want = 2
				}
				if a.s.head.Height != want {
					t.Fatal("partial commit")
				}
				_, e = a.ExecuteBlock(context.Background(), w.Blocks[1])
				must(t, e)
				must(t, a.Audit(context.Background()))
			})
		}
	}
}

func TestCorruptionRejected(t *testing.T) {
	for _, kind := range []string{"history", "flat", "path", "ledger"} {
		t.Run(kind, func(t *testing.T) {
			w, addr, _ := dataset(t)
			a, e := Open(filepath.Join(t.TempDir(), "db"), DefaultConfig(kind), true, false)
			must(t, e)
			defer a.Close()
			for _, b := range w.Blocks {
				_, e = a.ExecuteBlock(context.Background(), b)
				must(t, e)
			}
			q := dq.Request{Height: 3, Address: addr[1], Kind: dq.Storage}
			rec, e := a.Result(3)
			must(t, e)
			switch kind {
			case "history":
				reader := a.s.live(context.Background())
				prefix := reader.historyPrefix(reader.key('P', []byte{0}, nil))
				it := reader.historyIterator(reader.key('P', []byte{0}, nil))
				if !it.Seek(append(prefix, reader.heightBytes(4)...)) {
					t.Fatal("no reverse entry")
				}

				k := append([]byte(nil), it.Key()...)
				if cursor, ok := it.(*groupCursor); ok {
					loc := cursor.locations[cursor.pos]
					k = groupKey([]byte{'P', 0}, loc.height, loc.chunk)
				}
				it.Release()
				must(t, a.s.db.Put(k, []byte{1, 0xff}))
			case "flat":
				// Flat data cannot change a trie-backed answer. Supplying the
				// corrupted account alongside the genuine proof must still fail.
				q.Height = 10
				rec, e = a.Result(10)
				must(t, e)
				before, e := a.Query(context.Background(), q)
				must(t, e)
				key := latestKey('F', []byte{0}, ds.AccountKey(q.Address))
				v, e := a.s.db.Get(key)
				must(t, e)
				v[8] ^= 1
				must(t, a.s.db.Put(key, v))
				out, e := a.Query(context.Background(), q)
				must(t, e)
				equal(t, out, before)
				_, e = dq.Verify(context.Background(), rec.Root, q, out, dq.DefaultLimits())
				must(t, e)
				flat, e := a.s.live(context.Background()).currentValue(key)
				must(t, e)
				corrupt, e := a.s.cfg.codec().DecodeAccount(flat)
				must(t, e)
				out.Account = &corrupt
				if _, e = dq.Verify(context.Background(), rec.Root, q, out, dq.DefaultLimits()); e == nil {
					t.Fatal("corrupted flat account matched the proof")
				}
				return
			case "path":
				q.Height = 10
				rec, e = a.Result(10)
				must(t, e)
				r := a.s.live(context.Background())
				must(t, a.s.db.Put(r.key('P', []byte{0}, nil), []byte("bad")))
			case "ledger":
				must(t, a.s.db.Put(heightKey('L', 3), []byte("bad")))
				if a.Audit(context.Background()) == nil {
					t.Fatal("bad ledger accepted")
				}
				return
			}
			out, e := a.Query(context.Background(), q)
			if e == nil {
				_, e = dq.Verify(context.Background(), rec.Root, q, out, dq.DefaultLimits())
			}
			if e == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}

func TestZeroGenesisAndSharedStorageRoot(t *testing.T) {
	for _, format := range []string{DifferentialFormat, Format, IndexedFormat, PackedFormat} {
		cfg := DefaultConfig("zero")
		cfg.Format = format
		cfg.ZeroGenesis = true
		a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		must(t, e)
		defer a.Close()
		addr := []common.Address{common.HexToAddress("0x1000"), common.HexToAddress("0x2000")}
		b := archive.InputBlock{Height: 0, ID: word(1), Context: archive.Context{GasLimit: 1000000, BaseFee: big.NewInt(0), BlobBaseFee: big.NewInt(0)}}
		for _, v := range addr {
			b.Allocations = append(b.Allocations, archive.Allocation{Address: v, Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{{}: word(42)}})
		}
		rec, e := a.ExecuteBlock(context.Background(), b)
		must(t, e)
		verifyHistory(t, a, []mpt.Hash{rec.Root}, addr, []common.Hash{{}, word(1)})
		must(t, a.Audit(context.Background()))
	}
}
