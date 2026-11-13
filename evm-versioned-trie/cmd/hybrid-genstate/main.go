// Command hybrid-genstate writes a frozen, deterministic workload whose genesis
// state is shaped like a chain's: many externally owned accounts, a minority of
// contracts with heavy-tailed storage sizes, pseudo-random addresses and slot
// keys, and Zipf-skewed access. It is synthetic data, not recovered mainnet state.
// The output feeds hybrid-compare through its WorkloadFile option.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"

	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
)

// A recorded source account row holds all of its storage inline and must fit
// the engine value limit (64 MiB) at roughly 150 bytes per slot.
const maxRowSlots = 400_000

type options struct {
	Accounts, Contracts, MaxSlots, Blocks, TxPerBlock int
	Seed                                              int64
	Alpha, ZipfS, TransferShare, NewSlotShare         float64
	MinSlots, MaxContractSlots                        int
}

type stats struct {
	Options                  options
	WorkloadID               string
	EOAs, Contracts          int
	GenesisSlots             int
	LargestContractSlots     int
	MedianContractSlots      int
	Transfers, StorageWrites int
	NewSlotWrites            int
	WorkloadBytes            int
	Note                     string
}

func main() {
	var o options
	out := flag.String("out", "", "new output directory")
	flag.IntVar(&o.Accounts, "accounts", 100000, "externally owned accounts")
	flag.IntVar(&o.Contracts, "contracts", 3000, "contracts")
	flag.IntVar(&o.MaxSlots, "max-genesis-slots", 0, "optional cap on total genesis storage slots (0 means none)")
	flag.IntVar(&o.MinSlots, "min-contract-slots", 5, "smallest contract storage size before the tail")
	flag.IntVar(&o.MaxContractSlots, "max-contract-slots", 0, "optional cap on one contract's genesis storage (0 means none)")
	flag.Float64Var(&o.Alpha, "pareto-alpha", 1.0, "contract storage size tail exponent (smaller is heavier)")
	flag.IntVar(&o.Blocks, "blocks", 200, "execution blocks")
	flag.IntVar(&o.TxPerBlock, "tx", 64, "transactions per block")
	flag.Float64Var(&o.TransferShare, "transfer-share", 0.6, "fraction of transactions that are value transfers")
	flag.Float64Var(&o.NewSlotShare, "new-slot-share", 0.15, "fraction of storage writes that create a new slot")
	flag.Float64Var(&o.ZipfS, "zipf", 1.1, "Zipf exponent for account and contract popularity (>1)")
	flag.Int64Var(&o.Seed, "seed", 1, "deterministic seed")
	flag.Parse()
	if *out == "" || o.Accounts < 1 || o.Contracts < 1 || o.Blocks < 1 || o.TxPerBlock < 1 || o.ZipfS <= 1 || o.Alpha <= 0 ||
		o.TransferShare < 0 || o.TransferShare > 1 || o.NewSlotShare < 0 || o.NewSlotShare > 1 || o.MinSlots < 1 || o.MaxContractSlots < 0 || o.MaxSlots < 0 {
		fmt.Fprintln(os.Stderr, "usage: hybrid-genstate -out NEWDIR [flags]")
		os.Exit(2)
	}
	if e := run(o, *out); e != nil {
		fmt.Fprintln(os.Stderr, "hybrid-genstate:", e)
		os.Exit(1)
	}
}

func run(o options, out string) error {
	if _, e := os.Stat(out); e == nil {
		return fmt.Errorf("output %s already exists", out)
	}
	rng := rand.New(rand.NewSource(o.Seed))
	addr := func() (a common.Address) { rng.Read(a[:]); return }
	hash := func() (h common.Hash) { rng.Read(h[:]); return }
	nonzero := func() (h common.Hash) {
		for h == (common.Hash{}) {
			rng.Read(h[:])
		}
		return
	}

	// The sender is funded but is not part of the popularity-ranked set.
	from := common.HexToAddress("0x1000")
	w := block.Workload{Schema: block.WorkloadSchema, ZeroGenesis: true}
	ctx := func(h uint64) block.Context {
		return block.Context{Time: h, GasLimit: uint64(o.TxPerBlock) * 200_000, BaseFee: new(big.Int), BlobBaseFee: big.NewInt(1), Coinbase: common.HexToAddress("0xcafe")}
	}
	genesis := block.InputBlock{Height: 0, Context: ctx(0)}
	genesis.Allocations = append(genesis.Allocations, block.Allocation{Address: from, Balance: new(big.Int).Mul(big.NewInt(1_000_000_000), big.NewInt(1_000_000_000))})

	used := map[common.Address]bool{from: true}
	fresh := func() common.Address {
		for {
			a := addr()
			if !used[a] {
				used[a] = true
				return a
			}
		}
	}
	eoas := make([]common.Address, o.Accounts)
	for i := range eoas {
		eoas[i] = fresh()
		genesis.Allocations = append(genesis.Allocations, block.Allocation{
			Address: eoas[i],
			Balance: new(big.Int).Add(big.NewInt(1), new(big.Int).Rand(rng, big.NewInt(1_000_000_000_000_000))),
			Nonce:   uint64(rng.Intn(50)),
		})
	}

	// Same minimal contract as the synthetic workload: calldata word 0 is the
	// slot and word 1 the new value.
	code := common.FromHex("0x60003560203590556000355460005260206000f3")
	type contract struct {
		addr  common.Address
		slots []common.Hash
	}
	contracts := make([]contract, o.Contracts)
	total := 0
	sizes := make([]int, o.Contracts)
	for i := range contracts {
		contracts[i].addr = fresh()
		// Pareto tail: most contracts are small, a few hold most of the storage.
		n := int(float64(o.MinSlots) * math.Pow(1-rng.Float64(), -1/o.Alpha))
		if o.MaxContractSlots > 0 && n > o.MaxContractSlots {
			n = o.MaxContractSlots
		}
		if o.MaxSlots > 0 && n > o.MaxSlots-total {
			n = o.MaxSlots - total
		}
		if n > maxRowSlots {
			return fmt.Errorf("contract %d would hold %d slots; its recorded source row (about 150 bytes per slot) would exceed the 64 MiB engine value limit, so set -max-contract-slots to at most %d", i, n, maxRowSlots)
		}
		if n < 0 {
			n = 0
		}
		sizes[i] = n
		total += n
	}
	// Large contracts are the popular ones, as on a real chain.
	order := append([]int(nil), sizes...)
	sortDesc(order)
	for i := range contracts {
		sizes[i] = order[i]
	}
	for i := range contracts {
		a := block.Allocation{Address: contracts[i].addr, Balance: new(big.Int), Nonce: 1, Code: code}
		if sizes[i] > 0 {
			a.Storage = make(map[common.Hash]common.Hash, sizes[i])
			for j := 0; j < sizes[i]; j++ {
				k := hash()
				a.Storage[k] = nonzero()
				contracts[i].slots = append(contracts[i].slots, k)
			}
		}
		genesis.Allocations = append(genesis.Allocations, a)
	}
	setID := func(b *block.InputBlock) {
		raw, _ := json.Marshal(b)
		b.ID = common.Hash(sha256.Sum256(raw))
	}
	setID(&genesis)
	w.Blocks = append(w.Blocks, genesis)

	zrng := rand.New(rand.NewSource(o.Seed + 1))
	eoaZipf := rand.NewZipf(zrng, o.ZipfS, 1, uint64(len(eoas)-1))
	conZipf := rand.NewZipf(zrng, o.ZipfS, 1, uint64(len(contracts)-1))
	var st stats
	for h := 1; h <= o.Blocks; h++ {
		b := block.InputBlock{Height: uint64(h), ParentID: w.Blocks[len(w.Blocks)-1].ID, Context: ctx(uint64(h))}
		for j := 0; j < o.TxPerBlock; j++ {
			var tx rt.ExperimentTransaction
			if rng.Float64() < o.TransferShare {
				to := eoas[eoaZipf.Uint64()]
				tx = rt.ExperimentTransaction{From: from, To: &to, Gas: 100_000, Value: big.NewInt(int64(rng.Intn(1000) + 1))}
				st.Transfers++
			} else {
				c := &contracts[conZipf.Uint64()]
				var slot common.Hash
				if len(c.slots) == 0 || rng.Float64() < o.NewSlotShare {
					slot = hash()
					c.slots = append(c.slots, slot)
					st.NewSlotWrites++
				} else {
					slot = c.slots[rng.Intn(len(c.slots))]
				}
				data := make([]byte, 64)
				copy(data[:32], slot[:])
				v := nonzero()
				copy(data[32:], v[:])
				to := c.addr
				tx = rt.ExperimentTransaction{From: from, To: &to, Gas: 100_000, Data: data}
				st.StorageWrites++
			}
			wire, e := rt.EncodeExperimentTransaction(tx)
			if e != nil {
				return e
			}
			b.Transactions = append(b.Transactions, wire)
		}
		setID(&b)
		w.Blocks = append(w.Blocks, b)
	}
	id, wire, e := w.Freeze()
	if e != nil {
		return e
	}
	st.Options, st.WorkloadID, st.EOAs, st.Contracts, st.WorkloadBytes = o, id, len(eoas), len(contracts), len(wire)
	st.GenesisSlots = total
	st.LargestContractSlots = sizes[0]
	st.MedianContractSlots = sizes[len(sizes)/2]
	st.Note = "synthetic chain-shaped data; not recovered mainnet state"
	if e = os.MkdirAll(out, 0o755); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, "workload.json"), wire, 0o644); e != nil {
		return e
	}
	meta, _ := json.MarshalIndent(st, "", "  ")
	if e = os.WriteFile(filepath.Join(out, "stats.json"), append(meta, '\n'), 0o644); e != nil {
		return e
	}
	fmt.Printf("workload %s: %d EOAs, %d contracts, %d genesis slots, %d blocks, %d bytes\n", id, st.EOAs, st.Contracts, st.GenesisSlots, o.Blocks, len(wire))
	return nil
}

func sortDesc(a []int) {
	for i := 1; i < len(a); i++ {
		for j := i; j > 0 && a[j] > a[j-1]; j-- {
			a[j], a[j-1] = a[j-1], a[j]
		}
	}
}
