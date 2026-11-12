// Package workload supplies a small deterministic live-EVM workload. These are
// synthetic experiment transactions, not recovered Ethereum mainnet inputs.
package workload

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
)

const DefaultWarmupBlocks = 1000

type Options struct {
	BackgroundAccounts, Blocks, TransactionsPerBlock, Contracts int
	Seed                                                        int64
	// Empty Pattern preserves the original mixed workload byte for byte.
	Pattern      string `json:",omitempty"`
	StorageSlots int    `json:",omitempty"`
}

func Synthetic(o Options) (block.Workload, error) {
	w := block.Workload{Schema: block.WorkloadSchema, ZeroGenesis: true}
	if o.BackgroundAccounts < 1 || o.BackgroundAccounts > 100_000 || o.Blocks < 1 || o.Blocks > 2000 || o.TransactionsPerBlock < 1 || o.TransactionsPerBlock > 1000 || o.Contracts < 1 || o.Contracts > 1000 {
		return w, fmt.Errorf("synthetic workload outside small-development limits")
	}
	if o.StorageSlots == 0 {
		o.StorageSlots = 4
	}
	if o.StorageSlots < 1 || o.StorageSlots > 4096 {
		return w, fmt.Errorf("storage slot count outside development limits")
	}
	switch o.Pattern {
	case "", "mixed", "account-hot", "account-sweep", "storage-hot", "rotate", "migrate", "revisit":
	default:
		return w, fmt.Errorf("unknown workload pattern %q", o.Pattern)
	}
	from := common.HexToAddress("0x1000")
	context := func(h uint64) block.Context {
		return block.Context{Time: h, GasLimit: uint64(o.TransactionsPerBlock) * 200_000, BaseFee: new(big.Int), BlobBaseFee: big.NewInt(1), Coinbase: common.HexToAddress("0xcafe")}
	}
	genesis := block.InputBlock{Height: 0, Context: context(0)}
	genesis.Allocations = append(genesis.Allocations, block.Allocation{Address: from, Balance: big.NewInt(1_000_000_000)})
	if o.Pattern == "account-sweep" {
		// Each permutation visits every background account before revisiting one.
		// Later passes transfer a different amount, so warmup wires are not replayed.
		n := int64(o.Blocks) * int64(o.TransactionsPerBlock)
		funding := n*(n/int64(o.BackgroundAccounts)+1) + 1
		if funding > 1_000_000_000 {
			genesis.Allocations[0].Balance = big.NewInt(funding)
		}
	}
	for i := 0; i < o.BackgroundAccounts; i++ {
		genesis.Allocations = append(genesis.Allocations, block.Allocation{Address: common.BigToAddress(big.NewInt(int64(i) + 0x100000)), Balance: big.NewInt(1)})
	}
	var contracts []common.Address
	// calldata word 0 selects the slot; word 1 supplies its new value.
	code := common.FromHex("0x60003560203590556000355460005260206000f3")
	for i := 0; i < o.Contracts; i++ {
		a := common.BigToAddress(big.NewInt(int64(i) + 0x2000))
		contracts = append(contracts, a)
		genesis.Allocations = append(genesis.Allocations, block.Allocation{Address: a, Balance: new(big.Int), Nonce: 1, Code: code})
	}
	setID := func(b *block.InputBlock) {
		raw, _ := json.Marshal(b)
		sum := sha256.Sum256(raw)
		b.ID = common.Hash(sum)
	}
	setID(&genesis)
	w.Blocks = append(w.Blocks, genesis)
	rng := rand.New(rand.NewSource(o.Seed))
	var sweep []int
	for h := 1; h <= o.Blocks; h++ {
		b := block.InputBlock{Height: uint64(h), ParentID: w.Blocks[len(w.Blocks)-1].ID, Context: context(uint64(h))}
		for j := 0; j < o.TransactionsPerBlock; j++ {
			if o.Pattern == "account-sweep" {
				index := (h-1)*o.TransactionsPerBlock + j
				if index%o.BackgroundAccounts == 0 {
					sweep = rng.Perm(o.BackgroundAccounts)
				}
				to := common.BigToAddress(big.NewInt(int64(sweep[index%o.BackgroundAccounts]) + 0x100000))
				wire, e := rt.EncodeExperimentTransaction(rt.ExperimentTransaction{
					From: from, To: &to, Gas: 100_000,
					Value: big.NewInt(int64(index/o.BackgroundAccounts) + 1),
				})
				if e != nil {
					return w, e
				}
				b.Transactions = append(b.Transactions, wire)
				continue
			}
			to := contracts[rng.Intn(len(contracts))]
			// A common first execution block warms enough distinct contracts to
			// allocate the reference's slots before the measured profile starts.
			if o.Pattern != "" && h == 1 {
				to = contracts[j%len(contracts)]
			} else {
				switch o.Pattern {
				case "storage-hot":
					to = contracts[0]
				case "rotate":
					to = contracts[(h+j)%len(contracts)]
				case "migrate":
					to = contracts[((h-1)*3/(o.Blocks+1))%len(contracts)]
				case "revisit":
					if h < o.Blocks/3 || h > 2*o.Blocks/3 {
						to = contracts[0]
					} else {
						if len(contracts) == 1 {
							to = contracts[0]
						} else {
							to = contracts[1+(h+j)%(len(contracts)-1)]
						}
					}
				}
			}
			data := make([]byte, 64)
			slot := rng.Intn(o.StorageSlots)
			slotWord := common.BigToHash(big.NewInt(int64(slot)))
			copy(data[:32], slotWord[:])
			if (h*o.TransactionsPerBlock+j)%17 != 0 {
				word := common.BigToHash(big.NewInt(int64(h*o.TransactionsPerBlock + j + 1)))
				copy(data[32:], word[:])
			}
			tx := rt.ExperimentTransaction{From: from, To: &to, Gas: 100_000, Data: data}
			if j%10 == 9 || o.Pattern == "account-hot" && h > 1 {
				to = common.BigToAddress(big.NewInt(int64(rng.Intn(o.BackgroundAccounts)) + 0x100000))
				if o.Pattern == "account-hot" && h > 1 {
					to = common.BigToAddress(big.NewInt(int64(j%minInt(8, o.BackgroundAccounts)) + 0x100000))
				}
				tx.To = &to
				tx.Data = nil
				tx.Value = big.NewInt(1)
			}
			wire, e := rt.EncodeExperimentTransaction(tx)
			if e != nil {
				return w, e
			}
			b.Transactions = append(b.Transactions, wire)
		}
		setID(&b)
		w.Blocks = append(w.Blocks, b)
	}
	_, _, e := w.Freeze()
	return w, e
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
