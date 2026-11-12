package backend

import (
	"bytes"
	"errors"
	"math/big"
	"testing"

	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

func rtChainBeforeCancun() *params.ChainConfig {
	c := rt.DefaultChainConfig()
	c.ShanghaiTime, c.CancunTime, c.BlobScheduleConfig = nil, nil, nil
	return c
}
func encodeCall(from, to common.Address, data []byte) ([]byte, error) {
	return rt.EncodeExperimentTransaction(rt.ExperimentTransaction{From: from, To: &to, Gas: 500_000, Data: data})
}

func TestEVMSelfDestructAndSameAddressCREATE2Redeployment(t *testing.T) {
	runTestEVMSelfDestructAndSameAddressCREATE2Redeployment(t, legacyTestOpener)
}

func runTestEVMSelfDestructAndSameAddressCREATE2Redeployment(t *testing.T, open testOpener) {
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := smallConfig(mode)
			path := t.TempDir() + "/db"
			b, e := open(t, path, cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { b.Close() }()
			from, factory, beneficiary := common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
			// Before Cancun, an existing contract can be destroyed. Test that
			// actual CREATE2/SSTORE/SELFDESTRUCT transactions propagate the
			// lifecycle event, rather than constructing manual final-state rows.
			chain := rtChainBeforeCancun()
			factoryCode := common.FromHex("0x36600060003760003660006000f560005260206000f3")
			workerCode := append(common.FromHex("0x36601a5773"), beneficiary[:]...)
			workerCode = append(workerCode, common.FromHex("0xff5b60003560005500")...)
			initcode := append([]byte{0x60, byte(len(workerCode)), 0x60, 0x0c, 0x60, 0, 0x39, 0x60, byte(len(workerCode)), 0x60, 0, 0xf3}, workerCode...)
			worker := crypto.CreateAddress2(factory, [32]byte{}, crypto.Keccak256(initcode))
			genesis := inputAt(0)
			genesis.Allocations = []block.Allocation{{Address: from, Balance: big.NewInt(100)}, {Address: factory, Balance: new(big.Int), Nonce: 1, Code: factoryCode}}
			apply(t, b, genesis, chain)
			call := func(h uint64, to common.Address, data []byte) *Result {
				input := inputAt(h)
				wire, e := encodeCall(from, to, data)
				if e != nil {
					t.Fatal(e)
				}
				input.Transactions = [][]byte{wire}
				r := apply(t, b, input, chain)
				if r.Outcomes[0].VMError != "" {
					t.Fatal(r.Outcomes[0].VMError)
				}
				return r
			}
			r := call(1, factory, initcode)
			if common.BytesToAddress(r.Outcomes[0].ReturnData) != worker {
				t.Fatal("CREATE2 failed")
			}
			v, e := b.ViewAt(1)
			if e != nil {
				t.Fatal(e)
			}
			id, active, e := v.StorageID(worker)
			v.Close()
			if e != nil || !active {
				t.Fatal("missing deployment ID", e)
			}
			data := make([]byte, 32)
			data[31] = 42
			call(2, worker, data)
			call(3, worker, nil)
			v, e = b.ViewAt(3)
			if e != nil {
				t.Fatal(e)
			}
			_, exists, e := v.Account(worker)
			_, active, lifeErr := v.StorageID(worker)
			v.Close()
			if e != nil || lifeErr != nil || exists || active {
				t.Fatal("SELFDESTRUCT did not retire instance", e, lifeErr)
			}
			if e = b.Close(); e != nil {
				t.Fatal(e)
			}
			b, e = open(t, path, cfg, false, false)
			if e != nil {
				t.Fatal(e)
			}
			call(4, factory, initcode)
			v, e = b.ViewAt(4)
			if e != nil {
				t.Fatal(e)
			}
			newID, active, e := v.StorageID(worker)
			account, exists, err := v.Account(worker)
			code, codeErr := v.Code(account.CodeHash)
			word, slotErr := v.SlotAt(worker, account.StorageRoot, common.Hash{})
			v.Close()
			if e != nil || err != nil || codeErr != nil || slotErr != nil || !active || !exists || newID <= id || word != (common.Hash{}) || !bytes.Equal(code, workerCode) {
				t.Fatal("recreated instance did not start empty", e, err, codeErr, slotErr)
			}
			v, e = b.ViewAt(2)
			if e != nil {
				t.Fatal(e)
			}
			defer v.Close()
			account, _, e = v.Account(worker)
			if e != nil {
				t.Fatal(e)
			}
			word, e = v.SlotAt(worker, account.StorageRoot, common.Hash{})
			if e != nil || word != common.BytesToHash(data) {
				t.Fatal("old instance no longer queryable", e)
			}
		})
	}
}

func TestAtomicLimitFailureDoesNotPublishInitialization(t *testing.T) {
	runTestAtomicLimitFailureDoesNotPublishInitialization(t, legacyTestOpener)
}

func runTestAtomicLimitFailureDoesNotPublishInitialization(t *testing.T, open testOpener) {
	cfg := smallConfig(codec.Ethereum)
	cfg.MaxBatchBytes = 4096
	b, e := open(t, t.TempDir()+"/db", cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	a := block.Allocation{Address: common.HexToAddress("0x1000"), Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{}}
	for i := int64(0); i < 150; i++ {
		a.Storage[common.BigToHash(big.NewInt(i))] = common.HexToHash("0x01")
	}
	input := inputAt(0)
	input.Allocations = []block.Allocation{a}
	if _, e = b.Apply(input, nil); !errors.Is(e, vt.ErrLimit) {
		t.Fatal("expected batch limit", e)
	}
	head, e := b.Head()
	if e != nil || head.Initialized || head.NextTrieID != 1 {
		t.Fatal("failed initialization leaked", e)
	}
	a.Storage = map[common.Hash]common.Hash{{}: common.HexToHash("0x01")}
	input.Allocations = []block.Allocation{a}
	apply(t, b, input, nil)
}

func TestCancunRetainsExistingContractAndEmptyDeploymentGetsID(t *testing.T) {
	runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID(t, legacyTestOpener)
}

func runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID(t *testing.T, open testOpener) {
	b, e := open(t, t.TempDir()+"/db", smallConfig(codec.Ethereum), true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	from, worker := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	genesis := inputAt(0)
	genesis.Allocations = []block.Allocation{{Address: from, Balance: big.NewInt(100)}, {Address: worker, Balance: big.NewInt(1), Nonce: 1, Code: common.FromHex("0x6000ff"), Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}}}
	apply(t, b, genesis, nil)
	v, e := b.ViewAt(0)
	if e != nil {
		t.Fatal(e)
	}
	oldID, _, _ := v.StorageID(worker)
	v.Close()
	input := inputAt(1)
	wire, e := encodeCall(from, worker, nil)
	if e != nil {
		t.Fatal(e)
	}
	input.Transactions = [][]byte{wire}
	// Initcode returns empty runtime code: a real CREATE still needs an ID,
	// although account codeHash/storageRoot alone cannot identify its creation.
	create, e := rt.EncodeExperimentTransaction(rt.ExperimentTransaction{From: from, Gas: 100_000, Data: common.FromHex("0x60006000f3")})
	if e != nil {
		t.Fatal(e)
	}
	input.Transactions = append(input.Transactions, create)
	r := apply(t, b, input, nil)
	for _, o := range r.Outcomes {
		if o.VMError != "" {
			t.Fatal(o.VMError)
		}
	}
	v, e = b.ViewAt(1)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	id, active, e := v.StorageID(worker)
	account, exists, err := v.Account(worker)
	if e != nil || err != nil || !active || !exists || id != oldID || account.StorageRoot == mpt.EmptyRootHash || account.CodeHash == state.EmptyCode {
		t.Fatal("Cancun incorrectly deleted/reset existing contract", e, err)
	}
	deployed := r.Outcomes[1].Receipt.ContractAddress
	id, active, e = v.StorageID(deployed)
	if e != nil || !active || id == 0 || id == oldID {
		t.Fatal("empty-code deployment lost lifecycle", e)
	}
}
