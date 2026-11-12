package block

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type traceReader struct{ account state.Account }

func (r traceReader) Account(common.Address) (state.Account, bool, error) {
	return r.account, true, nil
}

func TestLogicalTraceBindingOwnershipAndLifecycle(t *testing.T) {
	input := InputBlock{Height: 1, ID: common.Hash{31: 2}, ParentID: common.Hash{31: 1}, Context: Context{GasLimit: 1, BaseFee: new(big.Int), BlobBaseFee: new(big.Int)}}
	a, deleted := common.Address{19: 1}, common.Address{19: 2}
	account := state.EmptyAccount()
	account.Nonce = 7
	account.Balance.SetUint64(23)
	changes := map[common.Address]state.Change{
		a:       {Account: &account, Slots: map[common.Hash]common.Hash{{31: 1}: {31: 2}}, ResetStorage: true, ContractCreated: true},
		deleted: {},
	}
	b, e := NewLogicalBlock(input, codec.Ethereum, mpt.EmptyRootHash, changes, nil)
	if e != nil {
		t.Fatal(e)
	}
	id, raw, e := b.Freeze(input, codec.Ethereum)
	if e != nil {
		t.Fatal(e)
	}
	back, e := ParseLogicalBlock(raw, id, input, codec.Ethereum)
	if e != nil {
		t.Fatal(e)
	}
	old := state.EmptyAccount()
	old.StorageRoot[0] = 1
	bound, e := back.ChangesFor(traceReader{old})
	if e != nil || !bound[a].ResetStorage || !bound[a].ContractCreated || bound[a].Account.StorageRoot != mpt.EmptyRootHash || bound[deleted].Account != nil {
		t.Fatal("lifecycle flags/storage reset lost", e)
	}
	back.Accounts[0].ResetStorage = false
	bound, e = back.ChangesFor(traceReader{old})
	if e != nil || bound[a].Account.StorageRoot != old.StorageRoot {
		t.Fatal("storage base not rebound", e)
	}
	bound[a].Slots[common.Hash{31: 1}] = common.Hash{31: 99}
	bound[a].Account.Nonce = 100
	account.Nonce = 200
	changes[a].Slots[common.Hash{31: 1}] = common.Hash{31: 100}
	if b.Accounts[0].Value.Nonce != 7 || b.Accounts[0].Slots[common.Hash{31: 1}] != (common.Hash{31: 2}) {
		t.Fatal("trace retains mutable source/destination data")
	}
	input.Context.Time++
	if e = b.Validate(input, codec.Ethereum); e == nil {
		t.Fatal("changed context with same ID accepted")
	}
	input.Context.Time--
	if _, e = ParseLogicalBlock(append(raw, '\n'), id, input, codec.Ethereum); e == nil {
		t.Fatal("noncanonical artifact accepted")
	}
	if _, e = ParseLogicalBlock(raw, "wrong", input, codec.Ethereum); e == nil {
		t.Fatal("wrong artifact identity accepted")
	}
	if bytes.Contains(raw, []byte("StorageRoot")) || bytes.Contains(raw, []byte("TrieID")) {
		t.Fatal("backend-specific binding leaked into trace")
	}
}

func TestLogicalTraceRejectsMalformedRows(t *testing.T) {
	input := InputBlock{Height: 1, ID: common.Hash{31: 2}, ParentID: common.Hash{31: 1}}
	a := state.EmptyAccount()
	b, e := NewLogicalBlock(input, codec.Ethereum, mpt.EmptyRootHash, map[common.Address]state.Change{{19: 1}: {Account: &a}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*LogicalBlock){
		func(v *LogicalBlock) { v.Schema = "old" },
		func(v *LogicalBlock) { v.Codec = codec.Legacy },
		func(v *LogicalBlock) { v.Root = mpt.Hash{} },
		func(v *LogicalBlock) { v.Accounts = append(v.Accounts, v.Accounts[0]) },
		func(v *LogicalBlock) { v.Accounts[0].Deleted = true },
		func(v *LogicalBlock) { v.Accounts[0].Value = nil },
		func(v *LogicalBlock) { v.Accounts[0].Code = []byte{1} },
	} {
		copy := b
		copy.Accounts = append([]LogicalAccount(nil), b.Accounts...)
		mutate(&copy)
		if e = copy.Validate(input, codec.Ethereum); e == nil {
			t.Fatal("malformed trace accepted")
		}
	}
}
