package state

import (
	"github.com/holiman/uint256"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/params"
	"testing"
)

func newState(t *testing.T) *StateDB {
	t.Helper()
	s, e := New()
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func TestSnapshotRestoresAllMutableState(t *testing.T) {
	s := newState(t)
	addr := common.HexToAddress("0x1000")
	key := common.HexToHash("0x01")
	one := common.HexToHash("0x01")
	s.AddBalance(addr, uint256.NewInt(9))
	s.SetState(addr, key, one)
	s.SetCode(addr, []byte{0})
	s.Finalise(true)
	outer := s.Snapshot()
	s.SetState(addr, key, common.HexToHash("0x02"))
	s.SetNonce(addr, 7)
	s.SetCode(addr, []byte{1})
	s.AddBalance(addr, uint256.NewInt(1))
	s.AddRefund(100)
	s.AddSlotToAccessList(addr, key)
	s.SetTransientState(addr, key, one)
	s.CreateContract(addr)
	inner := s.Snapshot()
	s.SelfDestruct(addr)
	s.RevertToSnapshot(inner)
	if s.HasSelfDestructed(addr) || s.GetBalance(addr).Uint64() != 10 {
		t.Fatal("inner rollback")
	}
	s.RevertToSnapshot(outer)
	if s.GetState(addr, key) != one || s.GetBalance(addr).Uint64() != 9 || s.GetNonce(addr) != 0 || s.GetCode(addr)[0] != 0 || s.GetRefund() != 0 || s.AddressInAccessList(addr) || s.GetTransientState(addr, key) != (common.Hash{}) || s.accounts[addr].created {
		t.Fatal("snapshot state leaked")
	}
}
func TestOriginalStorageAcrossTransactions(t *testing.T) {
	s := newState(t)
	addr := common.HexToAddress("0x1000")
	key := common.Hash{}
	one := common.HexToHash("0x01")
	two := common.HexToHash("0x02")
	s.AddBalance(addr, uint256.NewInt(1))
	s.SetState(addr, key, one)
	s.Finalise(true)
	s.SetState(addr, key, two)
	s.SetState(addr, key, common.HexToHash("0x03"))
	current, original := s.GetStateAndCommittedState(addr, key)
	if current != common.HexToHash("0x03") || original != one {
		t.Fatal("original changed within transaction")
	}
	s.Finalise(true)
	_, original = s.GetStateAndCommittedState(addr, key)
	if original != current {
		t.Fatal("new transaction original")
	}
	s.SetState(addr, key, common.Hash{})
	if s.HasStorage(addr) {
		t.Fatal("zero slot not deleted")
	}
}
func TestTransactionLocalStateAndIsolation(t *testing.T) {
	s := newState(t)
	addr := common.HexToAddress("0x1000")
	slot := common.Hash{}
	value := common.HexToHash("0x01")
	s.AddBalance(addr, uint256.NewInt(1))
	s.SetCode(addr, []byte{1})
	s.GetBalance(addr).Clear()
	s.GetCode(addr)[0] = 99
	if s.GetBalance(addr).Uint64() != 1 || s.GetCode(addr)[0] != 1 {
		t.Fatal("borrowed mutable state")
	}
	s.SetTransientState(addr, slot, value)
	s.AddSlotToAccessList(addr, slot)
	s.Prepare(params.Rules{IsEIP2929: true, IsShanghai: true}, addr, addr, nil, nil, nil)
	if s.GetTransientState(addr, slot) != (common.Hash{}) {
		t.Fatal("transient leak")
	}
	_, warm := s.SlotInAccessList(addr, slot)
	if warm {
		t.Fatal("warm slot leak")
	}
}
func TestLifecycleAndStoragePresence(t *testing.T) {
	s := newState(t)
	addr := common.HexToAddress("0x1000")
	s.AddBalance(addr, uint256.NewInt(1))
	s.SetCode(addr, []byte{0})
	s.Finalise(true)
	if _, destroyed := s.SelfDestruct6780(addr); destroyed {
		t.Fatal("existing contract destroyed")
	}
	s.CreateContract(addr)
	s.SelfDestruct6780(addr)
	if !s.HasSelfDestructed(addr) {
		t.Fatal("new contract not destroyed")
	}
	s.Finalise(true)
	if s.Exist(addr) {
		t.Fatal("destroyed account retained")
	}
	s.CreateAccount(addr)
	s.Finalise(true)
	if s.Exist(addr) {
		t.Fatal("empty touched account retained")
	}
}
