// Package state is an intentionally small in-memory EVM state backend.
// It stores logical values only: no trie, node database, proofs or persistence.
package state

import (
	"fmt"
	"github.com/holiman/uint256"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
)

type account struct {
	balance                     uint256.Int
	nonce                       uint64
	code                        []byte
	storage                     map[common.Hash]common.Hash
	destroyed, created, touched bool
}
type slotKey struct {
	address common.Address
	slot    common.Hash
}
type image struct {
	accounts  map[common.Address]*account
	original  map[slotKey]common.Hash
	transient map[slotKey]common.Hash
	access    map[common.Address]map[common.Hash]bool
	refund    uint64
}
type revision struct {
	id    int
	image image
}

// StateDB uses deep-copy snapshots for clarity. This is a correctness-oriented
// test backend, not an optimized large-state executor. Not safe for concurrent use.
type StateDB struct {
	image
	revisions    []revision
	nextRevision int
}

func New() (*StateDB, error) {
	return &StateDB{image: image{
		accounts: make(map[common.Address]*account), original: make(map[slotKey]common.Hash),
		transient: make(map[slotKey]common.Hash), access: make(map[common.Address]map[common.Hash]bool),
	}}, nil
}
func (s *StateDB) Error() error { return nil }
func (s *StateDB) ensure(addr common.Address) *account {
	a := s.accounts[addr]
	if a == nil {
		a = &account{storage: make(map[common.Hash]common.Hash)}
		s.accounts[addr] = a
	}
	return a
}
func (s *StateDB) CreateAccount(addr common.Address) {
	a := &account{storage: make(map[common.Hash]common.Hash), touched: true}
	if prev := s.accounts[addr]; prev != nil {
		a.balance = prev.balance
	}
	s.accounts[addr] = a
}
func (s *StateDB) CreateContract(addr common.Address) {
	a := s.ensure(addr)
	a.created = true
	a.touched = true
}
func (s *StateDB) GetBalance(addr common.Address) *uint256.Int {
	if a := s.accounts[addr]; a != nil {
		return new(uint256.Int).Set(&a.balance)
	}
	return new(uint256.Int)
}
func (s *StateDB) AddBalance(addr common.Address, v *uint256.Int) uint256.Int {
	a := s.ensure(addr)
	old := a.balance
	a.balance.Add(&a.balance, v)
	a.touched = true
	return old
}
func (s *StateDB) SubBalance(addr common.Address, v *uint256.Int) uint256.Int {
	a := s.ensure(addr)
	old := a.balance
	a.balance.Sub(&a.balance, v)
	a.touched = true
	return old
}
func (s *StateDB) GetNonce(addr common.Address) uint64 {
	if a := s.accounts[addr]; a != nil {
		return a.nonce
	}
	return 0
}
func (s *StateDB) SetNonce(addr common.Address, n uint64) {
	a := s.ensure(addr)
	a.nonce = n
	a.touched = true
}
func (s *StateDB) GetCode(addr common.Address) []byte {
	if a := s.accounts[addr]; a != nil {
		return common.CopyBytes(a.code)
	}
	return nil
}
func (s *StateDB) GetCodeSize(addr common.Address) int {
	if a := s.accounts[addr]; a != nil {
		return len(a.code)
	}
	return 0
}
func (s *StateDB) GetCodeHash(addr common.Address) common.Hash {
	if a := s.accounts[addr]; a != nil {
		return crypto.Keccak256Hash(a.code)
	}
	return common.Hash{}
}
func (s *StateDB) SetCode(addr common.Address, code []byte) []byte {
	a := s.ensure(addr)
	old := common.CopyBytes(a.code)
	a.code = common.CopyBytes(code)
	a.touched = true
	return old
}
func (s *StateDB) Exist(addr common.Address) bool { return s.accounts[addr] != nil }
func (s *StateDB) Empty(addr common.Address) bool {
	a := s.accounts[addr]
	return a == nil || a.balance.IsZero() && a.nonce == 0 && len(a.code) == 0
}
func (s *StateDB) GetState(addr common.Address, slot common.Hash) common.Hash {
	if a := s.accounts[addr]; a != nil {
		return a.storage[slot]
	}
	return common.Hash{}
}
func (s *StateDB) GetStateAndCommittedState(addr common.Address, slot common.Hash) (common.Hash, common.Hash) {
	current := s.GetState(addr, slot)
	original, ok := s.original[slotKey{addr, slot}]
	if !ok {
		original = current
	}
	return current, original
}
func (s *StateDB) SetState(addr common.Address, slot, value common.Hash) common.Hash {
	old := s.GetState(addr, slot)
	key := slotKey{addr, slot}
	if _, ok := s.original[key]; !ok {
		s.original[key] = old
	}
	a := s.ensure(addr)
	if value == (common.Hash{}) {
		delete(a.storage, slot)
	} else {
		a.storage[slot] = value
	}
	a.touched = true
	return old
}

// HasStorage answers CREATE collision checks without manufacturing a trie root.
func (s *StateDB) HasStorage(addr common.Address) bool {
	a := s.accounts[addr]
	return a != nil && len(a.storage) > 0
}
func (s *StateDB) GetTransientState(addr common.Address, slot common.Hash) common.Hash {
	return s.transient[slotKey{addr, slot}]
}
func (s *StateDB) SetTransientState(addr common.Address, slot, value common.Hash) {
	key := slotKey{addr, slot}
	if value == (common.Hash{}) {
		delete(s.transient, key)
	} else {
		s.transient[key] = value
	}
}
func (s *StateDB) SelfDestruct(addr common.Address) uint256.Int {
	a := s.ensure(addr)
	old := a.balance
	a.balance.Clear()
	a.destroyed = true
	a.touched = true
	return old
}
func (s *StateDB) HasSelfDestructed(addr common.Address) bool {
	a := s.accounts[addr]
	return a != nil && a.destroyed
}
func (s *StateDB) SelfDestruct6780(addr common.Address) (uint256.Int, bool) {
	a := s.ensure(addr)
	old := a.balance
	if a.created {
		s.SelfDestruct(addr)
		return old, true
	}
	return old, false
}
func (s *StateDB) AddRefund(n uint64) { s.refund += n }
func (s *StateDB) SubRefund(n uint64) {
	if n > s.refund {
		panic("refund underflow")
	}
	s.refund -= n
}
func (s *StateDB) GetRefund() uint64                            { return s.refund }
func (s *StateDB) AddressInAccessList(addr common.Address) bool { _, ok := s.access[addr]; return ok }
func (s *StateDB) SlotInAccessList(addr common.Address, slot common.Hash) (bool, bool) {
	slots, ok := s.access[addr]
	return ok, slots[slot]
}
func (s *StateDB) AddAddressToAccessList(addr common.Address) {
	if _, ok := s.access[addr]; !ok {
		s.access[addr] = make(map[common.Hash]bool)
	}
}
func (s *StateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	s.AddAddressToAccessList(addr)
	s.access[addr][slot] = true
}

// Prepare starts transaction-local access and transient state. Original slot
// values are captured lazily on first write, and discarded by Finalise.
func (s *StateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dest *common.Address, precompiles []common.Address, list types.AccessList) {
	s.access = make(map[common.Address]map[common.Hash]bool)
	s.transient = make(map[slotKey]common.Hash)
	if rules.IsEIP2929 {
		s.AddAddressToAccessList(sender)
		if dest != nil {
			s.AddAddressToAccessList(*dest)
		}
		for _, addr := range precompiles {
			s.AddAddressToAccessList(addr)
		}
		for _, tuple := range list {
			s.AddAddressToAccessList(tuple.Address)
			for _, slot := range tuple.StorageKeys {
				s.AddSlotToAccessList(tuple.Address, slot)
			}
		}
		if rules.IsShanghai {
			s.AddAddressToAccessList(coinbase)
		}
	}
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	n := make(map[K]V, len(m))
	for k, v := range m {
		n[k] = v
	}
	return n
}
func (s *StateDB) copyImage() image {
	im := s.image
	im.accounts = make(map[common.Address]*account, len(s.accounts))
	for addr, a := range s.accounts {
		v := *a
		v.code = common.CopyBytes(a.code)
		v.storage = cloneMap(a.storage)
		im.accounts[addr] = &v
	}
	im.original = cloneMap(s.original)
	im.transient = cloneMap(s.transient)
	im.access = make(map[common.Address]map[common.Hash]bool, len(s.access))
	for addr, slots := range s.access {
		im.access[addr] = cloneMap(slots)
	}

	return im
}
func (s *StateDB) Snapshot() int {
	id := s.nextRevision
	s.nextRevision++
	s.revisions = append(s.revisions, revision{id, s.copyImage()})
	return id
}
func (s *StateDB) RevertToSnapshot(id int) {
	for i := len(s.revisions) - 1; i >= 0; i-- {
		if s.revisions[i].id == id {
			s.image = s.revisions[i].image
			s.revisions = s.revisions[:i]
			return
		}
	}
	panic(fmt.Sprintf("invalid state snapshot %d", id))
}
func (s *StateDB) Finalise(deleteEmpty bool) {
	for addr, a := range s.accounts {
		if a.destroyed || deleteEmpty && a.touched && s.Empty(addr) {
			delete(s.accounts, addr)
		} else {
			a.created = false
			a.touched = false
		}
	}
	s.original = make(map[slotKey]common.Hash)
	s.refund = 0
	s.revisions = nil
}
