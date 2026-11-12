package state

import (
	"bytes"
	"fmt"
	"github.com/holiman/uint256"
	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
)

type object struct {
	value                          Account
	slots                          map[common.Hash]common.Hash
	code                           []byte
	loaded, created, dead, touched bool
	storageReset                   bool
	contractCreated                bool
}
type location struct {
	address common.Address
	slot    common.Hash
}
type checkpoint struct{ id, undo int }

// State owns a lazy key write set and an incremental transaction journal.
// It is single-owner. Backend errors remain sticky across VM rollbacks.
type State struct {
	reader              Reader
	objects             map[common.Address]*object
	dirty               map[common.Address]bool
	undo                []func()
	checkpoints         []checkpoint
	next                int
	original, transient map[location]common.Hash
	warm                map[common.Address]map[common.Hash]bool
	refund              uint64
	err                 error
}

var _ rt.State = (*State)(nil)

func New(r Reader) *State {
	return &State{reader: r, objects: map[common.Address]*object{}, dirty: map[common.Address]bool{}, original: map[location]common.Hash{}, transient: map[location]common.Hash{}, warm: map[common.Address]map[common.Hash]bool{}}
}
func (s *State) Error() error { return s.err }
func (s *State) fail(e error) {
	if s.err == nil {
		s.err = e
	}
}
func (s *State) load(a common.Address) *object {
	if o, ok := s.objects[a]; ok {
		return o
	}
	v, ok, e := s.reader.Account(a)
	s.fail(e)
	if e != nil || !ok {
		s.objects[a] = nil
		return nil
	}
	o := &object{value: v, slots: map[common.Hash]common.Hash{}}
	s.objects[a] = o
	return o
}
func (s *State) ensure(a common.Address) *object {
	if o := s.load(a); o != nil {
		return o
	}
	o := &object{value: EmptyAccount(), slots: map[common.Hash]common.Hash{}, loaded: true, storageReset: true}
	s.undo = append(s.undo, func() { s.objects[a] = nil })
	s.objects[a] = o
	return o
}
func (s *State) touch(a common.Address, o *object) {
	old := o.touched
	s.undo = append(s.undo, func() { o.touched = old })
	o.touched = true
	s.dirty[a] = true
}
func (s *State) CreateAccount(a common.Address) {
	old := s.load(a)
	o := &object{value: EmptyAccount(), slots: map[common.Hash]common.Hash{}, loaded: true, touched: true, storageReset: true}
	if old != nil {
		o.value.Balance = old.value.Balance
	}
	s.undo = append(s.undo, func() { s.objects[a] = old })
	s.objects[a] = o
	s.dirty[a] = true
}
func (s *State) CreateContract(a common.Address) {
	o := s.ensure(a)
	old := o.created
	oldContractCreated := o.contractCreated
	s.undo = append(s.undo, func() { o.created = old; o.contractCreated = oldContractCreated })
	o.created = true
	o.contractCreated = true
	s.touch(a, o)
}
func (s *State) GetBalance(a common.Address) *uint256.Int {
	if o := s.load(a); o != nil {
		return new(uint256.Int).Set(&o.value.Balance)
	}
	return new(uint256.Int)
}
func (s *State) balance(a common.Address, v *uint256.Int, add bool) uint256.Int {
	o := s.ensure(a)
	old := o.value.Balance
	s.undo = append(s.undo, func() { o.value.Balance = old })
	if add {
		o.value.Balance.Add(&o.value.Balance, v)
	} else {
		o.value.Balance.Sub(&o.value.Balance, v)
	}
	s.touch(a, o)
	return old
}
func (s *State) AddBalance(a common.Address, v *uint256.Int) uint256.Int {
	return s.balance(a, v, true)
}
func (s *State) SubBalance(a common.Address, v *uint256.Int) uint256.Int {
	return s.balance(a, v, false)
}
func (s *State) GetNonce(a common.Address) uint64 {
	if o := s.load(a); o != nil {
		return o.value.Nonce
	}
	return 0
}
func (s *State) SetNonce(a common.Address, v uint64) {
	o := s.ensure(a)
	old := o.value.Nonce
	s.undo = append(s.undo, func() { o.value.Nonce = old })
	o.value.Nonce = v
	s.touch(a, o)
}
func (s *State) GetCodeHash(a common.Address) common.Hash {
	if o := s.load(a); o != nil {
		return o.value.CodeHash
	}
	return common.Hash{}
}
func (s *State) GetCode(a common.Address) []byte {
	o := s.load(a)
	if o == nil {
		return nil
	}
	if !o.loaded {
		b, e := s.reader.Code(o.value.CodeHash)
		if e == nil && crypto.Keccak256Hash(b) != o.value.CodeHash {
			e = ErrCorrupt
		}
		s.fail(e)
		if e != nil {
			return nil
		}
		o.code = bytes.Clone(b)
		o.loaded = true
	}
	return bytes.Clone(o.code)
}
func (s *State) GetCodeSize(a common.Address) int { return len(s.GetCode(a)) }
func (s *State) SetCode(a common.Address, b []byte) []byte {
	previous := s.GetCode(a)
	o := s.ensure(a)
	old, hash, loaded := o.code, o.value.CodeHash, o.loaded
	s.undo = append(s.undo, func() { o.code = old; o.value.CodeHash = hash; o.loaded = loaded })
	o.code = bytes.Clone(b)
	o.value.CodeHash = crypto.Keccak256Hash(b)
	o.loaded = true
	s.touch(a, o)
	return previous
}
func (s *State) Exist(a common.Address) bool { return s.load(a) != nil }
func empty(o *object) bool {
	return o == nil || o.value.Nonce == 0 && o.value.Balance.IsZero() && o.value.CodeHash == EmptyCode
}
func (s *State) Empty(a common.Address) bool { return empty(s.load(a)) }
func (s *State) GetState(a common.Address, k common.Hash) common.Hash {
	o := s.load(a)
	if o == nil {
		return common.Hash{}
	}
	if v, ok := o.slots[k]; ok {
		return v
	}
	var v common.Hash
	var e error
	if r, ok := s.reader.(OwnedReader); ok {
		v, e = r.SlotAt(a, o.value.StorageRoot, k)
	} else {
		v, e = s.reader.Slot(o.value.StorageRoot, k)
	}
	s.fail(e)
	return v
}
func (s *State) GetStateAndCommittedState(a common.Address, k common.Hash) (common.Hash, common.Hash) {
	v := s.GetState(a, k)
	old, ok := s.original[location{a, k}]
	if !ok {
		old = v
	}
	return v, old
}
func mapUndo[K comparable, V any](s *State, m map[K]V, k K, v V) {
	old, ok := m[k]
	s.undo = append(s.undo, func() {
		if ok {
			m[k] = old
		} else {
			delete(m, k)
		}
	})
	m[k] = v
}
func (s *State) SetState(a common.Address, k, v common.Hash) common.Hash {
	old := s.GetState(a, k)
	loc := location{a, k}
	if _, ok := s.original[loc]; !ok {
		mapUndo(s, s.original, loc, old)
	}
	o := s.ensure(a)
	mapUndo(s, o.slots, k, v)
	s.touch(a, o)
	return old
}
func (s *State) HasStorage(a common.Address) bool {
	o := s.load(a)
	if o == nil {
		return false
	}
	var v bool
	var e error
	if r, ok := s.reader.(OwnedReader); ok {
		v, e = r.HasStorageAt(a, o.value.StorageRoot, o.slots)
	} else {
		v, e = s.reader.HasStorage(o.value.StorageRoot, o.slots)
	}
	s.fail(e)
	return v
}
func (s *State) GetTransientState(a common.Address, k common.Hash) common.Hash {
	return s.transient[location{a, k}]
}
func (s *State) SetTransientState(a common.Address, k, v common.Hash) {
	mapUndo(s, s.transient, location{a, k}, v)
}
func (s *State) SelfDestruct(a common.Address) uint256.Int {
	o := s.ensure(a)
	old, dead := o.value.Balance, o.dead
	s.undo = append(s.undo, func() { o.value.Balance = old; o.dead = dead })
	o.value.Balance.Clear()
	o.dead = true
	s.touch(a, o)
	return old
}
func (s *State) HasSelfDestructed(a common.Address) bool { o := s.load(a); return o != nil && o.dead }
func (s *State) SelfDestruct6780(a common.Address) (uint256.Int, bool) {
	o := s.ensure(a)
	old := o.value.Balance
	if o.created {
		s.SelfDestruct(a)
		return old, true
	}
	return old, false
}
func (s *State) AddRefund(v uint64) {
	old := s.refund
	s.undo = append(s.undo, func() { s.refund = old })
	s.refund += v
}
func (s *State) SubRefund(v uint64) {
	if v > s.refund {
		panic("refund underflow")
	}
	old := s.refund
	s.undo = append(s.undo, func() { s.refund = old })
	s.refund -= v
}
func (s *State) GetRefund() uint64                         { return s.refund }
func (s *State) AddressInAccessList(a common.Address) bool { _, ok := s.warm[a]; return ok }
func (s *State) SlotInAccessList(a common.Address, k common.Hash) (bool, bool) {
	m, ok := s.warm[a]
	return ok, m[k]
}
func (s *State) AddAddressToAccessList(a common.Address) {
	if _, ok := s.warm[a]; !ok {
		mapUndo(s, s.warm, a, map[common.Hash]bool{})
	}
}
func (s *State) AddSlotToAccessList(a common.Address, k common.Hash) {
	s.AddAddressToAccessList(a)
	if !s.warm[a][k] {
		mapUndo(s, s.warm[a], k, true)
	}
}
func (s *State) Prepare(r params.Rules, sender, coinbase common.Address, dest *common.Address, pre []common.Address, list types.AccessList) {
	oldW, oldT := s.warm, s.transient
	s.undo = append(s.undo, func() { s.warm = oldW; s.transient = oldT })
	s.warm = map[common.Address]map[common.Hash]bool{}
	s.transient = map[location]common.Hash{}
	if r.IsEIP2929 {
		s.AddAddressToAccessList(sender)
		if dest != nil {
			s.AddAddressToAccessList(*dest)
		}
		for _, a := range pre {
			s.AddAddressToAccessList(a)
		}
		for _, a := range list {
			s.AddAddressToAccessList(a.Address)
			for _, k := range a.StorageKeys {
				s.AddSlotToAccessList(a.Address, k)
			}
		}
		if r.IsShanghai {
			s.AddAddressToAccessList(coinbase)
		}
	}
}
func (s *State) Snapshot() int {
	id := s.next
	s.next++
	s.checkpoints = append(s.checkpoints, checkpoint{id, len(s.undo)})
	return id
}
func (s *State) RevertToSnapshot(id int) {
	for i := len(s.checkpoints) - 1; i >= 0; i-- {
		c := s.checkpoints[i]
		if c.id == id {
			for j := len(s.undo) - 1; j >= c.undo; j-- {
				s.undo[j]()
			}
			s.undo = s.undo[:c.undo]
			s.checkpoints = s.checkpoints[:i]
			return
		}
	}
	panic(fmt.Sprintf("unknown snapshot %d", id))
}
func (s *State) Finalise(deleteEmpty bool) {
	// Only loaded memory is inspected. This cannot introduce a backend error.
	for a, o := range s.objects {
		if o == nil {
			continue
		}
		if o.dead || deleteEmpty && o.touched && empty(o) {
			s.objects[a] = nil
			s.dirty[a] = true
		} else {
			o.created = false
			o.touched = false
		}
	}
	s.undo = nil
	s.checkpoints = nil
	s.original = map[location]common.Hash{}
	s.transient = map[location]common.Hash{}
	s.warm = map[common.Address]map[common.Hash]bool{}
	s.refund = 0
}

// ResetTransaction releases the applied transaction's write set. The runtime
// keeps the same State pointer and reads the next transaction from r.
// Call after Finalise and after Changes has been applied successfully.
func (s *State) ResetTransaction(r Reader) { *s = *New(r) }

// Changes returns owned key writes and tombstones since the last reset. Legacy
// callers may also accumulate them until block end.
func (s *State) Changes() (map[common.Address]Change, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := map[common.Address]Change{}
	for a := range s.dirty {
		o := s.objects[a]
		if o == nil {
			out[a] = Change{}
			continue
		}
		v := o.value
		c := Change{Account: &v, Slots: map[common.Hash]common.Hash{}, Code: bytes.Clone(o.code), CodeLoaded: o.loaded, ResetStorage: o.storageReset, ContractCreated: o.contractCreated}
		for k, v := range o.slots {
			c.Slots[k] = v
		}
		out[a] = c
	}
	return out, nil
}
