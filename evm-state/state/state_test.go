package state_test

import (
	"bytes"
	"errors"
	"github.com/holiman/uint256"
	"math/rand"
	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

type mem struct {
	*mpt.MemoryStore
	codes map[common.Hash][]byte
	reads int
	fault bool
}

func (m *mem) Get(k []byte) ([]byte, error) {
	m.reads++
	if m.fault {
		return nil, errors.New("injected read error")
	}
	return m.MemoryStore.Get(k)
}
func (m *mem) Code(h common.Hash) ([]byte, error) {
	m.reads++
	if m.fault {
		return nil, errors.New("injected code error")
	}
	b, ok := m.codes[h]
	if !ok {
		return nil, errors.New("missing code")
	}
	return bytes.Clone(b), nil
}
func flush(t *testing.T, s *state.State, f *forest.View, m *mem) *forest.View {
	t.Helper()
	c, e := s.Changes()
	if e != nil {
		t.Fatal(e)
	}
	p, e := f.Materialize(c, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	batch := m.NewBatch()
	for h, b := range p.Nodes {
		if e = batch.Put(h[:], b); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.Write(); e != nil {
		t.Fatal(e)
	}
	for h, b := range p.Codes {
		m.codes[h] = b
	}
	out, e := forest.Open(p.Root, m)
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func compare(t *testing.T, a, b rt.State, addresses []common.Address) {
	t.Helper()
	if a.Error() != nil {
		t.Fatal(a.Error())
	}
	for _, addr := range addresses {
		if a.Exist(addr) != b.Exist(addr) || a.Empty(addr) != b.Empty(addr) || a.GetNonce(addr) != b.GetNonce(addr) || a.GetBalance(addr).Cmp(b.GetBalance(addr)) != 0 || a.GetCodeHash(addr) != b.GetCodeHash(addr) || !bytes.Equal(a.GetCode(addr), b.GetCode(addr)) || a.HasStorage(addr) != b.HasStorage(addr) || a.HasSelfDestructed(addr) != b.HasSelfDestructed(addr) {
			t.Fatalf("account mismatch %s", addr)
		}
		for n := byte(0); n < 4; n++ {
			var k common.Hash
			k[31] = n
			ac, ao := a.GetStateAndCommittedState(addr, k)
			bc, bo := b.GetStateAndCommittedState(addr, k)
			if ac != bc || ao != bo || a.GetTransientState(addr, k) != b.GetTransientState(addr, k) {
				t.Fatal("storage mismatch")
			}
			aa, as := a.SlotInAccessList(addr, k)
			ba, bs := b.SlotInAccessList(addr, k)
			if aa != ba || as != bs {
				t.Fatal("warm mismatch")
			}
		}
	}
	if a.GetRefund() != b.GetRefund() {
		t.Fatal("refund mismatch")
	}
}
func TestJournalAgainstIndependentMapState(t *testing.T) {
	m := &mem{MemoryStore: mpt.NewMemoryStore(), codes: map[common.Hash][]byte{}}
	f, e := forest.Open(mpt.EmptyRootHash, m)
	if e != nil {
		t.Fatal(e)
	}
	s := state.New(f)
	oracle, _ := rt.NewMemoryState()
	addresses := []common.Address{common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x3000")}
	rng := rand.New(rand.NewSource(17))
	for round := 0; round < 25; round++ {
		ids := [][2]int{}
		for i := 0; i < 60; i++ {
			addr := addresses[rng.Intn(len(addresses))]
			var k, v common.Hash
			k[31] = byte(rng.Intn(4))
			v[31] = byte(rng.Intn(4))
			op := rng.Intn(13)
			switch op {
			case 0:
				ids = append(ids, [2]int{s.Snapshot(), oracle.Snapshot()})
			case 1:
				if len(ids) > 0 {
					x := ids[len(ids)-1]
					ids = ids[:len(ids)-1]
					s.RevertToSnapshot(x[0])
					oracle.RevertToSnapshot(x[1])
				}
			default:
				for _, b := range []rt.State{s, oracle} {
					switch op {
					case 2:
						b.SetState(addr, k, v)
					case 3:
						b.AddBalance(addr, uint256.NewInt(uint64(v[31])))
					case 4:
						b.SetNonce(addr, uint64(v[31]))
					case 5:
						b.SetCode(addr, []byte{v[31]})
					case 6:
						b.CreateAccount(addr)
					case 7:
						b.CreateContract(addr)
					case 8:
						b.SelfDestruct6780(addr)
					case 9:
						b.SetTransientState(addr, k, v)
					case 10:
						b.AddSlotToAccessList(addr, k)
					case 11:
						b.AddRefund(uint64(v[31]))
					case 12:
						if b.GetRefund() > 0 {
							b.SubRefund(1)
						}
					}
				}
			}
			compare(t, s, oracle, addresses)
		}
		s.Finalise(true)
		oracle.Finalise(true)
		oracle.Prepare(params.Rules{}, common.Address{}, common.Address{}, nil, nil, nil)
		f = flush(t, s, f, m)
		s = state.New(f)
		compare(t, s, oracle, addresses)
	}
}
func TestStickyReadErrorAndMemoryOnlyFinalise(t *testing.T) {
	m := &mem{MemoryStore: mpt.NewMemoryStore(), codes: map[common.Hash][]byte{}}
	f, e := forest.Open(mpt.EmptyRootHash, m)
	if e != nil {
		t.Fatal(e)
	}
	s := state.New(f)
	addr := common.HexToAddress("0x1000")
	s.SetCode(addr, []byte{0})
	s.SetState(addr, common.Hash{}, common.HexToHash("0x2a"))
	s.Finalise(true)
	f = flush(t, s, f, m)
	s = state.New(f)
	s.SetNonce(addr, 1)
	before := m.reads
	m.fault = true
	s.Finalise(true)
	if m.reads != before || s.Error() != nil {
		t.Fatal("Finalise performed IO")
	}
	id := s.Snapshot()
	s.GetCode(addr)
	if s.Error() == nil {
		t.Fatal("missing sticky error")
	}
	s.RevertToSnapshot(id)
	if s.Error() == nil {
		t.Fatal("revert cleared IO error")
	}
	if _, e = s.Changes(); e == nil {
		t.Fatal("corrupt candidate accepted")
	}
}
func TestExactPresenceResetAndOriginalSlots(t *testing.T) {
	m := &mem{MemoryStore: mpt.NewMemoryStore(), codes: map[common.Hash][]byte{}}
	f, _ := forest.Open(mpt.EmptyRootHash, m)
	a := common.HexToAddress("0x1000")
	k := common.Hash{}
	v := common.HexToHash("0x2a")
	s := state.New(f)
	s.SetState(a, k, v)
	s.SetCode(a, []byte{0})
	s.Finalise(true)
	f = flush(t, s, f, m)
	s = state.New(f)
	id := s.Snapshot()
	s.SetState(a, k, common.Hash{})
	if s.HasStorage(a) {
		t.Fatal("last slot remained")
	}
	_, original := s.GetStateAndCommittedState(a, k)
	if original != v {
		t.Fatal("original changed")
	}
	s.RevertToSnapshot(id)
	if !s.HasStorage(a) || s.GetState(a, k) != v {
		t.Fatal("slot rollback")
	}
	s.SelfDestruct(a)
	s.Finalise(true)
	s.CreateAccount(a)
	if s.HasStorage(a) || s.GetState(a, k) != (common.Hash{}) {
		t.Fatal("old storage resurrected")
	}
}
