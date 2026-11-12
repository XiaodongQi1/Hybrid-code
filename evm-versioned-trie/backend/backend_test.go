package backend

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"reflect"
	"testing"

	"github.com/holiman/uint256"
	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/params"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

type testOpener func(*testing.T, string, vt.Config, bool, bool) (*Backend, error)

func legacyTestOpener(_ *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
	return Open(path, c, create, ro)
}

func smallConfig(mode codec.Mode) vt.Config {
	c := vt.DefaultConfig(string(mode), "live-evm-correctness/v1")
	c.Storage.CacheBytes = 0
	c.Storage.BlockCacheBytes = 2 << 20
	c.Storage.WriteBufferBytes = 1 << 20
	return c
}
func contextAt(h uint64) block.Context {
	return block.Context{Time: h, GasLimit: 30_000_000, BaseFee: big.NewInt(1), BlobBaseFee: big.NewInt(1), Coinbase: common.HexToAddress("0xcafe")}
}
func inputAt(h uint64) block.InputBlock {
	b := block.InputBlock{Height: h, ID: common.BigToHash(new(big.Int).SetUint64(h + 1)), Context: contextAt(h)}
	if h > 0 {
		b.ParentID = common.BigToHash(new(big.Int).SetUint64(h))
	}
	return b
}
func encoded(t *testing.T, from, to common.Address, value byte, gas uint64) []byte {
	t.Helper()
	data := make([]byte, 32)
	data[31] = value
	wire, e := rt.EncodeExperimentTransaction(rt.ExperimentTransaction{From: from, To: &to, Gas: gas, Data: data})
	if e != nil {
		t.Fatal(e)
	}
	return wire
}
func apply(t *testing.T, b *Backend, input block.InputBlock, chain *params.ChainConfig) *Result {
	t.Helper()
	r, e := b.Apply(input, chain)
	if e != nil {
		t.Fatalf("block %d: %v", input.Height, e)
	}
	return r
}

type expectedAccount struct {
	value  state.Account
	exists bool
	code   []byte
	slots  map[common.Hash]common.Hash
}
type expectedState map[common.Address]expectedAccount

type rootAccount struct {
	Address, Balance, CodeHash string
	Nonce                      uint64
	Storage                    map[string]string
}
type rootCase struct {
	Mode, Root string
	Accounts   []rootAccount
}

// Build an independent full hash-addressed forest from the map-backed runtime's
// visible values. No compact reader, projection, lifecycle or materializer is
// used in this reference. All touched addresses/slots are explicit in the test.
func reference(t *testing.T, s rt.State, addresses []common.Address, mode codec.Mode) (mpt.Hash, expectedState) {
	t.Helper()
	mem := mpt.NewMemoryStore()
	at, _ := mpt.NewEmpty(mem)
	want := expectedState{}
	for _, addr := range addresses {
		row := expectedAccount{exists: s.Exist(addr), slots: map[common.Hash]common.Hash{}}
		if row.exists {
			st, _ := mpt.NewEmpty(mem)
			for n := int64(0); n < 4; n++ {
				k := common.BigToHash(big.NewInt(n))
				word := s.GetState(addr, k)
				row.slots[k] = word
				raw, e := mode.EncodeStorage(word)
				if e != nil {
					t.Fatal(e)
				}
				if e = st.Update(state.SlotKey(k), raw); e != nil {
					t.Fatal(e)
				}
			}
			root, e := st.Commit()
			if e != nil {
				t.Fatal(e)
			}
			row.value = state.Account{Nonce: s.GetNonce(addr), Balance: *s.GetBalance(addr), StorageRoot: root, CodeHash: s.GetCodeHash(addr)}
			row.code = bytes.Clone(s.GetCode(addr))
			raw, e := mode.EncodeAccount(row.value)
			if e != nil {
				t.Fatal(e)
			}
			if e = at.Update(state.AccountKey(addr), raw); e != nil {
				t.Fatal(e)
			}
		}
		want[addr] = row
	}
	root, e := at.Commit()
	if e != nil {
		t.Fatal(e)
	}
	return root, want
}

type proofCase struct {
	Root, Key string
	Proof     []string
	Value     string
	Exists    bool
}

func verify(t *testing.T, root mpt.Hash, key []byte, p mpt.Proof, want []byte, exists bool, cases *[]proofCase) {
	t.Helper()
	got, ok, e := mpt.VerifyProof(root, key, p)
	if e != nil || ok != exists || !bytes.Equal(got, want) {
		t.Fatalf("proof: exists=%t err=%v", ok, e)
	}
	c := proofCase{Root: hex.EncodeToString(root[:]), Key: hex.EncodeToString(key), Value: hex.EncodeToString(want), Exists: exists}
	for _, raw := range p {
		c.Proof = append(c.Proof, hex.EncodeToString(raw))
	}
	*cases = append(*cases, c)
}

func TestLiveEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, legacyTestOpener)
}

func runTestLiveEVMRootsReceiptsHistoryAndReopen(t *testing.T, open testOpener) {
	var cases []proofCase
	var rootCases []rootCase
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			cfg := smallConfig(mode)
			path := t.TempDir() + "/db"
			b, e := open(t, path, cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { b.Close() }()
			mem, _ := rt.NewMemoryState()
			key, _ := crypto.HexToECDSA("0123456789012345678901234567890123456789012345678901234567890123")
			sender := crypto.PubkeyToAddress(key.PublicKey)
			contract1, contract2, reverter, hashReader := common.HexToAddress("0x1001"), common.HexToAddress("0x1002"), common.HexToAddress("0x1003"), common.HexToAddress("0x1004")
			other, recipient := common.HexToAddress("0x2000"), common.HexToAddress("0x3000")
			deployed := crypto.CreateAddress(sender, 1)
			addresses := []common.Address{sender, other, recipient, contract1, contract2, reverter, hashReader, deployed, contextAt(0).Coinbase, common.HexToAddress("0xdead")}
			code := common.FromHex("0x60003560005560005460005260206000f3")
			genesis := inputAt(0)
			genesis.Allocations = []block.Allocation{
				{Address: sender, Balance: big.NewInt(1_000_000_000)},
				{Address: other, Balance: big.NewInt(1_000_000)},
				{Address: contract1, Nonce: 1, Balance: new(big.Int), Code: code, Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x11")}},
				{Address: contract2, Nonce: 1, Balance: new(big.Int), Code: code, Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x22")}},
				{Address: reverter, Nonce: 1, Balance: new(big.Int), Code: common.FromHex("0x602a60005560006000fd")},
				{Address: hashReader, Nonce: 1, Balance: new(big.Int), Code: common.FromHex("0x60004060005260206000f3")},
			}
			for _, a := range genesis.Allocations {
				mem.CreateAccount(a.Address)
				v, _ := uint256.FromBig(a.Balance)
				mem.AddBalance(a.Address, v)
				mem.SetNonce(a.Address, a.Nonce)
				mem.SetCode(a.Address, a.Code)
				for k, v := range a.Storage {
					mem.SetState(a.Address, k, v)
				}
			}
			var roots []mpt.Hash
			var expected []expectedState
			checkRoot := func(r *Result) {
				root, want := reference(t, mem, addresses, mode)
				if r.Root != root {
					t.Fatalf("canonical root mismatch at %d: %x != %x", r.Height, r.Root, root)
				}
				roots = append(roots, root)
				expected = append(expected, want)
				c := rootCase{Mode: string(mode), Root: hex.EncodeToString(root[:])}
				for _, addr := range addresses {
					a := want[addr]
					if !a.exists {
						continue
					}
					row := rootAccount{Address: addr.Hex(), Balance: a.value.Balance.Dec(), CodeHash: a.value.CodeHash.Hex(), Nonce: a.value.Nonce, Storage: map[string]string{}}
					for k, v := range a.slots {
						if v != (common.Hash{}) {
							row.Storage[k.Hex()] = v.Hex()
						}
					}
					c.Accounts = append(c.Accounts, row)
				}
				rootCases = append(rootCases, c)
			}
			checkRoot(apply(t, b, genesis, nil))
			v, e := b.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			id1, _, _ := v.StorageID(contract1)
			id2, _, _ := v.StorageID(contract2)
			if id1 == id2 || id1 == 0 || id2 == 0 {
				t.Fatal("storage namespace collision")
			}
			if _, e := v.Slot(mpt.EmptyRootHash, common.Hash{}); !errors.Is(e, ErrOwnerRequired) {
				t.Fatal(e)
			}
			v.Close()
			signed := func(nonce uint64, to *common.Address, data []byte, value int64) []byte {
				tx, e := types.SignTx(types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(1337), Nonce: nonce, To: to, Gas: 500_000, GasFeeCap: big.NewInt(3), GasTipCap: big.NewInt(1), Value: big.NewInt(value), Data: data}), types.LatestSignerForChainID(big.NewInt(1337)), key)
				if e != nil {
					t.Fatal(e)
				}
				wire, e := tx.MarshalBinary()
				if e != nil {
					t.Fatal(e)
				}
				return wire
			}
			for h := uint64(1); h <= 12; h++ {
				input := inputAt(h)
				if h == 1 {
					initcode := append([]byte{0x60, byte(len(code)), 0x60, 0x0c, 0x60, 0, 0x39, 0x60, byte(len(code)), 0x60, 0, 0xf3}, code...)
					input.Transactions = append(input.Transactions, signed(0, &recipient, nil, 123), signed(1, nil, initcode, 0))
				}
				input.Transactions = append(input.Transactions, encoded(t, other, contract1, byte(h), 100_000), encoded(t, other, contract2, byte(h+30), 100_000), encoded(t, other, deployed, byte(h+60), 100_000), encoded(t, other, reverter, 0, 100_000), encoded(t, other, contract1, 99, 22_000))
				if h == 2 {
					input.Transactions = append(input.Transactions, encoded(t, other, hashReader, 0, 100_000))
				}
				if h == 6 {
					input.Transactions = append(input.Transactions, encoded(t, other, contract1, 0, 100_000))
				}
				ctx := input.Context
				engine, e := rt.New(mem, nil, rt.Block{Number: h, Time: ctx.Time, GasLimit: ctx.GasLimit, Coinbase: ctx.Coinbase, BaseFee: ctx.BaseFee, BlobBaseFee: ctx.BlobBaseFee, Hash: input.ID, GetHash: func(n uint64) common.Hash { return inputAt(n).ID }})
				if e != nil {
					t.Fatal(e)
				}
				var outcomes []block.Outcome
				for _, wire := range input.Transactions {
					r, e := engine.ApplyEncoded(wire)
					if e != nil {
						t.Fatal(e)
					}
					o := block.Outcome{Receipt: r.Receipt, ReturnData: r.ReturnData}
					if r.VMError != nil {
						o.VMError = r.VMError.Error()
					}
					outcomes = append(outcomes, o)
				}
				r := apply(t, b, input, nil)
				if !reflect.DeepEqual(r.Outcomes, outcomes) {
					t.Fatalf("receipt/return/VM-error mismatch block %d", h)
				}
				checkRoot(r)
				if h == 5 {
					if e = b.Close(); e != nil {
						t.Fatal(e)
					}
					b, e = open(t, path, cfg, false, false)
					if e != nil {
						t.Fatal(e)
					}
				}
			}
			// Hold an old view while appending an empty block; the pinned reader
			// must continue using its old height and namespace mappings.
			pinned, e := b.ViewAt(1)
			if e != nil {
				t.Fatal(e)
			}
			checkRoot(apply(t, b, inputAt(13), nil))
			old, e := pinned.SlotAt(contract1, expected[1][contract1].value.StorageRoot, common.Hash{})
			if e != nil || old != expected[1][contract1].slots[common.Hash{}] {
				t.Fatal("pinned history", e)
			}
			pinned.Close()
			if e = b.Close(); e != nil {
				t.Fatal(e)
			}
			b, e = open(t, path, cfg, false, true)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = b.Apply(inputAt(14), nil); !errors.Is(e, vt.ErrReadOnly) {
				t.Fatal(e)
			}
			for h, want := range expected {
				v, e := b.ViewAt(uint64(h))
				if e != nil {
					t.Fatal(e)
				}
				for addr, row := range want {
					got, ok, e := v.Account(addr)
					if e != nil || ok != row.exists || ok && got != row.value {
						t.Fatalf("historical account h%d %s %v", h, addr, e)
					}
					p, e := v.AccountProof(addr)
					if e != nil {
						t.Fatal(e)
					}
					var raw []byte
					if ok {
						raw, e = mode.EncodeAccount(row.value)
						if e != nil {
							t.Fatal(e)
						}
					}
					verify(t, roots[h], state.AccountKey(addr), p, raw, ok, &cases)
					if ok {
						code, e := v.Code(got.CodeHash)
						if e != nil || !bytes.Equal(code, row.code) {
							t.Fatal("historical code", e)
						}
					}
					for n := int64(0); n < 4; n++ {
						k := common.BigToHash(big.NewInt(n))
						root, p, e := v.StorageProof(addr, k)
						if e != nil {
							t.Fatal(e)
						}
						word := row.slots[k]
						raw, e := mode.EncodeStorage(word)
						if e != nil {
							t.Fatal(e)
						}
						verify(t, root, state.SlotKey(k), p, raw, word != (common.Hash{}), &cases)
					}
				}
				v.Close()
			}
		})
	}
	if path := os.Getenv("VT_EVM_PROOF_REPORT"); path != "" {
		raw, e := json.MarshalIndent(struct {
			Schema string
			Cases  []proofCase
		}{"versioned-trie-proof-cases/v1", cases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("verified %d live-EVM historical account/storage proof cases", len(cases))
	if path := os.Getenv("VT_EVM_STATE_REPORT"); path != "" {
		raw, e := json.MarshalIndent(struct {
			Schema string
			Cases  []rootCase
		}{"versioned-evm-state-cases/v1", rootCases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(raw, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}

// Legacy roots also match the unchanged raw-MPT materializer, not only the
// map-backed EVM reference used by the larger execution test.
func TestLegacyExistingForestMaterializer(t *testing.T) {
	b, e := Open(t.TempDir()+"/db", smallConfig(codec.Legacy), true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	a := common.HexToAddress("0x1000")
	input := inputAt(0)
	input.Allocations = []block.Allocation{{Address: a, Balance: big.NewInt(50), Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}}}
	r := apply(t, b, input, nil)
	m := &codeStore{MemoryStore: mpt.NewMemoryStore()}
	f, e := forest.Open(mpt.EmptyRootHash, m)
	if e != nil {
		t.Fatal(e)
	}
	s := state.New(f)
	s.AddBalance(a, uint256.NewInt(50))
	s.SetState(a, common.Hash{}, common.HexToHash("0x12"))
	c, e := s.Changes()
	if e != nil {
		t.Fatal(e)
	}
	p, e := f.Materialize(c, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if r.Root != p.Root {
		t.Fatal("legacy root changed")
	}
}

type codeStore struct{ *mpt.MemoryStore }

func (*codeStore) Code(common.Hash) ([]byte, error) { return nil, vt.ErrNotFound }

func TestRejectedBlockLeavesDurableHeadUnchanged(t *testing.T) {
	runTestRejectedBlockLeavesDurableHeadUnchanged(t, legacyTestOpener)
}

func runTestRejectedBlockLeavesDurableHeadUnchanged(t *testing.T, open testOpener) {
	b, e := open(t, t.TempDir()+"/db", smallConfig(codec.Ethereum), true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	from, to := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	genesis := inputAt(0)
	genesis.Allocations = []block.Allocation{{Address: from, Balance: big.NewInt(100)}}
	apply(t, b, genesis, nil)
	head, _ := b.Head()
	input := inputAt(1)
	input.Transactions = [][]byte{encoded(t, from, to, 0, 50_000), {0xff}}
	if _, e = b.Apply(input, nil); e == nil {
		t.Fatal("malformed second transaction accepted")
	}
	got, _ := b.Head()
	if got != head {
		t.Fatal("partial block committed")
	}
	v, e := b.ViewAt(0)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	a, _, e := v.Account(from)
	if e != nil || a.Nonce != 0 {
		t.Fatal("private nonce leaked")
	}
	bad := inputAt(1)
	bad.ParentID = common.Hash{}
	if _, e = b.Apply(bad, nil); !errors.Is(e, vt.ErrConflict) {
		t.Fatal(e)
	}
	apply(t, b, inputAt(1), nil)
}

func TestLifecycleResetSurvivesFinaliseAndRollback(t *testing.T) {
	runTestLifecycleResetSurvivesFinaliseAndRollback(t, legacyTestOpener)
}

func runTestLifecycleResetSurvivesFinaliseAndRollback(t *testing.T, open testOpener) {
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			b, e := open(t, t.TempDir()+"/db", smallConfig(mode), true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close()
			a := common.HexToAddress("0x1000")
			code := []byte{0x00}
			genesis := inputAt(0)
			genesis.Allocations = []block.Allocation{{Address: a, Balance: big.NewInt(1), Nonce: 1, Code: code, Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}}}
			apply(t, b, genesis, nil)
			v, e := b.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			oldID, _, _ := v.StorageID(a)
			s := state.New(v)
			snap := s.Snapshot()
			s.CreateAccount(a)
			s.SetCode(a, code)
			s.RevertToSnapshot(snap)
			s.SetNonce(a, 2)
			s.Finalise(true)
			c, e := s.Changes()
			if e != nil {
				t.Fatal(e)
			}
			if c[a].ResetStorage {
				t.Fatal("reverted reset leaked")
			}
			if _, e = b.commit(v, 1, [32]byte(inputAt(1).ID), c); e != nil {
				t.Fatal(e)
			}
			v.Close()
			v, e = b.ViewAt(1)
			if e != nil {
				t.Fatal(e)
			}
			id, _, _ := v.StorageID(a)
			if id != oldID {
				t.Fatal("ordinary update changed ID")
			}
			s = state.New(v)
			s.CreateAccount(a)
			s.CreateContract(a)
			s.SetNonce(a, 1)
			s.SetCode(a, code)
			s.Finalise(true)
			c, e = s.Changes()
			if e != nil {
				t.Fatal(e)
			}
			if !c[a].ResetStorage {
				t.Fatal("Finalise lost reset")
			}
			if _, e = b.commit(v, 2, [32]byte(inputAt(2).ID), c); e != nil {
				t.Fatal(e)
			}
			v.Close()
			v, e = b.ViewAt(2)
			if e != nil {
				t.Fatal(e)
			}
			id, active, e := v.StorageID(a)
			if e != nil || !active || id <= oldID {
				t.Fatal("reset reused old ID", e)
			}
			account, _, _ := v.Account(a)
			word, e := v.SlotAt(a, account.StorageRoot, common.Hash{})
			if e != nil || word != (common.Hash{}) {
				t.Fatal("old slot survived reset", e)
			}
			s = state.New(v)
			s.SelfDestruct(a)
			s.Finalise(true)
			c, e = s.Changes()
			if e != nil {
				t.Fatal(e)
			}
			if _, e = b.commit(v, 3, [32]byte(inputAt(3).ID), c); e != nil {
				t.Fatal(e)
			}
			v.Close()
			v, e = b.ViewAt(3)
			if e != nil {
				t.Fatal(e)
			}
			_, active, e = v.StorageID(a)
			if e != nil || active {
				t.Fatal("deleted lifecycle still active", e)
			}
			s = state.New(v)
			s.CreateAccount(a)
			s.SetNonce(a, 1)
			s.SetCode(a, code)
			s.SetState(a, common.Hash{}, common.HexToHash("0x34"))
			s.Finalise(true)
			c, e = s.Changes()
			if e != nil {
				t.Fatal(e)
			}
			if _, e = b.commit(v, 4, [32]byte(inputAt(4).ID), c); e != nil {
				t.Fatal(e)
			}
			v.Close()
			v, e = b.ViewAt(4)
			if e != nil {
				t.Fatal(e)
			}
			newID, active, e := v.StorageID(a)
			v.Close()
			if e != nil || !active || newID <= id {
				t.Fatal("redeployment reused ID", e)
			}
			v, e = b.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			defer v.Close()
			account, _, _ = v.Account(a)
			word, e = v.SlotAt(a, account.StorageRoot, common.Hash{})
			if e != nil || word != common.HexToHash("0x12") {
				t.Fatal("old lifecycle corrupted", e)
			}
		})
	}
}
