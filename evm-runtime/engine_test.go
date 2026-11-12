package evmruntime

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/vm"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/state"
)

type harness struct {
	s      *state.StateDB
	e      *Engine
	key    *ecdsa.PrivateKey
	sender common.Address
}

func setup(t *testing.T) harness {
	t.Helper()
	s, err := NewMemoryState()
	if err != nil {
		t.Fatal(err)
	}
	key, err := crypto.HexToECDSA("0123456789012345678901234567890123456789012345678901234567890123")
	if err != nil {
		t.Fatal(err)
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	s.AddBalance(sender, uint256.NewInt(1_000_000_000))
	s.Finalise(true)
	e, err := New(s, nil, Block{Number: 1, Time: 1, GasLimit: 10_000_000, Coinbase: common.HexToAddress("0xcafe"), BaseFee: big.NewInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	return harness{s, e, key, sender}
}
func (h harness) sign(t *testing.T, to *common.Address, input []byte, gas, value uint64) *types.Transaction {
	t.Helper()
	tx, err := types.SignTx(types.NewTx(&types.DynamicFeeTx{
		ChainID: big.NewInt(1337), Nonce: h.s.GetNonce(h.sender), To: to, Data: input,
		Gas: gas, GasFeeCap: big.NewInt(3), GasTipCap: big.NewInt(1), Value: new(big.Int).SetUint64(value),
	}), types.LatestSignerForChainID(big.NewInt(1337)), h.key)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}
func (h harness) apply(t *testing.T, to common.Address, input []byte, gas, value uint64) *Result {
	t.Helper()
	r, err := h.e.Apply(h.sign(t, &to, input, gas, value))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// initcode copies and returns an arbitrary short runtime; no Solidity compiler.
func initcode(runtime []byte) []byte {
	if len(runtime) > 255 {
		panic("test runtime too large")
	}
	return append([]byte{0x60, byte(len(runtime)), 0x60, 0x0c, 0x60, 0, 0x39, 0x60, byte(len(runtime)), 0x60, 0, 0xf3}, runtime...)
}
func (h harness) deploy(t *testing.T, runtime []byte) common.Address {
	t.Helper()
	nonce := h.s.GetNonce(h.sender)
	r, err := h.e.Apply(h.sign(t, nil, initcode(runtime), 500_000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if r.VMError != nil {
		t.Fatal(r.VMError)
	}
	addr := crypto.CreateAddress(h.sender, nonce)
	if r.Receipt.ContractAddress != addr || !bytes.Equal(h.s.GetCode(addr), runtime) {
		t.Fatal("deployment/code mismatch")
	}
	return addr
}
func install(h harness, addr common.Address, code []byte) {
	h.s.CreateAccount(addr)
	h.s.SetNonce(addr, 1)
	h.s.SetCode(addr, code)
	h.s.Finalise(true)
}
func TestTransferAndFees(t *testing.T) {
	h := setup(t)
	to := common.HexToAddress("0x1234")
	before := new(uint256.Int).Set(h.s.GetBalance(h.sender))
	r := h.apply(t, to, nil, 21000, 123)
	if r.VMError != nil || r.Receipt.Status != 1 || r.Receipt.GasUsed != 21000 || r.Receipt.EffectiveGasPrice.Uint64() != 2 {
		t.Fatalf("receipt: %+v", r)
	}
	if h.s.GetBalance(to).Uint64() != 123 || h.s.GetNonce(h.sender) != 1 {
		t.Fatal("transfer/nonce")
	}
	want := new(uint256.Int).Sub(before, uint256.NewInt(123+42000))
	if h.s.GetBalance(h.sender).Cmp(want) != 0 || h.s.GetBalance(h.e.block.Coinbase).Uint64() != 21000 {
		t.Fatal("fee accounting")
	}
	if h.e.GasRemaining() != 10_000_000-21000 {
		t.Fatal("block gas")
	}
}
func TestDeployStorageAndOrderedTransactions(t *testing.T) {
	h := setup(t)
	// calldata[0:32] -> slot 0; emit LOG1(topic=1, data=value); return value.
	code := common.FromHex("0x600035600055600054600052600160206000a160206000f3")
	addr := h.deploy(t, code)
	var previous uint64
	for i, v := range []byte{42, 77} {
		input := make([]byte, 32)
		input[31] = v
		r := h.apply(t, addr, input, 100000, 0)
		if r.VMError != nil || !bytes.Equal(r.ReturnData, input) || h.s.GetState(addr, common.Hash{}) != common.BytesToHash(input) {
			t.Fatal("storage/result")
		}
		if r.Receipt.TransactionIndex != uint(i+1) || r.Receipt.CumulativeGasUsed <= previous {
			t.Fatal("receipt ordering")
		}
		previous = r.Receipt.CumulativeGasUsed
	}
	// A new block reuses the same state and starts a fresh gas pool/tx index.
	next, err := New(h.s, nil, Block{Number: 2, Time: 2, GasLimit: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	h.e = next
	input := make([]byte, 32)
	input[31] = 99
	r := h.apply(t, addr, input, 100000, 0)
	if r.VMError != nil || r.Receipt.TransactionIndex != 0 || r.Receipt.CumulativeGasUsed != r.Receipt.GasUsed {
		t.Fatal("new block")
	}
}
func TestRevertAndOutOfGas(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		gas        uint64
		want       error
	}{
		{"revert", "0x602a60005560006000a060006000fd", 100000, vm.ErrExecutionReverted},
		{"out_of_gas", "0x602a60005500", 22000, vm.ErrOutOfGas},
		{"invalid_opcode", "0xfe", 100000, &vm.ErrInvalidOpCode{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := setup(t)
			addr := common.HexToAddress("0x1000")
			install(h, addr, common.FromHex(tc.code))
			balance := h.s.GetBalance(h.sender).Uint64()
			r := h.apply(t, addr, nil, tc.gas, 100)
			if r.VMError == nil || r.Receipt.Status != 0 {
				t.Fatal("expected included failure")
			}
			if tc.name != "invalid_opcode" && !errors.Is(r.VMError, tc.want) {
				t.Fatalf("VM error %v", r.VMError)
			}
			if h.s.GetNonce(h.sender) != 1 || h.s.GetState(addr, common.Hash{}) != (common.Hash{}) || h.s.GetBalance(addr).Sign() != 0 {
				t.Fatal("rollback")
			}
			if h.s.GetBalance(h.sender).Uint64() != balance-r.Receipt.GasUsed*2 {
				t.Fatal("failed tx must pay gas")
			}
		})
	}
}
func TestRejectedTransactionRestoresGasAndState(t *testing.T) {
	h := setup(t)
	addr := common.HexToAddress("0x1000")
	// Intrinsic gas rejection happens after upfront gas purchase.
	tx := h.sign(t, &addr, []byte{1}, 21000, 0)
	before := h.s.GetBalance(h.sender).Uint64()
	gas := h.e.GasRemaining()
	if _, err := h.e.Apply(tx); err == nil {
		t.Fatal("expected intrinsic gas rejection")
	}
	if h.s.GetBalance(h.sender).Uint64() != before || h.s.GetNonce(h.sender) != 0 || h.e.GasRemaining() != gas {
		t.Fatal("rejected transaction mutated state")
	}
	valid := h.sign(t, &addr, nil, 21000, 0)
	r, err := h.e.Apply(valid)
	if err != nil || r.Receipt.TransactionIndex != 0 {
		t.Fatal("could not recover after rejection", err)
	}
	if _, err = h.e.Apply(valid); err == nil {
		t.Fatal("nonce replay accepted")
	}
	// Block gas exhaustion also leaves state untouched.
	h.e.gas.SetGas(20000)
	before = h.s.GetBalance(h.sender).Uint64()
	if _, err = h.e.Apply(h.sign(t, &addr, nil, 21000, 0)); err == nil {
		t.Fatal("block gas accepted")
	}
	if h.e.GasRemaining() != 20000 || h.s.GetBalance(h.sender).Uint64() != before {
		t.Fatal("block gas rejection mutated state")
	}
}
func TestSignaturesAndForkGating(t *testing.T) {
	h := setup(t)
	addr := common.HexToAddress("0x1234")
	unsigned := types.NewTx(&types.LegacyTx{To: &addr, Gas: 21000, GasPrice: big.NewInt(2)})
	if _, err := h.e.Apply(unsigned); err == nil {
		t.Fatal("unsigned transaction accepted")
	}
	wrong, err := types.SignTx(types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(1), To: &addr, Gas: 21000, GasFeeCap: big.NewInt(3), GasTipCap: big.NewInt(1)}), types.LatestSignerForChainID(big.NewInt(1)), h.key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.e.Apply(wrong); err == nil {
		t.Fatal("wrong chain accepted")
	}
	// The unsupported EIP-7702 envelope is rejected at decoding as well.
    var unsupported types.Transaction
    if err := unsupported.UnmarshalBinary([]byte{0x04,0xc0}); !errors.Is(err,types.ErrTxTypeNotSupported) { t.Fatal("unsupported transaction envelope accepted",err) }

}
func TestPrecompile(t *testing.T) {
	h := setup(t)
	input := []byte("statera EVM")
	addr := common.BytesToAddress([]byte{2})
	r := h.apply(t, addr, input, 100000, 0)
	sum := sha256.Sum256(input)
	if r.VMError != nil || !bytes.Equal(r.ReturnData, sum[:]) {
		t.Fatal("SHA256 precompile")
	}
}
func TestTransientStorageReset(t *testing.T) {
	h := setup(t)
	addr := common.HexToAddress("0x1000")
	// Return old transient slot, then set it to 42. Each transaction must see 0.
	install(h, addr, common.FromHex("0x60005c600052602a60005d60206000f3"))
	for i := 0; i < 2; i++ {
		r := h.apply(t, addr, nil, 100000, 0)
		if r.VMError != nil || len(r.ReturnData) != 32 || common.BytesToHash(r.ReturnData) != (common.Hash{}) {
			t.Fatal("transient storage leaked", r.VMError)
		}
	}
}
func TestNestedCallRollback(t *testing.T) {
	h := setup(t)
	child := common.HexToAddress("0x1001")
	parent := common.HexToAddress("0x1002")
	install(h, child, common.FromHex("0x602a60005560006000fd"))
	// CALL child; ignore failure; set own slot to 99. Child changes must revert.
	code := common.FromHex("0x6000600060006000600073")
	code = append(code, child.Bytes()...)
	code = append(code, common.FromHex("0x620186a0f150606360005500")...)
	install(h, parent, code)
	r := h.apply(t, parent, nil, 300000, 0)
	if r.VMError != nil || h.s.GetState(child, common.Hash{}) != (common.Hash{}) || h.s.GetState(parent, common.Hash{}) != common.BigToHash(big.NewInt(99)) {
		t.Fatal("nested rollback", r.VMError)
	}
}
func TestSelfDestructCancun(t *testing.T) {
	h := setup(t)
	beneficiary := common.HexToAddress("0xbeef")
	addr := common.HexToAddress("0x1000")
	code := append([]byte{0x73}, beneficiary.Bytes()...)
	code = append(code, 0xff)
	install(h, addr, code)
	r := h.apply(t, addr, nil, 100000, 123)
	if r.VMError != nil || h.s.GetBalance(beneficiary).Uint64() != 123 || !bytes.Equal(h.s.GetCode(addr), code) {
		t.Fatal("EIP6780 must retain existing code")
	}
}
func TestStaticCallCannotWrite(t *testing.T) {
	h := setup(t)
	child := common.HexToAddress("0x1001")
	parent := common.HexToAddress("0x1002")
	install(h, child, common.FromHex("0x602a60005500"))
	code := common.FromHex("0x600060006000600073")
	code = append(code, child.Bytes()...)
	code = append(code, common.FromHex("0x620186a0fa60005260206000f3")...)
	install(h, parent, code)
	r := h.apply(t, parent, nil, 300000, 0)
	if r.VMError != nil || common.BytesToHash(r.ReturnData) != (common.Hash{}) || h.s.GetState(child, common.Hash{}) != (common.Hash{}) {
		t.Fatal("STATICCALL wrote state")
	}
}

func TestLegacyAndAccessListTransactions(t *testing.T) {
	for _, kind := range []string{"legacy", "access-list"} {
		t.Run(kind, func(t *testing.T) {
			h := setup(t)
			addr := common.HexToAddress("0x1234")
			var data types.TxData
			if kind == "legacy" {
				data = &types.LegacyTx{To: &addr, Gas: 50000, GasPrice: big.NewInt(2), Value: big.NewInt(7)}
			} else {
				data = &types.AccessListTx{ChainID: big.NewInt(1337), To: &addr, Gas: 50000, GasPrice: big.NewInt(2), Value: big.NewInt(7), AccessList: types.AccessList{{Address: addr, StorageKeys: []common.Hash{{}}}}}
			}
			tx, err := types.SignTx(types.NewTx(data), types.LatestSignerForChainID(big.NewInt(1337)), h.key)
			if err != nil {
				t.Fatal(err)
			}
			r, err := h.e.Apply(tx)
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(21000)
			if kind == "access-list" {
				want += 2400 + 1900
			}
			if r.VMError != nil || r.Receipt.GasUsed != want || h.s.GetBalance(addr).Uint64() != 7 {
				t.Fatal("envelope/intrinsic gas")
			}
		})
	}
}
func TestDelegateCallUsesCallerStorage(t *testing.T) {
	h := setup(t)
	child := common.HexToAddress("0x1001")
	parent := common.HexToAddress("0x1002")
	install(h, child, common.FromHex("0x602a60005500"))
	code := common.FromHex("0x600060006000600073")
	code = append(code, child.Bytes()...)
	code = append(code, common.FromHex("0x620186a0f45000")...)
	install(h, parent, code)
	r := h.apply(t, parent, nil, 300000, 0)
	if r.VMError != nil || h.s.GetState(child, common.Hash{}) != (common.Hash{}) || h.s.GetState(parent, common.Hash{}) != common.BigToHash(big.NewInt(42)) {
		t.Fatal("delegate storage context")
	}
}
func TestBlockContext(t *testing.T) {
	h := setup(t)
	addr := common.HexToAddress("0x1000")
	// Return NUMBER, TIMESTAMP, CHAINID, BASEFEE and BLOCKHASH(0).
	install(h, addr, common.FromHex("0x4360005242602052466040524860605260004060805260a06000f3"))
	hash := common.HexToHash("0xabcd")
	h.e.block.GetHash = func(n uint64) common.Hash {
		if n != 0 {
			t.Fatalf("ancestor %d", n)
		}
		return hash
	}
	r := h.apply(t, addr, nil, 100000, 0)
	if r.VMError != nil || len(r.ReturnData) != 160 {
		t.Fatal("block context", r.VMError)
	}
	for i, want := range []uint64{1, 1, 1337, 1} {
		if new(big.Int).SetBytes(r.ReturnData[i*32:(i+1)*32]).Uint64() != want {
			t.Fatal("context word", i)
		}
	}
	if common.BytesToHash(r.ReturnData[128:]) != hash {
		t.Fatal("BLOCKHASH")
	}
}

func TestBlobExecutionAndBlockLimit(t *testing.T) {
	h := setup(t)
	addr := common.HexToAddress("0x1000")
	install(h, addr, common.FromHex("0x6000496000524a60205260406000f3"))
	hash := common.HexToHash("0x0100000000000000000000000000000000000000000000000000000000000042")
	sign := func(hashes []common.Hash) *types.Transaction {
		tx, err := types.SignTx(types.NewTx(&types.BlobTx{
			ChainID: uint256.NewInt(1337), Nonce: h.s.GetNonce(h.sender), To: addr, Gas: 100000,
			GasFeeCap: uint256.NewInt(3), GasTipCap: uint256.NewInt(1), BlobFeeCap: uint256.NewInt(2), BlobHashes: hashes,
		}), types.LatestSignerForChainID(big.NewInt(1337)), h.key)
		if err != nil {
			t.Fatal(err)
		}
		return tx
	}
	before := h.s.GetBalance(h.sender).Uint64()
	r, err := h.e.Apply(sign([]common.Hash{hash}))
	if err != nil {
		t.Fatal(err)
	}
	if r.VMError != nil || len(r.ReturnData) != 64 || common.BytesToHash(r.ReturnData[:32]) != hash || new(big.Int).SetBytes(r.ReturnData[32:]).Uint64() != 1 {
		t.Fatal("BLOBHASH/BLOBBASEFEE")
	}
	if r.Receipt.BlobGasUsed != 131072 || r.Receipt.BlobGasPrice.Uint64() != 1 || h.s.GetBalance(h.sender).Uint64() != before-r.Receipt.GasUsed*2-131072 {
		t.Fatal("blob fees")
	}
	nonce := h.s.GetNonce(h.sender)
	gas := h.e.GasRemaining()
	if _, err = h.e.Apply(sign([]common.Hash{{}})); err == nil {
		t.Fatal("invalid blob version accepted")
	}
	if _, err = h.e.Apply(sign(nil)); err == nil {
		t.Fatal("empty blob hashes accepted")
	}
	if _, err = h.e.Apply(sign([]common.Hash{hash, hash, hash, hash, hash, hash})); err == nil {
		t.Fatal("block blob limit exceeded")
	}
	if h.s.GetNonce(h.sender) != nonce || h.e.GasRemaining() != gas || h.e.BlobGasUsed() != 131072 {
		t.Fatal("blob rejection mutated state")
	}
	if _, err = h.e.Apply(sign([]common.Hash{hash, hash, hash, hash, hash})); err != nil {
		t.Fatal(err)
	}
	if h.e.BlobGasUsed() != 6*131072 {
		t.Fatal("block blob accounting")
	}
}

func TestCreateCollisionUsesStoragePresence(t *testing.T) {
	h := setup(t)
	addr := crypto.CreateAddress(h.sender, 0)
	h.s.AddBalance(addr, uint256.NewInt(1))
	h.s.SetState(addr, common.Hash{}, common.HexToHash("0x01"))
	h.s.Finalise(true)
	r, err := h.e.Apply(h.sign(t, nil, initcode([]byte{0}), 200000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(r.VMError, vm.ErrContractAddressCollision) || !h.s.HasStorage(addr) || h.s.GetBalance(addr).Uint64() != 1 {
		t.Fatal("storage collision", r.VMError)
	}
}
func TestUnsupportedForkRejected(t *testing.T) {
	h := setup(t)
	chain := DefaultChainConfig()
	zero := uint64(0)
	chain.PragueTime = &zero
	if _, err := New(h.s, chain, Block{Number: 1, Time: 1, GasLimit: 100000}); err == nil {
		t.Fatal("unsupported Prague configuration silently accepted")
	}
}

func TestLogOpcodesDiscardEventsAndChargeGas(t *testing.T) {
	for topics := 0; topics <= 4; topics++ {
		t.Run(fmt.Sprintf("LOG%d", topics), func(t *testing.T) {
			h := setup(t)
			addr := common.HexToAddress("0x1000")
			var code []byte
			for i := 0; i < topics; i++ {
				code = append(code, 0x60, byte(i+1))
			}
			code = append(code, common.FromHex("0x60206000")...)
			code = append(code, byte(0xa0+topics))
			code = append(code, common.FromHex("0x602a60005260206000f3")...)
			install(h, addr, code)
			r := h.apply(t, addr, nil, 100000, 0)
			wantGas := uint64(21000 + 3*(topics+2) + 375 + 375*topics + 32*8 + 3 + 15)
			if r.VMError != nil || len(r.ReturnData) != 32 || r.ReturnData[31] != 42 || r.Receipt.GasUsed != wantGas {
				t.Fatalf("LOG execution/gas: err=%v gas=%d want=%d", r.VMError, r.Receipt.GasUsed, wantGas)
			}
		})
	}
	t.Run("STATICCALL rejects LOG", func(t *testing.T) {
		h := setup(t)
		child := common.HexToAddress("0x1001")
		parent := common.HexToAddress("0x1002")
		install(h, child, common.FromHex("0x60006000a000"))
		code := common.FromHex("0x600060006000600073")
		code = append(code, child.Bytes()...)
		code = append(code, common.FromHex("0x620186a0fa60005260206000f3")...)
		install(h, parent, code)
		r := h.apply(t, parent, nil, 300000, 0)
		if r.VMError != nil || len(r.ReturnData) != 32 || common.BytesToHash(r.ReturnData) != (common.Hash{}) {
			t.Fatal("LOG must fail inside STATICCALL")
		}
	})
}
