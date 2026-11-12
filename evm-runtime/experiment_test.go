package evmruntime

import (
	"bytes"
	"github.com/holiman/uint256"
	"math/big"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"testing"
)

func TestExperimentNoAdmissionAndRollback(t *testing.T) {
	s, _ := NewMemoryState()
	from, to, miner := common.HexToAddress("0x1234"), common.HexToAddress("0x2000"), common.HexToAddress("0xcafe")
	// A zero-balance contract account with an arbitrary current nonce can send.
	s.SetCode(from, []byte{0})
	s.SetNonce(from, 73)
	s.SetCode(to, common.FromHex("336000523260205260406000f3")) // CALLER, ORIGIN
	s.Finalise(true)
	e, err := New(s, nil, Block{Number: 2, GasLimit: 1000000, Coinbase: miner, BaseFee: big.NewInt(1000000000)})
	if err != nil {
		t.Fatal(err)
	}
	tx := ExperimentTransaction{From: from, To: &to, Gas: 100000}
	r, err := e.ApplyExperiment(tx)
	if err != nil || r.VMError != nil {
		t.Fatalf("%+v %v", r, err)
	}
	want := append(common.LeftPadBytes(from[:], 32), common.LeftPadBytes(from[:], 32)...)
	if !bytes.Equal(r.ReturnData, want) || s.GetNonce(from) != 74 || !s.GetBalance(from).IsZero() || s.Exist(miner) || r.Receipt.EffectiveGasPrice.Sign() != 0 || r.Receipt.GasUsed == 0 {
		t.Fatal("wrong free execution semantics")
	}
	for _, code := range []string{"602a60005560006000fd", "5b600056"} {
		s.SetCode(to, common.FromHex(code))
		s.Finalise(true)
		before := s.GetNonce(from)
		r, err = e.ApplyExperiment(tx)
		if err != nil || r.VMError == nil || r.Receipt.Status != 0 || s.GetNonce(from) != before+1 || s.GetState(to, common.Hash{}) != (common.Hash{}) {
			t.Fatal("failed-call rollback/nonce", r, err)
		}
	}
	// Funds for a real value transfer remain necessary; rejection is atomic.
	nonce, gas := s.GetNonce(from), e.GasRemaining()
	tx.Value = big.NewInt(1)
	if _, err = e.ApplyExperiment(tx); err == nil || s.GetNonce(from) != nonce || e.GasRemaining() != gas {
		t.Fatal("invalid value transition was not atomic")
	}
	s.AddBalance(from, uint256.NewInt(3))
	s.SetCode(to, []byte{0})
	s.Finalise(true)
	r, err = e.ApplyExperiment(tx)
	if err != nil || r.VMError != nil || s.GetBalance(from).Uint64() != 2 || s.GetBalance(to).Uint64() != 1 || s.Exist(miner) {
		t.Fatal("value transfer", r, err)
	}
}

func TestExperimentDeploymentAndEncoding(t *testing.T) {
	s, _ := NewMemoryState()
	from := common.HexToAddress("0x87654321")
	s.SetNonce(from, 91)
	s.Finalise(true)
	e, err := New(s, nil, Block{Number: 2, GasLimit: 1000000, BaseFee: big.NewInt(9)})
	if err != nil {
		t.Fatal(err)
	}
	code := common.FromHex("602a60005500")
	tx := ExperimentTransaction{From: from, Gas: 500000, Data: initcode(code)}
	wire, err := EncodeExperimentTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}
	d, err := DecodeExecutionTransaction(wire)
	if err != nil || !d.IsExperiment() {
		t.Fatal(err)
	}
	// Decoder owns bytes even if its input buffer is changed later.
	wire[len(wire)-1] ^= 1
	r, err := d.Apply(e)
	address := crypto.CreateAddress(from, 91)
	if err != nil || r.VMError != nil || r.Receipt.ContractAddress != address || !bytes.Equal(s.GetCode(address), code) || s.GetNonce(from) != 92 {
		t.Fatal("deploy", r, err)
	}
	r, err = e.ApplyExperiment(ExperimentTransaction{From: from, To: &address, Gas: 100000})
	if err != nil || r.VMError != nil || s.GetState(address, common.Hash{}) != common.BigToHash(big.NewInt(42)) {
		t.Fatal("deployed code did not run", r, err)
	}
	if _, err = DecodeExecutionTransaction(wire); err == nil {
		t.Fatal("accepted corrupt encoding")
	}
	for _, bad := range []ExperimentTransaction{{Gas: 0}, {Gas: 1, Value: big.NewInt(-1)}, {Gas: 1, Value: new(big.Int).Lsh(big.NewInt(1), 256)}} {
		if _, err = EncodeExperimentTransaction(bad); err == nil {
			t.Fatal("accepted invalid execution representation")
		}
	}
}
