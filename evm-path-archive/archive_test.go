package patharchive

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/vm"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/vm/program"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/rlp"
	archive "github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func word(n uint64) common.Hash { var h common.Hash; binary.BigEndian.PutUint64(h[24:], n); return h }
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}
func equal(t *testing.T, a, b any) {
	t.Helper()
	x, e := json.Marshal(a)
	must(t, e)
	y, e := json.Marshal(b)
	must(t, e)
	if !bytes.Equal(x, y) {
		t.Fatalf("mismatch\n%s\n%s", x, y)
	}
}
func dataset(t *testing.T) (archive.Workload, []common.Address, []common.Hash) {
	key, e := crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	must(t, e)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	setter := common.FromHex("60003560005560005460005260206000f3")
	deployed := crypto.CreateAddress(sender, 0)
	reverter := common.HexToAddress("0x2000")
	loop := common.HexToAddress("0x3000")
	delegate := common.HexToAddress("0x4000")
	coinbase := common.HexToAddress("0xcafe")
	recipient := common.HexToAddress("0x9000")
	delegateCode := program.New().Push(32).Push(0).Push(0).Op(vm.CALLDATACOPY).DelegateCall(nil, deployed, 0, 32, 0, 32).Op(vm.POP).Return(0, 32).Bytes()
	allocations := []archive.Allocation{{Address: sender, Balance: big.NewInt(10000000000)}, {Address: reverter, Balance: big.NewInt(0), Code: common.FromHex("602a60005560006000fd")}, {Address: loop, Balance: big.NewInt(0), Code: common.FromHex("5b600056")}, {Address: delegate, Balance: big.NewInt(0), Code: delegateCode}}
	w := archive.Workload{Schema: archive.WorkloadSchema}
	nonce := uint64(0)
	add := func(to *common.Address, data []byte, gas, value uint64) []byte {
		tx, e := types.SignTx(types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(1337), Nonce: nonce, Gas: gas, GasFeeCap: big.NewInt(3), GasTipCap: big.NewInt(1), To: to, Value: new(big.Int).SetUint64(value), Data: data}), types.LatestSignerForChainID(big.NewInt(1337)), key)
		must(t, e)
		nonce++
		b, e := tx.MarshalBinary()
		must(t, e)
		return b
	}
	for h := uint64(1); h <= 10; h++ {
		b := archive.InputBlock{Height: h, ID: word(100 + h), Cursor: fmt.Sprint(h), Context: archive.Context{Time: 1000 + h, GasLimit: 10000000, Coinbase: coinbase, BaseFee: big.NewInt(1), BlobBaseFee: big.NewInt(1)}}
		if h > 1 {
			b.ParentID = w.Blocks[len(w.Blocks)-1].ID
		}
		switch h {
		case 1:
			b.Allocations = allocations
		case 2:
			b.Transactions = [][]byte{add(nil, program.New().ReturnViaCodeCopy(setter).Bytes(), 500000, 0)}
		case 3:
			b.Transactions = [][]byte{add(&deployed, word(42).Bytes(), 100000, 0), add(&deployed, word(77).Bytes(), 100000, 0)}
		case 4:
			b.Transactions = [][]byte{add(&reverter, nil, 100000, 0)}
		case 5:
			b.Transactions = [][]byte{add(&deployed, word(0).Bytes(), 100000, 0)}
		case 6:
			b.Transactions = [][]byte{add(&delegate, word(123).Bytes(), 100000, 0)}
		case 7:
			b.Transactions = [][]byte{add(&loop, nil, 30000, 0)}
		case 8:
			b.Transactions = [][]byte{add(&recipient, nil, 21000, 123)}
		case 9:
			b.Transactions = [][]byte{add(&deployed, word(88).Bytes(), 100000, 0)}
		}
		w.Blocks = append(w.Blocks, b)
	}
	return w, []common.Address{sender, deployed, reverter, loop, delegate, coinbase, recipient, common.HexToAddress("0xdead")}, []common.Hash{{}, word(1)}
}

// Independent oracle encoding: read all known fixture keys from map-backed
// runtime state, build raw MPTs directly without our State/Forest encoder.
func reference(t *testing.T, s rt.State, addresses []common.Address, slots []common.Hash) mpt.Hash {
	return referenceWithCodec(t, s, addresses, slots, codec.Legacy)
}

func referenceWithCodec(t *testing.T, s rt.State, addresses []common.Address, slots []common.Hash, mode codec.Mode) mpt.Hash {
	db := mpt.NewMemoryStore()
	accounts, e := mpt.NewEmpty(db)
	must(t, e)
	for _, a := range addresses {
		if !s.Exist(a) {
			continue
		}
		tr, e := mpt.NewEmpty(db)
		must(t, e)
		for _, k := range slots {
			v := s.GetState(a, k)
			if v != (common.Hash{}) {
				raw := v[:]
				if mode == codec.Ethereum {
					var e error
					raw, e = rlp.EncodeToBytes(bytes.TrimLeft(v[:], "\x00"))
					must(t, e)
				}
				must(t, tr.Update(crypto.Keccak256(k[:]), raw))
			}
		}
		root, e := tr.Commit()
		must(t, e)
		b := make([]byte, 105)
		b[0] = 1
		binary.BigEndian.PutUint64(b[1:9], s.GetNonce(a))
		s.GetBalance(a).WriteToSlice(b[9:41])
		copy(b[41:73], root[:])
		h := s.GetCodeHash(a)
		copy(b[73:], h[:])
		if mode == codec.Ethereum {
			b, e = rlp.EncodeToBytes([]interface{}{s.GetNonce(a), s.GetBalance(a).ToBig(), root, h})
			must(t, e)
		}
		must(t, accounts.Update(crypto.Keccak256(a[:]), b))
	}
	root, e := accounts.Commit()
	must(t, e)
	return root
}
func runOracle(t *testing.T, w archive.Workload, addresses []common.Address, slots []common.Hash, chain *params.ChainConfig) ([]mpt.Hash, [][]archive.Outcome) {
	return runOracleWithCodec(t, w, addresses, slots, chain, codec.Legacy)
}

func runOracleWithCodec(t *testing.T, w archive.Workload, addresses []common.Address, slots []common.Hash, chain *params.ChainConfig, mode codec.Mode) ([]mpt.Hash, [][]archive.Outcome) {
	mem, e := rt.NewMemoryState()
	must(t, e)
	roots := []mpt.Hash{mpt.EmptyRootHash}
	var results [][]archive.Outcome
	for _, b := range w.Blocks {
		var out []archive.Outcome
		if b.Height == 1 {
			for _, a := range b.Allocations {
				mem.CreateAccount(a.Address)
				v, _ := uint256.FromBig(a.Balance)
				mem.AddBalance(a.Address, v)
				mem.SetNonce(a.Address, a.Nonce)
				mem.SetCode(a.Address, a.Code)
				for k, v := range a.Storage {
					mem.SetState(a.Address, k, v)
				}
			}
			// Allocations are not a transaction. Start gas accounting from the
			// initialized slots and preserve explicitly allocated empty accounts.
			mem.Finalise(false)
		} else {
			engine, e := rt.New(mem, chain, rt.Block{Number: b.Height, Time: b.Context.Time, GasLimit: b.Context.GasLimit, Coinbase: b.Context.Coinbase, BaseFee: b.Context.BaseFee, BlobBaseFee: b.Context.BlobBaseFee, Random: b.Context.Random, Hash: b.ID, GetHash: func(h uint64) common.Hash {
				if h > 0 && h <= uint64(len(w.Blocks)) {
					return w.Blocks[h-1].ID
				}
				return common.Hash{}
			}})
			must(t, e)
			for _, wire := range b.Transactions {
				var tx types.Transaction
				must(t, tx.UnmarshalBinary(wire))
				r, e := engine.Apply(&tx)
				must(t, e)
				o := archive.Outcome{Receipt: r.Receipt, ReturnData: r.ReturnData}
				if r.VMError != nil {
					o.VMError = r.VMError.Error()
				}
				out = append(out, o)
			}
		}
		roots = append(roots, referenceWithCodec(t, mem, addresses, slots, mode))
		results = append(results, out)
	}
	return roots, results
}
