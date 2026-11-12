package codec

import (
	"bytes"
	"math/big"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/rlp"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"testing"
)

func TestFormatsAndStorageBoundaries(t *testing.T) {
	for _, mode := range []Mode{Legacy, Ethereum} {
		a := state.EmptyAccount()
		a.Nonce = 128
		a.Balance.SetUint64(256)
		b, e := mode.EncodeAccount(a)
		if e != nil {
			t.Fatal(e)
		}
		got, e := mode.DecodeAccount(b)
		if e != nil || got != a {
			t.Fatalf("account %s: %v", mode, e)
		}
		if mode == Legacy && !bytes.Equal(b, state.Encode(a)) {
			t.Fatal("legacy bytes changed")
		}
		for _, n := range []uint64{0, 1, 127, 128, 255, 256} {
			word := common.BigToHash(new(big.Int).SetUint64(n))
			raw, e := mode.EncodeStorage(word)
			if e != nil {
				t.Fatal(e)
			}
			back, e := mode.DecodeStorage(raw)
			if e != nil || back != word {
				t.Fatalf("word %s %d: %v", mode, n, e)
			}
		}
		max := common.Hash{}
		for i := range max {
			max[i] = 255
		}
		raw, _ := mode.EncodeStorage(max)
		back, e := mode.DecodeStorage(raw)
		if e != nil || back != max {
			t.Fatal("uint256 maximum")
		}
	}
	one := common.Hash{31: 1}
	b, _ := Ethereum.EncodeStorage(one)
	if !bytes.Equal(b, []byte{1}) {
		t.Fatal("native integer encoding")
	}
	for _, raw := range [][]byte{{0}, {0x80}, {0x81, 1}, {0xc0}, append([]byte{0xa1}, make([]byte, 33)...)} {
		if _, e := Ethereum.DecodeStorage(raw); e == nil {
			t.Fatalf("accepted malformed/zero stored leaf %x", raw)
		}
	}
	for _, raw := range [][]byte{nil, {0xc0}, {0xf8, 0x00}} {
		if _, e := Ethereum.DecodeAccount(raw); e == nil {
			t.Fatal("accepted malformed account")
		}
	}
	huge := make([]byte, 33)
	huge[0] = 1
	raw, _ := rlp.EncodeToBytes([]interface{}{uint64(0), huge, state.EmptyAccount().StorageRoot, state.EmptyCode})
	if _, e := Ethereum.DecodeAccount(raw); e == nil {
		t.Fatal("overflow balance")
	}
	if _, e := Mode("unknown").EncodeStorage(one); e == nil {
		t.Fatal("unknown format")
	}
}
