// Package codec defines explicit leaf formats without changing legacy defaults.
package codec

import (
	"bytes"
	"fmt"
	"math/big"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/rlp"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type Mode string

const Legacy Mode = "legacy-raw105"
const Ethereum Mode = "ethereum-mpt"

func (m Mode) Validate() error {
	if m != Legacy && m != Ethereum {
		return fmt.Errorf("unknown codec %q", m)
	}
	return nil
}

type accountRLP struct {
	Nonce   uint64
	Balance *big.Int
	Root    mpt.Hash
	Code    common.Hash
}

func (m Mode) EncodeAccount(a state.Account) ([]byte, error) {
	if e := m.Validate(); e != nil {
		return nil, e
	}
	if m == Legacy {
		return state.Encode(a), nil
	}
	return rlp.EncodeToBytes(accountRLP{a.Nonce, a.Balance.ToBig(), a.StorageRoot, a.CodeHash})
}
func (m Mode) DecodeAccount(b []byte) (state.Account, error) {
	var a state.Account
	if e := m.Validate(); e != nil {
		return a, e
	}
	if m == Legacy {
		return state.Decode(b)
	}
	var v accountRLP
	if e := rlp.DecodeBytes(b, &v); e != nil {
		return a, e
	}
	if v.Balance == nil || v.Balance.Sign() < 0 || v.Balance.BitLen() > 256 {
		return a, state.ErrCorrupt
	}
	a.Nonce = v.Nonce
	a.Balance.SetFromBig(v.Balance)
	a.StorageRoot = v.Root
	a.CodeHash = v.Code
	return a, nil
}
func (m Mode) EncodeStorage(v common.Hash) ([]byte, error) {
	if e := m.Validate(); e != nil {
		return nil, e
	}
	if v == (common.Hash{}) {
		return nil, nil
	}
	if m == Legacy {
		return bytes.Clone(v[:]), nil
	}
	return rlp.EncodeToBytes(bytes.TrimLeft(v[:], "\x00"))
}
func (m Mode) DecodeStorage(b []byte) (common.Hash, error) {
	if e := m.Validate(); e != nil {
		return common.Hash{}, e
	}
	if len(b) == 0 {
		return common.Hash{}, nil
	}
	if m == Legacy {
		if len(b) != 32 || common.BytesToHash(b) == (common.Hash{}) {
			return common.Hash{}, state.ErrCorrupt
		}
		return common.BytesToHash(b), nil
	}
	var v []byte
	if e := rlp.DecodeBytes(b, &v); e != nil {
		return common.Hash{}, e
	}
	if len(v) == 0 || len(v) > 32 || v[0] == 0 {
		return common.Hash{}, state.ErrCorrupt
	}
	return common.BytesToHash(v), nil
}
