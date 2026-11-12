// Package state implements an independent raw EVM StateDB. No Statera history
// envelopes, replay metadata or shared-worker state is used here.
package state

import (
	"encoding/binary"
	"errors"
	"github.com/holiman/uint256"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

const Format = "evm-raw105/v1"

var ErrCorrupt = errors.New("evm-state: corrupt state")
var EmptyCode = crypto.Keccak256Hash(nil)

type Account struct {
	Nonce       uint64
	Balance     uint256.Int
	StorageRoot mpt.Hash
	CodeHash    common.Hash
}

func Encode(a Account) []byte {
	b := make([]byte, 105)
	b[0] = 1
	binary.BigEndian.PutUint64(b[1:9], a.Nonce)
	a.Balance.WriteToSlice(b[9:41])
	copy(b[41:73], a.StorageRoot[:])
	copy(b[73:], a.CodeHash[:])
	return b
}
func Decode(b []byte) (a Account, err error) {
	if len(b) != 105 || b[0] != 1 {
		return a, ErrCorrupt
	}
	a.Nonce = binary.BigEndian.Uint64(b[1:9])
	a.Balance.SetBytes(b[9:41])
	copy(a.StorageRoot[:], b[41:73])
	copy(a.CodeHash[:], b[73:])
	return
}
func AccountKey(a common.Address) []byte { return crypto.Keccak256(a[:]) }
func SlotKey(k common.Hash) []byte       { return crypto.Keccak256(k[:]) }
func EmptyAccount() Account              { return Account{StorageRoot: mpt.EmptyRootHash, CodeHash: EmptyCode} }

// Reader is immutable for the lifetime of one State. Absence is distinct from
// a failed read. HasStorage applies the supplied private overlay to root.
type Reader interface {
	Account(common.Address) (Account, bool, error)
	Code(common.Hash) ([]byte, error)
	Slot(mpt.Hash, common.Hash) (common.Hash, error)
	HasStorage(mpt.Hash, map[common.Hash]common.Hash) (bool, error)
}
type Change struct {
	Account    *Account
	Slots      map[common.Hash]common.Hash
	Code       []byte
	CodeLoaded bool
	// ResetStorage survives transaction Finalise: a same-address recreation
	// starts a new physical storage-trie lifecycle even if both roots are empty.
	ResetStorage bool
	// ContractCreated records a surviving CREATE/CREATE2 even when its runtime
	// code and storage are empty. It is block-local and not an account field.
	ContractCreated bool
}

// OwnedReader supplies contract identity to path-addressed storage backends.
type OwnedReader interface {
	SlotAt(common.Address, mpt.Hash, common.Hash) (common.Hash, error)
	HasStorageAt(common.Address, mpt.Hash, map[common.Hash]common.Hash) (bool, error)
}
