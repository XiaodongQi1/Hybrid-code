package block

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
)

// Legacy wire tag retained to preserve existing frozen workload identities.
// This tag does not select a storage backend.
const WorkloadSchema = "evm-delta-workload/v1"

type Context struct {
	Time, GasLimit       uint64
	Coinbase             common.Address
	BaseFee, BlobBaseFee *big.Int
	Random               common.Hash
}
type Allocation struct {
	Address common.Address
	Balance *big.Int
	Nonce   uint64
	Code    []byte
	Storage map[common.Hash]common.Hash
}

// ID is the workload's frozen execution block ID (independent of state roots).
// Input digests bind it, so changing ID also changes the workload identity.
type InputBlock struct {
	Height       uint64
	ID, ParentID common.Hash
	Cursor       string
	Context      Context
	Allocations  []Allocation
	Transactions [][]byte
}
type Workload struct {
	ZeroGenesis bool `json:",omitempty"`
	Schema      string
	Blocks      []InputBlock
}

func (w Workload) Freeze() (string, []byte, error) {
	if w.Schema != WorkloadSchema || len(w.Blocks) == 0 {
		return "", nil, errors.New("invalid workload")
	}
	var prev common.Hash
	seen := map[common.Hash]bool{}
	for i, b := range w.Blocks {
		if b.Height != uint64(i)+w.InitialHeight() || b.ParentID != prev || seen[b.ID] {
			return "", nil, errors.New("noncontinuous workload")
		}
		if e := b.validate(); e != nil {
			return "", nil, e
		}
		seen[b.ID] = true
		prev = b.ID
	}
	wire, e := json.Marshal(w)
	if e != nil {
		return "", nil, e
	}
	sum := sha256.Sum256(append([]byte(WorkloadSchema+"\x00"), wire...))
	return hex.EncodeToString(sum[:]), wire, nil
}
func ParseWorkload(wire []byte, expected string) (Workload, error) {
	var w Workload
	if e := json.Unmarshal(wire, &w); e != nil {
		return w, e
	}
	id, canonical, e := w.Freeze()
	if e != nil {
		return w, e
	}
	if id != expected || !bytes.Equal(canonical, wire) {
		return w, errors.New("workload identity/encoding mismatch")
	}
	return w, nil
}
func (b InputBlock) validate() error {
	if b.ID == (common.Hash{}) || b.Context.GasLimit == 0 || b.Context.BaseFee == nil || b.Context.BaseFee.Sign() < 0 || b.Context.BaseFee.BitLen() > 256 || b.Context.BlobBaseFee == nil || b.Context.BlobBaseFee.Sign() < 0 || b.Context.BlobBaseFee.BitLen() > 256 {
		return errors.New("invalid block context")
	}
	if b.Height == 0 || b.Height == 1 && b.ParentID == (common.Hash{}) {
		if b.ParentID != (common.Hash{}) || len(b.Transactions) > 0 {
			return errors.New("initialization is allocation-only")
		}
	} else if len(b.Allocations) > 0 {
		return errors.New("allocations only in the initialization block")
	}
	addresses := map[common.Address]bool{}
	for _, a := range b.Allocations {
		if addresses[a.Address] || a.Balance == nil || a.Balance.Sign() < 0 || a.Balance.BitLen() > 256 {
			return errors.New("invalid allocation")
		}
		addresses[a.Address] = true
	}
	for _, wire := range b.Transactions {
		if _, e := rt.DecodeExecutionTransaction(wire); e != nil {
			return e
		}
	}
	return nil
}
func (w Workload) InitialHeight() uint64 {
	if w.ZeroGenesis {
		return 0
	}
	return 1
}
