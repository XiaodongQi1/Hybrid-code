package block

import (
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// EvaluatedBlock is trusted experiment input, not a consensus validation API.
// The caller must authenticate the artifact and verify resulting canonical roots.
// It deliberately has no dependency on the Statera integration implementation.
type EvaluatedBlock struct {
	Accounts []EvaluatedAccount
	Outcomes []Outcome
}
type EvaluatedAccount struct {
	Address                           common.Address
	Deleted, ResetStorage, CodeLoaded bool
	Value                             []byte
	Slots                             map[common.Hash]common.Hash
	Code                              []byte
	ContractCreated                   bool `json:",omitempty"`
}

// ChangesFor binds trusted final values to this backend's current storage roots.
func (b *EvaluatedBlock) ChangesFor(f interface {
	Account(common.Address) (state.Account, bool, error)
}) (map[common.Address]state.Change, error) {
	out := make(map[common.Address]state.Change, len(b.Accounts))
	for _, p := range b.Accounts {
		if _, ok := out[p.Address]; ok {
			return nil, state.ErrCorrupt
		}
		if p.Deleted {
			out[p.Address] = state.Change{}
			continue
		}
		v, e := state.Decode(p.Value)
		if e != nil {
			return nil, e
		}
		old, exists, e := f.Account(p.Address)
		if e != nil {
			return nil, e
		}
		v.StorageRoot = mpt.EmptyRootHash
		if exists && !p.ResetStorage {
			v.StorageRoot = old.StorageRoot
		}
		out[p.Address] = state.Change{Account: &v, Slots: p.Slots, Code: p.Code, CodeLoaded: p.CodeLoaded, ResetStorage: p.ResetStorage, ContractCreated: p.ContractCreated}
	}
	return out, nil
}
