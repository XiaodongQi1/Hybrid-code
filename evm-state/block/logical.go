package block

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/holiman/uint256"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

const LogicalSchema = "evm-logical-block/v2"

// AccountFields deliberately excludes StorageRoot. Each destination rebuilds
// its storage trie from the same logical slot changes; the expected block root
// authenticates the final account/storage forest. No physical ID or leaf bytes
// enter this record.
type AccountFields struct {
	Nonce    uint64
	Balance  uint256.Int
	CodeHash common.Hash
}

type LogicalAccount struct {
	Address         common.Address
	Deleted         bool
	Value           *AccountFields              `json:",omitempty"`
	Slots           map[common.Hash]common.Hash `json:",omitempty"`
	Code            []byte                      `json:",omitempty"`
	CodeLoaded      bool
	ResetStorage    bool
	ContractCreated bool
}

// LogicalBlock is an authenticated experiment artifact, not consensus input.
// Its digest must be bound by the workload manifest. ExpectedRoot is checked by
// an importer BEFORE publishing state. Allocation snapshots remain separate.
type LogicalBlock struct {
	Schema       string
	Codec        codec.Mode
	InputSHA256  string
	Height       uint64
	ID, ParentID common.Hash
	Root         mpt.Hash
	Accounts     []LogicalAccount
	Outcomes     []Outcome
}

func inputDigest(input InputBlock) (string, error) {
	raw, e := json.Marshal(input)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}

// NewLogicalBlock captures owned, sorted block-final changes after execution.
// Account values are typed fields, independent of either physical trie layout.
func NewLogicalBlock(input InputBlock, mode codec.Mode, root mpt.Hash, changes map[common.Address]state.Change, outcomes []Outcome) (LogicalBlock, error) {
	digest, e := inputDigest(input)
	if e != nil {
		return LogicalBlock{}, e
	}
	b := LogicalBlock{Schema: LogicalSchema, Codec: mode, InputSHA256: digest, Height: input.Height, ID: input.ID, ParentID: input.ParentID, Root: root}
	for addr, c := range changes {
		p := LogicalAccount{Address: addr, Deleted: c.Account == nil, CodeLoaded: c.CodeLoaded, ResetStorage: c.ResetStorage, ContractCreated: c.ContractCreated, Code: bytes.Clone(c.Code)}
		if c.Account != nil {
			p.Value = &AccountFields{Nonce: c.Account.Nonce, Balance: c.Account.Balance, CodeHash: c.Account.CodeHash}
		}
		if c.Slots != nil {
			p.Slots = make(map[common.Hash]common.Hash, len(c.Slots))
			for k, v := range c.Slots {
				p.Slots[k] = v
			}
		}
		b.Accounts = append(b.Accounts, p)
	}
	sort.Slice(b.Accounts, func(i, j int) bool { return bytes.Compare(b.Accounts[i].Address[:], b.Accounts[j].Address[:]) < 0 })
	// Receipts contain slices, logs and big integers. This also tests the exact
	// artifact serialization instead of retaining pointers into the EVM result.
	raw, e := json.Marshal(outcomes)
	if e == nil {
		e = json.Unmarshal(raw, &b.Outcomes)
	}
	if e != nil {
		return LogicalBlock{}, e
	}
	return b, b.Validate(input, mode)
}

func (b LogicalBlock) validateRows() error {
	if b.Schema != LogicalSchema || b.Root == (mpt.Hash{}) || b.Codec.Validate() != nil {
		return fmt.Errorf("%w: logical trace header", state.ErrCorrupt)
	}
	for i, p := range b.Accounts {
		if i > 0 && bytes.Compare(b.Accounts[i-1].Address[:], p.Address[:]) >= 0 {
			return fmt.Errorf("%w: duplicate/unsorted logical accounts", state.ErrCorrupt)
		}
		if p.Deleted {
			if p.Value != nil || len(p.Slots) != 0 || len(p.Code) != 0 || p.CodeLoaded || p.ResetStorage || p.ContractCreated {
				return fmt.Errorf("%w: deletion payload", state.ErrCorrupt)
			}
			continue
		}
		if p.Value == nil || p.Value.CodeHash == (common.Hash{}) || !p.CodeLoaded && len(p.Code) != 0 {
			return fmt.Errorf("%w: logical account fields", state.ErrCorrupt)
		}
		if p.CodeLoaded && crypto.Keccak256Hash(p.Code) != p.Value.CodeHash {
			return fmt.Errorf("%w: logical bytecode", state.ErrCorrupt)
		}
	}
	return nil
}

// Validate binds even the context, allocation and transaction bytes, rather
// than assuming a caller-supplied experiment ID is an Ethereum header hash.
func (b LogicalBlock) Validate(input InputBlock, mode codec.Mode) error {
	if e := b.validateRows(); e != nil {
		return e
	}
	if b.Codec != mode || b.Height != input.Height || b.ID != input.ID || b.ParentID != input.ParentID || b.ID == (common.Hash{}) || b.ParentID == (common.Hash{}) || len(input.Allocations) != 0 || len(b.Outcomes) != len(input.Transactions) {
		return fmt.Errorf("%w: logical trace/input mismatch", state.ErrCorrupt)
	}
	digest, e := inputDigest(input)
	if e != nil {
		return e
	}
	if digest != b.InputSHA256 {
		return fmt.Errorf("%w: logical trace input digest", state.ErrCorrupt)
	}
	return nil
}

// ChangesFor binds storage bases to one immutable destination view. It returns
// owned data so materialization cannot mutate a trace reused by other methods.
func (b LogicalBlock) ChangesFor(f interface {
	Account(common.Address) (state.Account, bool, error)
}) (map[common.Address]state.Change, error) {
	if e := b.validateRows(); e != nil {
		return nil, e
	}
	out := make(map[common.Address]state.Change, len(b.Accounts))
	for _, p := range b.Accounts {
		if p.Deleted {
			out[p.Address] = state.Change{}
			continue
		}
		old, exists, e := f.Account(p.Address)
		if e != nil {
			return nil, e
		}
		a := state.Account{Nonce: p.Value.Nonce, Balance: p.Value.Balance, StorageRoot: mpt.EmptyRootHash, CodeHash: p.Value.CodeHash}
		if exists && !p.ResetStorage {
			a.StorageRoot = old.StorageRoot
		}
		c := state.Change{Account: &a, Slots: make(map[common.Hash]common.Hash, len(p.Slots)), Code: bytes.Clone(p.Code), CodeLoaded: p.CodeLoaded, ResetStorage: p.ResetStorage, ContractCreated: p.ContractCreated}
		for k, v := range p.Slots {
			c.Slots[k] = v
		}
		out[p.Address] = c
	}
	return out, nil
}

func (b LogicalBlock) Freeze(input InputBlock, mode codec.Mode) (string, []byte, error) {
	if e := b.Validate(input, mode); e != nil {
		return "", nil, e
	}
	raw, e := json.Marshal(b)
	if e != nil {
		return "", nil, e
	}
	h := sha256.Sum256(append([]byte(LogicalSchema+"\x00"), raw...))
	return hex.EncodeToString(h[:]), raw, nil
}

func ParseLogicalBlock(raw []byte, expected string, input InputBlock, mode codec.Mode) (LogicalBlock, error) {
	var b LogicalBlock
	if e := json.Unmarshal(raw, &b); e != nil {
		return b, e
	}
	id, canonical, e := b.Freeze(input, mode)
	if e != nil {
		return b, e
	}
	if id != expected || !bytes.Equal(raw, canonical) {
		return b, fmt.Errorf("%w: logical trace identity/encoding", state.ErrCorrupt)
	}
	return b, nil
}
