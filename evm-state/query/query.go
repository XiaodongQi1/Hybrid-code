// Package query verifies account, storage and code responses against trusted
// historical roots. It has no execution, storage service, HTTP or peer dependencies.
package query

import (
	"bytes"
	"context"
	"errors"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	leafcodec "github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	codec "github.com/XiaodongQi1/Hybrid-code/evm-state/internal/triecodec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

var ErrRequest = errors.New("evm-state query: invalid request or limits")
var ErrBusy = errors.New("evm-state query: request capacity exhausted")
var ErrProof = errors.New("evm-state query: response/proof mismatch")
var ErrLimit = codec.ErrLimit

type Kind string

const (
	Account Kind = "account"
	Storage Kind = "storage"
	Code    Kind = "code"
)

type Request struct {
	Height  uint64
	Kind    Kind
	Address common.Address
	Slot    common.Hash
}

func (q Request) validate() error {
	if q.Kind != Account && q.Kind != Storage && q.Kind != Code {
		return ErrRequest
	}
	if q.Kind != Storage && q.Slot != (common.Hash{}) {
		return ErrRequest
	}
	return nil
}

type Limits struct {
	MaxConcurrent, MaxQueued                                                              uint64
	MaxProofNodes, MaxProofBytes, MaxCodeBytes, MaxInputBytes, MaxReturnBytes, MaxCallGas uint64
	Reads                                                                                 ReadBudget
}

func DefaultLimits() Limits {
	return Limits{MaxConcurrent: 8, MaxQueued: 32, MaxProofNodes: 256, MaxProofBytes: 1 << 20, MaxCodeBytes: 1 << 20, MaxInputBytes: 128 << 10, MaxReturnBytes: 1 << 20, MaxCallGas: 30000000, Reads: ReadBudget{MaxReads: 100000, MaxBytes: 64 << 20}}
}
func (l Limits) validate() error {
	if l.MaxConcurrent == 0 || l.MaxConcurrent > 1024 || l.MaxQueued > 65536 || l.MaxProofNodes == 0 || l.MaxProofNodes > 4096 || l.MaxProofBytes == 0 || l.MaxProofBytes > 64<<20 || l.MaxCodeBytes == 0 || l.MaxCodeBytes > 64<<20 || l.MaxInputBytes == 0 || l.MaxInputBytes > 16<<20 || l.MaxReturnBytes == 0 || l.MaxReturnBytes > 64<<20 || l.MaxCallGas == 0 || l.MaxCallGas > 1000000000 || l.Reads.MaxReads == 0 || l.Reads.MaxReads > 10000000 || l.Reads.MaxBytes == 0 || l.Reads.MaxBytes > 1<<30 {
		return ErrRequest
	}
	return nil
}

type ProofFormat string

const (
	// ExpandedProof preserves existing response JSON and legacy path proofs.
	ExpandedProof ProofFormat = ""
	CompactProof  ProofFormat = "ethereum-compact/v1"
)

type Response struct {
	ProofFormat                ProofFormat `json:",omitempty"`
	Request                    Request
	Root                       mpt.Hash
	Account                    *state.Account // nil means authenticated absence
	SlotExists                 bool
	Value                      common.Hash
	Code                       []byte
	AccountProof, StorageProof mpt.Proof
}

// Compact converts locally generated proofs without changing logical fields.
// VerifyWithCodec remains responsible for authenticating received responses.
func (r Response) Compact() Response {
	r.ProofFormat = CompactProof
	r.AccountProof = mpt.CompactProof(r.AccountProof)
	r.StorageProof = mpt.CompactProof(r.StorageProof)
	return r
}

// Verified contains only proof-derived or hash-verified logical data. The
// caller supplies the trusted root for the requested height independently.
type Verified struct {
	Account    *state.Account
	SlotExists bool
	Value      common.Hash
	Code       []byte
}

func Verify(ctx context.Context, trustedRoot mpt.Hash, q Request, r Response, l Limits) (Verified, error) {
	return VerifyWithCodec(ctx, trustedRoot, q, r, l, leafcodec.Legacy)
}

// VerifyWithCodec authenticates logical results under an explicitly selected
// leaf format. The caller binds that format and the trusted root independently
// of the response. Physical trie IDs, source hints and cache state are irrelevant.
func VerifyWithCodec(ctx context.Context, trustedRoot mpt.Hash, q Request, r Response, l Limits, mode leafcodec.Mode) (Verified, error) {
	fail := func(e error) (Verified, error) { return Verified{}, e }
	if e := mode.Validate(); e != nil {
		return fail(e)
	}
	if r.ProofFormat != ExpandedProof && r.ProofFormat != CompactProof {
		return fail(ErrProof)
	}
	if e := l.validate(); e != nil {
		return fail(e)
	}
	if e := q.validate(); e != nil {
		return fail(e)
	}
	if e := ctx.Err(); e != nil {
		return fail(e)
	}
	if r.Request != q || r.Root != trustedRoot {
		return fail(ErrProof)
	}
	if uint64(len(r.AccountProof))+uint64(len(r.StorageProof)) > l.MaxProofNodes || uint64(len(r.Code)) > l.MaxCodeBytes {
		return fail(ErrLimit)
	}
	var n uint64
	for _, p := range []mpt.Proof{r.AccountProof, r.StorageProof} {
		for _, b := range p {
			if uint64(len(b)) > l.MaxProofBytes-n {
				return fail(ErrLimit)
			}
			n += uint64(len(b))
		}
	}
	raw, exists, e := verifyProof(ctx, trustedRoot, state.AccountKey(q.Address), r.AccountProof, r.ProofFormat)
	if e != nil {
		return fail(e)
	}
	v := Verified{}
	if exists {
		a, e := mode.DecodeAccount(raw)
		if e != nil {
			return fail(e)
		}
		if r.Account == nil || a != *r.Account {
			return fail(ErrProof)
		}
		v.Account = &a
	} else if r.Account != nil {
		return fail(ErrProof)
	}
	switch q.Kind {
	case Storage:
		if len(r.Code) != 0 {
			return fail(ErrProof)
		}
		if exists {
			b, found, e := verifyProof(ctx, v.Account.StorageRoot, state.SlotKey(q.Slot), r.StorageProof, r.ProofFormat)
			if e != nil {
				return fail(e)
			}
			if found {
				word, e := mode.DecodeStorage(b)
				if e != nil {
					return fail(e)
				}
				if word == (common.Hash{}) {
					return fail(state.ErrCorrupt)
				}
				v.SlotExists = true
				v.Value = word
			}
		} else if len(r.StorageProof) != 0 {
			return fail(ErrProof)
		}
		if r.SlotExists != v.SlotExists || r.Value != v.Value {
			return fail(ErrProof)
		}
	case Code:
		if len(r.StorageProof) != 0 || r.SlotExists || r.Value != (common.Hash{}) {
			return fail(ErrProof)
		}
		if exists {
			if crypto.Keccak256Hash(r.Code) != v.Account.CodeHash {
				return fail(ErrProof)
			}
			v.Code = bytes.Clone(r.Code)
		} else if len(r.Code) != 0 {
			return fail(ErrProof)
		}
	case Account:
		if len(r.Code) != 0 || len(r.StorageProof) != 0 || r.SlotExists || r.Value != (common.Hash{}) {
			return fail(ErrProof)
		}
	}
	if e = ctx.Err(); e != nil {
		return fail(e)
	}
	return v, nil
}

type proofStore map[mpt.Hash][]byte

func (p proofStore) Get(k []byte) ([]byte, error) {
	var h mpt.Hash
	if len(k) != 32 {
		return nil, ErrProof
	}
	copy(h[:], k)
	b, ok := p[h]
	if !ok {
		return nil, ErrProof
	}
	return bytes.Clone(b), nil
}

type noBatch struct{}

func (noBatch) Put([]byte, []byte) error { return ErrReadOnly }
func (noBatch) Write() error             { return ErrReadOnly }
func (p proofStore) NewBatch() mpt.Batch { return noBatch{} }
func verifyProof(ctx context.Context, root mpt.Hash, key []byte, p mpt.Proof, format ProofFormat) ([]byte, bool, error) {
	if format == CompactProof {
		for _, b := range p {
			if e := ctx.Err(); e != nil {
				return nil, false, e
			}
			if e := codec.ValidateNode(b); e != nil {
				return nil, false, e
			}
		}
		value, exists, e := mpt.VerifyCompactProof(root, key, p)
		return value, exists, e
	}
	nodes := proofStore{}
	for _, b := range p {
		if e := ctx.Err(); e != nil {
			return nil, false, e
		}
		if e := codec.ValidateNode(b); e != nil {
			return nil, false, e
		}
		h := codec.NodeHash(b)
		if _, ok := nodes[h]; ok {
			return nil, false, ErrProof
		}
		nodes[h] = b
	}
	value, exists, e := mpt.VerifyProof(root, key, p)
	if e != nil {
		return nil, false, e
	}
	// Require precisely the canonical path, including generated inline nodes.
	// This rejects surplus/duplicate/reordered nodes and empty-root junk.
	t, e := mpt.Open(root, nodes)
	if e != nil {
		return nil, false, e
	}
	canonical, e := t.Prove(key)
	if e != nil {
		return nil, false, e
	}
	if len(canonical) != len(p) {
		return nil, false, ErrProof
	}
	for i := range p {
		if !bytes.Equal(canonical[i], p[i]) {
			return nil, false, ErrProof
		}
	}
	return value, exists, ctx.Err()
}

type ReadBudget struct{ MaxReads, MaxBytes uint64 }

var ErrReadOnly = errors.New("read-only proof store")
