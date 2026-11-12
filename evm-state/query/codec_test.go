package query_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type memory struct {
	*mpt.MemoryStore
	codes map[common.Hash][]byte
}

func (m memory) Code(h common.Hash) ([]byte, error) { return bytes.Clone(m.codes[h]), nil }

func queryFixture(t *testing.T, mode codec.Mode) *forest.View {
	t.Helper()
	m := memory{mpt.NewMemoryStore(), map[common.Hash][]byte{}}
	f, e := forest.OpenWithCodec(mpt.EmptyRootHash, m, mode)
	if e != nil {
		t.Fatal(e)
	}
	code := []byte{0x60, 0x01, 0x00}
	a := state.EmptyAccount()
	a.Nonce = 4
	a.CodeHash = crypto.Keccak256Hash(code)
	p, e := f.Materialize(map[common.Address]state.Change{
		{19: 1}: {Account: &a, Code: code, CodeLoaded: true, Slots: map[common.Hash]common.Hash{{}: {31: 1}, {31: 1}: {31: 128}}},
	}, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	batch := m.NewBatch()
	for h, raw := range p.Nodes {
		if e = batch.Put(h[:], raw); e != nil {
			t.Fatal(e)
		}
	}
	if e = batch.Write(); e != nil {
		t.Fatal(e)
	}
	for h, raw := range p.Codes {
		m.codes[h] = raw
	}
	f, e = forest.OpenWithCodec(p.Root, m, mode)
	if e != nil {
		t.Fatal(e)
	}
	var visits uint64
	if e = f.Audit(context.Background(), &visits, 10000); e != nil {
		t.Fatal(e)
	}
	return f
}

func response(t *testing.T, f *forest.View, req q.Request) q.Response {
	t.Helper()
	r := q.Response{Request: req, Root: f.Root()}
	var e error
	r.AccountProof, e = f.AccountProof(req.Address)
	if e != nil {
		t.Fatal(e)
	}
	a, exists, e := f.Account(req.Address)
	if e != nil {
		t.Fatal(e)
	}
	if exists {
		r.Account = &a
		switch req.Kind {
		case q.Storage:
			r.Value, e = f.SlotAt(req.Address, a.StorageRoot, req.Slot)
			if e == nil {
				r.StorageProof, e = f.StorageProofAt(req.Address, a.StorageRoot, req.Slot)
			}
			r.SlotExists = r.Value != (common.Hash{})
		case q.Code:
			r.Code, e = f.Code(a.CodeHash)
		}
	}
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func TestLeafCodecsAndCompactResponses(t *testing.T) {
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			f := queryFixture(t, mode)
			for _, addr := range []common.Address{{19: 1}, {19: 2}} {
				for _, kind := range []q.Kind{q.Account, q.Storage, q.Code} {
					for _, slot := range []common.Hash{{}, {31: 1}, {31: 2}} {
						if kind != q.Storage && slot != (common.Hash{}) {
							continue
						}
						req := q.Request{Height: 1, Kind: kind, Address: addr, Slot: slot}
						expanded := response(t, f, req)
						fused, err := q.Read(context.Background(), req, f)
						if err != nil || !reflect.DeepEqual(fused, expanded.Compact()) {
							t.Fatalf("combined response differs for %#v: %v", req, err)
						}
						for _, r := range []q.Response{expanded, fused} {
							v, e := q.VerifyWithCodec(context.Background(), f.Root(), req, r, q.DefaultLimits(), mode)
							if e != nil || (v.Account != nil) != (addr == (common.Address{19: 1})) || v.Value != r.Value {
								t.Fatal("proof/codec mismatch", mode, kind, slot, e)
							}
							if mode == codec.Legacy {
								if _, e = q.Verify(context.Background(), f.Root(), req, r, q.DefaultLimits()); e != nil {
									t.Fatal("legacy verifier changed", e)
								}
							}
						}
					}
				}
			}
		})
	}
}

func TestNativeCompactProofRejectsTamperingAndHonorsLimits(t *testing.T) {
	f := queryFixture(t, codec.Ethereum)
	req := q.Request{Height: 1, Kind: q.Storage, Address: common.Address{19: 1}}
	r := response(t, f, req).Compact()
	for _, mutate := range []func(*q.Response){
		func(v *q.Response) { v.ProofFormat = "unknown" },
		func(v *q.Response) { v.Value[31]++ },
		func(v *q.Response) { v.SlotExists = false },
		func(v *q.Response) { v.Root[0] ^= 1 },
		func(v *q.Response) { v.Request.Height++ },
		func(v *q.Response) { v.AccountProof = append(v.AccountProof, v.AccountProof[0]) },
		func(v *q.Response) { v.StorageProof = nil },
		func(v *q.Response) { v.Code = []byte{0} },
		func(v *q.Response) { a := *v.Account; a.Nonce++; v.Account = &a },
	} {
		copy := r
		copy.AccountProof = append(mpt.Proof(nil), r.AccountProof...)
		mutate(&copy)
		if _, e := q.VerifyWithCodec(context.Background(), f.Root(), req, copy, q.DefaultLimits(), codec.Ethereum); e == nil {
			t.Fatal("tampered response accepted")
		}
	}
	if _, e := q.Verify(context.Background(), f.Root(), req, r, q.DefaultLimits()); e == nil {
		t.Fatal("native leaf accepted as raw105")
	}
	limits := q.DefaultLimits()
	limits.MaxProofBytes = 1
	if _, e := q.VerifyWithCodec(context.Background(), f.Root(), req, r, limits, codec.Ethereum); !errors.Is(e, q.ErrLimit) {
		t.Fatal("proof budget ignored", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := q.VerifyWithCodec(ctx, f.Root(), req, r, q.DefaultLimits(), codec.Ethereum); !errors.Is(e, context.Canceled) {
		t.Fatal("cancellation ignored", e)
	}
	// A malformed RLP node with a matching caller-supplied root is still rejected.
	malformed := []byte{0xc3, 0x20, 0x81, 0x01}
	r.Root = mpt.Hash(crypto.Keccak256Hash(malformed))
	r.AccountProof = mpt.Proof{malformed}
	if _, e := q.VerifyWithCodec(context.Background(), r.Root, req, r, q.DefaultLimits(), codec.Ethereum); e == nil {
		t.Fatal("noncanonical RLP accepted")
	}
}
