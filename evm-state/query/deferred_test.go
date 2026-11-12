package query

import (
	"context"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

type readyProofReader struct {
	Reader
	response Response
}

func (r readyProofReader) ReadWithProof(context.Context, Request) (Response, error) {
	return r.response, nil
}

func TestDeferredProofResponseTransfersCompactBytes(t *testing.T) {
	called := 0
	raw := []byte{0xc0}
	proof := mpt.Proof{raw}
	r, e := ReadTrieDeferred(context.Background(), Request{Kind: Account}, mpt.EmptyRootHash, codec.Ethereum,
		func(*common.Address, mpt.Hash, []byte) (mpt.ProofResult, error) {
			called++
			return mpt.ProofResult{}, nil
		},
		func(common.Hash) ([]byte, error) { t.Fatal("unexpected code lookup"); return nil, nil },
		func() (mpt.Proof, mpt.Proof, error) {
			if called != 1 {
				t.Fatal("completion before lookup")
			}
			return proof, mpt.Proof{}, nil
		})
	if e != nil || r.ProofFormat != CompactProof || &r.AccountProof[0][0] != &raw[0] {
		t.Fatal("deferred result copied", e)
	}
	r, e = Read(context.Background(), Request{Kind: Account}, readyProofReader{response: r})
	if e != nil || &r.AccountProof[0][0] != &raw[0] {
		t.Fatal("outer Read recopied a compact proof", e)
	}
	c := r.Compact()
	if &c.AccountProof[0][0] == &raw[0] {
		t.Fatal("public Compact lost copy semantics")
	}
}
