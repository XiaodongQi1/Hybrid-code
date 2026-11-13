package hybridtrie

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func randomBranch(rng *rand.Rand) Record {
	r := Record{Kind: mpt.CanonicalBranch}
	rng.Read(r.Hash[:])
	for i := range r.Children {
		switch rng.Intn(5) {
		case 0:
			r.Children[i] = Reference{Kind: Prefix}
		case 1:
			r.Children[i].Kind = Hash
			rng.Read(r.Children[i].Hash[:])
		case 2:
			// A valid short node: leaf with a one-nibble key and a one-byte value.
			r.Children[i] = Reference{Kind: Embed, Inline: []byte{0xc2, 0x20 + byte(i%2), 0x01}}
		}
	}
	return r
}

func TestDeltaEncodeDecodeApplyRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for n := 0; n < 500; n++ {
		base := randomBranch(rng)
		cur := base
		rng.Read(cur.Hash[:])
		for i := range cur.Children {
			if rng.Intn(4) == 0 {
				cur.Children[i] = randomBranch(rng).Children[i]
			}
		}
		if base.validate() != nil || cur.validate() != nil {
			continue
		}
		hashBase := rng.Intn(2) == 0
		var digest mpt.Hash
		rng.Read(digest[:])
		depth := uint8(1 + rng.Intn(5))
		if hashBase {
			depth = 1
		}
		raw, ok, err := encodeDelta(base, cur, hashBase, digest, depth)
		if err != nil || !ok {
			t.Fatal("encode", ok, err)
		}
		if !isDelta(raw) || storedDepth(raw) != depth {
			t.Fatal("flags", raw[0], storedDepth(raw))
		}
		if _, err = DecodeRecord(raw); err != errNeedsBase {
			t.Fatal("plain decode of a delta must need a base", err)
		}
		if _, err = recordKind(raw[0]); err != nil {
			t.Fatal(err)
		}
		d, err := decodeDelta(raw)
		if err != nil || d.Depth != depth || d.HashBase != hashBase || (hashBase && d.Base != digest) || d.Hash != cur.Hash {
			t.Fatal("decode", d, err)
		}
		got, err := applyDelta(base, d)
		if err != nil || got.Hash != cur.Hash {
			t.Fatal("apply", err)
		}
		for i := range cur.Children {
			if !sameReference(got.Children[i], cur.Children[i]) {
				t.Fatal("slot", i)
			}
		}
		// Truncation and trailing bytes are corrupt.
		for cut := 1; cut < len(raw); cut++ {
			if _, e := decodeDelta(raw[:cut]); e == nil && cut < len(raw) {
				// A prefix can only be valid if the remaining bytes were an empty tail.
				if !bytes.Equal(raw[cut:], nil) {
					t.Fatal("truncated delta accepted at", cut)
				}
			}
		}
		if _, e := decodeDelta(append(append([]byte(nil), raw...), 0)); e == nil {
			t.Fatal("trailing byte accepted")
		}
	}
}

func TestDeltaNotApplicable(t *testing.T) {
	a := randomBranch(rand.New(rand.NewSource(1)))
	leaf := Record{Kind: mpt.CanonicalLeaf, Fragment: []byte{1}, Value: []byte{1}}
	if _, ok, _ := encodeDelta(a, leaf, false, mpt.Hash{}, 1); ok {
		t.Fatal("kind change encoded as delta")
	}
	b := a
	b.Value = []byte{9}
	if _, ok, _ := encodeDelta(a, b, false, mpt.Hash{}, 1); ok {
		t.Fatal("value change encoded as delta")
	}
	if _, ok, _ := encodeDelta(a, a, false, mpt.Hash{}, 0); ok {
		t.Fatal("depth zero accepted")
	}
	if _, ok, _ := encodeDelta(a, a, true, mpt.Hash{}, MaxDeltaInterval+1); ok {
		t.Fatal("depth above the maximum accepted")
	}
}
