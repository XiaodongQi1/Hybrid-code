package mpt

import (
	"bytes"
	"testing"
)

var materializeSink MaterializedNode

func TestFusedMaterializationLocatorsIsolationAndAllocations(t *testing.T) {
	d := CanonicalNode{Kind: CanonicalBranch}
	var locations [16]NodeLocation
	for i := range d.Children {
		d.Children[i] = CanonicalReference{Present: true, Hash: Hash{byte(i + 1)}}
		locations[i] = NodeLocation{Kind: 2, Exact: true, Height: uint64(i)}
	}
	raw, hash, err := RebuildCanonicalNode(d)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, NodeImageMaxBytes(uint32(len(raw))))
	image, err := CaptureLocalFieldsImage(buf, d, hash, uint32(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	old := testing.AllocsPerRun(50, func() {
		n, e := image.Materialize()
		if e != nil {
			t.Fatal(e)
		}
		materializeSink = n.WithChildLocations(locations)
	})
	fused := testing.AllocsPerRun(50, func() {
		n, e := image.MaterializeWithChildLocations(locations)
		if e != nil {
			t.Fatal(e)
		}
		materializeSink = n
	})
	if fused >= old {
		t.Fatalf("lost fused allocation advantage: %.0f >= %.0f", fused, old)
	}
	t.Logf("allocations per branch: two-pass=%.0f fused=%.0f", old, fused)
	n := materializeSink
	b := n.root.(*branchNode)
	for i, ch := range b.children {
		if ch.(*locatedHashNode).location != locations[i] {
			t.Fatal("wrong locator")
		}
	}
	for i := range buf {
		buf[i] = 0
	}
	encoded, h, e := encodeNode(n.root, nil)
	if e != nil || h != hash || !bytes.Equal(encoded, raw) {
		t.Fatal("arena reuse or locator changed canonical node", e)
	}
}

type fusedLookupStore struct {
	Store
	raw             []byte
	expected        Hash
	calls, resolves int
	bad             bool
}

func (s *fusedLookupStore) LookupLocalValue(_ []byte, h Hash, _ []byte, _ *NodeLocation, _ TrieAccessObserver) ([]byte, MaterializedNode, bool, error) {
	s.calls++
	n, e := DecodeTrustedLocalNode(s.raw, h)
	if s.bad {
		n.hash = Hash{99}
	}
	return nil, n, false, e
}
func (s *fusedLookupStore) GetMaterializedNode(_ []byte, _ Hash) (MaterializedNode, error) {
	s.resolves++
	return MaterializedNode{}, ErrInvalidPath
}
func TestLocalLookupDoesNotResolveAgainAndChecksDigest(t *testing.T) {
	d := CanonicalNode{Kind: CanonicalLeaf, Value: []byte("value")}
	d.Path, _ = EncodeCompactPath([]byte{1, 2}, true)
	raw, h, _ := RebuildCanonicalNode(d)
	s := &fusedLookupStore{Store: NewMemoryStore(), raw: raw, expected: h}
	tr := &Trie{root: hashNode(h), store: s}
	v, e := tr.Get([]byte{0x12})
	if e != nil || string(v) != "value" || s.calls != 1 || s.resolves != 0 {
		t.Fatal("duplicate resolve", s.calls, s.resolves, e)
	}
	s.bad = true
	if _, e = tr.Get([]byte{0x12}); e == nil {
		t.Fatal("accepted wrong digest")
	}
}

func TestInspectWithHashPreservesStrictCanonicalValidation(t *testing.T) {
	d := CanonicalNode{Kind: CanonicalLeaf, Value: []byte("v")}
	d.Path, _ = EncodeCompactPath([]byte{1}, true)
	raw, h, e := RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	got, digest, e := InspectCanonicalNodeWithHash(raw)
	if e != nil || digest != h || !bytes.Equal(got.Value, d.Value) {
		t.Fatal(digest, h, e)
	}
	for _, bad := range [][]byte{{0xc3, 0x81, 0x31, 0x76}, append(bytes.Clone(raw), 0), {0xf8, 0x01, 0x80}} {
		if _, _, e = InspectCanonicalNodeWithHash(bad); e == nil {
			t.Fatalf("accepted noncanonical node %x", bad)
		}
	}
}
