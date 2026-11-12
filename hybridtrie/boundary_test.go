package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
	"unsafe"
)

func TestBoundaryAllocationAndConfiguration(t *testing.T) {
	c := DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
	f, e := newBoundaryForest(c)
	if e != nil {
		t.Fatal(e)
	}
	s := f.statsSnapshot()
	a := uint64(len(f.current.Nodes))*uint64(unsafe.Sizeof(boundaryNode{})) + uint64(len(f.current.Payload))
	p := uint64(cap(f.current.changed))*4 + uint64(len(f.current.marked))*8
	p += uint64(cap(f.selection.slots))*uint64(unsafe.Sizeof(selectionSlot{})) + 4*uint64(cap(f.selection.pools[0])+cap(f.selection.pools[1]))
	if a != s.NodeAllocatedBytes || p != s.PlanAllocatedBytes || s.HashAllocatedBytes != 0 {
		t.Fatal("allocation accounting", s, a, p)
	}
	for _, edit := range []func(*BoundaryConfig){func(c *BoundaryConfig) { c.PolicyIntervalBlocks = 0 }, func(c *BoundaryConfig) { c.MaxNodeBytes = 1024 }, func(c *BoundaryConfig) { c.PlanBytes = 1 }, func(c *BoundaryConfig) { c.MetadataBytes = 1 }, func(c *BoundaryConfig) { c.NodeBudget.TotalBytes = 1 }} {
		bad := c
		edit(&bad)
		if _, e = newBoundaryForest(bad); e == nil {
			t.Fatal("bad configuration")
		}
	}
	if !f.policyDue(16) || f.policyDue(0) || f.policyDue(15) {
		t.Fatal("policy phase")
	}
	f.cfg.PolicyOriginHeight = 100
	if !f.policyDue(116) || f.policyDue(16) {
		t.Fatal("origin")
	}
}
func boundaryTestNode(t *testing.T, p []byte, value byte) (boundaryObservation, []byte) {
	t.Helper()
	d := mpt.CanonicalNode{Kind: mpt.CanonicalLeaf, Value: bytes.Repeat([]byte{value}, 64)}
	d.Path, _ = mpt.EncodeCompactPath([]byte{0}, true)
	raw, h, e := mpt.RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	return boundaryObservation{Pos: position(0, p), boundaryNode: boundaryNode{Hash: h, Version: forestVersion{1, Prefix, true}, Used: true}}, raw
}
func TestBoundaryHeatTieUsesChildrenFirst(t *testing.T) {
	a, _ := boundaryTestNode(t, []byte{1}, 1)
	b, _ := boundaryTestNode(t, []byte{1, 2}, 2)
	a.Heat = residentHeat{Value: 10}
	b.Heat = residentHeat{Value: 10}
	if !boundaryLess(b, a, 0, 64) || boundaryLess(a, b, 0, 64) {
		t.Fatal("equal heat did not prefer child")
	}
	a.Heat = residentHeat{Value: 11}
	if !boundaryLess(b, a, 0, 64) {
		t.Fatal("heat order")
	}
}

func TestBoundaryPolicyOnlyCurrentUpdatedFrontier(t *testing.T) {
	for _, tc := range []struct {
		name    string
		h       uint64
		updated bool
		want    uint64
	}{{"adjusted", 4, true, 1}, {"ordinary", 3, true, 0}, {"previous-block-only", 4, false, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			c := DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
			plan, err := BoundaryCacheAllocation(c)
			if err != nil {
				t.Fatal(err)
			}
			slotBytes := plan.Boundary.SlotBytes
			// Numerous free slots are reserved for storage. An account must
			// replace a sibling with a budget far below a full victim scan.
			c.NodeBudget = ClassBudget{4096 * slotBytes, 0, 4094 * slotBytes}
			c.PlanBytes = 8 << 20
			c.WorkUnits = 4200
			c.PolicyIntervalBlocks = 4
			f, e := newBoundaryForest(c)
			if e != nil {
				t.Fatal(e)
			}
			if e = f.begin(tc.h); e != nil {
				t.Fatal(e)
			}
			old, rawOld := boundaryTestNode(t, []byte{1}, 1)
			fresh, rawFresh := boundaryTestNode(t, []byte{2}, 2)
			old.Heat = residentHeat{Value: 1, Height: tc.h}
			old.Updated = tc.updated
			fresh.Updated = true
			fresh.Version.Height = tc.h
			d := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
			d.Children[1] = mpt.CanonicalReference{Present: true, Hash: old.Hash}
			d.Children[2] = mpt.CanonicalReference{Present: true, Hash: fresh.Hash}
			rawRoot, hash, e := mpt.RebuildCanonicalNode(d)
			if e != nil {
				t.Fatal(e)
			}
			root := boundaryObservation{Pos: position(0, nil), boundaryNode: boundaryNode{Hash: hash, Heat: residentHeat{Value: 10, Height: tc.h}, Version: forestVersion{1, Prefix, true}, Used: true}}
			root.Edges[1] = edgeAt(1, boundaryPrefix)
			root.Edges[2] = edgeAt(tc.h, boundaryPrefix)
			f.current.put(0, root, rawRoot)
			f.current.put(1, old, rawOld)
			f.rootFor(&f.current, 0, hash, root.Version)
			for j := 0; j < 5; j++ {
				f.event(fresh.Pos, tc.h)
			}
			f.observe(fresh, rawFresh)
			f.repairBoundary()
			if err := f.runBoundary(tc.h, nil); err != nil {
				t.Fatal(err)
			}
			if f.stats.PolicyEvictions != tc.want {
				t.Fatal("wrong victim eligibility", f.stats)
			}
			if tc.want == 1 {
				if f.stats.SampleComparisons != 1 || f.stats.WorkExhaustions != 0 {
					t.Fatal("unbounded replacement", f.stats)
				}
				s := &f.selection
				freshSlot := f.current.find(fresh.Pos)
				if freshSlot < 0 || s.slots[freshSlot].parent != 0 || s.slots[0].children != 1 || len(s.pools[0]) != 1 || int(s.pools[0][0]) != freshSlot || s.slots[freshSlot].offset != 0 || s.use[0] != 2*slotBytes || f.current.counts[0]+f.current.counts[1] != 2 {
					t.Fatal("indexes after replacement")
				}
				if f.current.find(fresh.Pos) < 0 || f.current.find(old.Pos) >= 0 {
					t.Fatal("no exchange")
				}
				if (*f.current.node(0)).Edges[1].height() != 1 {
					t.Fatal("eviction lost Prefix height")
				}
			}
		})
	}
}

func TestBoundaryIndexReusesTombstones(t *testing.T) {
	c := DefaultBoundaryConfig()
	c.NodeBudget = ClassBudget{4 * boundaryCharge(), 0, 0}
	f, e := newBoundaryForest(c)
	if e != nil {
		t.Fatal(e)
	}
	for k := 0; k < 100; k++ {
		n, raw := boundaryTestNode(t, nil, byte(k))
		n.Pos.ID = uint64(k % 16)
		if err := f.current.put(k%4, n, raw); err != nil {
			t.Fatal(err)
		}
		if f.current.find(n.Pos) != k%4 {
			t.Fatal("index put")
		}
		f.current.remove(k % 4)
		if f.current.find(n.Pos) >= 0 {
			t.Fatal("index deletion")
		}
	}
}

func TestBoundaryHeatPersistsWithoutRetainingDirtyCandidates(t *testing.T) {
	c := DefaultBoundaryConfig()
	f, e := newBoundaryForest(c)
	if e != nil {
		t.Fatal(e)
	}
	pos := position(3, []byte{1, 2})
	for i := 0; i < 10; i++ {
		f.event(pos, 1)
	}
	f.begin(2)
	if x := f.count(pos, 2); x <= 9 {
		t.Fatal("begin erased history", x)
	}
	f.event(pos, 2)
	if x := f.count(pos, 2); x <= 10 {
		t.Fatal("history was block deduplicated", x)
	}
	if f.candidateN != 0 || f.candidateUsed != 0 {
		t.Fatal("retained a previous dirty queue")
	}
}

func TestBoundaryPreparationBodiesNeverBecomeAdmissions(t *testing.T) {
	f, e := newBoundaryForest(DefaultBoundaryConfig())
	if e != nil {
		t.Fatal(e)
	}
	n, raw := boundaryTestNode(t, nil, 1)
	n.PreparationOnly = true
	f.observe(n, raw)
	f.runBoundary(16, nil)
	if f.current.find(n.Pos) >= 0 || f.stats.Admissions != 0 {
		t.Fatal("physical preparation warmed retained cache")
	}
	f.reindexCandidates()
	i := f.findCandidate(n.Pos, n.Hash)
	if i < 0 {
		t.Fatal("lost bounded body reuse")
	}
	c := f.candidates[i]
	if !bytes.Equal(f.candidateBody[c.Offset:c.Offset+c.Node.Size], raw) {
		t.Fatal("wrong reused body")
	}
}

func TestBoundaryHeightUsesAll48Bits(t *testing.T) {
	for _, h := range []uint64{0, 1, 1 << 40, MaxHeight} {
		e := edgeAt(h, boundaryPrefix)
		if e.height() != h || e.Kind != boundaryPrefix {
			t.Fatal(h, e)
		}
	}
}
