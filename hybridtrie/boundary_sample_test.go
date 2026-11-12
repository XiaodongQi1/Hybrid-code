package hybridtrie

import (
	"math/rand"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

// Exercise the logical union of uneven class pools. Unique heat scores make
// the exhaustive reference independent of the order produced by the sampler.
func TestBoundarySamplePermutationAndProtection(t *testing.T) {
	c := DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
	c.NodeBudget.AccountReserveBytes = 0
	c.NodeBudget.StorageReserveBytes = 0
	f, err := newBoundaryForest(c)
	if err != nil {
		t.Fatal(err)
	}
	f.heat.Factors = boundaryFrequency{1}
	s := &f.selection
	for class := 0; class < 2; class++ {
		var desc mpt.CanonicalNode
		desc.Kind = mpt.CanonicalBranch
		for i := 0; i < 12; i++ {
			if (i >= 3) != (class == 1) {
				continue
			}
			n, _ := boundaryTestNode(t, []byte{byte(i)}, byte(i))
			desc.Children[i] = mpt.CanonicalReference{Present: true, Hash: n.Hash}
		}
		raw, hash, err := mpt.RebuildCanonicalNode(desc)
		if err != nil {
			t.Fatal(err)
		}
		n := boundaryObservation{Pos: position(uint64(class), nil), boundaryNode: boundaryNode{Hash: hash}}
		if err := f.current.put(12+class, n, raw); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 12; i++ {
		id := uint64(0)
		if i >= 3 {
			id = 1
		}
		n, raw := boundaryTestNode(t, []byte{byte(i)}, byte(i))
		n.Pos.ID, n.Updated, n.Heat = id, true, residentHeat{Value: float64(i)}
		if err := f.current.put(i, n, raw); err != nil {
			t.Fatal(err)
		}
		s.slots[i].offset = -1
		f.offerBoundary(i)
		s.use[cacheClass(id)] += f.charge()
	}
	pool0, pool1 := append([]int32(nil), s.pools[0]...), append([]int32(nil), s.pools[1]...)
	after := s.use
	after[0] += f.charge()
	draw := func(seed uint64, size uint32, work uint64) (int, bool) {
		f.cfg.SampleSize = size
		src := boundarySeed(seed)
		return f.sampledVictim(0, 0, after, rand.New(&src), &work)
	}
	seen := map[int]bool{}
	for seed := uint64(0); seed < 256; seed++ {
		got, ex := draw(seed, 1, 1)
		rep, _ := draw(seed, 1, 1)
		if got <= 0 || got >= 12 || ex || got != rep {
			t.Fatal(seed, got, rep, ex)
		}
		seen[got] = true
		// Among eleven distinct ranks 1..11, the minimum of five
		// cannot exceed seven. Exercise multi-draw cross-class undo.
		partial, exhausted := draw(seed, 5, 5)
		if exhausted || partial < 1 || partial > 7 {
			t.Fatal("partial sample", seed, partial)
		}
		if !reflect.DeepEqual(pool0, s.pools[0]) || !reflect.DeepEqual(pool1, s.pools[1]) {
			t.Fatal("partial shuffle leaked across attempts")
		}
		all, ex := draw(seed, 64, 11)
		if all != 1 || ex {
			t.Fatal("exhaustive minimum", all, ex)
		}
		if !reflect.DeepEqual(pool0, s.pools[0]) || !reflect.DeepEqual(pool1, s.pools[1]) {
			t.Fatal("reverse index invalidated")
		}
	}
	if len(seen) != 11 {
		t.Fatal("unreachable candidates", seen)
	}
	// Protect all storage capacity. The two remaining account siblings fit;
	// exhausting the comparison budget must not touch either pool.
	f.cfg.NodeBudget.StorageReserveBytes = s.use[1]
	if _, ex := draw(0, 5, 1); !ex {
		t.Fatal("budget")
	}
	for seed := uint64(0); seed < 32; seed++ {
		got, ex := draw(seed, 1, 1)
		if got != 1 && got != 2 || ex {
			t.Fatal("quota", got)
		}
	}
	if _, ex := draw(1, 64, 2); ex {
		t.Fatal("budget must only charge eligible comparisons")
	}
}

func TestBoundarySamplingConfigCompatibility(t *testing.T) {
	c := DefaultBoundaryConfig()
	c.SampleSize = 0
	f, err := newBoundaryForest(c)
	if err != nil || f.cfg.SampleSize != 5 {
		t.Fatal("legacy sample default", err)
	}
	c.SampleSize = 65
	if _, err = planBoundary(c); err == nil {
		t.Fatal("oversized sample")
	}
}

// Only the last child's removal can expose its parent. A newly installed
// sibling must immediately protect its own parent and enter the correct pool.
func TestBoundarySampleTracksMovingFrontier(t *testing.T) {
	c := DefaultBoundaryConfig()
	c.NodeBudget = ClassBudget{4 * boundaryCharge(), 0, 0}
	c.PolicyIntervalBlocks = 1
	f, err := newBoundaryForest(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.begin(1); err != nil {
		t.Fatal(err)
	}
	left, lraw := boundaryTestNode(t, []byte{1, 3}, 1)
	right, rraw := boundaryTestNode(t, []byte{2, 4}, 2)
	branch := func(path []byte, children ...boundaryObservation) (boundaryObservation, []byte) {
		var d mpt.CanonicalNode
		d.Kind = mpt.CanonicalBranch
		for _, child := range children {
			k := child.Pos.Path[len(path)]
			d.Children[k] = mpt.CanonicalReference{Present: true, Hash: child.Hash}
		}
		raw, hash, e := mpt.RebuildCanonicalNode(d)
		if e != nil {
			t.Fatal(e)
		}
		return boundaryObservation{Pos: position(0, path), boundaryNode: boundaryNode{Hash: hash, Updated: true, Version: forestVersion{1, Prefix, true}, Heat: residentHeat{Value: 10, Height: 1}}}, raw
	}
	a, araw := branch([]byte{1}, left)
	b, braw := branch([]byte{2}, right)
	root, raw := branch(nil, a, b)
	left.Updated = true
	right.Updated = true
	for i, ent := range []struct {
		n   boundaryObservation
		raw []byte
	}{{root, raw}, {a, araw}, {b, braw}, {left, lraw}} {
		if e := f.current.put(i, ent.n, ent.raw); e != nil {
			t.Fatal(e)
		}
	}
	f.rootFor(&f.current, 0, root.Hash, root.Version)
	for i := 0; i < 5; i++ {
		f.event(right.Pos, 1)
	}
	f.observe(right, rraw)
	f.repairBoundary()
	if err = f.runBoundary(1, nil); err != nil {
		t.Fatal(err)
	}
	s := &f.selection
	if f.stats.PolicyEvictions != 1 || f.current.find(right.Pos) != 3 || s.slots[1].offset < 0 || s.slots[1].children != 0 || s.slots[2].offset >= 0 || s.slots[2].children != 1 || s.slots[3].parent != 2 || s.slots[3].offset < 0 || len(s.pools[0]) != 2 {
		t.Fatal("moving frontier", s.slots, s.pools)
	}
}
