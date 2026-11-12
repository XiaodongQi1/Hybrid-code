package hybridtrie

import (
	"fmt"
	"math"
	"math/rand"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
	"unsafe"
)

func smallHeat(t *testing.T, width uint32) *boundaryForest {
	t.Helper()
	c := DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
	c.HeatSketchWidth = width
	f, e := newBoundaryForest(c)
	if e != nil {
		t.Fatal(e)
	}
	return f
}
func closeHeat(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-10*math.Max(1, math.Abs(want)) {
		t.Fatalf("heat %.15g want %.15g", got, want)
	}
}
func beginHeat(t *testing.T, f *boundaryForest, h uint64) {
	t.Helper()
	if e := f.begin(h); e != nil {
		t.Fatal(e)
	}
}
func TestBoundaryHeatAlignedWeightedOracle(t *testing.T) {
	f := smallHeat(t, 16384)
	pos := position(0, []byte{1, 2})
	type observation struct {
		h uint64
		n int
	}
	var history []observation
	rng := rand.New(rand.NewSource(19))
	for h := uint64(0); h <= 1024; h++ {
		beginHeat(t, f, h)
		n := rng.Intn(12)
		history = append(history, observation{h, n})
		for j := 0; j < n; j++ {
			f.event(pos, h)
		}
		want := 0.0
		for _, o := range history {
			if h/64-o.h/64 < 4 {
				want += float64(o.n) * math.Exp2(-float64(h-o.h)/64)
			}
		}
		closeHeat(t, f.count(pos, h), want)
		if h == 255 || h == 319 {
			if f.statsSnapshot().Boundary.HeatCoverageBlocks != 256 {
				t.Fatal("full aligned coverage")
			}
		}
		if h == 256 || h == 320 {
			if f.statsSnapshot().Boundary.HeatCoverageBlocks != 193 {
				t.Fatal("partial aligned coverage")
			}
		}
	}
	if f.stats.HeatRotations != 17 || f.stats.HeatExpiredBuckets != 13 {
		t.Fatal(f.stats)
	}
}
func TestBoundaryHeatPartialStartupEmptyJumpAndRetry(t *testing.T) {
	f := smallHeat(t, 16384)
	pos := position(7, nil)
	beginHeat(t, f, 70)
	if f.heat.Buckets[1].Epoch != 1 || f.heat.Buckets[1].Anchor != 70 {
		t.Fatal(f.heat.Buckets)
	}
	f.event(pos, 70)
	f.event(pos, 70)
	beginHeat(t, f, 70)
	closeHeat(t, f.count(pos, 70), 2)
	for h := uint64(71); h <= 76; h++ {
		beginHeat(t, f, h)
		f.event(pos, h)
	}
	want := math.Exp2(-6.0 / 64)
	for t0 := uint64(70); t0 <= 76; t0++ {
		want += math.Exp2(-float64(76-t0) / 64)
	}
	closeHeat(t, f.count(pos, 76), want)
	if f.statsSnapshot().Boundary.HeatCoverageBlocks != 7 {
		t.Fatal("partial startup inflated")
	}
	before := f.stats.HeatUpdates
	beginHeat(t, f, 127)
	if f.stats.HeatUpdates != before {
		t.Fatal("empty block invented events")
	}
	closeHeat(t, f.count(pos, 127), want*math.Exp2(-51.0/64))
	beginHeat(t, f, 128)
	if !f.heat.Buckets[1].Valid || !f.heat.Buckets[2].Valid {
		t.Fatal("early expiry")
	}
	beginHeat(t, f, MaxHeight)
	closeHeat(t, f.count(pos, MaxHeight), 0)
	if f.heat.Buckets[(MaxHeight/64)%4].Anchor != MaxHeight {
		t.Fatal("jump fabricated history")
	}
	old := f.heat
	if f.begin(MaxHeight-1) == nil || f.begin(MaxHeight+1) == nil {
		t.Fatal("backward/overflow clock accepted")
	}
	if old.Buckets != f.heat.Buckets || old.Height != f.heat.Height {
		t.Fatal("failed clock mutated state")
	}
}
func TestBoundaryHeatWithinBucketTimingAndSum(t *testing.T) {
	f := smallHeat(t, 16384)
	early, late := position(0, []byte{1}), position(0, []byte{2})
	f.event(early, 0)
	beginHeat(t, f, 63)
	f.event(late, 63)
	closeHeat(t, f.count(early, 63), math.Exp2(-63.0/64))
	closeHeat(t, f.count(late, 63), 1)
	f.event(early, 64)
	closeHeat(t, f.count(early, 64), 1.5) // sum, not max across buckets
	for j := 0; j < 1000; j++ {
		f.event(late, 64)
	}
	closeHeat(t, f.count(late, 64), 1000+math.Exp2(-1.0/64)) // no age cap or dedup
	beginHeat(t, f, 255)
	if f.count(early, 255) == 0 {
		t.Fatal("premature expiry")
	}
	beginHeat(t, f, 256)
	closeHeat(t, f.count(early, 256), math.Exp2(-192.0/64))
	beginHeat(t, f, 320)
	closeHeat(t, f.count(early, 320), 0)
}
func TestBoundaryHeatClassPartitionAndCollisions(t *testing.T) {
	f := smallHeat(t, 2) // one column per class: deliberately force collisions
	storage := position(1, []byte{1})
	other := position(2, []byte{2})
	account := position(0, []byte{1})
	for j := 0; j < 7; j++ {
		f.event(storage, 1)
	}
	closeHeat(t, f.count(other, 1), 7)
	closeHeat(t, f.count(account, 1), 0)
	f.event(account, 1)
	closeHeat(t, f.count(storage, 1), 7)
	closeHeat(t, f.count(account, 1), 1)
	normal := smallHeat(t, 16384)
	a, b, c := position(1, []byte{1}), position(2, []byte{1}), position(1, []byte{1, 0})
	for _, pair := range [][2]forestPosition{{a, b}, {a, c}} {
		same := true
		for row := 0; row < 4; row++ {
			if normal.heatColumn(pair[0], row) != normal.heatColumn(pair[1], row) {
				same = false
			}
		}
		if same {
			t.Fatal("key namespace/length lost")
		}
	}
}
func TestBoundaryHeatResidentDecayIndependentOfSketchExpiry(t *testing.T) {
	f := smallHeat(t, 16384)
	n, raw := boundaryTestNode(t, nil, 1)
	beginHeat(t, f, 0)
	for j := 0; j < 10; j++ {
		f.event(n.Pos, 0)
	}
	n.Heat = f.initialHeat(n.Pos)
	if e := f.current.put(0, n, raw); e != nil {
		t.Fatal(e)
	}
	f.event(n.Pos, 0)
	closeHeat(t, f.count(n.Pos, 0), 11)
	f.current.remove(0)
	closeHeat(t, f.count(n.Pos, 0), 11) // all observations already in CMS; no transfer
	n.Heat = f.initialHeat(n.Pos)
	if e := f.current.put(0, n, raw); e != nil {
		t.Fatal(e)
	}
	f.event(n.Pos, 64)
	beginHeat(t, f, 256)
	if f.current.Nodes[0].Heat.Height != 64 {
		t.Fatal("bucket expiration rewrote resident heat")
	}
	closeHeat(t, f.count(n.Pos, 256), 11*math.Exp2(-256.0/64)+math.Exp2(-192.0/64))
	if f.stats.HeatResetBytes != uint64(4*16384*8) {
		t.Fatal("reset bytes", f.stats.HeatResetBytes)
	}
	f.reset()
	if f.heat.Started || f.statsSnapshot().Boundary.NodeEntries != 0 || f.count(n.Pos, 256) != 0 {
		t.Fatal("cold reset retained history")
	}
}
func TestResidentHeatScalarDecayAndParentOrder(t *testing.T) {
	parent := residentHeat{Value: 10, Height: 100}
	child := residentHeat{Value: 20, Height: 164}
	if !child.clip(parent, 164, 64) || child != (residentHeat{Value: 5, Height: 164}) {
		t.Fatal("admission cap", child)
	}
	for _, height := range []uint64{164, 164, 256, 320, 1000} {
		parent.event(height, 64)
		child.event(height, 64)
		if !child.boundedBy(parent, height, 64) {
			t.Fatal("parent order", height, parent, child)
		}
	}
	v := residentHeat{Value: 8, Height: 0}
	v.event(64, 64)
	v.event(64, 64)
	closeHeat(t, v.Value, 6) // repeated same-block visits count individually
	before := v
	closeHeat(t, v.score(128, 64), 3)
	if v != before {
		t.Fatal("comparison changed the last-update height")
	}
}
func TestBoundaryHeatFixedMemoryAndConfig(t *testing.T) {
	f := smallHeat(t, 16384)
	base := f.statsSnapshot().Boundary
	ptr := &f.heat.Counters[0]
	for i := uint64(0); i < 10000; i++ {
		f.event(position(i, []byte{byte(i % 16)}), i/10)
	}
	after := f.statsSnapshot().Boundary
	if ptr != &f.heat.Counters[0] || base.MetadataAllocatedBytes != after.MetadataAllocatedBytes || after.HeatSketchBytes != 2<<20 || unsafe.Sizeof(residentHeat{}) != 16 {
		t.Fatal("unbounded statistics", after)
	}
	for _, edit := range []func(*BoundaryConfig){
		func(c *BoundaryConfig) { c.HeatBucketCount = 3 }, func(c *BoundaryConfig) { c.HeatBucketBlocks = 63 },
		func(c *BoundaryConfig) { c.HeatSketchRows = 0 }, func(c *BoundaryConfig) { c.HeatSketchRows = 17 },
		func(c *BoundaryConfig) { c.HeatSketchWidth = 3 }, func(c *BoundaryConfig) { c.HeatSketchWidth = 1 << 21 },
		func(c *BoundaryConfig) { c.HeatSketchWidth = 1 << 20; c.MetadataBytes = 1 << 20 },
	} {
		c := DefaultBoundaryConfig()
		edit(&c)
		if _, err := newBoundaryForest(c); err == nil {
			t.Fatal("invalid matrix configuration")
		}
	}
}

func TestBoundaryHeatAdmissionScoresClippedScalar(t *testing.T) {
	for _, victimHeat := range []float64{3, 7} {
		t.Run(fmt.Sprint(victimHeat), func(t *testing.T) {
			c := DefaultBoundaryConfig()
			c.NodeBudget = ClassBudget{3 * boundaryCharge(), 0, 0}
			c.PolicyIntervalBlocks = 4
			c.HeatSketchWidth = 2
			f, e := newBoundaryForest(c)
			if e != nil {
				t.Fatal(e)
			}
			beginHeat(t, f, 4)
			old, oldRaw := boundaryTestNode(t, []byte{1}, 1)
			old.Heat = residentHeat{Value: victimHeat, Height: 4}
			old.Updated = true
			fresh, freshRaw := boundaryTestNode(t, []byte{2}, 2)
			fresh.Pos.ID = 1
			fresh.Updated = true
			makeRoot := func(id uint64, child boundaryObservation, heat float64) (boundaryObservation, []byte) {
				d := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
				d.Children[child.Pos.Path[0]] = mpt.CanonicalReference{Present: true, Hash: child.Hash}
				raw, hash, err := mpt.RebuildCanonicalNode(d)
				if err != nil {
					t.Fatal(err)
				}
				return boundaryObservation{Pos: position(id, nil), boundaryNode: boundaryNode{Hash: hash, Version: forestVersion{1, Prefix, true}, Heat: residentHeat{Value: heat, Height: 4}, Used: true}}, raw
			}
			root, raw := makeRoot(0, old, 10)
			storage, storageRaw := makeRoot(1, fresh, 5)
			for i, x := range []struct {
				n   boundaryObservation
				raw []byte
			}{{root, raw}, {storage, storageRaw}, {old, oldRaw}} {
				if e = f.current.put(i, x.n, x.raw); e != nil {
					t.Fatal(e)
				}
			}
			f.rootFor(&f.current, 0, root.Hash, root.Version)
			f.rootFor(&f.current, 1, storage.Hash, storage.Version)
			for i := 0; i < 100; i++ {
				f.event(fresh.Pos, 4)
			} // CMS collision/estimate exceeds parent
			f.observe(fresh, freshRaw)
			f.repairBoundary()
			if e = f.runBoundary(4, nil); e != nil {
				t.Fatal(e)
			}
			at := f.current.find(fresh.Pos)
			if victimHeat == 3 {
				if at < 0 || (*f.current.node(at)).Heat != (residentHeat{Value: 5, Height: 4}) {
					t.Fatal("wrong installed initial scalar", at)
				}
				if f.stats.PolicyEvictions != 1 {
					t.Fatal("no exchange")
				}
			} else if at >= 0 || f.stats.PolicyEvictions != 0 {
				t.Fatal("raw estimate bypassed parent cap")
			}
			if f.stats.HeatParentClamps != 1 {
				t.Fatal("missing clamp metric")
			}
		})
	}
}

func TestResidentHeatMatchesUntruncatedVisitHistory(t *testing.T) {
	f := smallHeat(t, 16384)
	n, raw := boundaryTestNode(t, nil, 1)
	beginHeat(t, f, 0)
	n.Heat = f.initialHeat(n.Pos)
	if err := f.current.put(0, n, raw); err != nil {
		t.Fatal(err)
	}
	type visit struct {
		height uint64
		count  int
	}
	var visits []visit
	rng := rand.New(rand.NewSource(33))
	queries := f.stats.HeatQueries
	for h := uint64(0); h <= 1024; h++ {
		beginHeat(t, f, h)
		count := rng.Intn(5)
		visits = append(visits, visit{h, count})
		for i := 0; i < count; i++ {
			f.event(n.Pos, h)
		}
		want := 0.0
		for _, v := range visits {
			want += float64(v.count) * math.Exp2(-float64(h-v.height)/64)
		}
		before := f.current.Nodes[0].Heat
		closeHeat(t, f.count(n.Pos, h), want)
		if f.current.Nodes[0].Heat != before || f.current.Nodes[0].Version.Height != n.Version.Height {
			t.Fatal("comparison changed resident metadata")
		}
	}
	if f.stats.HeatQueries != queries {
		t.Fatal("resident scores consulted sketch")
	}
	f.current.remove(0)
	want := 0.0
	for _, v := range visits {
		if uint64(1024)/64-v.height/64 < 4 {
			want += float64(v.count) * math.Exp2(-float64(1024-v.height)/64)
		}
	}
	closeHeat(t, f.count(n.Pos, 1024), want)
}

func TestResidentHeatEqualScoresAcrossDifferentAnchors(t *testing.T) {
	parent := residentHeat{Value: 1}
	for anchor := uint64(1); anchor < 128; anchor++ {
		child := residentHeat{Value: parent.score(anchor, 64), Height: anchor}
		for height := anchor; height < 300; height++ {
			if !child.boundedBy(parent, height, 64) {
				t.Fatal("rounding rejected an equal score", anchor, height)
			}
		}
	}
	if (residentHeat{Value: 1.001}).boundedBy(parent, 0, 64) {
		t.Fatal("real heat inversion accepted")
	}
}
