package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
	"unsafe"

	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func TestTypedAllocationAccountingAndBounds(t *testing.T) {
	if got := unsafe.Sizeof(boundaryHeader{}); got != 80 {
		t.Fatalf("header=%d", got)
	}
	for _, budget := range []uint64{8 << 20, 16 << 20, 64 << 20} {
		c, plan, e := FitBoundaryCache(budget, DefaultAdaptiveEthereumForestCacheConfig().Boundary)
		if e != nil {
			t.Fatal(e)
		}
		f, e := newBoundaryForest(c)
		if e != nil {
			t.Fatal(e)
		}
		b := &f.current
		s := f.statsSnapshot()
		actual := uint64(len(b.Headers))*uint64(unsafe.Sizeof(boundaryHeader{})) + uint64(len(b.Payload))
		if len(b.Nodes) != 0 || s.NodeAllocatedBytes != actual || s.AllocatedBytes() != plan.AllocatedBytes() || s.AllocatedBytes() > budget {
			t.Fatal("backing budget", s, plan)
		}
		if len(b.Headers) != 4*len(b.frames.entries) || s.Boundary.FrameMetadataBytes != uint64(len(b.frames.entries))*uint64(unsafe.Sizeof(boundaryFrame{})) {
			t.Fatal("metadata")
		}
	}
	c := DefaultAdaptiveEthereumForestCacheConfig().Boundary
	c.MaxNodeBytes = mpt.MaxCanonicalNodeBytes
	if _, e := newBoundaryForest(c); e == nil {
		t.Fatal("typed generic values accepted")
	}
	f, _ := newBoundaryForest(DefaultAdaptiveEthereumForestCacheConfig().Boundary)
	for _, kind := range []mpt.CanonicalKind{mpt.CanonicalLeaf, mpt.CanonicalExtension, mpt.CanonicalBranch} {
		d := mpt.CanonicalNode{Kind: kind}
		pos := position(0, []byte{3})
		switch kind {
		case mpt.CanonicalLeaf:
			d.Path, _ = mpt.EncodeCompactPath(bytes.Repeat([]byte{15}, 63), true)
			d.Value = bytes.Repeat([]byte{255}, 110)
		case mpt.CanonicalExtension:
			d.Path, _ = mpt.EncodeCompactPath(bytes.Repeat([]byte{15}, 62), false)
			d.Children[0] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{1}}
		case mpt.CanonicalBranch:
			for i := range d.Children {
				d.Children[i] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{byte(i + 1)}}
			}
		}
		raw, h, e := mpt.RebuildCanonicalNode(d)
		if e != nil {
			t.Fatal(e)
		}
		if e = f.validateFields(pos, len(raw), d); e != nil {
			t.Fatal(e)
		}
		var buf [804]byte
		im, e := mpt.CaptureLocalFieldsImage(buf[:], d, h, uint32(len(raw)))
		if e != nil {
			t.Fatal(e)
		}
		class, e := typedImageClass(im, pos)
		if e != nil {
			t.Fatal(e)
		}
		t.Logf("kind=%d canonical=%d fields=%d capacity=%d", kind, len(raw), im.DataBytes(), typedCapacity(class))
	}
}

func TestTypedFrameChurnAndFragmentation(t *testing.T) {
	var a boundaryFrames
	a.init(2)
	var offsets [8]uint32
	for i := range offsets {
		var ok bool
		offsets[i], ok = a.allocate(1, i)
		if !ok {
			t.Fatal("allocation")
		}
	}
	for _, i := range []int{0, 1, 4, 5} {
		a.release(offsets[i])
	}
	if a.available(3) {
		t.Fatal("noncontiguous free bytes counted as a full frame")
	}
	a.release(offsets[2])
	a.release(offsets[3])
	if !a.available(3) {
		t.Fatal("empty frame not reusable")
	}
	p, ok := a.allocate(3, 10)
	if !ok {
		t.Fatal("retype")
	}
	a.release(p)
	a.reset()
	rng := rand.New(rand.NewSource(19))
	live := map[uint32]int{}
	for k := 0; k < 10000; k++ {
		if len(live) > 0 && rng.Intn(2) == 0 {
			for p := range live {
				a.release(p)
				delete(live, p)
				break
			}
		} else {
			class := uint8(1 + rng.Intn(3))
			if p, ok := a.allocate(class, k); ok {
				for q, n := range live {
					if int(p) < int(q)+n && int(q) < int(p)+typedCapacity(class) {
						t.Fatal("overlap")
					}
				}
				live[p] = typedCapacity(class)
			}
		}
		for c, head := range a.heads {
			steps := 0
			prev := int32(-1)
			for head >= 0 {
				f := a.entries[head]
				if !f.Listed || int(f.Class) != c || f.Prev != prev {
					t.Fatal("free-list corrupt")
				}
				prev, head = head, f.Next
				steps++
				if steps > len(a.entries) {
					t.Fatal("cycle")
				}
			}
		}
	}
}

func nativeLeaf(t *testing.T, id uint64, path []byte, value byte) (boundaryObservation, []byte) {
	t.Helper()
	d := mpt.CanonicalNode{Kind: mpt.CanonicalLeaf, Value: bytes.Repeat([]byte{value}, 32)}
	d.Path, _ = mpt.EncodeCompactPath(make([]byte, 64-len(path)), true)
	raw, h, e := mpt.RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	return boundaryObservation{Pos: position(id, path), boundaryNode: boundaryNode{Hash: h, Version: forestVersion{0, Prefix, true}, Used: true}}, raw
}
func adaptiveFixture(t *testing.T, typed bool) (*boundaryForest, boundaryObservation, []byte) {
	t.Helper()
	c := DefaultAdaptiveEthereumForestCacheConfig().Boundary
	c.PolicyIntervalBlocks = 4
	c.SampleSize = 64
	c.WorkUnits = 1 << 20
	if typed {
		c.NodeBudget = ClassBudget{2 * 1344, 1104, 1584}
	} else {
		c.ResidentLayout = BoundaryFixedLayout
		c.NodeBudget = ClassBudget{6 * 1068, 1068, 4 * 1068}
	}
	f, e := newBoundaryForest(c)
	if e != nil {
		t.Fatal(e)
	}
	f.begin(4)
	var children [5]boundaryObservation
	var raws [5][]byte
	d := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
	for i := range children {
		children[i], raws[i] = nativeLeaf(t, 0, []byte{byte(i)}, byte(i+1))
		children[i].Heat = residentHeat{Value: 1, Height: 4}
		d.Children[i] = mpt.CanonicalReference{Present: true, Hash: children[i].Hash}
	}
	raw, h, e := mpt.RebuildCanonicalNode(d)
	if e != nil {
		t.Fatal(e)
	}
	root := boundaryObservation{Pos: position(0, nil), boundaryNode: boundaryNode{Hash: h, Heat: residentHeat{Value: 100, Height: 4}, Version: forestVersion{0, Prefix, true}}}
	for i := range children {
		root.Edges[i] = edgeAt(0, boundaryPrefix)
	}
	if e = f.current.put(0, root, raw); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		if e = f.current.put(i+1, children[i], raws[i]); e != nil {
			t.Fatal(e)
		}
	}
	return f, children[4], raws[4]
}
func TestTypedHintsRoundTrip(t *testing.T) {
	f, _, _ := adaptiveFixture(t, true)
	b := &f.current
	for i := 0; i < 5; i++ {
		if b.edgeAtSlot(0, i).Kind != boundaryPrefix || b.edgeAtSlot(0, i).height() != 0 {
			t.Fatal("lost height zero")
		}
	}
	edges := b.edgesAt(0)
	edges[1] = boundaryEdge{Kind: boundaryHash}
	edges[2] = edgeAt(MaxHeight, boundaryPrefix)
	b.setEdges(0, edges)
	if b.edgeAtSlot(0, 1).Kind != boundaryHash || b.edgeAtSlot(0, 2).height() != MaxHeight {
		t.Fatal("edge roundtrip")
	}
}

func TestAdaptiveFrontierEligibilityAndFailedAdmission(t *testing.T) {
	for _, mode := range []string{BoundaryUpdatedVictims, BoundaryFrontierVictims} {
		f, n, raw := adaptiveFixture(t, false)
		f.cfg.VictimEligibility = mode
		if mode == BoundaryUpdatedVictims {
			f.current.frontier = nil
			f.selection.resetPools()
		}
		// Free header exists, but strict storage reservation creates pressure.
		f.cfg.ReservationPolicy = BoundaryStrictReservation
		f.cfg.NodeBudget.StorageReserveBytes = f.cfg.NodeBudget.TotalBytes - f.current.usage[0]
		n.Updated = true
		n.Heat = residentHeat{Value: 10, Height: 4}
		f.observe(n, raw)
		for i := 0; i < 20; i++ {
			f.event(n.Pos, 4)
		}
		if e := f.runBoundary(4, nil); e != nil {
			t.Fatal(e)
		}
		want := uint64(0)
		if mode == BoundaryFrontierVictims {
			want = 1
		}
		if f.stats.PolicyEvictions != want {
			t.Fatalf("mode=%s evictions=%d", mode, f.stats.PolicyEvictions)
		}
	}
	f, n, raw := adaptiveFixture(t, true)
	n.Updated = true
	f.cfg.NodeBudget.StorageReserveBytes = f.cfg.NodeBudget.TotalBytes - f.current.usage[0]
	f.cfg.ReservationPolicy = BoundaryStrictReservation
	before := append([]boundaryHeader(nil), f.current.Headers...)
	n.Size = 0
	f.observe(n, nil)
	sentinel := errors.New("injected read error")
	if e := f.runBoundary(4, func(boundaryObservation) ([]byte, boundaryObservation, error) {
		return nil, boundaryObservation{}, sentinel
	}); !errors.Is(e, sentinel) {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, f.current.Headers) || f.stats.PolicyEvictions != 0 {
		t.Fatal("evicted before failed preparation")
	}
	// Explicit I/O budget exhaustion also leaves residency intact.
	if e := f.runBoundary(4, func(boundaryObservation) ([]byte, boundaryObservation, error) { return raw, n, errAdmissionReadLimit }); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(before, f.current.Headers) || f.stats.AdmissionReadLimitSkips != 1 {
		t.Fatal("limit mutated cache")
	}
}

func TestAdaptiveGroupedHeatAndReservation(t *testing.T) {
	f, n, _ := adaptiveFixture(t, true)
	b := &f.current
	// Four small siblings share a frame. A branch needs all four, not one.
	g, ok := f.victimGroup(1, 0, 0, 3, 1104, 4)
	if !ok || g.n != 4 || g.heat != 4 {
		t.Fatal(g, ok)
	}
	n.Heat = residentHeat{Value: 2, Height: 4}
	if n.Heat.score(4, 64) > g.heat {
		t.Fatal("group heat")
	}
	g, ok = f.victimGroup(1, 1, 0, 3, 1104, 4)
	if ok {
		t.Fatal("candidate parent included", g)
	}
	// Storage can reclaim above account's own floor even when the donor is hotter.
	g, ok = f.victimGroup(1, -1, 1, 3, 1104, 4)
	if !ok || !g.priority || g.reclaimed != 4*336 {
		t.Fatal("reserve priority", g, ok)
	}
	f.cfg.NodeBudget.AccountReserveBytes = b.usage[0]
	if _, ok = f.victimGroup(1, -1, 1, 3, 1104, 4); ok {
		t.Fatal("stole protected reserve")
	}
}

func TestAdaptiveReadBudgetStopsBeforeIO(t *testing.T) {
	a := &admissionReadBudget{maxNodes: 2, maxBytes: 50, bound: 30}
	b := readBudget{admission: a, cfg: DefaultConfig("ethereum-mpt", "budget")}
	if e := b.beforeRead(); e != nil {
		t.Fatal(e)
	}
	if e := b.charge(25); e != nil {
		t.Fatal(e)
	}
	if e := b.beforeRead(); !errors.Is(e, errAdmissionReadLimit) {
		t.Fatal("read would exceed bytes", e)
	}
	if a.nodes != 1 || a.bytes != 25 {
		t.Fatal(a)
	}
}

func TestAdaptiveForestRootsAndHistoricalProofs(t *testing.T) {
	for _, layout := range []string{BoundaryFixedLayout, BoundaryTypedLayout} {
		for _, policy := range []string{BoundaryStrictReservation, BoundaryBorrowReservation} {
			t.Run(layout+"-"+policy, func(t *testing.T) {
				cfg := DefaultConfig("ethereum-mpt", "adaptive-forest")
				opt := kvstore.DefaultOptions(kvstore.LevelDB)
				cfg.Storage = &opt
				s, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
				if e != nil {
					t.Fatal(e)
				}
				defer s.Close()
				cache := DefaultAdaptiveEthereumForestCacheConfig()
				cache.Boundary.ResidentLayout = layout
				cache.Boundary.ReservationPolicy = policy
				cache.Boundary.NodeBudget = ClassBudget{32 << 10, 8 << 10, 8 << 10}
				cache.Boundary.PolicyIntervalBlocks = 2
				f, e := OpenCachedForest(s, cache)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				oracle, _ := mpt.NewEmpty(mpt.NewMemoryStore())
				var head Head
				roots := []mpt.Hash{}
				keys := make([][32]byte, 128)
				for i := range keys {
					keys[i] = sha256.Sum256([]byte{byte(i), 7})
				}
				for h := uint64(0); h <= 12; h++ {
					b, e := f.BeginBlock(h, head.CommitID)
					if e != nil {
						t.Fatal(e)
					}
					var tree *mpt.Trie
					if h == 0 {
						tree, _ = mpt.NewEmpty(mpt.NewMemoryStore())
					} else {
						reader, err := b.TrieReader(0, roots[h-1])
						if err != nil {
							t.Fatal(err)
						}
						tree, e = mpt.Open(roots[h-1], reader)
						if e != nil {
							t.Fatal(e)
						}
					}
					for j := 0; j < 128; j++ {
						if h > 0 && j%3 != int(h)%3 {
							continue
						}
						value := bytes.Repeat([]byte{byte(h + 1)}, 40)
						if h > 0 && j%11 == 0 {
							value = nil
						}
						for _, tr := range []*mpt.Trie{tree, oracle} {
							if value == nil {
								e = tr.Delete(keys[j][:])
							} else {
								e = tr.Update(keys[j][:], value)
							}
							if e != nil {
								t.Fatal(e)
							}
						}
					}
					if e = b.SetTrie(0, tree); e != nil {
						t.Fatal(e)
					}
					head, _, e = b.Commit()
					if e != nil {
						t.Fatalf("h=%d: %v", h, e)
					}
					want, _ := oracle.Hash()
					root, _ := tree.Hash()
					if want != root {
						t.Fatal("root")
					}
					roots = append(roots, root)
					bank := &f.boundary.current
					var usage [2]uint64
					var fields, hints uint64
					for i := 0; i < bank.capacity(); i++ {
						if !bank.usedAt(i) {
							continue
						}
						n := bank.snapshot(i)
						usage[cacheClass(bank.trieID(n))] += bank.entryCharge(i)
						fields += uint64(n.Image.DataBytes())
						hints += bank.routingSize(i)
						if n.Link.Parent != 0 {
							p := int(n.Link.Parent) - 1
							if !bank.usedAt(p) || !n.Heat.boundedBy(bank.heatAt(p), h, cache.Boundary.HalfLife) {
								t.Fatal("disconnected or inverted heat")
							}
						}
					}
					if usage != bank.usage || fields != bank.fieldBytes || hints != bank.hintBytes {
						t.Fatal("counters", usage, bank.usage, fields, bank.fieldBytes, hints, bank.hintBytes)
					}
				}
				for h, root := range roots {
					v, e := s.ViewAt(uint64(h))
					if e != nil {
						t.Fatal(e)
					}
					for _, i := range []int{0, 11, 50, 127} {
						proof, _, e := v.ProveWithStats(0, root, keys[i][:])
						if e != nil {
							t.Fatal(e)
						}
						_, _, e = mpt.VerifyProof(root, keys[i][:], proof)
						if e != nil {
							t.Fatal(e)
						}
					}
					v.Close()
				}
				stats := f.boundary.statsSnapshot().Boundary
				t.Logf("nodes=%d evictions=%d reads=%d root/proofs verified", stats.NodeEntries, stats.PolicyEvictions, stats.AdmissionReads)
			})
		}
	}
}

func TestAdaptiveReserveRecallAndKGate(t *testing.T) {
	for _, h := range []uint64{3, 4} {
		f, _, _ := adaptiveFixture(t, true)
		n, raw := nativeLeaf(t, 7, nil, 9)
		n.Size = uint32(len(raw))
		n.Updated = true
		n.Heat = residentHeat{Value: .1, Height: h}
		work := uint64(1 << 20)
		e := f.admitAdaptive(n, raw, -1, -1, h, f.policyDue(h), rand.New(rand.NewSource(3)), &work, nil)
		if e != nil {
			t.Fatal(e)
		}
		if h == 3 {
			if f.stats.PolicyEvictions != 0 || f.current.find(n.Pos) >= 0 {
				t.Fatal("non-K eviction")
			}
		} else {
			if f.stats.PolicyEvictions != 1 || f.stats.ReserveAdmissions != 1 || f.current.find(n.Pos) < 0 || f.current.usage[0] < f.cfg.NodeBudget.AccountReserveBytes {
				t.Fatal("priority reclaim failed", f.stats)
			}
		}
	}
}
func TestTypedGrowthDropsOnlyChangedBoundary(t *testing.T) {
	f, _, _ := adaptiveFixture(t, true)
	b := &f.current
	child := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
	child.Children[0] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{91}}
	child.Children[1] = mpt.CanonicalReference{Present: true, Hash: mpt.Hash{92}}
	raw, h, e := mpt.RebuildCanonicalNode(child)
	if e != nil {
		t.Fatal(e)
	}
	n := b.observation(1)
	n.Hash = h
	n.Updated = true
	n.Version.Height = 3
	// Publishing a changed parent precedes its child, as in production.
	root := b.observation(0)
	rootRaw, _ := root.Image.RLP()
	fields, e := mpt.DecodeLocalNodeFields(rootRaw)
	if e != nil {
		t.Fatal(e)
	}
	fields.Children[0].Hash = h
	rootRaw, root.Hash, e = mpt.RebuildCanonicalNode(fields)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.put(0, root, rootRaw); e != nil {
		t.Fatal(e)
	}
	if e = b.put(1, n, raw); e != nil {
		t.Fatal(e)
	}
	if b.usedAt(1) || b.ResizeDrops != 1 || f.stats.PolicyEvictions != 0 {
		t.Fatal("growth did not drop just the structural boundary")
	}
	for _, i := range []int{0, 2, 3, 4} {
		if !b.usedAt(i) {
			t.Fatal("unrelated eviction")
		}
	}
	if got, ok := b.imageAt(0).ChildHash(0); !ok || got != h {
		t.Fatal("parent lost changed child hash")
	}
}
