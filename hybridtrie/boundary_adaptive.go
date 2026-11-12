package hybridtrie

import (
	"bytes"
	"errors"
	"math/rand"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
	"unsafe"
)

func boundaryModeNames(c BoundaryConfig) (layout, victims, reservation string) {
	layout, victims, reservation = c.ResidentLayout, c.VictimEligibility, c.ReservationPolicy
	if layout == "" {
		layout = BoundaryFixedLayout
	}
	if victims == "" {
		victims = BoundaryUpdatedVictims
	}
	if reservation == "" {
		reservation = BoundaryStrictReservation
	}
	return
}
func validBoundaryModes(c BoundaryConfig) bool {
	l, v, r := boundaryModeNames(c)
	return (l == BoundaryFixedLayout || l == BoundaryTypedLayout && c.MaxNodeBytes == 532) && (v == BoundaryUpdatedVictims || v == BoundaryFrontierVictims) && (r == BoundaryStrictReservation || r == BoundaryBorrowReservation) && c.AdmissionReadNodes <= 1<<20 && c.AdmissionReadBytes <= 128<<20
}
func (f *boundaryForest) adaptive() bool {
	l, v, r := boundaryModeNames(f.cfg)
	return l == BoundaryTypedLayout || v == BoundaryFrontierVictims || r == BoundaryBorrowReservation
}

// Explicit opt-in; the old preset and configurations retain their semantics.
func DefaultAdaptiveEthereumForestCacheConfig() ForestCacheConfig {
	c := DefaultEthereumPrefixBoundaryForestCacheConfig()
	c.Boundary.ResidentLayout = BoundaryTypedLayout
	c.Boundary.VictimEligibility = BoundaryFrontierVictims
	c.Boundary.ReservationPolicy = BoundaryBorrowReservation
	c.Boundary.AdmissionReadNodes = 64
	c.Boundary.AdmissionReadBytes = 256 << 10
	return c
}
func (s *boundarySelection) resetPools() {
	for c := range s.pools {
		for _, i := range s.pools[c] {
			s.slots[i].offset = -1
		}
		s.pools[c] = s.pools[c][:0]
		s.use[c] = 0
	}
}
func (b *boundaryBank) refreshFrontier(i int) {
	s := b.frontier
	if s == nil {
		return
	}
	p := &s.slots[i]
	eligible := false
	c := 0
	if b.usedAt(i) {
		link := b.linkAt(i)
		c = cacheClass(b.Roots[link.Root].ID)
		eligible = link.FirstChild == 0 && !(c == 0 && link.Parent == 0)
	}
	if p.offset >= 0 && (!eligible || int(p.class) != c) {
		pool := s.pools[p.class]
		last := pool[len(pool)-1]
		pool[p.offset] = last
		s.slots[last].offset = p.offset
		s.pools[p.class] = pool[:len(pool)-1]
		p.offset = -1
	}
	if eligible && p.offset < 0 {
		p.class = uint8(c)
		p.offset = int32(len(s.pools[c]))
		s.pools[c] = append(s.pools[c], int32(i))
	}
}
func (f *boundaryForest) quotaFits(use [2]uint64) bool {
	if f.cfg.ReservationPolicy != BoundaryBorrowReservation {
		return f.cfg.NodeBudget.fits(use)
	}
	return use[0] <= f.cfg.NodeBudget.TotalBytes && use[1] <= f.cfg.NodeBudget.TotalBytes-use[0]
}
func (f *boundaryForest) admissionCharge(class uint8) uint64 {
	if f.current.typed() {
		return uint64(unsafe.Sizeof(boundaryHeader{})) + uint64(typedCapacity(class))
	}
	return f.charge()
}

type boundaryVictims struct {
	positions [4]forestPosition
	ids       [4]int
	n         int
	heat      float64
	priority  bool
	reclaimed uint64
}

func (f *boundaryForest) victimGroup(seed, parent, request int, class uint8, cost uint64, h uint64) (g boundaryVictims, ok bool) {
	b := &f.current
	g.ids[0], g.n = seed, 1
	if b.typed() {
		frame := b.frames.entries[int(b.Headers[seed].Offset)/typedFrameBytes]
		if uint8(frame.Class) != class {
			g.n = 0
			for _, id := range frame.Owners {
				if id != 0 {
					g.ids[g.n] = int(id - 1)
					g.n++
				}
			}
		}
	}
	before, after := b.usage, b.usage
	after[request] += cost
	g.priority = f.cfg.ReservationPolicy == BoundaryBorrowReservation && before[request]+cost <= f.cfg.NodeBudget.reserve(request)
	for j := 0; j < g.n; j++ {
		i := g.ids[j]
		if !b.usedAt(i) || i == parent {
			return g, false
		}
		link := b.linkAt(i)
		c := cacheClass(b.Roots[link.Root].ID)
		if link.FirstChild != 0 || c == 0 && link.Parent == 0 || f.cfg.VictimEligibility != BoundaryFrontierVictims && !b.updatedAt(i) {
			return g, false
		}
		size := b.entryCharge(i)
		after[c] -= size
		g.heat += b.heatAt(i).score(h, f.cfg.HalfLife)
		g.positions[j] = b.position(i)
		if c == request {
			g.priority = false
		} else {
			g.reclaimed += size
		}
	}
	other := 1 - request
	if !f.quotaFits(after) || after[other] < minBoundary(before[other], f.cfg.NodeBudget.reserve(other)) {
		return g, false
	}
	if after[other] < f.cfg.NodeBudget.reserve(other) {
		g.priority = false
	}
	return g, true
}
func victimPositionLess(a, b forestPosition) bool {
	if a.Length != b.Length {
		return a.Length > b.Length
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return bytes.Compare(a.Path[:a.Length], b.Path[:b.Length]) < 0
}
func (f *boundaryForest) groupLess(a, b boundaryVictims, h uint64) bool {
	if a.priority != b.priority {
		return a.priority
	}
	if a.heat != b.heat {
		return a.heat < b.heat
	}
	// The bounded sample already carried each identity upward once. Never repeat
	// an ancestor walk while sorting/tie-breaking the same four-member group.
	sortKeys := func(g *boundaryVictims) {
		for i := 1; i < g.n; i++ {
			for j := i; j > 0 && victimPositionLess(g.positions[j], g.positions[j-1]); j-- {
				g.positions[j], g.positions[j-1] = g.positions[j-1], g.positions[j]
			}
		}
	}
	sortKeys(&a)
	sortKeys(&b)
	for i := 0; i < a.n && i < b.n; i++ {
		if a.positions[i] != b.positions[i] {
			return victimPositionLess(a.positions[i], b.positions[i])
		}
	}
	return a.n < b.n
}

func (f *boundaryForest) sampleGroup(parent, request int, class uint8, cost uint64, h uint64, rng *rand.Rand, work *uint64) (best boundaryVictims) {
	s := &f.selection
	total := len(s.pools[0]) + len(s.pools[1])
	draw := int(f.cfg.SampleSize)
	if draw > total {
		draw = total
	}
	// Includes worst-case ancestor reconstruction for tie comparisons, bounded 64 nibbles.
	unit := uint64(4*65 + 64)
	if uint64(draw)*unit > *work {
		f.stats.WorkExhaustions++
		return
	}
	*work -= uint64(draw) * unit
	f.stats.SampleAttempts++
	f.stats.EligibleSum += uint64(total)
	f.stats.SampleComparisons += uint64(draw)
	if total == 0 {
		f.stats.EmptySamples++
		return
	}
	// Floyd sample: exactly draw distinct indices, no unbounded rejection retry.
	var sampled [64]int
	var seenFrames [64]int
	seen := 0
	for k, j := 0, total-draw; j < total; j, k = j+1, k+1 {
		x := rng.Intn(j + 1)
		for q := 0; q < k; q++ {
			if sampled[q] == x {
				x = j
				break
			}
		}
		sampled[k] = x
		var id int
		if x < len(s.pools[0]) {
			id = int(s.pools[0][x])
		} else {
			id = int(s.pools[1][x-len(s.pools[0])])
		}
		if f.current.typed() {
			frame := int(f.current.Headers[id].Offset) / typedFrameBytes
			if uint8(f.current.frames.entries[frame].Class) != class {
				duplicate := false
				for q := 0; q < seen; q++ {
					if seenFrames[q] == frame {
						duplicate = true
						break
					}
				}
				if duplicate {
					continue
				}
				seenFrames[seen] = frame
				seen++
			}
		}
		g, ok := f.victimGroup(id, parent, request, class, cost, h)
		if ok && (best.n == 0 || f.groupLess(g, best, h)) {
			best = g
		}
	}
	return
}

func (f *boundaryForest) admitAdaptive(n boundaryObservation, raw []byte, parent, parentEdge int, h uint64, due bool, rng *rand.Rand, work *uint64, load func(boundaryObservation) ([]byte, boundaryObservation, error)) error {
	b := &f.current
	if n.Size == 0 {
		// Unknown body size: never read for a pressure-ineligible candidate when no body/header capacity can be used.
		minClass := uint8(1)
		if n.Pos.ID == AccountTrieID && n.Pos.Length == 0 {
			minClass = 3
		}
		after := b.usage
		after[cacheClass(n.Pos.ID)] += f.admissionCharge(minClass)
		possibleBody := !b.typed() || b.frames.available(minClass) || minClass == 1 && (b.frames.heads[2] >= 0 || b.frames.heads[3] >= 0)
		if (b.freeSlot() < 0 || !possibleBody || !f.quotaFits(after)) && (!due || !n.Updated) {
			f.stats.AdmissionRejects++
			return nil
		}
		if load == nil {
			return ErrCorrupt
		}
		body, loaded, e := load(n)
		if errors.Is(e, errAdmissionReadLimit) {
			f.stats.AdmissionReadLimitSkips++
			return nil
		}
		if e != nil {
			return e
		}
		raw, n.Edges = body, loaded.Edges
	}
	if len(raw) > int(f.cfg.MaxNodeBytes) {
		f.stats.AdmissionRejects++
		return nil
	}
	fields, e := mpt.DecodeLocalNodeFields(raw)
	if e != nil {
		return e
	}
	if e = f.validateFields(n.Pos, len(raw), fields); e != nil {
		return e
	}
	var scratch [804]byte
	buffer := scratch[:]
	if !b.typed() && b.Stride > len(buffer) {
		buffer = make([]byte, b.Stride)
	}
	image, e := mpt.CaptureLocalFieldsImage(buffer, fields, n.Hash, uint32(len(raw)))
	if e != nil {
		return e
	}
	var class uint8
	if b.typed() {
		class, e = typedImageClass(image, n.Pos)
		if e != nil {
			return e
		}
	}
	request := cacheClass(n.Pos.ID)
	cost := f.admissionCharge(class)
	after := b.usage
	after[request] += cost
	fit := b.freeSlot() >= 0 && f.quotaFits(after) && (!b.typed() || b.frames.available(class))
	var victims boundaryVictims
	if !fit {
		if !due || !n.Updated {
			f.stats.AdmissionRejects++
			return nil
		}
		start := time.Now()
		victims = f.sampleGroup(parent, request, class, cost, h, rng, work)
		f.stats.SamplingNanos += uint64(time.Since(start))
		if victims.n == 0 {
			f.stats.ProtectedRejects++
			if b.typed() && !b.frames.available(class) {
				f.stats.FragmentationRejects++
			} else {
				f.stats.QuotaRejects++
			}
			return nil
		}
		if !victims.priority && n.Heat.score(h, f.cfg.HalfLife) <= victims.heat {
			f.stats.HeatRejects++
			return nil
		}
	}
	// All fallible reads/shape/identity/quota checks have completed. putImage below
	// uses an available or just-freed frame; no other writer runs in this section.
	for j := 0; j < victims.n; j++ {
		slot := victims.ids[j]
		oldParent := int(b.linkAt(slot).Parent) - 1
		if b.frontier == nil {
			f.withdrawBoundary(slot)
		}
		b.remove(slot)
		if b.frontier == nil && oldParent >= 0 {
			f.refreshSelection(oldParent)
			f.offerBoundary(oldParent)
		}
		f.stats.PolicyEvictions++
		f.stats.VictimGroupNodes++
	}
	if victims.priority {
		f.stats.ReserveAdmissions++
		f.stats.ReserveReclaimedBytes += victims.reclaimed
	}
	slot := b.freeSlot()
	if slot < 0 {
		panic("hybridtrie: checked admission has no header")
	}
	if e = b.putLocatedImage(slot, n, image, parent, parentEdge); e != nil {
		return e
	}
	if b.frontier == nil && due {
		f.selection.slots[slot] = selectionSlot{parent: int32(parent), offset: -1}
		if parent >= 0 {
			f.withdrawBoundary(parent)
			f.refreshSelection(parent)
		}
		f.offerBoundary(slot)
	}
	f.selection.use = b.usage
	f.stats.Admissions++
	return nil
}

// Floors are logical priorities over the actually allocated node pool. Round
// proportional floors down, retaining the protected account-root minimum.
func effectiveTypedBudget(b ClassBudget, total uint64) ClassBudget {
	a, s := b.AccountReserveBytes*total/b.TotalBytes, b.StorageReserveBytes*total/b.TotalBytes
	minimum := uint64(typedFrameBytes) + uint64(unsafe.Sizeof(boundaryHeader{}))
	if a < minimum {
		a = minimum
	}
	if s > total-a {
		s = total - a
	}
	return ClassBudget{total, a, s}
}
