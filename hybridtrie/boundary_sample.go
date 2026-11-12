package hybridtrie

import (
	"math/rand"
	"unsafe"
)

// Eligibility is block-local. Reset only previous pool members and index
// changed slots; the committed free list and usage counters are persistent.
type boundarySelection struct {
	slots []selectionSlot
	pools [2][]int32
	use   [2]uint64
	swaps [64]struct{ class, index int32 }
}

type selectionSlot struct {
	parent   int32
	offset   int32 // index in its class pool, or -1
	children uint8
	class    uint8
}

func selectionBytes(slots, writes int) uint64 {
	return uint64(slots)*uint64(unsafe.Sizeof(selectionSlot{})) + uint64(writes)*8
}
func newSelection(n, w int) boundarySelection {
	s := boundarySelection{slots: make([]selectionSlot, n), pools: [2][]int32{make([]int32, 0, w), make([]int32, 0, w)}}
	for i := range s.slots {
		s.slots[i].offset = -1
		s.slots[i].parent = -1
	}
	return s
}
func (f *boundaryForest) refreshSelection(i int) {
	link := f.current.linkAt(i)
	s := &f.selection
	s.slots[i].parent = link.Parent - 1
	s.slots[i].children = 0
	for child := link.FirstChild; child != 0; child = f.current.linkAt(int(child - 1)).NextSibling {
		s.slots[i].children++
	}
}
func (f *boundaryForest) indexSelection(due bool) error {
	s := &f.selection
	if f.current.frontier != nil {
		s.use = f.current.usage
		return nil
	}
	for c := range s.pools {
		for _, i := range s.pools[c] {
			s.slots[i].offset = -1
		}
		s.pools[c] = s.pools[c][:0]
		s.use[c] = f.current.usage[c]
	}
	if due {
		for _, i := range f.current.changed {
			f.refreshSelection(int(i))
			f.offerBoundary(int(i))
		}
	}
	return nil
}

func (f *boundaryForest) offerBoundary(i int) {
	link := f.current.linkAt(i)
	s := &f.selection
	if !f.current.usedAt(i) || !f.current.updatedAt(i) || s.slots[i].children != 0 || s.slots[i].offset >= 0 || f.current.Roots[link.Root].ID == AccountTrieID && link.Parent == 0 {
		return
	}
	c := cacheClass(f.current.Roots[link.Root].ID)
	s.slots[i].class = uint8(c)
	s.slots[i].offset = int32(len(s.pools[c]))
	s.pools[c] = append(s.pools[c], int32(i))
}

func (f *boundaryForest) withdrawBoundary(i int) {
	s := &f.selection
	offset := s.slots[i].offset
	if offset < 0 {
		return
	}
	c := cacheClass(f.current.Roots[f.current.linkAt(i).Root].ID)
	pool := s.pools[c]
	last := pool[len(pool)-1]
	pool[offset] = last
	s.slots[last].offset = offset
	s.slots[i].offset = -1
	s.pools[c] = pool[:len(pool)-1]
}

// SplitMix64 supplies a reproducible per-block stream; rand.Intn handles
// unbiased range reduction. No process-global RNG or buffered source.
type boundarySeed uint64

func (s *boundarySeed) Seed(seed int64) { *s = boundarySeed(seed) }
func (s *boundarySeed) Int63() int64 {
	*s += 0x9e3779b97f4a7c15
	x := uint64(*s)
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return int64((x ^ (x >> 31)) >> 1)
}

// Draw without replacement from the union of permitted classes. Temporarily
// shuffle class tails and restore every swap before callers use reverse indexes.
func (f *boundaryForest) sampledVictim(request, parent int, after [2]uint64, rng *rand.Rand, work *uint64) (victim int, exhausted bool) {
	s := &f.selection
	var count [2]int
	for class, pool := range s.pools {
		if len(pool) == 0 {
			continue
		}
		next := after
		next[class] -= f.charge()
		if f.cfg.NodeBudget.fits(next) && (class == request || next[class] >= minBoundary(s.use[class], f.cfg.NodeBudget.reserve(class))) {
			count[class] = len(pool)
		}
	}
	protected := -1
	if parent >= 0 && s.slots[parent].offset >= 0 && count[request] > 0 {
		protected = int(s.slots[parent].offset)
		count[request]--
	}
	total := count[0] + count[1]
	draw := int(f.cfg.SampleSize)
	if draw > total {
		draw = total
	}
	if uint64(draw) > *work {
		return -1, true
	}
	*work -= uint64(draw)
	f.stats.SampleAttempts++
	f.stats.EligibleSum += uint64(total)
	f.stats.SampleComparisons += uint64(draw)
	if total == 0 {
		f.stats.EmptySamples++
		return -1, false
	}
	if draw == total {
		victim = -1
		for class, n := range count {
			if n == 0 {
				continue
			}
			for _, slot := range s.pools[class] {
				i := int(slot)
				if i != parent && (victim < 0 || boundaryLess(f.current.observation(i), f.current.observation(victim), f.heat.Height, f.cfg.HalfLife)) {
					victim = i
				}
			}
		}
		return victim, false
	}
	if protected >= 0 {
		p := s.pools[request]
		last := count[request]
		p[protected], p[last] = p[last], p[protected]
	}
	swaps := &s.swaps
	victim = -1
	for j := 0; j < draw; j++ {
		k := rng.Intn(total - j)
		class := 0
		if k >= count[0] {
			class = 1
			k -= count[0]
		}
		p := s.pools[class]
		i := int(p[k])
		if victim < 0 || boundaryLess(f.current.observation(i), f.current.observation(victim), f.heat.Height, f.cfg.HalfLife) {
			victim = i
		}
		count[class]--
		last := count[class]
		p[k], p[last] = p[last], p[k]
		swaps[j].class, swaps[j].index = int32(class), int32(k)
	}
	for j := draw - 1; j >= 0; j-- {
		class, k := swaps[j].class, swaps[j].index
		p := s.pools[class]
		last := count[class]
		p[k], p[last] = p[last], p[k]
		count[class]++
	}
	if protected >= 0 {
		p := s.pools[request]
		last := count[request]
		p[protected], p[last] = p[last], p[protected]
	}
	return victim, false
}
