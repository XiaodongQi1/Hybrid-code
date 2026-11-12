package hybridtrie

import (
	"math"
	"testing"
	"unsafe"
)

func TestForestQuotaExhaustive(t *testing.T) {
	// Independent formulation: each class has a private allocation; all overflow
	// competes for the residual. This also exercises every rounding boundary.
	for total := uint64(0); total <= 12; total++ {
		for a := uint64(0); a <= total; a++ {
			for s := uint64(0); s <= total-a; s++ {
				b := ClassBudget{total, a, s}
				for x := uint64(0); x <= total+1; x++ {
					for y := uint64(0); y <= total+1; y++ {
						privateA, privateS := x, y
						if privateA > a {
							privateA = a
						}
						if privateS > s {
							privateS = s
						}
						want := x+y-privateA-privateS <= total-a-s
						if b.fits([2]uint64{x, y}) != want {
							t.Fatal(b, x, y, want)
						}
					}
				}
			}
		}
	}
	if (ClassBudget{math.MaxUint64, 1, 1}).fits([2]uint64{math.MaxUint64, 2}) {
		t.Fatal("overflow accepted")
	}
}
func TestForestSharedAllocationAndConfig(t *testing.T) {
	c := DefaultReservedSharedConfig()
	f, err := newForestShared(c)
	if err != nil {
		t.Fatal(err)
	}
	bankN := uint64(cap(f.current.Nodes))*uint64(unsafe.Sizeof(forestN{})) + uint64(cap(f.current.Body))
	bankH := uint64(cap(f.current.Hashes)) * uint64(unsafe.Sizeof(forestH{}))
	plan := bankN + bankH + uint64(cap(f.scratch.Roots))*uint64(unsafe.Sizeof(forestRoot{})) + uint64(cap(f.graph))*uint64(unsafe.Sizeof(forestPlanNode{})) + uint64(cap(f.undo))*uint64(unsafe.Sizeof(forestUndo{})) + uint64(cap(f.roots)+cap(f.order)+cap(f.demotions))*4 + uint64(cap(f.fenwick))*8 + uint64(cap(f.index)+cap(f.scratch.Index))*4
	meta := uint64(cap(f.current.Roots))*uint64(unsafe.Sizeof(forestRoot{})) + uint64(cap(f.heat.events))*uint64(unsafe.Sizeof(forestEvent{})) + uint64(cap(f.current.Index))*4
	for _, s := range f.heat.sketch {
		meta += uint64(cap(s.counters)) * 2
	}
	if bankN > f.nodeAllocated || bankH > f.hashAllocated || plan > f.planAllocated || meta > f.metadataAllocated || f.planAllocated > c.PlanBytes || meta > c.MetadataBytes {
		t.Fatal("unbilled allocation", bankN, bankH, plan, meta, f.cacheStats())
	}
	for _, change := range []func(*ReservedSharedConfig){func(c *ReservedSharedConfig) { c.NodeBudget.AccountReserveBytes = c.NodeBudget.TotalBytes }, func(c *ReservedSharedConfig) { c.PlanBytes = 1 }, func(c *ReservedSharedConfig) { c.MetadataBytes = 1 }, func(c *ReservedSharedConfig) { c.Epsilon = math.NaN() }, func(c *ReservedSharedConfig) { c.MaxResidentTries = 0 }} {
		bad := c
		change(&bad)
		if _, err := newForestShared(bad); err == nil {
			t.Fatal("accepted invalid config", bad)
		}
	}
	// Event identity includes both namespace and position, unlike the single
	// trie event table. Event exhaustion is an optional-cache miss, not an error.
	f.begin(10)
	if !f.event(position(0, []byte{1})) || !f.event(position(1, []byte{1})) || f.event(position(0, []byte{1})) {
		t.Fatal("event isolation")
	}
	for i := 0; i < int(c.EventCapacity)+10; i++ {
		f.event(position(uint64(i+10), nil))
	}
	if f.stats.DroppedEvents == 0 {
		t.Fatal("missing bounded event fallback")
	}
}
