package hybridtrie

import "testing"

func TestBoundaryBudgetUsesSinglePoolAllocation(t *testing.T) {
	template := DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
	for _, budget := range []uint64{8 << 20, 16 << 20, 64 << 20} {
		cfg, plan, e := FitBoundaryCache(budget, template)
		if e != nil {
			t.Fatal(e)
		}
		f, e := newBoundaryForest(cfg)
		if e != nil {
			t.Fatal(e)
		}
		actual := f.statsSnapshot()
		if actual.AllocatedBytes() != plan.AllocatedBytes() || actual.AllocatedBytes() > budget || actual.NodeAllocatedBytes != plan.NodeAllocatedBytes || actual.PlanAllocatedBytes != plan.PlanAllocatedBytes || actual.PolicyAllocatedBytes < actual.Boundary.HeatSketchBytes {
			t.Fatal("budget ignored policy indexes/index/heat allocations", budget, plan, actual)
		}
		if budget == 64<<20 {
			t.Logf("requested=%d actual=%d slack=%d slots=%d node=%d policy indexes=%d metadata=%d candidates=%d instance=%d", budget, actual.AllocatedBytes(), budget-actual.AllocatedBytes(), actual.Boundary.Slots, actual.NodeAllocatedBytes, actual.PlanAllocatedBytes, actual.PolicyAllocatedBytes, actual.CandidateAllocatedBytes, actual.InstanceBytes)
		}
	}
	if _, _, e := FitBoundaryCache(1<<20, template); e == nil {
		t.Fatal("undersized budget accepted")
	}
}

// Larger Ethereum background tries can require more than 64 MiB in the live
// node bank alone. The planner still needs to cover its policy indexes and metadata.
func TestBoundaryBudgetAbove64MiB(t *testing.T) {
	c, initial, e := FitBoundaryCache(8<<20, DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary)
	if e != nil {
		t.Fatal(e)
	}
	active := uint64(69363) * initial.Boundary.SlotBytes
	c.NodeBudget = ClassBudget{active, active / 4, active / 4}
	c.PlanBytes = 128 << 20
	c.MetadataBytes = 64 << 20
	a, e := BoundaryCacheAllocation(c)
	if e != nil {
		t.Fatal(e)
	}
	fitted, actual, e := FitBoundaryCache(a.AllocatedBytes(), DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary)
	if e != nil || actual.Boundary.Slots != 69363 || fitted.NodeBudget.TotalBytes != active {
		t.Fatal("slot count", actual.Boundary.Slots, e)
	}
	old := DefaultReservedSharedConfig()
	old.NodeBudget = c.NodeBudget
	if old.valid() {
		t.Fatal("changed legacy shared cache ceiling")
	}
	c.NodeBudget.TotalBytes = maxBoundaryNodeBytes + 1
	if _, e := BoundaryCacheAllocation(c); e == nil {
		t.Fatal("unbounded node bank")
	}
}
