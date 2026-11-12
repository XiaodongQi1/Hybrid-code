package main

import (
	"bytes"
	"encoding/json"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/comparison"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestParallelProofTemplateAndDefaultIdentity(t *testing.T) {
	var c config
	if e := strictFile("../../../artifact/configs/hybrid-compare-parallel-proof.example.json", &c); e != nil {
		t.Fatal(e)
	}
	if e := c.ParallelProof.Validate(); e != nil || !c.ParallelProof.Enabled || c.ParallelProof.Workers != 2 {
		t.Fatal(c.ParallelProof, e)
	}
	raw, e := json.Marshal(config{})
	if e != nil || bytes.Contains(raw, []byte("ParallelProof")) {
		t.Fatal("zero policy changed old config identity", e, string(raw))
	}
	for _, n := range []uint32{3, 5, 8} {
		c.ParallelProof.Workers = n
		if c.ParallelProof.Validate() == nil {
			t.Fatal("unsupported worker count")
		}
	}
}

func TestAdaptiveBoundaryOverridesPreserveDefaultsAndBudget(t *testing.T) {
	var c config
	if e := json.Unmarshal([]byte(`{"HybridBoundary":{"ResidentLayout":"typed","VictimEligibility":"frontier","ReservationPolicy":"borrow"}}`), &c); e != nil {
		t.Fatal(e)
	}
	if c.HybridBoundary == nil || c.HybridBoundary.MaxNodeBytes != 532 || c.HybridBoundary.PolicyIntervalBlocks != 16 {
		t.Fatal("default lost")
	}
	template := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	template.Boundary = ht.BoundaryConfig(*c.HybridBoundary)
	budget := comparison.DefaultResourceBudget()
	budget.ApplicationCacheBytes = 8 << 20
	p, e := budget.Resolve(kvstore.LevelDB, template)
	if e != nil {
		t.Fatal(e)
	}
	if p.HybridAllocation.Boundary.HeaderBytes != 80 || p.Hybrid.AllocationQuantumBytes == 0 || p.Hybrid.ApplicationAllocatedBytes > budget.ApplicationCacheBytes || p.Archive.ApplicationAllocatedBytes > p.CommonApplicationTargetBytes || p.Path.ApplicationAllocatedBytes > p.CommonApplicationTargetBytes {
		t.Fatal("unmatched budget", p)
	}
	if e = json.Unmarshal([]byte(`{"HybridBoundary":{"Unexpected":true}}`), &c); e == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestExecutionReadTemplateAndExplicitExtraBudget(t *testing.T) {
	var c config
	if err := strictFile("../../../artifact/configs/hybrid-compare-async.example.json", &c); err != nil {
		t.Fatal(err)
	}
	template := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	template.Boundary = ht.BoundaryConfig(*c.HybridBoundary)
	budget := comparison.DefaultResourceBudget()
	budget.ApplicationCacheBytes = 8 << 20
	plan, err := budget.Resolve(kvstore.LevelDB, template)
	if err != nil {
		t.Fatal(err)
	}
	reads := plan.HybridCache.Boundary.ExecutionReads
	if !c.ParallelProof.Enabled || c.ParallelProof.Workers != 2 || c.ParallelProof.Validate() != nil {
		t.Fatal("combined template omitted the query switch")
	}
	if !reads.Enabled || reads.Workers != 1 || plan.Hybrid.ExecutionReadBudgetBytes <= reads.BodyBytes || plan.Hybrid.ExecutionReadBudgetBytes != plan.HybridAllocation.ExecutionReadBudgetBytes {
		t.Fatal("read budget missing or folded into resident allocation", plan)
	}
	if plan.Hybrid.ApplicationAllocatedBytes > budget.ApplicationCacheBytes {
		t.Fatal("resident cache budget exceeded")
	}
	for _, bad := range []ht.ExecutionReadConfig{{Enabled: true}, {Enabled: true, Workers: 9, Queue: 16, Entries: 64, BodyBytes: 4096}} {
		template.Boundary.ExecutionReads = bad
		if _, err = budget.Resolve(kvstore.LevelDB, template); err == nil {
			t.Fatal("invalid read bounds accepted")
		}
	}
}
