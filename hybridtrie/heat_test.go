package hybridtrie

import (
	"math"
	"testing"
)

func TestHeatDecayAndStaggeredGenerations(t *testing.T) {
	f := initialFrequency(8, 128)
	if got := f.at(256, 64); got != 2 {
		t.Fatal(got)
	}
	if f.Height != 128 {
		t.Fatal("score read changed event time")
	}
	f.event(256, 64)
	f.event(256, 64)
	if f.Value != 3 {
		t.Fatal("same-block event double counted", f)
	}
	cfg := DefaultCacheConfig()
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := &c.policy.heat
	s.begin(128)
	s.begin(256)
	// At h=256 sketch A is age 1, B is age 129, as in the paper.
	if s.start != [2]uint64{256, 128} {
		t.Fatal(s.start)
	}
	width := int(s.cfg.SketchWidth)
	for i := 0; i < 4*width; i++ {
		s.counters[i] = 1
		s.counters[4*width+i] = 24
	}
	if score := s.estimate(0, []byte{1}); score < 12.9 || score > 13.1 {
		t.Fatal("paper success score", score)
	}
	for i := 4 * width; i < len(s.counters); i++ {
		s.counters[i] = 8
	}
	if score := s.estimate(0, []byte{1}); score < 4.3 || score > 4.4 {
		t.Fatal("paper rejection score", score)
	}
	s.begin(384)
	if s.start != [2]uint64{256, 384} {
		t.Fatal(s.start)
	}
	if s.counters[0] != 1 || s.counters[4*width] != 0 {
		t.Fatal("wrong sketch reset")
	}
	// Starting late must not invent observations before startup.
	late, _, _, err := newCachePolicy(DefaultPolicyConfig(), 10, 10, 10, 1<<16, 1<<16, 1<<16, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	late.heat.begin(250)
	if late.heat.start != [2]uint64{250, 250} || late.heat.estimate(0, nil) != 0 {
		t.Fatal("startup history fabricated")
	}
	late.heat.begin(256)
	if late.heat.start != [2]uint64{256, 250} {
		t.Fatal(late.heat.start)
	}
}
func TestHeatDedupNamespacesAndSaturation(t *testing.T) {
	cfg := DefaultCacheConfig()
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.trieID = 7
	for i := 0; i < 5; i++ {
		if err = c.observeBody([]byte{3}, 100); err != nil {
			t.Fatal(err)
		}
	}
	if c.policy.heat.accessEvents != 1 || math.Abs(c.policy.heat.estimate(7, []byte{3})-1) > 1e-10 {
		t.Fatal("CMS dedup")
	}
	if c.policy.heat.estimate(8, []byte{3}) != 0 {
		t.Fatal("namespace key omitted")
	}
	c.current.n = 1
	c.current.nodes[0] = nodeEntry{Length: 1}
	c.current.nodes[0].Path[0] = 4
	for i := 0; i < 5; i++ {
		c.observeBody([]byte{4}, 100)
	}
	if c.current.nodes[0].A.Value != 1 || c.policy.heat.estimate(7, []byte{4}) != 0 {
		t.Fatal("resident event also sent to CMS")
	}
	for i := range c.policy.heat.counters {
		c.policy.heat.counters[i] = 65535
	}
	c.policy.heat.increment(7, []byte{3})
	for _, n := range c.policy.heat.counters {
		if n != 65535 {
			t.Fatal("counter wrapped")
		}
	}
	if score := c.policy.heat.estimate(7, []byte{3}); score > 1.00001 {
		t.Fatal("count not capped to sketch age", score)
	}
}

func TestPolicyMemoryAndConfig(t *testing.T) {
	cfg := DefaultCacheConfig()
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := c.stats()
	t.Logf("N=%d H=%d candidates=%d plan=%d statistics=%d instance=%d (bytes)", s.NodeAllocatedBytes, s.HashAllocatedBytes, s.CandidateAllocatedBytes, s.PlanAllocatedBytes, s.PolicyAllocatedBytes, s.InstanceBytes)
	for _, change := range []func(*PolicyConfig){func(p *PolicyConfig) { p.HalfLife = 0 }, func(p *PolicyConfig) { p.SketchPeriod = 255 }, func(p *PolicyConfig) { p.SketchWidth = 0 }, func(p *PolicyConfig) { p.EventCapacity = 0 }, func(p *PolicyConfig) { p.NodePool = 0 }, func(p *PolicyConfig) { p.StatsBytes = 0 }, func(p *PolicyConfig) { p.Epsilon = math.NaN() }} {
		bad := cfg
		change(&bad.Policy)
		if _, err := newResidentCache(bad); err == nil {
			t.Fatal("invalid policy accepted", bad.Policy)
		}
	}
	tiny := cfg
	tiny.Policy.EventCapacity = 1
	limited, err := newResidentCache(tiny)
	if err != nil {
		t.Fatal(err)
	}
	if err = limited.observeBody([]byte{1}, 1); err != nil {
		t.Fatal(err)
	}
	if err = limited.observeBody([]byte{2}, 1); err == nil {
		t.Fatal("event table overflow accepted")
	}
}
