package hybridtrie

import (
	"bytes"
	"errors"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
	"unsafe"
)

// Assert both the connected N tree and its leaf-only H frontier against native
// canonical bodies, not just entry counts or reported counters.
func assertCache(t *testing.T, c *residentCache, root mpt.Hash) {
	t.Helper()
	b := &c.current
	s := c.stats()
	if s.NodeAllocatedBytes > s.NodeBudgetBytes || s.HashAllocatedBytes > s.HashBudgetBytes || s.PlanAllocatedBytes > s.PlanBudgetBytes || s.CandidateAllocatedBytes > s.CandidateBudgetBytes || s.NodeLiveBytes > s.NodeAllocatedBytes || s.HashLiveBytes > s.HashAllocatedBytes {
		t.Fatal("quota", s)
	}
	if uint64(cap(b.nodes))*uint64(unsafe.Sizeof(nodeEntry{}))+uint64(cap(b.body)) > s.NodeAllocatedBytes || uint64(cap(b.hashes))*uint64(unsafe.Sizeof(hashEntry{})) > s.HashAllocatedBytes {
		t.Fatal("backing arrays exceed charge")
	}
	shadow := &c.scratch
	planBytes := uint64(cap(shadow.nodes))*uint64(unsafe.Sizeof(nodeEntry{})) + uint64(cap(shadow.body)) + uint64(cap(shadow.hashes))*uint64(unsafe.Sizeof(hashEntry{})) + uint64(cap(c.pending))*uint64(unsafe.Sizeof(pendingEntry{}))
	candidateBytes := uint64(cap(c.observed.entries))*uint64(unsafe.Sizeof(observedEntry{})) + uint64(cap(c.observed.body))
	p := c.policy
	planBytes += uint64(cap(p.graph))*uint64(unsafe.Sizeof(policyNode{})) + uint64(cap(p.undo))*uint64(unsafe.Sizeof(stateChange{})) + uint64(cap(p.order)+cap(p.temp))*4
	policyBytes := uint64(cap(p.heat.counters))*2 + uint64(cap(p.heat.events))*uint64(unsafe.Sizeof(heatEvent{})) + uint64(cap(p.nodes.slots)+cap(p.hashes.slots))*uint64(unsafe.Sizeof(sampleHandle{}))
	if policyBytes > s.PolicyAllocatedBytes || s.PolicyAllocatedBytes > s.PolicyStatsBudgetBytes {
		t.Fatal("policy backing arrays exceed quota")
	}
	for i := 0; i < p.count; i++ {
		if p.graph[i].Body != nil {
			t.Fatal("planning graph retained body after prepare")
		}
	}
	if planBytes > s.PlanAllocatedBytes || candidateBytes > s.CandidateAllocatedBytes {
		t.Fatal("temporary backing arrays exceed charge")
	}
	if root == mpt.EmptyRootHash {
		if b.n != 0 || b.h != 0 {
			t.Fatal("empty cache retains nodes")
		}
		return
	}
	if b.n == 0 || b.nodes[0].Hash != root || b.nodes[0].Length != 0 || b.nodes[0].Parent != 0 {
		t.Fatal("root")
	}
	seenN, seenH := make([]bool, b.n), make([]bool, b.h)
	seenN[0] = true
	for i := 0; i < b.n; i++ {
		e := &b.nodes[i]
		if !seenN[i] || e.Generation != c.generation {
			t.Fatal("disconnected/stale N", i)
		}
		desc, err := canonicalHash(b.raw(i), e.Hash)
		if err != nil {
			t.Fatal("cached body hash", err)
		}
		for slot, h := range e.Children {
			if h == 0 {
				continue
			}
			ref := desc.Children[slot]
			if !ref.Present || len(ref.Inline) != 0 {
				t.Fatal("independent inline or absent child")
			}
			path := append(append([]byte(nil), e.Path[:e.Length]...), byte(slot))
			if desc.Kind == mpt.CanonicalExtension {
				fragment, _, _ := mpt.DecodeCompactPath(desc.Path)
				path = append(append([]byte(nil), e.Path[:e.Length]...), fragment...)
			}
			if h&hashHandle == 0 {
				j := int(h - 1)
				if j <= i || j >= b.n || seenN[j] {
					t.Fatal("N handle")
				}
				child := b.nodes[j]
				if child.Parent != uint32(i+1) || child.Hash != ref.Hash || !pathEqual(child.Path, child.Length, path) {
					t.Fatal("N edge")
				}
				seenN[j] = true
			} else {
				j := int((h &^ hashHandle) - 1)
				if j < 0 || j >= b.h || seenH[j] {
					t.Fatal("H handle")
				}
				child := b.hashes[j]
				if child.Parent != uint32(i+1) || child.Hash != ref.Hash || child.Generation != c.generation || !pathEqual(child.Path, child.Length, path) {
					t.Fatal("H edge")
				}
				seenH[j] = true
			}
		}
	}
	for _, seen := range seenH {
		if !seen {
			t.Fatal("orphan H")
		}
	}
}

func TestCachePlanConnectedQuotasAndEmptyTransition(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.NodeBytes, cfg.HashBytes = 16<<10, 8<<10
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 256; i++ {
		tree.Update([]byte{byte(i)}, bytes.Repeat([]byte{byte(i)}, 256))
	}
	root, _ := tree.Hash()
	_, raw, _, _ := tree.CanonicalNodeAtPath(nil)
	tree.WalkMaterialized(1000, func(v mpt.LogicalChange) error {
		c.observed.add(v.Path, v.Hash, v.RLP, uint64(len(v.Path)+1))
		return nil
	})
	stats, err := c.prepare(root, raw, 500)
	if err != nil {
		t.Fatal(err)
	}
	c.install()
	assertCache(t, c, root)
	if c.current.n <= 1 || c.current.h == 0 || stats.NodeAdmissions == 0 {
		t.Fatal("no actual N/H admission")
	}
	stats, err = c.prepare(mpt.EmptyRootHash, nil, 501)
	if err != nil || stats.NodeEvictions != uint64(c.current.n) || stats.HashEvictions != uint64(c.current.h) {
		t.Fatal("empty eviction accounting", stats, err)
	}
	c.install()
	assertCache(t, c, mpt.EmptyRootHash)
	if _, err = c.prepare(root, bytes.Repeat([]byte{1}, len(c.current.body)+1), 502); !errors.Is(err, ErrLimit) {
		t.Fatal("root quota", err)
	}
}

func TestCacheConfigBoundsAndCandidateOverflow(t *testing.T) {
	cfg := DefaultCacheConfig()
	for _, change := range []func(*CacheConfig){func(c *CacheConfig) { c.NodeBytes = 0 }, func(c *CacheConfig) { c.PlanBytes = 0 }, func(c *CacheConfig) { c.MaxBlockCharge = 0 }, func(c *CacheConfig) { c.MaxOperations = 0 }, func(c *CacheConfig) { c.HashBytes = 65 << 20 }} {
		bad := cfg
		change(&bad)
		if _, err := newResidentCache(bad); !errors.Is(err, ErrLimit) {
			t.Fatal("accepted invalid budget", bad, err)
		}
	}
	cfg.HashBytes, cfg.CandidateBytes = 0, 0
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	c.observed.add(nil, mpt.Hash{1}, []byte{1}, 1)
	if c.observed.dropped != 1 || len(c.observed.body) != 0 || len(c.current.hashes) != 0 {
		t.Fatal("zero pools")
	}
}

func TestCacheFrequencyPromotionAndDemotion(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.NodeBytes, cfg.HashBytes = 16<<10, 8<<10
	c, err := newResidentCache(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		tree.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 550))
	}
	for h := uint64(1); h < 20; h++ {
		for i := byte(1); i < 16; i++ {
			if err := c.observeBody([]byte{i}, h); err != nil {
				t.Fatal(err)
			}
		}
	}
	root, _ := tree.Hash()
	_, raw, _, _ := tree.CanonicalNodeAtPath(nil)
	if err = tree.WalkMaterialized(32, func(v mpt.LogicalChange) error {
		tick := uint64(1)
		if len(v.Path) != 0 {
			tick += uint64(v.Path[0])
		}
		c.observed.add(v.Path, v.Hash, v.RLP, tick)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = c.prepare(root, raw, 20); err != nil {
		t.Fatal(err)
	}
	c.install()
	assertCache(t, c, root)
	old, body, ok, err := tree.CanonicalNodeAtPath([]byte{0})
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, ok = c.current.findHash([]byte{0}, old); !ok {
		t.Fatal("least recent child not in H")
	}
	for h := uint64(21); h <= 120; h++ {
		if err := c.observeBody([]byte{0}, h); err != nil {
			t.Fatal(err)
		}
	}
	c.observed.add([]byte{0}, old, body, 100)
	stats, err := c.prepare(root, raw, 120)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Promotions != 1 || stats.Demotions != 1 {
		t.Fatal("frontier replacement", stats)
	}
	c.install()
	assertCache(t, c, root)
	if _, ok = c.current.findNode([]byte{0}, old); !ok {
		t.Fatal("recent H body not promoted")
	}
}
