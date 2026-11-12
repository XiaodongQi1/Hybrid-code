//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func TestPolicyRandomBlocksAcrossResets(t *testing.T) {
	for _, mode := range []string{"default", "collisions", "budget-zero"} {
		t.Run(mode, func(t *testing.T) {
			cfg := DefaultCacheConfig()
			cfg.NodeBytes = 16 << 10
			cfg.HashBytes = 8 << 10
			if mode == "collisions" {
				cfg.Policy.SketchWidth = 1
				cfg.Policy.WorkUnits = 128
				cfg.Policy.Seed = 83
			}
			if mode == "budget-zero" {
				cfg.Policy.WorkUnits = 0
			}
			s, c, native, want := cachedFixture(t, cfg, 64)
			random := rand.New(rand.NewSource(17))
			for h := uint64(1); h <= 280; h++ {
				before, _ := c.Stats()
				b, err := c.BeginBlock(h)
				if err != nil {
					t.Fatal(h, err)
				}
				for op := 0; op < 3; op++ {
					key := sha256.Sum256([]byte(fmt.Sprint(random.Intn(88))))
					value := bytes.Repeat([]byte{byte(h), byte(op)}, random.Intn(50)+1)
					if random.Intn(4) == 0 {
						value = nil
					}
					if err = b.Update(key[:], value); err != nil {
						t.Fatal(h, err)
					}
					native.Update(key[:], value)
					if len(value) == 0 {
						delete(want, string(key[:]))
					} else {
						want[string(key[:])] = value
					}
				}
				root, _ := native.Hash()
				got, err := b.Root()
				if err != nil || got != root {
					t.Fatal("root", h, err)
				}
				if _, _, err = b.Commit(); err != nil {
					t.Fatal(h, err)
				}
				assertCache(t, c.cache, root)
				after, _ := c.Stats()
				if after.WorkUnits-before.WorkUnits > uint64(cfg.Policy.WorkUnits) {
					t.Fatal("work budget exceeded", h, after.WorkUnits-before.WorkUnits)
				}
				if h%70 == 0 {
					keys := make([][]byte, 0, 4)
					for i := 0; i < 4; i++ {
						k := sha256.Sum256([]byte(fmt.Sprint(i)))
						keys = append(keys, append([]byte(nil), k[:]...))
					}
					assertProofs(t, s, h, root, want, keys)
				}
			}
			stats, _ := c.Stats()
			if stats.SketchResets != 4 {
				t.Fatal("staggered resets", stats.SketchResets)
			}
			t.Logf("blocks=280 work=%d cascades=%d rollbacks=%d forced=%d resets=%d N/H=%d/%d bytes plan=%d policy=%d", stats.WorkUnits, stats.CascadeNodes, stats.NodeRollbacks+stats.HashRollbacks, stats.ForcedRepairs, stats.SketchResets, stats.NodeAllocatedBytes, stats.HashAllocatedBytes, stats.PlanAllocatedBytes, stats.PolicyAllocatedBytes)
		})
	}
}
func TestPolicyNetUpdateAndBodyAccessEvents(t *testing.T) {
	_, c, _, want := cachedFixture(t, DefaultCacheConfig(), 16)
	key := sha256.Sum256([]byte("3"))
	original := want[string(key[:])]
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	b.Get(key[:])
	b.Get(key[:])
	b.Update(key[:], []byte{9})
	b.Update(key[:], original)
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	root := c.cache.current.nodes[0]
	if root.U.at(1, 64) != 0 || root.A.Value != 1 || root.A.Height != 1 {
		t.Fatal("physical work counted as logical update/access twice", root.A, root.U)
	}
	b, err = c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	b.Update(key[:], []byte{10})
	b.Update(key[:], []byte{11})
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	root = c.cache.current.nodes[0]
	if root.U.Value != 1 || root.U.Height != 2 {
		t.Fatal("logical update not deduplicated", root.U)
	}
	// A failed/retried block at the same height must not count its CMS events twice.
	b, err = c.BeginBlock(3)
	if err != nil {
		t.Fatal(err)
	}
	b.Get(key[:])
	events := c.cache.policy.heat.accessEvents
	b.Abort()
	b, err = c.BeginBlock(3)
	if err != nil {
		t.Fatal(err)
	}
	b.Get(key[:])
	b.Abort()
	if c.cache.policy.heat.accessEvents != events {
		t.Fatal("retry counted another event at same height")
	}
}
func TestPolicyForcedGrowthIgnoresOptionalBudget(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.NodeBytes = 16 << 10
	s, c, native, _ := cachedFixture(t, cfg, 16)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		key := sha256.Sum256([]byte(fmt.Sprint(i)))
		if _, err = b.Get(key[:]); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	var key []byte
	var path []byte
	for i := 1; i < c.cache.current.n; i++ {
		e := c.cache.current.nodes[i]
		desc, err := mpt.InspectCanonicalNode(c.cache.current.raw(i))
		if err != nil {
			t.Fatal(err)
		}
		if desc.Kind != mpt.CanonicalLeaf {
			continue
		}
		tail, _, _ := mpt.DecodeCompactPath(desc.Path)
		nibbles := append(append([]byte(nil), e.Path[:e.Length]...), tail...)
		if len(nibbles) != 64 {
			continue
		}
		key = make([]byte, 32)
		for j := range key {
			key[j] = nibbles[2*j]<<4 | nibbles[2*j+1]
		}
		path = append([]byte(nil), e.Path[:e.Length]...)
		break
	}
	if key == nil {
		t.Fatal("fixture did not admit leaf")
	}
	c.cache.policy.cfg.WorkUnits = 0
	before, _ := c.Stats()
	b, err = c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte{3}, 9<<10)
	if err = b.Update(key, value); err != nil {
		t.Fatal(err)
	}
	native.Update(key, value)
	expected, _ := native.Hash()
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	stats, _ := c.Stats()
	if c.root != expected || stats.ForcedRepairs <= before.ForcedRepairs || stats.WorkUnits != before.WorkUnits {
		t.Fatal("forced growth obeyed optional budget", stats)
	}
	if i, ok := c.cache.current.nodePosition(path); ok && len(c.cache.current.raw(i)) > 8<<10 {
		t.Fatal("oversize body retained")
	}
	assertCache(t, c.cache, expected)
	assertProofs(t, s, 2, expected, map[string][]byte{string(key): value}, [][]byte{key})
}
