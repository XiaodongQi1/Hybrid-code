//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"errors"
	"math"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

type identityWrite struct{ key, value []byte }
type identitySnapshot struct {
	height uint64
	root   mpt.Hash
	state  map[string][]byte
}

func identityCommit(t *testing.T, c *CachedTrie, native *mpt.Trie, want map[string][]byte, h uint64, writes []identityWrite) identitySnapshot {
	t.Helper()
	b, err := c.BeginBlock(h)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range writes {
		if err = b.Update(w.key, w.value); err != nil {
			t.Fatal(err)
		}
		if err = native.Update(w.key, w.value); err != nil {
			t.Fatal(err)
		}
		if len(w.value) == 0 {
			delete(want, string(w.key))
		} else {
			want[string(w.key)] = append([]byte(nil), w.value...)
		}
	}
	expected, err := native.Hash()
	if err != nil {
		t.Fatal(err)
	}
	root, err := b.Root()
	if err != nil || root != expected {
		t.Fatal("identity change modified root", err)
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	assertCache(t, c.cache, root)
	snapshot := identitySnapshot{h, root, map[string][]byte{}}
	for k, v := range want {
		snapshot.state[k] = append([]byte(nil), v...)
	}
	return snapshot
}
func hotIdentityNode(t *testing.T, c *CachedTrie, path []byte) nodeEntry {
	t.Helper()
	i, ok := c.cache.current.nodePosition(path)
	if !ok {
		t.Fatal("host not in N", path)
	}
	c.cache.current.nodes[i].A = initialFrequency(100, c.head.Height)
	c.cache.current.nodes[i].U = initialFrequency(70, c.head.Height)
	return c.cache.current.nodes[i]
}
func identityNode(t *testing.T, c *CachedTrie, path []byte) nodeEntry {
	t.Helper()
	i, ok := c.cache.current.nodePosition(path)
	if !ok {
		t.Fatal("host not retained", path)
	}
	return c.cache.current.nodes[i]
}
func TestIdentityRootTransitionsAndOrdinaryUpdates(t *testing.T) {
	v := bytes.Repeat([]byte{1}, 64)
	next := bytes.Repeat([]byte{2}, 64)
	pairs := func(keys ...[]byte) []identityWrite {
		var out []identityWrite
		for _, k := range keys {
			out = append(out, identityWrite{k, v})
		}
		return out
	}
	for _, tc := range []struct {
		name            string
		initial, writes []identityWrite
		reset           bool
	}{
		{"leaf-branch", pairs([]byte{0x10}), []identityWrite{{[]byte{0x20}, v}}, true},
		{"branch-leaf", pairs([]byte{0x10}, []byte{0x20}), []identityWrite{{[]byte{0x20}, nil}}, true},
		{"leaf-extension", pairs([]byte{0x10}), []identityWrite{{[]byte{0x11}, v}}, true},
		{"extension-leaf", pairs([]byte{0x10}, []byte{0x11}), []identityWrite{{[]byte{0x11}, nil}}, true},
		{"extension-shorter", pairs([]byte{0x12, 0x30}, []byte{0x12, 0x31}), []identityWrite{{[]byte{0x12, 0x40}, v}}, true},
		{"extension-longer", pairs([]byte{0x12, 0x30}, []byte{0x12, 0x31}, []byte{0x12, 0x40}), []identityWrite{{[]byte{0x12, 0x40}, nil}}, true},
		{"leaf-new-suffix", pairs([]byte{0x10}), []identityWrite{{[]byte{0x10}, nil}, {[]byte{0x20}, v}}, true},
		{"leaf-value", pairs([]byte{0x10}), []identityWrite{{[]byte{0x10}, next}}, false},
		{"branch-occupancy", pairs([]byte{0x10}, []byte{0x20}), []identityWrite{{[]byte{0x30}, v}}, false},
		{"extension-child-digest", pairs([]byte{0x12, 0x30}, []byte{0x12, 0x31}), []identityWrite{{[]byte{0x12, 0x30}, next}}, false},
		{"temporary-split-net-unchanged", pairs([]byte{0x10}), []identityWrite{{[]byte{0x20}, v}, {[]byte{0x20}, nil}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
			first := identityCommit(t, c, native, want, 1, tc.initial)
			old := hotIdentityNode(t, c, nil)
			second := identityCommit(t, c, native, want, 2, tc.writes)
			current := identityNode(t, c, nil)
			changed := second.root != first.root
			if tc.reset {
				estimate := c.cache.policy.heat.estimate(c.id, nil)
				if math.Abs(current.A.Value-estimate) > 1e-10 || current.A.Value >= 3 || current.U.Value != 1 || current.U.Height != 2 {
					t.Fatal("new host inherited exact history or double-counted candidate", current.A, current.U, estimate)
				}
				if current.Allocation == old.Allocation {
					t.Fatal("new host kept stale allocation")
				}
				stats, _ := c.Stats()
				if stats.IdentityResets == 0 {
					t.Fatal("reset not recorded")
				}
			} else {
				expectedA := old.A
				expectedA.event(2, 64)
				expectedU := old.U
				if changed {
					expectedU.event(2, 64)
				}
				if current.A != expectedA || current.U != expectedU {
					t.Fatal("ordinary update lost heat", current.A, current.U, expectedA, expectedU)
				}
				if !changed && current.Allocation != old.Allocation {
					t.Fatal("net-unchanged identity invalidated")
				}
			}
			keys := [][]byte{{0x10}, {0x11}, {0x20}, {0x12, 0x30}, {0x12, 0x31}, {0x12, 0x40}}
			assertProofs(t, s, first.height, first.root, first.state, keys)
			assertProofs(t, s, second.height, second.root, second.state, keys)
		})
	}
}
func TestIdentityUnchangedDescendantSurvivesAncestorSplit(t *testing.T) {
	v := bytes.Repeat([]byte{1}, 64)
	s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
	first := identityCommit(t, c, native, want, 1, []identityWrite{{[]byte{0x12, 0x30}, v}, {[]byte{0x12, 0x31}, v}})
	old := hotIdentityNode(t, c, []byte{1, 2, 3})
	hotIdentityNode(t, c, nil)
	second := identityCommit(t, c, native, want, 2, []identityWrite{{[]byte{0x12, 0x40}, v}})
	current := identityNode(t, c, []byte{1, 2, 3})
	if current.A != old.A || current.U != old.U || current.Hash != old.Hash || current.Allocation != old.Allocation {
		t.Fatal("untouched descendant inherited ancestor reset")
	}
	keys := [][]byte{{0x12, 0x30}, {0x12, 0x31}, {0x12, 0x40}}
	assertProofs(t, s, 1, first.root, first.state, keys)
	assertProofs(t, s, 2, second.root, second.state, keys)
}
func TestIdentityReplacementRequiresAdmissionAtZeroBudget(t *testing.T) {
	for _, startH := range []bool{false, true} {
		t.Run(map[bool]string{false: "old-N", true: "old-H"}[startH], func(t *testing.T) {
			v := bytes.Repeat([]byte{1}, 64)
			s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
			first := identityCommit(t, c, native, want, 1, []identityWrite{{[]byte{0x10}, v}, {[]byte{0x20}, v}})
			if startH {
				c.Close()
				var err error
				c, err = OpenCachedTrie(s, 0, first.root, DefaultCacheConfig())
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				if _, ok := c.cache.current.hashPosition([]byte{1}); !ok {
					t.Fatal("fixture did not retain H")
				}
			} else {
				hotIdentityNode(t, c, []byte{1})
			}
			c.cache.policy.cfg.WorkUnits = 0
			second := identityCommit(t, c, native, want, 2, []identityWrite{{[]byte{0x11}, v}})
			if _, ok := c.cache.current.nodePosition([]byte{1}); ok {
				t.Fatal("new host inherited N residency")
			}
			if _, ok := c.cache.current.hashPosition([]byte{1}); ok {
				t.Fatal("new host inherited H residency")
			}
			stats, _ := c.Stats()
			if stats.IdentityResets == 0 {
				t.Fatal("missing structural reset")
			}
			keys := [][]byte{{0x10}, {0x11}, {0x20}}
			assertProofs(t, s, 1, first.root, first.state, keys)
			assertProofs(t, s, 2, second.root, second.state, keys)
		})
	}
}
func TestIdentityRetryDedupAndRepairToOriginal(t *testing.T) {
	for _, repair := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry-new-host", true: "repair-original-host"}[repair], func(t *testing.T) {
			v := bytes.Repeat([]byte{1}, 64)
			s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
			first := identityCommit(t, c, native, want, 1, []identityWrite{{[]byte{0x10}, v}})
			old := hotIdentityNode(t, c, nil)
			b, err := c.BeginBlock(2)
			if err != nil {
				t.Fatal(err)
			}
			big := bytes.Repeat([]byte{2}, 8<<10)
			if err = b.Update([]byte{0x20}, big); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			limit := s.cfg.MaxBatchBytes
			s.cfg.MaxBatchBytes = 4096
			s.mu.Unlock()
			if _, _, err = b.Commit(); !errors.Is(err, ErrLimit) {
				t.Fatal("expected prewrite limit", err)
			}
			failed := identityNode(t, c, nil)
			if failed.Hash != old.Hash || failed.Allocation != old.Allocation || failed.U != old.U {
				t.Fatal("failed attempt published replacement")
			}
			estimate := c.cache.policy.heat.estimate(c.id, nil)
			s.mu.Lock()
			s.cfg.MaxBatchBytes = limit
			s.mu.Unlock()
			if repair {
				if err = b.Delete([]byte{0x20}); err != nil {
					t.Fatal(err)
				}
			} else {
				native.Update([]byte{0x20}, big)
				want[string([]byte{0x20})] = big
			}
			if _, _, err = b.Commit(); err != nil {
				t.Fatal(err)
			}
			current := identityNode(t, c, nil)
			if c.cache.policy.heat.estimate(c.id, nil) != estimate {
				t.Fatal("retry incremented replacement CMS twice")
			}
			expected, _ := native.Hash()
			if c.root != expected {
				t.Fatal("retry root mismatch")
			}
			if repair {
				if current.U != old.U || current.Allocation != old.Allocation || current.A != failed.A {
					t.Fatal("abandoned replacement reset the original host", current)
				}
			} else if current.U.Value != 1 || current.A.Value != estimate || current.Allocation == old.Allocation {
				t.Fatal("retry lost replacement identity")
			}
			keys := [][]byte{{0x10}, {0x20}}
			assertProofs(t, s, 1, first.root, first.state, keys)
			assertProofs(t, s, 2, c.root, want, keys)
		})
	}
}

func TestIdentityInlineHostRemovalAndReappearance(t *testing.T) {
	s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
	keys := [][]byte{{0x10}, {0x11}}
	first := identityCommit(t, c, native, want, 1, []identityWrite{{keys[0], []byte{1}}, {keys[1], []byte{1}}})
	if _, ok := c.cache.current.nodePosition([]byte{1}); ok {
		t.Fatal("inline branch gained an independent identity")
	}
	big := bytes.Repeat([]byte{3}, 64)
	second := identityCommit(t, c, native, want, 2, []identityWrite{{keys[0], big}})
	old := hotIdentityNode(t, c, []byte{1})
	c.cache.policy.cfg.WorkUnits = 0
	third := identityCommit(t, c, native, want, 3, []identityWrite{{keys[0], []byte{1}}})
	if _, ok := c.cache.current.nodePosition([]byte{1}); ok {
		t.Fatal("host becoming inline retained N")
	}
	if _, ok := c.cache.current.hashPosition([]byte{1}); ok {
		t.Fatal("host becoming inline retained H")
	}
	c.cache.policy.cfg.WorkUnits = 4096
	fourth := identityCommit(t, c, native, want, 4, []identityWrite{{keys[0], big}})
	current := identityNode(t, c, []byte{1})
	if current.Hash != old.Hash || current.Allocation == old.Allocation || current.U.Value != 1 || current.A.Value >= 3 {
		t.Fatal("same-content reappearance reused old host statistics/token", current)
	}
	for _, snapshot := range []identitySnapshot{first, second, third, fourth} {
		assertProofs(t, s, snapshot.height, snapshot.root, snapshot.state, keys)
	}
}
