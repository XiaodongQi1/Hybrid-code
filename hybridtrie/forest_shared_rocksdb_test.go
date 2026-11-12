//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func assertShared(t *testing.T, f *CachedForest) {
	t.Helper()
	c := f.shared
	s, _ := f.Stats()
	u := s.Shared
	if !c.cfg.NodeBudget.fits([2]uint64{u.Account.NodeLiveBytes, u.Storage.NodeLiveBytes}) || !c.cfg.HashBudget.fits([2]uint64{u.Account.HashLiveBytes, u.Storage.HashLiveBytes}) || s.NodeAllocatedBytes > c.cfg.NodeBudget.TotalBytes || s.HashAllocatedBytes > c.cfg.HashBudget.TotalBytes || s.PlanAllocatedBytes > c.cfg.PlanBytes || s.PolicyAllocatedBytes > c.cfg.MetadataBytes || s.CandidateAllocatedBytes > c.cfg.CandidateBytes || s.Partitions > uint64(c.cfg.MaxResidentTries) {
		t.Fatal("forest budgets", s)
	}
	roots := map[uint64]mpt.Hash{}
	for i := 0; i < c.current.R; i++ {
		r := c.current.Roots[i]
		roots[r.ID] = r.Hash
	}
	seen := map[forestPosition]mpt.Hash{}
	for i := 0; i < c.current.N; i++ {
		n := c.current.Nodes[i]
		if _, err := canonicalHash(c.current.raw(i), n.Hash); err != nil {
			t.Fatal(err)
		}
		if n.Pos.Length == 0 {
			if roots[n.Pos.ID] != n.Hash {
				t.Fatal("root binding")
			}
			seen[n.Pos] = n.Hash
			continue
		}
		connected := false
		for j := 0; j < c.current.N; j++ {
			parent := c.current.Nodes[j]
			if parent.Pos.ID != n.Pos.ID || parent.Pos.Length >= n.Pos.Length {
				continue
			}
			d, err := mpt.InspectCanonicalNode(c.current.raw(j))
			if err != nil {
				t.Fatal(err)
			}
			for slot, ref := range d.Children {
				if !ref.Present || len(ref.Inline) > 0 || ref.Hash != n.Hash {
					continue
				}
				path := append(append([]byte(nil), parent.Pos.Path[:parent.Pos.Length]...), byte(slot))
				if d.Kind == mpt.CanonicalExtension {
					frag, _, _ := mpt.DecodeCompactPath(d.Path)
					path = append(append([]byte(nil), parent.Pos.Path[:parent.Pos.Length]...), frag...)
				}
				if bytes.Equal(path, n.Pos.Path[:n.Pos.Length]) {
					connected = true
				}
			}
		}
		if !connected {
			t.Fatal("orphan N", n.Pos)
		}
		seen[n.Pos] = n.Hash
	}
	for i := 0; i < c.current.H; i++ {
		n := c.current.Hashes[i]
		found := false
		for j := 0; j < c.current.N; j++ {
			p := c.current.Nodes[j]
			if p.Pos.ID != n.Pos.ID {
				continue
			}
			d, _ := mpt.InspectCanonicalNode(c.current.raw(j))
			for slot, r := range d.Children {
				if !r.Present || len(r.Inline) > 0 || r.Hash != n.Hash {
					continue
				}
				path := append(append([]byte(nil), p.Pos.Path[:p.Pos.Length]...), byte(slot))
				if d.Kind == mpt.CanonicalExtension {
					frag, _, _ := mpt.DecodeCompactPath(d.Path)
					path = append(append([]byte(nil), p.Pos.Path[:p.Pos.Length]...), frag...)
				}
				if bytes.Equal(path, n.Pos.Path[:n.Pos.Length]) {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("orphan H", n.Pos)
		}
		if _, ok := seen[n.Pos]; ok {
			t.Fatal("N/H duplicate")
		}
	}
}

func TestForestSharedMultiTrieHistoryBoundsAndReopen(t *testing.T) {
	for _, count := range []int{1, 10, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "shared-forest")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			cc := DefaultReservedSharedForestCacheConfig()
			cc.ReservedShared.MaxResidentTries = 12
			f, err := OpenCachedForest(s, cc)
			if err != nil {
				t.Fatal(err)
			}
			b, _ := f.BeginBlock(0, [32]byte{})
			trees := map[uint64]*mpt.Trie{0: forestTree(t, 1)}
			b.SetTrie(0, trees[0])
			for i := 1; i <= count; i++ {
				id, err := b.CreateStorageTrie([20]byte{byte(i)})
				if err != nil {
					t.Fatal(err)
				}
				trees[id] = forestTree(t, byte(i+1))
				b.SetTrie(id, trees[id])
			}
			head, _, err := b.Commit()
			if err != nil {
				t.Fatal(err)
			}
			assertShared(t, f)
			for h := uint64(1); h <= 3; h++ {
				b, err = f.BeginBlock(h, head.CommitID)
				if err != nil {
					t.Fatal(err)
				}
				for id := uint64(0); id <= uint64(count); id++ {
					root, _ := trees[id].Hash()
					r, err := b.TrieReader(id, root)
					if err != nil {
						t.Fatal(err)
					}
					tree, err := mpt.Open(root, r)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = tree.Get([]byte{0x12}); err != nil {
						t.Fatal(err)
					}
					if err = tree.Update([]byte{0x12}, bytes.Repeat([]byte{byte(h)}, 64)); err != nil {
						t.Fatal(err)
					}
					trees[id] = tree
					if err = b.SetTrie(id, tree); err != nil {
						t.Fatal(err)
					}
				}
				head, _, err = b.Commit()
				if err != nil {
					t.Fatal(h, err)
				}
				assertShared(t, f)
			}
			stats, _ := f.Stats()
			if stats.NodeBodyHits == 0 {
				t.Fatal("new cache never hit", stats)
			}
			t.Logf("tries=%d partitions=%d allocated N/H/plan/meta=%d/%d/%d/%d hits/misses=%d/%d", count+1, stats.Partitions, stats.NodeAllocatedBytes, stats.HashAllocatedBytes, stats.PlanAllocatedBytes, stats.PolicyAllocatedBytes, stats.NodeBodyHits, stats.NodeBodyMisses)
			f.Close()
			s.Close()
			s = openTestStore(t, path, cfg, false, true)
			defer s.Close()
			v, err := s.ViewAt(3)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			for id, tree := range trees {
				root, _ := tree.Hash()
				p, err := v.Prove(id, root, []byte{0x12})
				if err != nil {
					t.Fatal(err)
				}
				got, exists, err := mpt.VerifyProof(root, []byte{0x12}, p)
				if err != nil || !exists || !bytes.Equal(got, bytes.Repeat([]byte{3}, 64)) {
					t.Fatal("cold proof", id, err)
				}
			}
		})
	}
}

func TestForestSharedAtomicFailureAndPublicationRecovery(t *testing.T) {
	for _, outcome := range []string{"before", "after", "publish"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "shared-fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			f, err := OpenCachedForest(s, DefaultReservedSharedForestCacheConfig())
			if err != nil {
				t.Fatal(err)
			}
			b, _ := f.BeginBlock(0, [32]byte{})
			tree := forestTree(t, 1)
			b.SetTrie(0, tree)
			head, _, err := b.Commit()
			if err != nil {
				t.Fatal(err)
			}
			generation := f.shared.generation
			b, _ = f.BeginBlock(1, head.CommitID)
			root, _ := tree.Hash()
			r, _ := b.TrieReader(0, root)
			tree, _ = mpt.Open(root, r)
			tree.Update([]byte{0x12}, bytes.Repeat([]byte{2}, 64))
			b.SetTrie(0, tree)
			real := s.write
			if outcome == "publish" {
				f.shared.beforePublish = func() error { return errors.New("publish fault") }
			} else {
				s.write = func(es []entry) error {
					if f.shared.generation != generation {
						t.Fatal("early publication")
					}
					if outcome == "after" {
						if err := real(es); err != nil {
							return err
						}
					}
					return errors.New("write outcome unknown")
				}
			}
			h, _, err := b.Commit()
			if outcome == "publish" {
				if err != nil || h.Height != 1 || f.shared.current.N != 0 || f.shared.head != h || f.shared.stats.PublicationFailures != 1 {
					t.Fatal(h, err)
				}
			} else if !errors.Is(err, ErrUncertain) || f.shared.generation != generation {
				t.Fatal(err)
			}
			if _, err = r.(mpt.PathStore).GetNode(nil, root); !errors.Is(err, ErrClosed) {
				t.Fatal("stale reader", err)
			}
			f.Close()
			s.Close()
			s = openTestStore(t, path, cfg, false, false)
			defer s.Close()
			h, err = s.Head()
			want := uint64(1)
			if outcome == "before" {
				want = 0
			}
			if err != nil || h.Height != want {
				t.Fatal(h, err)
			}
		})
	}
}

func TestForestSharedVersionContextAndHashHasNoHeightProbe(t *testing.T) {
	for _, work := range []uint32{0, 262144} {
		t.Run(fmt.Sprint(work), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "shared-version")
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			cc := DefaultReservedSharedForestCacheConfig()
			cc.ReservedShared.WorkUnits = work
			f, err := OpenCachedForest(s, cc)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			tree := forestTree(t, 1)
			b, _ := f.BeginBlock(0, [32]byte{})
			b.SetTrie(0, tree)
			head, _, err := b.Commit()
			if err != nil {
				t.Fatal(err)
			}
			b, _ = f.BeginBlock(1, head.CommitID)
			head, _, err = b.Commit()
			if err != nil {
				t.Fatal(err)
			}
			root, _ := tree.Hash()
			b, _ = f.BeginBlock(2, head.CommitID)
			r, err := b.TrieReader(0, root)
			if err != nil {
				t.Fatal(err)
			}
			reader := r.(*forestReader)
			raw, err := reader.GetNode(nil, root)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) == 0 {
				t.Fatal("root")
			}
			if work > 0 {
				i := f.shared.current.node(position(0, nil))
				if i < 0 || !f.shared.current.Nodes[i].Version.Known || f.shared.current.Nodes[i].Version.Height != 0 {
					t.Fatal("query height substituted for version height")
				}
			} else {
				d, _ := mpt.InspectCanonicalNode(raw)
				for slot, ref := range d.Children {
					if !ref.Present || len(ref.Inline) > 0 {
						continue
					}
					path := []byte{byte(slot)}
					if d.Kind == mpt.CanonicalExtension {
						path, _, _ = mpt.DecodeCompactPath(d.Path)
					}
					before := f.shared.stats.ExecutionResolution.PrefixReads
					if _, err = reader.GetNode(path, ref.Hash); err != nil {
						t.Fatal(err)
					}
					if f.shared.stats.ExecutionResolution.PrefixReads-before != 1 {
						t.Fatal("Hash target caused a Prefix height probe")
					}
					found := false
					for i := 0; i < f.shared.observed.N; i++ {
						e := f.shared.observed.Entries[i]
						if e.Pos == position(0, path) {
							found = true
							if e.Version.Known || e.Version.Height != 0 || e.Version.Source != Hash {
								t.Fatal("height attached to content address", e.Version)
							}
						}
					}
					if !found {
						t.Fatal("missing hash observation")
					}
					break
				}
			}
			b.Abort()
		})
	}
}

func TestForestSharedSummaryAuthenticationAndConcurrentStats(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "shared-summary")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	cc := DefaultReservedSharedForestCacheConfig()
	c := &cc.ReservedShared
	c.NodeBudget = ClassBudget{TotalBytes: 16 << 10}
	c.HashBudget = ClassBudget{TotalBytes: 8 << 10}
	f, err := OpenCachedForest(s, cc)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		tree.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i + 1)}, 550))
	}
	root, _ := tree.Hash()
	b, _ := f.BeginBlock(0, [32]byte{})
	b.SetTrie(0, tree)
	head, _, err := b.Commit()
	if err != nil {
		t.Fatal(err)
	}
	assertShared(t, f)
	if f.shared.current.H == 0 {
		t.Fatal("no H frontier")
	}
	target := f.shared.current.Hashes[0]
	b, _ = f.BeginBlock(1, head.CommitID)
	r, err := b.TrieReader(0, root)
	if err != nil {
		t.Fatal(err)
	}
	reader := r.(*forestReader)
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 20; i++ {
			if _, err := f.Stats(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	before := f.shared.stats.ExecutionResolution
	raw, err := reader.GetNode(target.Pos.Path[:target.Pos.Length], target.Hash)
	if err != nil || len(raw) == 0 {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	after := f.shared.stats.ExecutionResolution
	if after.PrefixReads-before.PrefixReads != 2 || after.HashOnlyHits <= before.HashOnlyHits {
		t.Fatal("no summary read reduction", before, after)
	}
	f.shared.current.Hashes[0].Hash[0] ^= 1
	if _, err = reader.GetNode(target.Pos.Path[:target.Pos.Length], target.Hash); !errors.Is(err, ErrHashMismatch) {
		t.Fatal("unchecked H digest", err)
	}
	b.Abort()
}
