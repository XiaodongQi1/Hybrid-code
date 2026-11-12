//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
	"testing"
)

func smallBoundary(k uint64) ForestCacheConfig {
	c := DefaultPrefixBoundaryForestCacheConfig()
	c.Boundary.NodeBudget = ClassBudget{8 << 20, 0, 0}
	c.Boundary.PolicyIntervalBlocks = k
	c.Boundary.PlanBytes = 10 << 20
	return c
}
func assertBoundary(t *testing.T, f *CachedForest) {
	t.Helper()
	p := f.boundary
	u := p.usage(&p.current)
	if !p.cfg.NodeBudget.fits(u) {
		t.Fatal("quota", u)
	}
	for i := range p.current.Nodes {
		if !p.current.Nodes[i].Used {
			continue
		}
		n := p.current.observation(i)
		if n.Version.Source == Prefix && !n.Version.Known {
			t.Fatal("unknown resident height")
		}
		if n.Pos.Length == 0 {
			continue
		}
		parent, slot := p.parent(&p.current, n.Pos, n.Hash)
		if parent < 0 {
			t.Fatal("disconnected node", i, n.Pos)
		}
		par := p.current.Nodes[parent]
		if !n.Heat.boundedBy(par.Heat, p.heat.Height, p.cfg.HalfLife) {
			t.Fatal("heat inversion", par.Heat, n.Heat)
		}
		if n.Version.Source == Prefix && par.Edges[slot].Kind != boundaryPrefix {
			t.Fatal("lost edge locator")
		}
	}
	s, _ := f.Stats()
	if s.HashAllocatedBytes != 0 || s.Boundary.HashEntries != 0 {
		t.Fatal("H allocated", s)
	}
}
func TestBoundaryRootsHistoryAndAccesses(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "boundary")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(4))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 32; i++ {
		tree.Update([]byte{byte(i)}, bytes.Repeat([]byte{byte(i + 1)}, 64))
	}
	b, _ := f.BeginBlock(0, [32]byte{})
	b.SetTrie(0, tree)
	head, cs, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if cs.HashRecords != 0 {
		t.Fatal("unexpected Hash writes", cs)
	}
	assertBoundary(t, f)
	var roots []mpt.Hash
	root, _ := tree.Hash()
	roots = append(roots, root)
	for h := uint64(1); h <= 8; h++ {
		b, e = f.BeginBlock(h, head.CommitID)
		if e != nil {
			t.Fatal(e)
		}
		rr, e := b.TrieReader(0, root)
		if e != nil {
			t.Fatal(e)
		}
		tr, e := mpt.Open(root, rr)
		if e != nil {
			t.Fatal(e)
		}
		a := f.boundary.stats.AccessEvents
		for i := 0; i < 20; i++ {
			if _, e = tr.Get([]byte{1}); e != nil {
				t.Fatal(e)
			}
		}
		if f.boundary.stats.AccessEvents-a != 20 {
			t.Fatal("per-access heat", f.boundary.stats.AccessEvents-a)
		}
		if e = tr.Update([]byte{byte(h)}, bytes.Repeat([]byte{byte(100 + h)}, 64)); e != nil {
			t.Fatal(e)
		}
		oldEv := f.boundary.stats.PolicyEvictions
		b.SetTrie(0, tr)
		head, cs, e = b.Commit()
		if e != nil {
			t.Fatal(h, e)
		}
		if h%4 != 0 && f.boundary.stats.PolicyEvictions != oldEv {
			t.Fatal("out-of-period eviction")
		}
		if cs.HashRecords != 0 {
			t.Fatal("eviction wrote Hash", cs)
		}
		root, _ = tr.Hash()
		roots = append(roots, root)
		assertBoundary(t, f)
	}
	for h, r := range roots {
		v, e := s.ViewAt(uint64(h))
		if e != nil {
			t.Fatal(e)
		}
		proof, e := v.Prove(0, r, []byte{1})
		v.Close()
		if e != nil {
			t.Fatal(e)
		}
		if _, _, e = mpt.VerifyProof(r, []byte{1}, proof); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("boundary %+v", f.boundary.statsSnapshot().Boundary)
}

func TestBoundaryDiffRemovesInlineCompressedAndEmptyResidents(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "boundary-shrink"), true, false)
	defer s.Close()
	f, err := OpenCachedForest(s, smallBoundary(4))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	tr, err := mpt.NewEmpty(mpt.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	keys := [][]byte{{0xa0}, {0xb0}}
	for _, key := range keys {
		if err := tr.Update(key, bytes.Repeat(key, 64)); err != nil {
			t.Fatal(err)
		}
	}
	var head Head
	commit := func(height uint64) {
		t.Helper()
		b, err := f.BeginBlock(height, head.CommitID)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.SetTrie(0, tr); err != nil {
			t.Fatal(err)
		}
		head, _, err = b.Commit()
		if err != nil {
			t.Fatal(err)
		}
		assertBoundary(t, f)
	}
	commit(0)
	bank := &f.boundary.current
	a, b := position(0, []byte{10}), position(0, []byte{11})
	if bank.find(a) < 0 || bank.find(b) < 0 {
		t.Fatal("expected both independent leaves to be resident")
	}
	if err := tr.Update(keys[0], []byte{1}); err != nil {
		t.Fatal(err)
	}
	commit(1)
	if bank.find(a) >= 0 || bank.find(b) < 0 {
		t.Fatal("only the inline child should have left the cache")
	}
	if err := tr.Delete(keys[0]); err != nil {
		t.Fatal(err)
	}
	commit(2)
	if bank.find(b) >= 0 || bank.root(0) < 0 {
		t.Fatal("branch compression should leave only the root")
	}
	if err := tr.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	commit(3)
	if bank.find(position(0, nil)) >= 0 || bank.root(0) >= 0 {
		t.Fatal("empty trie kept a resident root")
	}
}

func TestBoundaryColdWarmExactAndHashSource(t *testing.T) {
	for _, hashSource := range []bool{false, true} {
		t.Run(map[bool]string{false: "prefix", true: "hash"}[hashSource], func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "boundary-source")
			cfg.InitialHeight = 83
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			for i := 0; i < 16; i++ {
				tr.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i + 1)}, 64))
			}
			u, _ := s.BeginForest(83, [32]byte{})
			mask := uint16(0xffff)
			if hashSource {
				mask = 0
			}
			u.SetTrie(0, tr, mask)
			head, _, e := u.Commit()
			if e != nil {
				t.Fatal(e)
			}
			for h := uint64(84); h <= 85; h++ {
				u, _ = s.BeginForest(h, head.CommitID)
				head, _, e = u.Commit()
				if e != nil {
					t.Fatal(e)
				}
			}
			f, e := OpenCachedForest(s, smallBoundary(16))
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			root, _ := tr.Hash()
			b, _ := f.BeginBlock(86, head.CommitID)
			rr, _ := b.TrieReader(0, root)
			rd := rr.(*boundaryReader)
			loaded, e := mpt.Open(root, rd)
			if e != nil {
				t.Fatal(e)
			}
			loaded.Get([]byte{0})
			if f.boundary.stats.PrefixSeeks == 0 {
				t.Fatal("cold startup did not seek")
			}
			head, _, e = b.Commit()
			if e != nil {
				t.Fatal(e)
			}
			b, _ = f.BeginBlock(87, head.CommitID)
			rr, _ = b.TrieReader(0, root)
			rd = rr.(*boundaryReader)
			ri := f.boundary.current.find(position(0, nil))
			if ri < 0 {
				t.Fatal("root not warm")
			}
			n := f.boundary.current.observation(ri)
			pos, want, ok := boundaryImageChild(n, n.Image, 15)
			if !ok {
				t.Fatal("test edge")
			}
			if f.boundary.current.find(pos) >= 0 {
				t.Fatal("expected uncached child")
			}
			before := f.boundary.stats
			raw, e := rd.GetNode(pos.Path[:pos.Length], want)
			if e != nil {
				t.Fatal(e)
			}
			if !bytes.Contains(raw, bytes.Repeat([]byte{16}, 64)) {
				t.Fatal("wrong body")
			}
			after := f.boundary.stats
			if after.PrefixSeeks != before.PrefixSeeks {
				t.Fatal("warm edge used predecessor")
			}
			if hashSource {
				if after.HashGets-before.HashGets != 1 || after.PrefixExactGets != before.PrefixExactGets {
					t.Fatal("Hash source probed Prefix", after)
				}
			} else {
				if after.PrefixExactGets-before.PrefixExactGets != 1 {
					t.Fatal("not exact")
				}
				if f.boundary.current.Nodes[ri].Edges[15].height() != 83 {
					t.Fatal("used query height")
				}
			}
			// A wrong expected hash cannot use the authenticated parent edge.
			wrong := want
			wrong[0] ^= 1
			if _, e = rd.GetNode(pos.Path[:pos.Length], wrong); e == nil {
				t.Fatal("accepted wrong hash")
			}
			b.Abort()
		})
	}
}

func TestBoundaryFaultRecoveryAndStaleReaders(t *testing.T) {
	for _, outcome := range []string{"before", "after", "publish"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "boundary-fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			f, e := OpenCachedForest(s, smallBoundary(1))
			if e != nil {
				t.Fatal(e)
			}
			b, _ := f.BeginBlock(0, [32]byte{})
			tr := forestTree(t, 1)
			b.SetTrie(0, tr)
			head, _, e := b.Commit()
			if e != nil {
				t.Fatal(e)
			}
			gen := f.boundary.generation
			b, _ = f.BeginBlock(1, head.CommitID)
			root, _ := tr.Hash()
			r, _ := b.TrieReader(0, root)
			tr, e = mpt.Open(root, r)
			if e != nil {
				t.Fatal(e)
			}
			tr.Update([]byte{0x12}, bytes.Repeat([]byte{2}, 64))
			b.SetTrie(0, tr)
			real := s.write
			if outcome == "publish" {
				f.boundary.beforePublish = func() error { return errors.New("publish") }
			} else {
				s.write = func(es []entry) error {
					if f.boundary.generation != gen {
						t.Fatal("published before Sync")
					}
					if outcome == "after" {
						if e := real(es); e != nil {
							return e
						}
					}
					return errors.New("uncertain")
				}
			}
			got, _, e := b.Commit()
			if outcome == "publish" {
				if e != nil || got.Height != 1 || f.boundary.stats.PublicationFailures != 1 || f.boundary.statsSnapshot().Boundary.NodeEntries != 0 {
					t.Fatal(got, e)
				}
			} else {
				if !errors.Is(e, ErrUncertain) || f.boundary.generation != gen {
					t.Fatal(e)
				}
			}
			if _, e = r.(mpt.PathStore).GetNode(nil, root); !errors.Is(e, ErrClosed) {
				t.Fatal("stale reader", e)
			}
			f.Close()
			s.Close()
			s = openTestStore(t, path, cfg, false, false)
			defer s.Close()
			got, e = s.Head()
			want := uint64(1)
			if outcome == "before" {
				want = 0
			}
			if e != nil || got.Height != want {
				t.Fatal(got, e)
			}
		})
	}
}

func TestBoundaryActualVersionSurvivesEvictionAndCorruption(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "boundary-corrupt")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(1))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	tr := forestTree(t, 1)
	tr.Update([]byte{0x34}, bytes.Repeat([]byte{3}, 64))
	b, _ := f.BeginBlock(0, [32]byte{})
	b.SetTrie(0, tr)
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	// Drop a frontier body exactly as a policy removal would, retaining the parent.
	pi := -1
	slot := -1
	var child boundaryObservation
	for i := range f.boundary.current.Nodes {
		if !f.boundary.current.Nodes[i].Used {
			continue
		}
		n := f.boundary.current.observation(i)
		if n.Pos.Length == 0 {
			continue
		}
		if f.boundary.frontier(&f.boundary.current, i) {
			pi, slot = f.boundary.parent(&f.boundary.current, n.Pos, n.Hash)
			child = n
			f.boundary.current.remove(i)
			break
		}
	}
	if pi < 0 {
		t.Fatal("no boundary child")
	}
	edge := f.boundary.current.Nodes[pi].Edges[slot]
	if edge.Kind != boundaryPrefix || edge.height() != 0 {
		t.Fatal("height zero lost")
	}
	b, _ = f.BeginBlock(1, head.CommitID)
	root, _ := tr.Hash()
	r, _ := b.TrieReader(0, root)
	key, _ := EncodeNodeKey(NodeKey{0, child.Pos.Path[:child.Pos.Length], 0})
	snap := s.db.snapshot()
	raw, e := snap.get(prefixColumn, key)
	snap.close()
	if e != nil {
		t.Fatal(e)
	}
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 1
	if e = s.db.write([]entry{{prefixColumn, key, bad}}); e != nil {
		t.Fatal(e)
	}
	// The view predates the injected write; a fresh block sees the corruption.
	b.Abort()
	b, _ = f.BeginBlock(1, head.CommitID)
	r, _ = b.TrieReader(0, root)
	if _, e = r.(mpt.PathStore).GetNode(child.Pos.Path[:child.Pos.Length], child.Hash); e == nil {
		t.Fatal("bad exact record accepted")
	}
	b.Abort()
}

func TestBoundaryStatsConcurrentWithExecution(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "boundary-stats")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(4))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, _ := f.BeginBlock(0, [32]byte{})
	tr := forestTree(t, 1)
	b.SetTrie(0, tr)
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				f.Stats()
			}
		}
	}()
	for h := uint64(1); h <= 4; h++ {
		b, _ = f.BeginBlock(h, head.CommitID)
		root, _ := tr.Hash()
		r, _ := b.TrieReader(0, root)
		tr, e = mpt.Open(root, r)
		if e != nil {
			t.Fatal(e)
		}
		for i := 0; i < 10; i++ {
			tr.Get([]byte{0x12})
		}
		tr.Update([]byte{0x12}, bytes.Repeat([]byte{byte(h)}, 64))
		b.SetTrie(0, tr)
		head, _, e = b.Commit()
		if e != nil {
			t.Fatal(e)
		}
	}
	close(stop)
	wg.Wait()
}

func TestBoundaryMultipleTriesKeepOneBoundedPool(t *testing.T) {
	var allocated uint64
	for _, count := range []int{1, 10, 100} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "boundary-many")
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			f, e := OpenCachedForest(s, smallBoundary(4))
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			trees := map[uint64]*mpt.Trie{0: forestTree(t, 1)}
			b, _ := f.BeginBlock(0, [32]byte{})
			for i := 1; i <= count; i++ {
				var a [20]byte
				a[19] = byte(i)
				id, e := b.CreateStorageTrie(a)
				if e != nil {
					t.Fatal(e)
				}
				trees[id] = forestTree(t, byte(i+1))
			}
			for id, tr := range trees {
				if e = b.SetTrie(id, tr); e != nil {
					t.Fatal(e)
				}
			}
			head, _, e := b.Commit()
			if e != nil {
				t.Fatal(e)
			}
			for h := uint64(1); h <= 2; h++ {
				b, _ = f.BeginBlock(h, head.CommitID)
				for id, tr := range trees {
					root, _ := tr.Hash()
					r, e := b.TrieReader(id, root)
					if e != nil {
						t.Fatal(e)
					}
					tr, e = mpt.Open(root, r)
					if e != nil {
						t.Fatal(e)
					}
					if _, e = tr.Get([]byte{0x12}); e != nil {
						t.Fatal(e)
					}
					tr.Update([]byte{0x12}, bytes.Repeat([]byte{byte(id + h)}, 64))
					trees[id] = tr
					b.SetTrie(id, tr)
				}
				head, _, e = b.Commit()
				if e != nil {
					t.Fatal(e)
				}
				assertBoundary(t, f)
			}
			stats, _ := f.Stats()
			got := stats.NodeAllocatedBytes + stats.HashAllocatedBytes + stats.PlanAllocatedBytes + stats.PolicyAllocatedBytes + stats.CandidateAllocatedBytes
			if allocated == 0 {
				allocated = got
			} else if got != allocated {
				t.Fatal("per-trie pool multiplication", got, allocated)
			}
		})
	}
}

func TestBoundaryDiskVersionsIndependentOfK(t *testing.T) {
	var expected []map[string]string
	for _, k := range []uint64{1, 4, 16, 64} {
		t.Run(fmt.Sprint(k), func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "boundary-layout")
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			f, e := OpenCachedForest(s, smallBoundary(k))
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			real := s.write
			var batches []map[string]string
			s.write = func(es []entry) error {
				m := map[string]string{}
				for _, e := range es {
					m[string([]byte{byte(e.cf)})+string(e.key)] = string(e.value)
				}
				batches = append(batches, m)
				return real(es)
			}
			tr := forestTree(t, 1)
			tr.Update([]byte{0x34}, bytes.Repeat([]byte{3}, 64))
			var head Head
			for h := uint64(0); h <= 5; h++ {
				b, _ := f.BeginBlock(h, head.CommitID)
				if h > 0 {
					root, _ := tr.Hash()
					r, _ := b.TrieReader(0, root)
					tr, e = mpt.Open(root, r)
					if e != nil {
						t.Fatal(e)
					}
					tr.Get([]byte{0x12})
					tr.Update([]byte{0x12}, bytes.Repeat([]byte{byte(h)}, 64))
				}
				b.SetTrie(0, tr)
				var cs CommitStats
				head, cs, e = b.Commit()
				if e != nil {
					t.Fatal(e)
				}
				if cs.HashRecords != 0 {
					t.Fatal("cache caused Hash copy")
				}
			}
			if expected == nil {
				expected = batches
			} else if !reflect.DeepEqual(expected, batches) {
				t.Fatal("K changed durable node versions")
			}
		})
	}
}

func TestBoundaryPendingVersionCannotBeReadBeforePublish(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "boundary-pending")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(1))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	tr := forestTree(t, 1)
	b, _ := f.BeginBlock(0, [32]byte{})
	b.SetTrie(0, tr)
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	b, _ = f.BeginBlock(1, head.CommitID)
	root, _ := tr.Hash()
	r, _ := b.TrieReader(0, root)
	tr, e = mpt.Open(root, r)
	if e != nil {
		t.Fatal(e)
	}
	tr.Update([]byte{0x12}, bytes.Repeat([]byte{2}, 64))
	future, _ := tr.Hash()
	b.SetTrie(0, tr)
	f.boundary.beforePublish = func() error {
		if _, e := r.(mpt.PathStore).GetNode(nil, future); e == nil {
			t.Fatal("pending candidate escaped base scope")
		}
		return nil
	}
	if _, _, e = b.Commit(); e != nil {
		t.Fatal(e)
	}
}

func TestBoundaryHeatLifecyclePublication(t *testing.T) {
	for _, outcome := range []string{"abort", "validation", "write", "success"} {
		t.Run(outcome, func(t *testing.T) {
			cfg := DefaultConfig("raw-bytes/v1", "heat-lifecycle")
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
			defer s.Close()
			f, e := OpenCachedForest(s, smallBoundary(4))
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			address := [20]byte{7}
			b, e := f.BeginBlock(0, [32]byte{})
			if e != nil {
				t.Fatal(e)
			}
			id, e := b.CreateStorageTrie(address)
			if e != nil {
				t.Fatal(e)
			}
			tree := forestTree(t, 1)
			root, _ := tree.Hash()
			if e = b.SetTrie(0, tree); e != nil {
				t.Fatal(e)
			}
			if e = b.SetTrie(id, tree); e != nil {
				t.Fatal(e)
			}
			head, _, e := b.Commit()
			if e != nil {
				t.Fatal(e)
			}
			b, e = f.BeginBlock(1, head.CommitID)
			if e != nil {
				t.Fatal(e)
			}
			r, e := b.TrieReader(id, root)
			if e != nil {
				t.Fatal(e)
			}
			loaded, e := mpt.Open(root, r)
			if e != nil {
				t.Fatal(e)
			}
			for j := 0; j < 10; j++ {
				if _, e = loaded.Get([]byte{0x12}); e != nil {
					t.Fatal(e)
				}
			}
			pos := position(id, nil)
			before := f.boundary.count(pos, 1)
			if before == 0 {
				t.Fatal("no source heat")
			}
			if e = b.DeleteStorageTrie(address); e != nil {
				t.Fatal(e)
			}
			switch outcome {
			case "abort":
				b.Abort()
			case "validation":
				old := s.cfg.MaxBatchBytes
				s.cfg.MaxBatchBytes = 1
				_, _, e = b.Commit()
				s.cfg.MaxBatchBytes = old
				b.Abort()
				if !errors.Is(e, ErrLimit) {
					t.Fatal(e)
				}
			case "write":
				s.write = func([]entry) error { return errors.New("injected heat lifecycle write") }
				if _, _, e = b.Commit(); !errors.Is(e, ErrUncertain) {
					t.Fatal(e)
				}
			case "success":
				head, _, e = b.Commit()
				if e != nil {
					t.Fatal(e)
				}
			}
			if outcome != "success" {
				if f.boundary.count(pos, 1) != before {
					t.Fatal("failed retirement erased active heat")
				}
				return
			}
			if f.boundary.count(pos, 1) < before || f.boundary.current.find(pos) >= 0 {
				t.Fatal("retired namespace retained", f.boundary.stats)
			}
			// N retires on publication; shared CMS contributions expire as a bucket.
			// Historical storage remains authenticated.
			v, e := s.ViewAt(0)
			if e != nil {
				t.Fatal(e)
			}
			proof, e := v.Prove(id, root, []byte{0x12})
			v.Close()
			if e != nil {
				t.Fatal(e)
			}
			got, exists, e := mpt.VerifyProof(root, []byte{0x12}, proof)
			want, _ := tree.Get([]byte{0x12})
			if e != nil || !exists || !bytes.Equal(got, want) {
				t.Fatal("retired history changed", e)
			}
			b, e = f.BeginBlock(2, head.CommitID)
			if e != nil {
				t.Fatal(e)
			}
			next, e := b.CreateStorageTrie(address)
			if e != nil || next <= id {
				t.Fatal(next, e)
			}
			if f.boundary.count(position(next, nil), 2) != 0 {
				t.Fatal("new lifecycle inherited old heat")
			}
			b.Abort()
		})
	}
}

func TestBoundaryMaterializedHitsBankReuseAndAbort(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "boundary-fields")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(4))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	key := []byte{0x12}
	original := bytes.Repeat([]byte{1}, 64)
	tree.Update(key, original)
	b, e := f.BeginBlock(0, [32]byte{})
	if e != nil {
		t.Fatal(e)
	}
	b.SetTrie(0, tree)
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	root, _ := tree.Hash()
	var retained *mpt.Trie
	for height := uint64(1); height <= 4; height++ {
		b, e = f.BeginBlock(height, head.CommitID)
		if e != nil {
			t.Fatal(e)
		}
		reader, e := b.TrieReader(0, root)
		if e != nil {
			t.Fatal(e)
		}
		before := f.boundary.stats
		live, e := mpt.Open(root, reader)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = live.Get(key); e != nil {
			t.Fatal(e)
		}
		after := f.boundary.stats
		if after.MaterializedHits <= before.MaterializedHits || after.TemporaryRLPEncodes != before.TemporaryRLPEncodes {
			t.Fatal("N hit used byte encoding", before, after)
		}
		if height == 1 {
			retained = live.Copy()
		}
		live.Update(key, bytes.Repeat([]byte{byte(height + 1)}, 64))
		b.SetTrie(0, live)
		head, _, e = b.Commit()
		if e != nil {
			t.Fatal(e)
		}
		root, _ = live.Hash()
		if got, e := retained.Get(key); e != nil || !bytes.Equal(got, original) {
			t.Fatal("bank reuse changed older COW tree", e)
		}
		if f.boundary.statsSnapshot().Boundary.RetainedRLPBytes != 0 {
			t.Fatal("retained RLP")
		}
		for _, v := range f.boundary.candidateBody {
			if v != 0 {
				t.Fatal("block RLP survived release")
			}
		}
	}
	b, e = f.BeginBlock(5, head.CommitID)
	if e != nil {
		t.Fatal(e)
	}
	reader, _ := b.TrieReader(0, root)
	live, e := mpt.Open(root, reader)
	if e != nil {
		t.Fatal(e)
	}
	live.Update(key, []byte("aborted"))
	b.SetTrie(0, live)
	b.Abort()
	v, e := s.ViewAt(4)
	if e != nil {
		t.Fatal(e)
	}
	proof, e := v.Prove(0, root, key)
	v.Close()
	if e != nil {
		t.Fatal(e)
	}
	got, ok, e := mpt.VerifyProof(root, key, proof)
	if e != nil || !ok || !bytes.Equal(got, bytes.Repeat([]byte{5}, 64)) {
		t.Fatal("abort changed committed value", e)
	}
	assertBoundary(t, f)
}

func TestBoundaryHeatAlignedForestRotationsAndTopology(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "aligned-heat")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	f, e := OpenCachedForest(s, smallBoundary(16))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	b, e := f.BeginBlock(0, [32]byte{})
	if e != nil {
		t.Fatal(e)
	}
	id, e := b.CreateStorageTrie([20]byte{1})
	if e != nil {
		t.Fatal(e)
	}
	id2, e := b.CreateStorageTrie([20]byte{2})
	if e != nil {
		t.Fatal(e)
	}
	ids := []uint64{0, id, id2}
	refs := map[uint64]*mpt.Trie{}
	roots := map[uint64]mpt.Hash{}
	for _, id := range ids {
		tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
		for k := 0; k < 8; k++ {
			if e = tr.Update([]byte{byte(k)}, bytes.Repeat([]byte{byte(k + 1)}, 64)); e != nil {
				t.Fatal(e)
			}
		}
		refs[id] = tr
		roots[id], _ = tr.Hash()
		if e = b.SetTrie(id, tr); e != nil {
			t.Fatal(e)
		}
	}
	head, _, e := b.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if f.boundary.heat.Started || f.boundary.stats.HeatUpdates != 0 {
		t.Fatal("initialization supplied heat")
	}
	for h := uint64(1); h <= 320; h++ {
		if h == 130 {
			retry, err := f.BeginBlock(h, head.CommitID)
			if err != nil {
				t.Fatal(err)
			}
			rr, err := retry.TrieReader(0, roots[0])
			if err != nil {
				t.Fatal(err)
			}
			tr, err := mpt.Open(roots[0], rr)
			if err != nil {
				t.Fatal(err)
			}
			tr.Get([]byte{1})
			observed := f.boundary.stats.HeatUpdates
			retry.Abort()
			if f.boundary.stats.HeatUpdates != observed || observed == 0 {
				t.Fatal("abort erased actual visits")
			}
		}
		b, e = f.BeginBlock(h, head.CommitID)
		if e != nil {
			t.Fatal(h, e)
		}
		if h%17 != 0 { // empty blocks must still rotate/decay the shared time clock
			for _, id := range ids {
				rr, err := b.TrieReader(id, roots[id])
				if err != nil {
					t.Fatal(err)
				}
				tr, err := mpt.Open(roots[id], rr)
				if err != nil {
					t.Fatal(err)
				}
				key := []byte{byte((h*7 + id) % 16)}
				got, err := tr.Get(key)
				want, _ := refs[id].Get(key)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("cached answer", h, id, err)
				}
				if h%5 == 0 {
					err = tr.Delete(key)
					refs[id].Delete(key)
				} else {
					v := bytes.Repeat([]byte{byte(h + id)}, 64)
					err = tr.Update(key, v)
					refs[id].Update(key, v)
				}
				if err != nil {
					t.Fatal(err)
				}
				roots[id], err = tr.Hash()
				expected, _ := refs[id].Hash()
				if err != nil || roots[id] != expected {
					t.Fatal("canonical root", h, id, err)
				}
				if e = b.SetTrie(id, tr); e != nil {
					t.Fatal(e)
				}
			}
		}
		events := f.boundary.stats.HeatUpdates
		head, _, e = b.Commit()
		if e != nil {
			t.Fatal(h, e)
		}
		if f.boundary.stats.HeatUpdates != events {
			t.Fatal("preparation emitted heat")
		}
		assertBoundary(t, f)
		if h%64 == 0 {
			view, err := s.ViewAt(h)
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				proof, err := view.Prove(id, roots[id], []byte{1})
				if err != nil {
					t.Fatal(err)
				}
				got, exists, err := mpt.VerifyProof(roots[id], []byte{1}, proof)
				want, _ := refs[id].Get([]byte{1})
				if err != nil || exists != (len(want) > 0) || !bytes.Equal(got, want) {
					t.Fatal("historical proof", h, id, err)
				}
			}
			view.Close()
		}
	}
	stats := f.boundary.statsSnapshot().Boundary
	if stats.HeatRotations != 6 || stats.HeatExpiredBuckets != 2 || stats.HeatCoverageBlocks != 193 || stats.HeatHeight != 320 {
		t.Fatal("forest clock", stats)
	}
	t.Logf("aligned heat verified: updates=%d buckets=%+v clamps=%d", stats.HeatUpdates, stats.HeatBuckets, stats.HeatParentClamps)
}

// Independent full scans for the integration test oracle.
func (f *boundaryForest) usage(b *boundaryBank) (u [2]uint64) {
	for _, n := range b.Nodes {
		if n.Used {
			u[cacheClass(b.trieID(n))] += f.charge()
		}
	}
	return
}
func (f *boundaryForest) frontier(b *boundaryBank, i int) bool {
	if !b.Nodes[i].Used {
		return false
	}
	n := b.observation(i)
	if n.Pos.ID == 0 && n.Pos.Length == 0 {
		return false
	}
	for j := range b.Nodes {
		if !b.Nodes[j].Used {
			continue
		}
		child := b.observation(j)
		if child.Pos.ID != n.Pos.ID || child.Pos.Length <= n.Pos.Length {
			continue
		}
		if bytes.HasPrefix(child.Pos.Path[:child.Pos.Length], n.Pos.Path[:n.Pos.Length]) {
			return false
		}
	}
	return true
}

// The children are all outside N. A matching published parent supplies all
// sixteen digests and locators, even when the actual version height is zero.
func TestProjectionUsesCachedParentWithoutResidentChildren(t *testing.T) {
	s, _, tree, _ := summaryFixture(t, 0xffff)
	view, spy := queryView(t, s, 0)
	root, body, ok, err := tree.CanonicalNodeAtPath(nil)
	if err != nil || !ok {
		t.Fatal("root", err)
	}
	f, err := newBoundaryForest(DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary)
	if err != nil {
		t.Fatal(err)
	}
	parent := boundaryObservation{Pos: position(0, nil), boundaryNode: boundaryNode{Hash: root, Version: forestVersion{0, Prefix, true}}}
	for i := range parent.Edges {
		parent.Edges[i] = edgeAt(0, boundaryPrefix)
	}
	if err = f.current.put(0, parent, body); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		local   bool
		version uint64
		cold    bool
		reads   int
	}{
		{"warm", true, 0, false, 1}, {"one unknown edge", true, 0, true, 2}, {"wrong parent version", true, 1, false, 17}, {"strict reader", false, 0, false, 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy.reset()
			n := &f.current.Nodes[0]
			n.Version.Height = tc.version
			n.Edges[0] = edgeAt(0, boundaryPrefix)
			if tc.cold {
				n.Edges[0] = boundaryEdge{}
			}
			cursor := new(projectionCursor)
			defer cursor.close()
			reader := projectionReader{trieReader: &trieReader{snapshot: view.snapshot, cfg: s.cfg, root: root, rootSource: Prefix, local: tc.local}, routes: make(map[string]projectionRoute), cursor: cursor, baseNodes: &f.current}
			got, err := reader.GetNode(nil, root)
			if err != nil || !bytes.Equal(got, body) || spy.stats().prefix != tc.reads {
				t.Fatal("root, prefix reads", err, spy.stats(), tc.reads)
			}
			for i := 0; i < 16; i++ {
				want, _ := n.Image.ChildHash(i)
				route, ok := reader.routes[string([]byte{byte(i)})]
				if !ok || route.hash != want || route.height != 0 || route.source != Prefix {
					t.Fatal("child route", i, route)
				}
			}
		})
	}
}

func TestExecutionPrefixSiblingsUseAvailableSlots(t *testing.T) {
	for _, slots := range []uint64{1, 20} {
		t.Run(fmt.Sprint(slots), func(t *testing.T) {
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("ethereum-mpt", "siblings"), true, false)
			defer s.Close()
			tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			for i := 0; i < 16; i++ {
				key := make([]byte, 32)
				key[0] = byte(i << 4)
				if err := tree.Update(key, bytes.Repeat([]byte{byte(i + 1)}, 64)); err != nil {
					t.Fatal(err)
				}
			}
			u, err := s.BeginForest(0, [32]byte{})
			if err != nil {
				t.Fatal(err)
			}
			if err := u.SetTrie(0, tree, 0xffff); err != nil {
				t.Fatal(err)
			}
			head, _, err := u.Commit()
			if err != nil {
				t.Fatal(err)
			}
			cfg := smallBoundary(16)
			cfg.Boundary.MaxNodeBytes = 532
			plan, err := BoundaryCacheAllocation(cfg.Boundary)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Boundary.NodeBudget.TotalBytes = slots * plan.Boundary.SlotBytes
			f, err := OpenCachedForest(s, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			root, _ := tree.Hash()
			block, err := f.BeginBlock(1, head.CommitID)
			if err != nil {
				t.Fatal(err)
			}
			reader, err := block.TrieReader(0, root)
			if err != nil {
				t.Fatal(err)
			}
			// Rebuilding this parent requires all sixteen Prefix child digests.
			if _, err := reader.(*boundaryReader).GetMaterializedNode(nil, root); err != nil {
				t.Fatal(err)
			}
			// A same-block value read can consume the staged leaf without a second KV lookup.
			pos := position(0, []byte{15})
			var hash mpt.Hash
			for _, c := range f.boundary.candidates[:f.boundary.candidateN] {
				if c.Node.Pos == pos {
					hash = c.Node.Hash
					break
				}
			}
			before := f.boundary.stats
			got, handled, err := reader.(*boundaryReader).LocalValue([]byte{15}, hash, make([]byte, 63), nil)
			if err != nil || !handled || !bytes.Equal(got, bytes.Repeat([]byte{16}, 64)) ||
				f.boundary.stats.PrefixExactGets != before.PrefixExactGets || f.boundary.stats.PrefixSeeks != before.PrefixSeeks ||
				f.boundary.stats.TransientBodyHits != before.TransientBodyHits+1 {
				t.Fatalf("staged point read: %x %v %v", got, handled, err)
			}
			if _, _, err := block.Commit(); err != nil {
				t.Fatal(err)
			}
			stats := f.boundary.stats
			if stats.PreparationExactGets != 0 || stats.AccessEvents != 0 || stats.PolicyEvictions != 0 {
				t.Fatalf("extra body reads, heat or eviction: %+v", stats)
			}
			for i := 0; i < 16; i++ {
				pos := position(0, []byte{byte(i)})
				if got := f.boundary.current.find(pos) >= 0; got != (slots > 1) {
					t.Fatalf("slot %d resident=%t with budget %d", i, got, slots)
				}
			}
			assertBoundary(t, f)
		})
	}
}
