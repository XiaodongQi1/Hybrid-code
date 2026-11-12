package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type planReadSpy struct {
	diskSnapshot
	mu                           sync.Mutex
	reads                        map[string]int
	opens, closes, full, headers int
	hook                         func(NodeKey, bool) error
}
type planSpyCursor struct {
	diskPredecessor
	spy *planReadSpy
	cf  column
}

func (s *planReadSpy) record(cf column, key []byte, header bool) error {
	s.mu.Lock()
	s.reads[fmt.Sprintf("%d/%x", cf, key)]++
	if cf == prefixColumn {
		if header {
			s.headers++
		} else {
			s.full++
		}
	}
	s.mu.Unlock()
	if cf == prefixColumn && s.hook != nil {
		k, e := DecodeNodeKey(key)
		if e != nil {
			return e
		}
		return s.hook(k, header)
	}
	return nil
}
func (s *planReadSpy) get(cf column, key []byte) ([]byte, error) {
	if e := s.record(cf, key, false); e != nil {
		return nil, e
	}
	return s.diskSnapshot.get(cf, key)
}
func (s *planReadSpy) prev(cf column, key []byte) ([]byte, []byte, error) {
	if e := s.record(cf, key, false); e != nil {
		return nil, nil, e
	}
	return s.diskSnapshot.prev(cf, key)
}
func (s *planReadSpy) newPredecessor(cf column) (diskPredecessor, error) {
	c, e := s.diskSnapshot.(predecessorSource).newPredecessor(cf)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	s.opens++
	s.mu.Unlock()
	return &planSpyCursor{c, s, cf}, nil
}
func (c *planSpyCursor) prev(k []byte) ([]byte, []byte, error) {
	if e := c.spy.record(c.cf, k, false); e != nil {
		return nil, nil, e
	}
	return c.diskPredecessor.prev(k)
}
func (c *planSpyCursor) prevLimited(k []byte, n int) ([]byte, []byte, int, error) {
	if e := c.spy.record(c.cf, k, true); e != nil {
		return nil, nil, 0, e
	}
	return readLimited(c.diskPredecessor, k, n)
}
func (c *planSpyCursor) close() {
	c.diskPredecessor.close()
	c.spy.mu.Lock()
	c.spy.closes++
	c.spy.mu.Unlock()
}
func planSpy(t *testing.T, v *View) *planReadSpy {
	t.Helper()
	spy := &planReadSpy{diskSnapshot: v.snapshot, reads: map[string]int{}}
	v.snapshot = spy
	return spy
}
func assertPlanReads(t *testing.T, spy *planReadSpy) {
	t.Helper()
	spy.mu.Lock()
	defer spy.mu.Unlock()
	for k, n := range spy.reads {
		if n != 1 {
			t.Fatalf("duplicate record read %s: %d", k, n)
		}
	}
	if spy.opens != spy.closes {
		t.Fatalf("cursor leak: %d/%d", spy.opens, spy.closes)
	}
}

func planStore(t *testing.T, engine kvstore.Engine) *Store {
	t.Helper()
	c := DefaultConfig("raw-bytes/v1", "planned-proofs")
	o := kvstore.DefaultOptions(engine)
	c.Storage = &o
	s, e := Open(filepath.Join(t.TempDir(), "db"), c, true, false)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s
}
func planTree(t *testing.T, keys [][]byte, size int) *mpt.Trie {
	t.Helper()
	tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i, k := range keys {
		if e := tr.Update(k, bytes.Repeat([]byte{byte(i%250 + 1)}, size)); e != nil {
			t.Fatal(e)
		}
	}
	return tr
}
func planCommit(t *testing.T, s *Store, tr *mpt.Trie, mask uint16, hashRoot bool) {
	t.Helper()
	u, e := s.BeginForest(0, [32]byte{})
	if e != nil {
		t.Fatal(e)
	}
	defer u.Abort()
	if e = u.SetTrie(0, tr, mask); e != nil {
		t.Fatal(e)
	}
	id, e := u.CreateStorageTrie([20]byte{1})
	if e != nil {
		t.Fatal(e)
	}
	if e = u.SetTrie(id, tr.Copy(), mask); e != nil {
		t.Fatal(e)
	}
	if hashRoot {
		_, _, e = u.CommitInitialHash()
	} else {
		_, _, e = u.Commit()
	}
	if e != nil {
		t.Fatal(e)
	}
}
func TestPlannedProofLevelDB(t *testing.T) { runPlannedProofs(t, kvstore.LevelDB) }
func runPlannedProofs(t *testing.T, engine kvstore.Engine) {
	keys := [][]byte{{0x12, 0x30}, {0x12, 0x31}, {0x12, 0x40}, {0x12}, {0x99}}
	for _, layout := range []struct {
		name string
		mask uint16
		hash bool
		size int
		keys [][]byte
	}{
		{"prefix", 0xffff, false, 64, keys}, {"mixed", 0xaaaa, false, 64, keys}, {"hash-children", 0, false, 64, keys},
		{"hash-root", 0, true, 64, keys}, {"inline", 0xffff, false, 1, [][]byte{{0x12}, {0x13}}}, {"short-root", 0, false, 1, [][]byte{{0x12}}}, {"empty", 0, false, 1, nil},
	} {
		t.Run(layout.name, func(t *testing.T) {
			s := planStore(t, engine)
			tr := planTree(t, layout.keys, layout.size)
			planCommit(t, s, tr, layout.mask, layout.hash)
			root, _ := tr.Hash()
			queries := append(append([][]byte(nil), keys...), nil, []byte{0x12, 0x32}, []byte{0x11}, []byte{0xff}, []byte{0x12, 0x30, 0x00})
			for _, key := range queries {
				for _, workers := range []uint32{0, 1, 2, 4} {
					v, e := s.ProofViewAt(0)
					if e != nil {
						t.Fatal(e)
					}
					old, _, e := v.AssembleValueProofWithStats(0, root, key)
					if e != nil {
						t.Fatal(e)
					}
					// New spy only covers the operation being measured.
					spy := planSpy(t, v)
					cfg := DefaultParallelProofConfig()
					cfg.Workers = workers
					ps, e := v.BeginProofSession(context.Background(), cfg)
					if e != nil {
						t.Fatal(e)
					}
					gotRoot, e := ps.Root(0)
					if e != nil || gotRoot != root {
						ps.Close()
						t.Fatal(e)
					}
					value, e := ps.Walk(0, root, key)
					if e != nil {
						ps.Close()
						t.Fatal(layout.name, key, e)
					}
					got, stats, e := ps.Finish()
					if e != nil {
						t.Fatal(layout.name, key, workers, e)
					}
					if len(got) != 1 || !reflect.DeepEqual(old, got[0]) || value.Exists != old.Exists || !bytes.Equal(value.Value, old.Value) {
						t.Fatal("result differs", key, old, got)
					}
					x, exists, e := mpt.VerifyCompactProof(root, key, got[0].Nodes)
					if e != nil || exists != old.Exists || !bytes.Equal(x, old.Value) {
						t.Fatal(e)
					}
					if stats.DirectoryReads != 1 || stats.BodyDecodes > stats.ProofNodes || stats.ReadNodes != stats.PrefixReads+stats.HashReads {
						t.Fatal("counts", stats)
					}
					assertPlanReads(t, spy)
					if layout.hash && stats.WorkerStarts != 0 {
						t.Fatal("hash-only workers", stats)
					}
					if e = v.Close(); e != nil {
						t.Fatal(e)
					}
				}
			}
		})
	}
}

func TestPlannedProofHistory(t *testing.T) {
	s := planStore(t, kvstore.LevelDB)
	tr := planTree(t, [][]byte{{0x10}, {0x11}, {0x20}}, 48)
	var head Head
	var roots []mpt.Hash
	var expected []mpt.ProofResult
	for h := uint64(0); h < 5; h++ {
		if h == 2 {
			tr.Delete([]byte{0x10})
		} else if h > 0 {
			tr.Update([]byte{0x10}, bytes.Repeat([]byte{byte(h)}, 48))
		}
		var e error
		head, _, e = s.Commit(h, head.CommitID, []TrieUpdate{{Tree: tr, PrefixMask: []uint16{0xffff, 0, 0xaaaa}[h%3]}})
		if e != nil {
			t.Fatal(e)
		}
		r, _ := tr.Hash()
		roots = append(roots, r)
		v, e := s.ViewAt(h)
		if e != nil {
			t.Fatal(e)
		}
		p, _, e := v.AssembleValueProofWithStats(0, r, []byte{0x10})
		v.Close()
		if e != nil {
			t.Fatal(e)
		}
		expected = append(expected, p)
	}
	for h, r := range roots {
		v, _ := s.ProofViewAt(uint64(h))
		ps, e := v.BeginProofSession(context.Background(), DefaultParallelProofConfig())
		if e != nil {
			t.Fatal(e)
		}
		if _, e = ps.Walk(0, r, []byte{0x10}); e != nil {
			ps.Close()
			t.Fatal(e)
		}
		p, _, e := ps.Finish()
		v.Close()
		if e != nil || !reflect.DeepEqual(p[0], expected[h]) {
			t.Fatal(h, e)
		}
	}
}

func TestPlannedProofLifecycleAndNamespaces(t *testing.T) {
	s := planStore(t, kvstore.LevelDB)
	account := planTree(t, [][]byte{{0x10}, {0x20}}, 48)
	storage := planTree(t, [][]byte{{0x10}, {0x20}}, 64)
	var head Head
	var roots [3][2]mpt.Hash
	var ids [3]uint64
	address := [20]byte{7}
	for h := 0; h < 3; h++ {
		u, e := s.BeginForest(uint64(h), head.CommitID)
		if e != nil {
			t.Fatal(e)
		}
		if h > 0 {
			if e = u.DeleteStorageTrie(address); e != nil {
				t.Fatal(e)
			}
			storage.Update([]byte{0x10}, bytes.Repeat([]byte{byte(h + 77)}, 64))
		}
		id, e := u.CreateStorageTrie(address)
		if e != nil {
			t.Fatal(e)
		}
		ids[h] = id
		if e = u.SetTrie(0, account, 0xffff); e != nil {
			t.Fatal(e)
		}
		if e = u.SetTrie(id, storage, 0xffff); e != nil {
			t.Fatal(e)
		}
		head, _, e = u.Commit()
		if e != nil {
			t.Fatal(e)
		}
		roots[h][0], _ = account.Hash()
		roots[h][1], _ = storage.Hash()
	}
	for h := 0; h < 3; h++ {
		for _, workers := range []uint32{0, 1, 2, 4} {
			v, e := s.ProofViewAt(uint64(h))
			if e != nil {
				t.Fatal(e)
			}
			c := DefaultParallelProofConfig()
			c.Workers = workers
			ps, e := v.BeginProofSession(context.Background(), c)
			if e != nil {
				t.Fatal(e)
			}
			id, active, e := ps.StorageTrieID(address)
			if e != nil || !active || id != ids[h] {
				ps.Close()
				t.Fatal(id, e)
			}
			for i, trieID := range []uint64{0, id} {
				if _, e = ps.Walk(trieID, roots[h][i], []byte{0x10}); e != nil {
					ps.Close()
					t.Fatal(e)
				}
			}
			out, _, e := ps.Finish()
			v.Close()
			if e != nil {
				t.Fatal(e)
			}
			for i, r := range out {
				if _, found, e := mpt.VerifyCompactProof(roots[h][i], []byte{0x10}, r.Nodes); e != nil || !found {
					t.Fatal(e)
				}
			}
			if bytes.Equal(out[0].Value, out[1].Value) {
				t.Fatal("namespace collision")
			}
		}
	}
}

func TestPlannedProofOverlapsBothTriesAndBoundsQueue(t *testing.T) {
	s := planStore(t, kvstore.LevelDB)
	key := make([]byte, 32)
	keys := [][]byte{key}
	for depth := 0; depth < 20; depth++ {
		for n := byte(1); n < 16; n++ {
			k := bytes.Clone(key)
			k[depth/2] = n << uint(4*(1-depth%2))
			keys = append(keys, k)
		}
	}
	tr := planTree(t, keys, 64)
	planCommit(t, s, tr, 0xffff, false)
	root, _ := tr.Hash()
	v, _ := s.ViewAt(0)
	spy := planSpy(t, v)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var once sync.Once
	spy.hook = func(_ NodeKey, header bool) error {
		if header {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	}
	ps, e := v.BeginProofSession(context.Background(), DefaultParallelProofConfig())
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	defer ps.Close()
	defer once.Do(func() { close(release) })
	a, e := ps.Walk(0, root, key)
	if e != nil || !a.Exists {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no background read")
	}
	id, active, e := ps.StorageTrieID([20]byte{1})
	if e != nil || !active || id != 1 {
		t.Fatal(e)
	}
	b, e := ps.Walk(id, root, key)
	if e != nil || !b.Exists {
		t.Fatal("storage path waited for account siblings", e)
	}
	if ps.stats.DeferredGroups == 0 {
		t.Fatal("queue pressure not exercised")
	}
	closing, closed := make(chan struct{}), make(chan struct{})
	go func() { close(closing); v.Close(); close(closed) }()
	<-closing
	// Internal metadata lookup does not reenter View's RWMutex while Close waits.
	if _, _, e = ps.StorageTrieID([20]byte{2}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-closed:
		t.Fatal("view closed with active session")
	default:
	}
	once.Do(func() { close(release) })
	results, stats, e := ps.Finish()
	if e != nil {
		t.Fatal(e)
	}
	if len(results) != 2 || stats.WorkerStarts == 0 || stats.BackgroundReads == 0 || stats.ForegroundHeaderReads == 0 {
		t.Fatal(stats)
	}
	for _, p := range results {
		if _, exists, e := mpt.VerifyCompactProof(root, key, p.Nodes); e != nil || !exists {
			t.Fatal(e)
		}
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("close did not complete")
	}
	assertPlanReads(t, spy)
}

func TestPlannedProofForegroundClaimsQueuedSiblings(t *testing.T) {
	s := planStore(t, kvstore.LevelDB)
	keys := make([][]byte, 16)
	for i := range keys {
		keys[i] = []byte{byte(i << 4)}
	}
	tr := planTree(t, keys, 64)
	planCommit(t, s, tr, 0xffff, false)
	root, _ := tr.Hash()
	v, e := s.ProofViewAt(0)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	spy := planSpy(t, v)
	entered, release, helped := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	var unblock sync.Once
	spy.hook = func(k NodeKey, header bool) error {
		if !header {
			return nil
		}
		if len(k.Path) == 1 && k.Path[0] == 1 {
			close(entered)
			<-release // The only background reader owns the first group.
		} else {
			select {
			case helped <- struct{}{}:
			default:
			}
		}
		return nil
	}
	cfg := DefaultParallelProofConfig()
	cfg.Workers = 1
	ps, e := v.BeginProofSession(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer ps.Close()
	defer unblock.Do(func() { close(release) })
	if _, e = ps.Walk(0, root, keys[0]); e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("background reader did not claim the first group")
	}
	type result struct {
		proofs []mpt.ProofResult
		stats  ParallelProofStats
		err    error
	}
	finished := make(chan result, 1)
	// Transfer the foreground session to this goroutine after Walk returns.
	go func() {
		p, st, err := ps.Finish()
		finished <- result{p, st, err}
	}()
	var progressed bool
	select {
	case <-helped:
		progressed = true
	case <-time.After(5 * time.Second):
	}
	unblock.Do(func() { close(release) })
	got := <-finished // Reclaim the session before any deferred cleanup.
	if !progressed {
		t.Fatal("foreground waited instead of claiming queued sibling reads")
	}
	if got.err != nil || len(got.proofs) != 1 {
		t.Fatal(got.err)
	}
	st := got.stats
	if st.WorkerStarts != 1 || st.BackgroundReads == 0 || st.ForegroundHeaderReads == 0 || st.SiblingHeaderReads != 15 || st.BodyDecodes != 2 || st.ReadNodes != 17 {
		t.Fatal("work must be divided without adding reads", st)
	}
	if _, found, e := mpt.VerifyCompactProof(root, keys[0], got.proofs[0].Nodes); e != nil || !found {
		t.Fatal("invalid proof", e)
	}
	assertPlanReads(t, spy)
}

func TestPlannedProofLimitsFailureAndThreshold(t *testing.T) {
	s := planStore(t, kvstore.LevelDB)
	var keys [][]byte
	for i := 0; i < 16; i++ {
		keys = append(keys, []byte{byte(i << 4)})
	}
	tr := planTree(t, keys, 64)
	planCommit(t, s, tr, 0xffff, false)
	root, _ := tr.Hash()
	for _, test := range []string{"below-threshold", "cancel", "read-error", "node-limit", "byte-limit", "context-limit", "fallback"} {
		t.Run(test, func(t *testing.T) {
			v, _ := s.ViewAt(0)
			defer v.Close()
			spy := planSpy(t, v)
			cfg := DefaultParallelProofConfig()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			old := s.cfg
			defer func() { s.cfg = old }()
			switch test {
			case "below-threshold":
				cfg.MinBackgroundReads = 16
			case "cancel":
				spy.hook = func(_ NodeKey, header bool) error {
					if header {
						cancel()
					}
					return nil
				}
			case "read-error":
				spy.hook = func(_ NodeKey, header bool) error {
					if header {
						return ErrCorrupt
					}
					return nil
				}
			case "node-limit":
				s.cfg.MaxReadNodes = 2
			case "byte-limit":
				s.cfg.MaxReadBytes = 40
			case "context-limit":
				cfg.MaxContextBytes = 4096
			case "fallback":
				v.snapshot = struct{ diskSnapshot }{spy}
			}
			ps, e := v.BeginProofSession(ctx, cfg)
			if e != nil {
				if test != "context-limit" {
					t.Fatal(e)
				}
				return
			}
			_, walkErr := ps.Walk(0, root, []byte{0x30})
			p, stats, finishErr := ps.Finish()
			if test == "below-threshold" || test == "fallback" {
				if walkErr != nil || finishErr != nil || len(p) != 1 || stats.WorkerStarts != 0 || stats.SiblingHeaderReads != 15 || stats.BodyDecodes != 2 || stats.ReadNodes != 17 {
					t.Fatal(walkErr, finishErr, stats)
				}
			} else {
				if walkErr == nil && finishErr == nil || p != nil {
					t.Fatal("expected failure", walkErr, finishErr)
				}
			}
			assertPlanReads(t, spy)
		})
	}
	bad := DefaultParallelProofConfig()
	bad.Workers = 3
	if !errors.Is(bad.Validate(), ErrLimit) {
		t.Fatal("bad worker config accepted")
	}
}
