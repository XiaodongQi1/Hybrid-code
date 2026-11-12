package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
	"testing"
)

func TestExecutionPackedKeyMatchesCanonicalEncoding(t *testing.T) {
	for depth := 0; depth <= MaxDepth; depth++ {
		path := make([]byte, depth)
		for i := range path {
			path[i] = byte((i*7 + depth) % 16)
		}
		for _, id := range []uint64{0, 1, MaxTrieID} {
			for _, height := range []uint64{0, 1, MaxHeight} {
				k := executionKey(id, path)
				var scratch [nodePositionHeaderBytes + 32 + HeightBytes]byte
				got, err := k.appendNodeKey(scratch[:0], height)
				want, wantErr := EncodeNodeKey(NodeKey{id, path, height})
				if err != nil || wantErr != nil || !bytes.Equal(got, want) {
					t.Fatal("packed encoding differs", depth, id, height, err, wantErr)
				}
				// Version resolution compares the same encoded identity and reads
				// only the height, while still rejecting an adjacent logical node.
				old, _ := EncodeNodeKey(NodeKey{id, path, 0})
				if matchEncodedNodeKey(old, got) != nil || read48(old[len(old)-HeightBytes:]) != 0 {
					t.Fatal("old version rejected")
				}
				old[TrieIDBytes-1] ^= 1
				if matchEncodedNodeKey(old, got) == nil {
					t.Fatal("neighbor accepted")
				}
			}
		}
	}
	for _, k := range []executionReadKey{{ID: MaxTrieID + 1}, {Length: MaxDepth + 1}, {Length: 1, Path: [32]byte{0x11}}} {
		if _, err := k.appendNodeKey(nil, 0); err == nil {
			t.Fatal("invalid packed key accepted", k)
		}
	}
	if _, err := (executionReadKey{}).appendNodeKey(nil, MaxHeight+1); !errors.Is(err, ErrLimit) {
		t.Fatal("invalid height accepted", err)
	}
}

type asyncTestSnapshot struct {
	mu               sync.Mutex
	values           map[string][]byte
	counts           map[string]int
	block            string
	entered, unblock chan struct{}
	once             sync.Once
	closed           bool
}

func (s *asyncTestSnapshot) get(_ column, k []byte) ([]byte, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.counts[string(k)]++
	v, ok := s.values[string(k)]
	s.mu.Unlock()
	if string(k) == s.block {
		s.once.Do(func() { close(s.entered) })
		<-s.unblock
	}
	if !ok {
		return nil, ErrNotFound
	}
	return bytes.Clone(v), nil
}
func (s *asyncTestSnapshot) prev(c column, k []byte) ([]byte, []byte, error) {
	v, e := s.get(c, k)
	return bytes.Clone(k), v, e
}
func (s *asyncTestSnapshot) close() { s.mu.Lock(); s.closed = true; s.mu.Unlock() }
func asyncFixture(t *testing.T, size int) (*asyncTestSnapshot, executionReadKey, mpt.NodeLocation) {
	t.Helper()
	key := executionKey(3, []byte{1})
	physical, _ := EncodeNodeKey(NodeKey{3, []byte{1}, 0})
	raw, e := EncodeRecord(Record{Kind: mpt.CanonicalLeaf, Hash: mpt.Hash{1}, Fragment: []byte{2}, Value: bytes.Repeat([]byte{9}, size)})
	if e != nil {
		t.Fatal(e)
	}
	snap := &asyncTestSnapshot{values: map[string][]byte{string(physical): raw}, counts: map[string]int{}, entered: make(chan struct{}), unblock: make(chan struct{})}
	return snap, key, mpt.NodeLocation{Kind: uint8(Prefix)}
}
func TestExecutionReadsClaimQueuedAndWaitInFlight(t *testing.T) {
	snap, key, loc := asyncFixture(t, 64)
	p, _ := EncodeNodeKey(NodeKey{3, []byte{1}, 0})
	snap.block = string(p)
	key2 := executionKey(3, []byte{2})
	p2, _ := EncodeNodeKey(NodeKey{3, []byte{2}, 0})
	snap.values[string(p2)] = bytes.Clone(snap.values[string(p)])
	cfg := DefaultExecutionReadConfig()
	cfg.Entries = 16
	s := newExecutionReads(snap, 0, cfg)
	defer s.close()
	s.prefetch(key, loc)
	<-snap.entered
	s.prefetch(key2, loc)
	if _, e := s.need(key2, loc, mpt.Hash{}, false, true); e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := s.need(key, loc, mpt.Hash{}, false, true); done <- e }()
	s.mu.Lock()
	for s.stats.Waits == 0 {
		s.cond.Wait()
	}
	s.mu.Unlock()
	close(snap.unblock)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	s.sealAndDrain()
	stats := s.snapshotStats()
	if stats.RecordReads != 2 || stats.ForegroundClaims != 1 || stats.NodeDecodeCount != 0 || stats.BackgroundReads != 1 {
		t.Fatalf("%+v", stats)
	}
	for k, n := range snap.counts {
		if n != 1 {
			t.Fatalf("duplicate read %x: %d", k, n)
		}
	}
}
func TestExecutionReadsSummaryUpgradeAndLimits(t *testing.T) {
	snap, key, loc := asyncFixture(t, 6000)
	cfg := DefaultExecutionReadConfig()
	cfg.Entries = 16
	cfg.BodyBytes = 4096
	s := newExecutionReads(snap, 0, cfg)
	defer s.close()
	s.prefetch(key, loc)
	s.sealAndDrain()
	e, err := s.need(key, loc, mpt.Hash{}, false, false)
	if err != nil || e.raw != nil || e.hash != (mpt.Hash{1}) || !e.loc.Exact || e.loc.Height != 0 {
		t.Fatal(e, err)
	}
	e, err = s.need(key, loc, mpt.Hash{}, false, true)
	if err != nil || len(e.raw) < 6000 {
		t.Fatal(err)
	}
	if got := s.snapshotStats(); got.BodyUpgradeReads != 1 || got.PrefixExactGets != 1 || got.PeakBodyBytes > 4096 {
		t.Fatalf("%+v", got)
	}
	// Table saturation cannot fail required reads or retain unbilled bodies.
	for i := 2; i < 24; i++ {
		k := executionKey(3, []byte{byte(i / 16), byte(i % 16)})
		pk, _ := EncodeNodeKey(NodeKey{3, k.path(), 0})
		snap.values[string(pk)] = snap.values[string(mustAsyncKey(key))]
		entry, err := s.need(k, loc, mpt.Hash{}, false, false)
		if err != nil {
			t.Fatal(err)
		}
		if !entry.tracked && entry.rawCharge != 0 {
			t.Fatal("fallback billed as retained")
		}
	}
	if s.snapshotStats().TableFallbacks == 0 {
		t.Fatal("missing fallback")
	}
}
func mustAsyncKey(k executionReadKey) []byte {
	b, _ := EncodeNodeKey(NodeKey{k.ID, k.path(), 0})
	return b
}
func TestExecutionReadsErrorsAndClose(t *testing.T) {
	snap, key, loc := asyncFixture(t, 64)
	snap.values[string(mustAsyncKey(key))] = []byte{0xff}
	cfg := DefaultExecutionReadConfig()
	cfg.Workers = 0
	cfg.Entries = 16
	s := newExecutionReads(snap, 0, cfg)
	for i := 0; i < 2; i++ {
		if _, e := s.need(key, loc, mpt.Hash{}, false, false); !errors.Is(e, ErrCorrupt) {
			t.Fatal(e)
		}
	}
	if s.snapshotStats().RecordReads != 1 {
		t.Fatal("reread a cached error")
	}
	s.close()
	if _, e := s.need(key, loc, mpt.Hash{}, false, true); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}
func TestDeferredExecutionRootsProofsAndPhysicalMoves(t *testing.T) {
	for _, workers := range []uint32{0, 1, 2} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			store := pendingStore(t)
			original, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			keys := [][]byte{{0x10, 0x10}, {0x10, 0x11}, {0x10, 0x20}, {0x20}, {0x30}, {0x31}, {0x32}, {0x33}}
			for i, k := range keys {
				if e := original.Update(k, bytes.Repeat([]byte{byte(i + 1)}, 64)); e != nil {
					t.Fatal(e)
				}
			}
			head, _, err := store.Commit(0, [32]byte{}, []TrieUpdate{{Tree: original, PrefixMask: 0xffff}})
			if err != nil {
				t.Fatal(err)
			}
			cfg := DefaultPrefixBoundaryForestCacheConfig()
			cfg.Boundary.NodeBudget = ClassBudget{TotalBytes: 4 * boundaryCharge()}
			cfg.Boundary.PolicyIntervalBlocks = 1
			cfg.Boundary.ExecutionReads = DefaultExecutionReadConfig()
			cfg.Boundary.ExecutionReads.Workers = workers
			f, err := OpenCachedForest(store, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			reference := original.Copy()
			for height := uint64(1); height <= 3; height++ {
				root, _ := reference.Hash()
				b, err := f.BeginBlock(height, head.CommitID)
				if err != nil {
					t.Fatal(err)
				}
				reader, err := b.TrieReader(0, root)
				if err != nil {
					t.Fatal(err)
				}
				tr, err := mpt.Open(root, reader)
				if err != nil {
					t.Fatal(err)
				}
				frozen := tr.Copy()
				// Read then write the same path reuses both bytes and parsed nodes.
				before, _ := tr.Get(keys[0])
				again, _ := tr.Get(keys[0])
				if !bytes.Equal(before, again) {
					t.Fatal("unstable read")
				}
				for _, tree := range []*mpt.Trie{tr, reference} {
					if err = tree.Delete(keys[1]); err != nil {
						t.Fatal(err)
					}
					if err = tree.Delete(keys[2]); err != nil {
						t.Fatal(err)
					}
					if err = tree.Update([]byte{0x10, 0x12}, bytes.Repeat([]byte{byte(height)}, 64)); err != nil {
						t.Fatal(err)
					}
					if err = tree.Update(keys[4], []byte{byte(height)}); err != nil {
						t.Fatal(err)
					}
				}
				b.FinishReads()
				want, _ := reference.Hash()
				got, err := tr.Hash()
				if err != nil || got != want {
					t.Fatalf("root %x %x: %v", got, want, err)
				}
				equal, err := tr.Equal(reference)
				if err != nil || !equal {
					t.Fatal("equal", err)
				}
				if old, err := frozen.Hash(); err != nil || old != root {
					t.Fatal("COW", err)
				}
				for _, k := range append(keys, []byte{0x10, 0x12}, []byte{0xff}) {
					proof, err := tr.Prove(k)
					if err != nil {
						t.Fatal(err)
					}
					v, exists, err := mpt.VerifyProof(want, k, proof)
					expected, _ := reference.Get(k)
					if err != nil || exists != (expected != nil) || !bytes.Equal(v, expected) {
						t.Fatalf("proof %x: %v", k, err)
					}
				}
				if err = b.SetTrie(0, tr); err != nil {
					t.Fatal(err)
				}
				head, _, err = b.Commit()
				if err != nil {
					t.Fatal("commit", err)
				}
				view, err := store.ViewAt(height)
				if err != nil {
					t.Fatal(err)
				}
				disk, err := view.TrieReader(0, want)
				if err != nil {
					t.Fatal(err)
				}
				for _, k := range keys {
					proof, err := mpt.ProveValueAt(want, disk, k)
					if err != nil {
						t.Fatal("historical", err)
					}
					if _, _, err = mpt.VerifyProof(want, k, proof.Nodes); err != nil {
						t.Fatal(err)
					}
				}
				view.Close()
			}
			stats, _ := f.Stats()
			if stats.ExecutionReads.RecordReads == 0 || stats.ExecutionReads.NodeDecodeCount == 0 || stats.ExecutionReads.HashHeaderHits == 0 {
				t.Fatalf("not exercising lazy reads: %+v", stats.ExecutionReads)
			}
		})
	}
}

func TestExecutionLazyDecodeOnlyOnDemand(t *testing.T) {
	store := pendingStore(t)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		if e := tree.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	head, _, err := store.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultPrefixBoundaryForestCacheConfig()
	cfg.Boundary.ExecutionReads = DefaultExecutionReadConfig()
	f, err := OpenCachedForest(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	root, _ := tree.Hash()
	b, err := f.BeginBlock(1, head.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	reader, _ := b.TrieReader(0, root)
	tr, err := mpt.Open(root, reader)
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.Update([]byte{0}, bytes.Repeat([]byte{99}, 64)); err != nil {
		t.Fatal(err)
	}
	b.FinishReads()
	if _, err = tr.Hash(); err != nil {
		t.Fatal(err)
	}
	stats := b.reads.snapshotStats()
	if stats.NodeDecodeCount != 2 || stats.RecordReads != 17 || stats.HashHeaderHits != 15 {
		t.Fatalf("eager decode or duplicate I/O: %+v", stats)
	}
	for i := 0; i < 2; i++ {
		if _, err = tr.Get([]byte{0x20}); err != nil {
			t.Fatal(err)
		}
	}
	stats = b.reads.snapshotStats()
	if stats.NodeDecodeCount != 3 || stats.RecordReads != 17 {
		t.Fatalf("prefetched body not reused: %+v", stats)
	}
	// Strict canonical interface must not deadlock on the forest lock.
	if _, err = reader.(*boundaryReader).GetNode(nil, root); err != nil {
		t.Fatal(err)
	}
	b.Abort()
}
func TestExecutionAbortJoinsBeforeClosingSnapshot(t *testing.T) {
	snap, key, loc := asyncFixture(t, 64)
	snap.block = string(mustAsyncKey(key))
	cfg := DefaultExecutionReadConfig()
	s := newExecutionReads(snap, 0, cfg)
	s.prefetch(key, loc)
	<-snap.entered
	done := make(chan struct{})
	go func() { s.close(); snap.close(); close(done) }()
	// A blocked native read has not finished; close must be waiting, regardless
	// of whether the closing goroutine has been scheduled yet.
	select {
	case <-done:
		t.Fatal("snapshot closed before read completed")
	default:
	}
	close(snap.unblock)
	<-done
	if s.snapshotStats().RecordReads != 1 {
		t.Fatal("lost background work")
	}
}
func TestExecutionBackingReusedWithoutRetainingOldScope(t *testing.T) {
	snap, key, loc := asyncFixture(t, 64)
	cfg := DefaultExecutionReadConfig()
	cfg.Entries = 16
	cfg.Workers = 0
	s := newExecutionReads(snap, 0, cfg)
	if _, e := s.need(key, loc, mpt.Hash{}, false, true); e != nil {
		t.Fatal(e)
	}
	original := &s.entries[0]
	backing := s.close()
	next := newExecutionReads(snap, 0, cfg, backing)
	defer next.close()
	if &next.entries[0] != original || next.entries[0].raw != nil || next.count != 0 {
		t.Fatal("backing copied or results carried across blocks")
	}
	if _, e := s.need(key, loc, mpt.Hash{}, false, true); !errors.Is(e, ErrClosed) {
		t.Fatal("old reader reused new scope", e)
	}
	if _, e := next.need(key, loc, mpt.Hash{}, false, true); e != nil {
		t.Fatal(e)
	}
}

type executionGateSnapshot struct {
	diskSnapshot
	key              string
	entered, unblock chan struct{}
	once             sync.Once
}

func (g *executionGateSnapshot) prev(c column, k []byte) ([]byte, []byte, error) {
	if c == prefixColumn && string(k) == g.key {
		g.once.Do(func() { close(g.entered) })
		<-g.unblock
	}
	return g.diskSnapshot.prev(c, k)
}
func TestExecutionContinuesWhileSiblingIsReading(t *testing.T) {
	store := pendingStore(t)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		if e := tree.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	head, _, err := store.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultPrefixBoundaryForestCacheConfig()
	cfg.Boundary.ExecutionReads = DefaultExecutionReadConfig()
	f, err := OpenCachedForest(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b, err := f.BeginBlock(1, head.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := EncodeNodeKey(NodeKey{0, []byte{1}, 0})
	gate := &executionGateSnapshot{diskSnapshot: b.reads.snapshot, key: string(key), entered: make(chan struct{}), unblock: make(chan struct{})}
	b.reads.snapshot = gate
	var once sync.Once
	release := func() { once.Do(func() { close(gate.unblock) }) }
	defer release()
	root, _ := tree.Hash()
	reader, _ := b.TrieReader(0, root)
	live, err := mpt.Open(root, reader)
	if err != nil {
		t.Fatal(err)
	}
	if err = live.Update([]byte{0}, bytes.Repeat([]byte{99}, 64)); err != nil {
		t.Fatal(err)
	}
	<-gate.entered
	// The worker is still blocked on child 1; main can claim child 2 and finish
	// a second logical update. No wall-clock sleep or scheduling guess is used.
	if err = live.Update([]byte{0x20}, bytes.Repeat([]byte{88}, 64)); err != nil {
		t.Fatal(err)
	}
	if b.reads.snapshotStats().ForegroundClaims == 0 {
		t.Fatal("queued work was not synchronously claimed")
	}
	release()
	b.FinishReads()
	tree.Update([]byte{0}, bytes.Repeat([]byte{99}, 64))
	tree.Update([]byte{0x20}, bytes.Repeat([]byte{88}, 64))
	want, _ := tree.Hash()
	got, err := live.HashParallel(context.Background(), 2)
	if err != nil || got != want {
		t.Fatal("parallel hash", err)
	}
	b.Abort()
}

func TestExecutionSummarySurvivesBodyDecodeErrorAndRejectsLocatorMismatch(t *testing.T) {
	snap, key, loc := asyncFixture(t, 64)
	snap.values[string(mustAsyncKey(key))] = snap.values[string(mustAsyncKey(key))][:33]
	cfg := DefaultExecutionReadConfig()
	cfg.Workers = 0
	cfg.Entries = 16
	s := newExecutionReads(snap, 0, cfg)
	defer s.close()
	reader := &boundaryReader{block: &ForestBlock{owner: &CachedForest{}, reads: s}}
	ref := &executionReference{reader: reader, key: key, location: loc}
	if h, err := ref.Digest(); err != nil || h != (mpt.Hash{1}) {
		t.Fatal(h, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := ref.Materialize(); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	if h, err := ref.Digest(); err != nil || h != (mpt.Hash{1}) {
		t.Fatal("body error discarded valid summary", err)
	}
	if stats := s.snapshotStats(); stats.RecordReads != 1 || stats.NodeDecodeCount != 1 {
		t.Fatal(stats)
	}
	if _, err := s.need(key, mpt.NodeLocation{Kind: uint8(Hash)}, mpt.Hash{1}, true, true); !errors.Is(err, ErrPositionMismatch) {
		t.Fatal("inconsistent physical source", err)
	}
}

func TestExecutionCanonicalPreservesReadOnlyAdmissionHints(t *testing.T) {
	store := pendingStore(t)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < 16; i++ {
		tree.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 64))
	}
	head, _, err := store.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultPrefixBoundaryForestCacheConfig()
	cfg.Boundary.ExecutionReads = DefaultExecutionReadConfig()
	cfg.Boundary.ExecutionReads.Workers = 0
	f, err := OpenCachedForest(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	root, _ := tree.Hash()
	b, err := f.BeginBlock(1, head.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	storeReader, err := b.TrieReader(0, root)
	if err != nil {
		t.Fatal(err)
	}
	reader := storeReader.(*boundaryReader)
	_, obs, err := reader.executionCanonical(position(0, nil), root, mpt.NodeLocation{Kind: uint8(Prefix)})
	if err != nil {
		t.Fatal(err)
	}
	before := b.reads.snapshotStats().RecordReads
	for _, edge := range obs.Edges {
		if edge.Kind != boundaryPrefix || edge.height() != 0 {
			t.Fatal("lost height-zero Prefix hint", edge)
		}
	}
	if before != 17 {
		t.Fatal("extra work while exporting hints", before)
	}
	_, again, err := reader.executionCanonical(position(0, nil), root, mpt.NodeLocation{Kind: uint8(Prefix)})
	if err != nil || again.Edges != obs.Edges || b.reads.snapshotStats().RecordReads != before {
		t.Fatal("hints not reused", err)
	}
}
