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
	"time"
)

type queryCounts struct{ prefix, hash, opens, closes, active, maxActive int }
type querySnapshot struct {
	diskSnapshot
	mu          sync.Mutex
	counts      queryCounts
	failPrefix  int
	failError   error
	failOpen    bool
	corruptBody bool
	wrongKey    bool
	denyHash    bool
	entered     chan struct{}
	release     <-chan struct{}
}

func (s *querySnapshot) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts.active != 0 {
		panic("live cursor")
	}
	s.counts = queryCounts{}
}
func (s *querySnapshot) stats() queryCounts { s.mu.Lock(); defer s.mu.Unlock(); return s.counts }
func (s *querySnapshot) get(cf column, key []byte) ([]byte, error) {
	if cf == hashColumn {
		s.mu.Lock()
		s.counts.hash++
		deny := s.denyHash
		s.mu.Unlock()
		if deny {
			return nil, ErrNotFound
		}
	}
	return s.diskSnapshot.get(cf, key)
}
func (s *querySnapshot) readPrefix(key []byte) (bool, error) {
	s.mu.Lock()
	s.counts.prefix++
	n := s.counts.prefix
	fail := s.failPrefix
	failure := s.failError
	entered, release := s.entered, s.release
	s.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}
	if fail == n {
		if failure == nil {
			failure = ErrNotFound
		}
		return false, failure
	}
	return true, nil
}
func (s *querySnapshot) edit(key, k, v []byte) ([]byte, []byte, error) {
	ctx, err := DecodeNodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	if s.wrongKey {
		ctx.TrieID++
		k, err = EncodeNodeKey(ctx)
		return k, v, err
	}
	if s.corruptBody {
		r, err := DecodeRecord(v)
		if err != nil {
			return nil, nil, err
		}
		if r.Kind == mpt.CanonicalLeaf {
			r.Value = bytes.Repeat([]byte{99}, len(r.Value))
			v, err = EncodeRecord(r)
			return k, v, err
		}
	}
	return k, v, nil
}
func (s *querySnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	if cf != prefixColumn {
		return s.diskSnapshot.prev(cf, key)
	}
	s.mu.Lock()
	s.counts.opens++
	s.counts.active++
	if s.counts.active > s.counts.maxActive {
		s.counts.maxActive = s.counts.active
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.counts.closes++; s.counts.active--; s.mu.Unlock() }()
	if _, err := s.readPrefix(key); err != nil {
		return nil, nil, err
	}
	k, v, err := s.diskSnapshot.prev(cf, key)
	if err != nil {
		return nil, nil, err
	}
	return s.edit(key, k, v)
}
func (s *querySnapshot) newPredecessor(cf column) (diskPredecessor, error) {
	if s.failOpen {
		return nil, s.failError
	}
	source, ok := s.diskSnapshot.(predecessorSource)
	if !ok {
		return nil, ErrEngineUnavailable
	}
	cursor, err := source.newPredecessor(cf)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.counts.opens++
	s.counts.active++
	if s.counts.active > s.counts.maxActive {
		s.counts.maxActive = s.counts.active
	}
	s.mu.Unlock()
	return &queryCursor{source: cursor, owner: s, cf: cf}, nil
}

type queryCursor struct {
	source diskPredecessor
	owner  *querySnapshot
	cf     column
	closed bool
}

func (c *queryCursor) prev(key []byte) ([]byte, []byte, error) {
	if c.closed {
		return nil, nil, ErrClosed
	}
	if c.cf == prefixColumn {
		if _, err := c.owner.readPrefix(key); err != nil {
			return nil, nil, err
		}
	}
	k, v, err := c.source.prev(key)
	if err != nil {
		return nil, nil, err
	}
	if c.cf == prefixColumn {
		return c.owner.edit(key, k, v)
	}
	return k, v, nil
}
func (c *queryCursor) close() {
	if c.closed {
		return
	}
	c.source.close()
	c.closed = true
	c.owner.mu.Lock()
	c.owner.counts.closes++
	c.owner.counts.active--
	c.owner.mu.Unlock()
}
func queryView(t *testing.T, s *Store, h uint64) (*View, *querySnapshot) {
	t.Helper()
	v, err := s.ViewAt(h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { v.Close() })
	spy := &querySnapshot{diskSnapshot: v.snapshot}
	v.snapshot = spy
	return v, spy
}
func compactQueryProof(p mpt.Proof) mpt.Proof {
	var out mpt.Proof
	for i, raw := range p {
		if i == 0 || len(raw) >= 32 {
			out = append(out, raw)
		}
	}
	return out
}
func referenceQueryProof(v *View, id uint64, root mpt.Hash, key []byte) (mpt.Proof, error) {
	r, err := v.TrieReader(id, root)
	if err != nil {
		return nil, err
	}
	// The reference intentionally exercises the original authenticated byte
	// reader rather than the trusted local value shortcut.
	tree, err := mpt.Open(root, struct {
		mpt.Store
		mpt.PathStore
	}{r, r.(mpt.PathStore)})
	if err != nil {
		return nil, err
	}
	p, err := tree.Prove(key)
	return compactQueryProof(p), err
}

func TestQueryProofReusesRootPathAndPrefixIterator(t *testing.T) {
	s, c, _, want := summaryFixture(t, 0xffff)
	v, spy := queryView(t, s, 0)
	key := []byte{0x30}
	reference, err := referenceQueryProof(v, 0, c.root, key)
	if err != nil {
		t.Fatal(err)
	}
	baseline := spy.stats()
	// Generic Open now retains its decoded root, saving 17 predecessor reads.
	if baseline.prefix != 34 || baseline.opens != 34 || baseline.closes != 34 {
		t.Fatal("reference counts", baseline)
	}
	spy.reset()
	proof, stats, err := v.ProveWithStats(0, c.root, key)
	if err != nil {
		t.Fatal(err)
	}
	actual := spy.stats()
	if !reflect.DeepEqual(reference, proof) {
		t.Fatal("proof bytes changed")
	}
	value, exists, err := mpt.VerifyProof(c.root, key, proof)
	if err != nil || !exists || !bytes.Equal(value, want[string(key)]) {
		t.Fatal(err)
	}
	if actual.prefix != 17 || actual.opens != 1 || actual.closes != 1 || actual.active != 0 || stats.PrefixReads != 17 || stats.PrefixIteratorCreates != 1 || !stats.IteratorReuseEnabled || stats.PathReuses != 0 || stats.NodeRequests != 2 || stats.NodesRebuilt != 2 {
		t.Fatal("context/cursor reuse", actual, stats)
	}
	if stats.ReadNodes != 17 || stats.SiblingHeaderReads != 15 || stats.PeakContextBytes > 64<<10 || stats.ProofNodes != 2 {
		t.Fatal("query accounting", stats)
	}
	// The split query used the same proof plus a separate Open/Get for its value.
	spy.reset()
	r, err := v.TrieReader(0, c.root)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := mpt.Open(c.root, r)
	if err != nil {
		t.Fatal(err)
	}
	standalone, err := tree.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	extra := spy.stats()
	spy.reset()
	combined, both, err := v.ProveValueWithStats(0, c.root, key)
	if err != nil || !combined.Exists || !bytes.Equal(combined.Value, standalone) || !reflect.DeepEqual(combined.Nodes, proof) || spy.stats() != actual || both.ReadNodes != stats.ReadNodes {
		t.Fatal("value required extra reads", err, both)
	}
	t.Logf("split value+proof: %d logical Prefix reads; combined: %d", actual.prefix+extra.prefix, actual.prefix)
	// Whole-query limits use exactly the unique records, not a fresh allowance at
	// each node. The operation also reuses the target child already read above.
	old := s.cfg.MaxReadNodes
	s.cfg.MaxReadNodes = 17
	if _, _, err = v.ProveWithStats(0, c.root, key); err != nil {
		t.Fatal("exact read budget", err)
	}
	s.cfg.MaxReadNodes = 16
	if p, _, err := v.ProveWithStats(0, c.root, key); p != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("whole-query read limit", err)
	}
	s.cfg.MaxReadNodes = old
	t.Logf("Prefix calls %d -> %d; Prefix iterator creates %d -> %d; path reuses=%d", baseline.prefix, actual.prefix, baseline.opens, actual.opens, stats.PathReuses)
}
func TestQueryProofDeepMixedSourcesAndAbsence(t *testing.T) {
	for _, mask := range []uint16{0, 0xffff, 0xaaaa} {
		t.Run(fmt.Sprintf("%04x", mask), func(t *testing.T) {
			s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "query-deep"), true, false)
			defer s.Close()
			native, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			for i := 0; i < 256; i++ {
				native.Update([]byte{byte(i), 0}, bytes.Repeat([]byte{byte(i)}, 64))
			}
			root, _ := native.Hash()
			if _, _, err := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: native, PrefixMask: mask}}); err != nil {
				t.Fatal(err)
			}
			v, err := s.ViewAt(0)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			spy := &querySnapshot{diskSnapshot: v.snapshot}
			v.snapshot = spy
			for _, key := range [][]byte{{0x33, 0}, {0x33, 1}, {0xff, 0}, {0}, {}} {
				reference, err := referenceQueryProof(v, 0, root, key)
				if err != nil {
					t.Fatal(err)
				}
				baseline := spy.stats()
				spy.reset()
				result, stats, err := v.ProveValueWithStats(0, root, key)
				proof := result.Nodes
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(reference, proof) {
					t.Fatal("mixed-source proof mismatch", key)
				}
				if value, exists, e := mpt.VerifyProof(root, key, proof); e != nil || exists != result.Exists || !bytes.Equal(value, result.Value) {
					t.Fatal("terminal value differs from verified proof", key, e)
				}
				actual := spy.stats()
				if uint64(actual.prefix) != stats.PrefixReads || uint64(actual.hash) != stats.HashReads || actual.opens != 1 || actual.active != 0 || actual.closes != 1 {
					t.Fatal(actual, stats)
				}
				if bytes.Equal(key, []byte{0x33, 0}) && mask == 0xffff {
					if actual.prefix != 33 || baseline.prefix != 83 {
						t.Fatal("deep path not reused", baseline, actual)
					}
				}
				spy.reset()
			}
		})
	}
}
func TestQueryProofErrorsReleaseCursorAndReturnNoProof(t *testing.T) {
	sentinel := errors.New("injected iterator error")
	for _, mode := range []string{"open", "seek", "missing", "wrong-position", "corrupt-child", "missing-hash", "bytes"} {
		t.Run(mode, func(t *testing.T) {
			mask := uint16(0xffff)
			if mode == "missing-hash" {
				mask = 0
			}
			s, c, _, _ := summaryFixture(t, mask)
			v, spy := queryView(t, s, 0)
			switch mode {
			case "open":
				spy.failOpen = true
				spy.failError = sentinel
			case "seek":
				spy.failPrefix = 5
				spy.failError = sentinel
			case "missing":
				spy.failPrefix = 5
			case "wrong-position":
				spy.wrongKey = true
			case "corrupt-child":
				spy.corruptBody = true
			case "missing-hash":
				spy.denyHash = true
			case "bytes":
				s.cfg.MaxReadBytes = 1
			}
			p, _, err := v.ProveWithStats(0, c.root, []byte{0x30})
			if p != nil || err == nil {
				t.Fatal("failure became proof/absence", err)
			}
			if (mode == "open" || mode == "seek") && !errors.Is(err, sentinel) {
				t.Fatal("iterator error lost", err)
			}
			if mode == "corrupt-child" && !errors.Is(err, ErrHashMismatch) {
				t.Fatal("target body not authenticated", err)
			}
			counts := spy.stats()
			if counts.active != 0 || counts.closes != counts.opens {
				t.Fatal("leaked cursor", counts)
			}
		})
	}
}
func TestQueryProofScopeInlineAndEmpty(t *testing.T) {
	s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
	v0, _ := queryView(t, s, 0)
	proof, stats, err := v0.ProveWithStats(0, mpt.EmptyRootHash, nil)
	if err != nil || len(proof) != 0 || stats.PrefixReads != 0 || stats.PrefixIteratorCreates != 0 {
		t.Fatal("empty proof", stats, err)
	}
	v0.Close()
	snap := identityCommit(t, c, native, want, 1, []identityWrite{{[]byte{0x10}, []byte{1}}, {[]byte{0x11}, []byte{2}}})
	v, spy := queryView(t, s, 1)
	p, stats, err := v.ProveWithStats(0, snap.root, []byte{0x10})
	if err != nil {
		t.Fatal(err)
	}
	if len(p) != 1 || stats.ProofNodes != 1 {
		t.Fatal("inline nodes sent independently", len(p), stats)
	}
	if value, exists, err := mpt.VerifyProof(snap.root, []byte{0x10}, p); err != nil || !exists || !bytes.Equal(value, []byte{1}) {
		t.Fatal(err)
	}
	spy.reset()
	if p, _, err = v.ProveWithStats(0, mpt.Hash{9}, []byte{0x10}); p != nil || !errors.Is(err, ErrHashMismatch) {
		t.Fatal("untrusted root accepted", err)
	}
	if spy.stats().opens != 0 {
		t.Fatal("read data before root binding")
	}
	if p, _, err = v.ProveWithStats(99, snap.root, []byte{0x10}); p != nil || err == nil {
		t.Fatal("wrong trie accepted")
	}
	if _, _, err = v.ProveWithStats(0, snap.root, make([]byte, 33)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	// The fallback preserves wrappers' fault hooks rather than unwrapping them.
	plain, err := s.ViewAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plain.snapshot = summaryCorruptTarget{diskSnapshot: plain.snapshot, path: nil, corruptValue: false}
	if p, stats, err = plain.ProveWithStats(0, snap.root, []byte{0x10}); p != nil || err == nil || stats.IteratorReuseEnabled {
		t.Fatal("fallback bypassed wrapper", stats, err)
	}
	v.Close()
	if _, _, err = v.ProveWithStats(0, snap.root, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestQueryProofCursorAndSnapshotLifetimeConcurrent(t *testing.T) {
	s, c, _, _ := summaryFixture(t, 0xffff)
	v, spy := queryView(t, s, 0)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	spy.entered = entered
	spy.release = release
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, _, err := v.ProveWithStats(0, c.root, []byte{0x30})
			if err != nil {
				t.Error(err)
				return
			}
			if _, _, err = mpt.VerifyProof(c.root, []byte{0x30}, p); err != nil {
				t.Error(err)
			}
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("same-View proofs serialized or deadlocked")
		}
	}
	closed := make(chan error, 1)
	go func() { closed <- v.Close() }()
	close(release)
	wg.Wait()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot close deadlocked")
	}
	counts := spy.stats()
	if counts.active != 0 || counts.opens != 2 || counts.closes != 2 || counts.maxActive < 2 {
		t.Fatal("cursor shared or leaked", counts)
	}
	if _, _, err := v.ProveWithStats(0, c.root, nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestQueryNativeCursorOwnsReturnedBytes(t *testing.T) {
	s, _, _, _ := summaryFixture(t, 0xffff)
	snap := s.db.snapshot()
	defer snap.close()
	cursor, reusable, err := openPredecessor(snap, prefixColumn)
	if err != nil || !reusable {
		t.Fatal(err)
	}
	k0, _ := EncodeNodeKey(NodeKey{})
	k1, _ := EncodeNodeKey(NodeKey{Path: []byte{3}})
	key, value, err := cursor.prev(k0)
	if err != nil {
		t.Fatal(err)
	}
	savedKey, savedValue := append([]byte(nil), key...), append([]byte(nil), value...)
	if _, _, err = cursor.prev(k1); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(savedKey, key) || !bytes.Equal(savedValue, value) {
		t.Fatal("iterator key/value alias invalidated after seek")
	}
	cursor.close()
	cursor.close()
	if _, _, err = cursor.prev(k1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestTrustedPointReadsOnlySelectedChildAndProofStillChecksBody(t *testing.T) {
	for _, mask := range []uint16{0, 0xffff} {
		t.Run(fmt.Sprintf("mask-%x", mask), func(t *testing.T) {
			s, c, _, want := summaryFixture(t, mask)
			v, spy := queryView(t, s, 0)
			reader, e := v.TrieReader(0, c.root)
			if e != nil {
				t.Fatal(e)
			}
			tree, e := mpt.Open(c.root, reader)
			if e != nil {
				t.Fatal(e)
			}
			if count := spy.stats(); count.prefix != 0 || count.hash != 0 {
				t.Fatal("eager root resolution", count)
			}
			key := []byte{0x30}
			got, e := tree.Get(key)
			if e != nil || !bytes.Equal(got, want[string(key)]) {
				t.Fatal("point value", got, e)
			}
			count := spy.stats()
			if count.prefix+count.hash != 2 || count.opens != 1 || count.closes != 1 {
				t.Fatal("point read touched siblings", count)
			}
			if mask != 0 {
				spy.corruptBody = true
				got, e = tree.Get(key)
				if e != nil || !bytes.Equal(got, bytes.Repeat([]byte{99}, 64)) {
					t.Fatal("expected trusted local decoding", e)
				}
				if _, e = v.Prove(0, c.root, key); !errors.Is(e, ErrHashMismatch) {
					t.Fatal("proof accepted corrupt body", e)
				}
				spy.corruptBody = false
				spy.wrongKey = true
				if _, e = tree.Get(key); !errors.Is(e, ErrNotFound) {
					t.Fatal("accepted different predecessor position", e)
				}
			}
			if e = v.Close(); e != nil {
				t.Fatal(e)
			}
			if _, e = tree.Get(key); !errors.Is(e, ErrClosed) {
				t.Fatal("read escaped closed view", e)
			}
		})
	}
}

func TestTrustedPointReadCompressedAndEmbeddedAfterCOW(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "point-inline"), true, false)
	defer s.Close()
	reference, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	keys := [][]byte{{0x12}, {0x12, 0x31}, {0x12, 0x32}, {0xf0}}
	for i, k := range keys {
		value := []byte{byte(i + 1)}
		if i == len(keys)-1 {
			value = bytes.Repeat(value, 70)
		}
		if e := reference.Update(k, value); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: reference, PrefixMask: 0xffff}}); e != nil {
		t.Fatal(e)
	}
	v, e := s.ViewAt(0)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	root, _ := reference.Hash()
	r, e := v.TrieReader(0, root)
	if e != nil {
		t.Fatal(e)
	}
	live, e := mpt.Open(root, r)
	if e != nil {
		t.Fatal(e)
	}
	for pass := 0; pass < 2; pass++ {
		for _, k := range append(keys, []byte{0x12, 0x33}, []byte{0x10}, []byte{}) {
			got, e := live.Get(k)
			want, _ := reference.Get(k)
			if e != nil || !bytes.Equal(got, want) {
				t.Fatal(pass, k, got, want, e)
			}
		}
		if e := live.Update(keys[3], []byte{7}); e != nil {
			t.Fatal(e)
		}
		if e := reference.Update(keys[3], []byte{7}); e != nil {
			t.Fatal(e)
		}
		h, e := live.Hash()
		want, _ := reference.Hash()
		if e != nil || h != want {
			t.Fatal("COW hash", e)
		}
	}
}

func TestCompoundProofSharesPrefixCursorUntilViewClose(t *testing.T) {
	s, fixture, _, _ := summaryFixture(t, 0xffff)
	v, spy := queryView(t, s, 0)
	v.snapshot = &proofReadSnapshot{diskSnapshot: spy, source: spy}
	for i, key := range [][]byte{{0x30}, {0x40}} {
		proof, st, err := v.ProveWithStats(0, fixture.root, key)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = mpt.VerifyProof(fixture.root, key, proof); err != nil {
			t.Fatal(err)
		}
		wantCreates := uint64(1 - i)
		if st.PrefixIteratorCreates != wantCreates || st.PrefixReads != 17 || st.SiblingHeaderReads != 15 || st.NodesRebuilt != 2 || st.NodeRequests != 2 || spy.stats().opens != 2 || spy.stats().closes != 0 {
			t.Fatal("expected one metadata cursor and one Prefix cursor across both walks", i, st, spy.stats())
		}
	}
	if err := v.Close(); err != nil || spy.stats().closes != 2 || spy.stats().active != 0 {
		t.Fatal(err, spy.stats())
	}
}

func TestHistoricalProofAuthenticatesOffPathDigest(t *testing.T) {
	s, fixture, _, _ := summaryFixture(t, 0xffff)
	// Query slot 3 after corrupting only the stored digest of its sibling 4.
	at, err := EncodeNodeKey(NodeKey{Path: []byte{4}})
	if err != nil {
		t.Fatal(err)
	}
	snap := s.db.snapshot()
	wire, err := snap.get(prefixColumn, at)
	snap.close()
	if err != nil {
		t.Fatal(err)
	}
	wire[1] ^= 1
	if err := s.db.write([]entry{{prefixColumn, at, wire}}); err != nil {
		t.Fatal(err)
	}
	v, err := s.ProofViewAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	proof, stats, err := v.ProveWithStats(0, fixture.root, []byte{0x30})
	if !errors.Is(err, ErrHashMismatch) || proof != nil || stats.SiblingHeaderReads != 15 || stats.NodeRequests != 1 {
		t.Fatalf("expected parent digest rejection before descent, stats=%+v err=%v", stats, err)
	}
	key := []byte{0x30}
	served, _, err := v.AssembleValueProofWithStats(0, fixture.root, key)
	if err != nil || !served.Exists {
		t.Fatalf("expected assembly of structurally valid local bytes: %v", err)
	}
	if _, _, err := mpt.VerifyCompactProof(fixture.root, key, served.Nodes); err == nil {
		t.Fatal("client accepted a proof with tampered node content")
	}
}

func TestHistoricalProofAuthenticatesHashStoreTarget(t *testing.T) {
	s, fixture, tree, _ := summaryFixture(t, 0)
	h, raw, found, err := tree.CanonicalNodeAtPath([]byte{3})
	if err != nil || !found {
		t.Fatal("missing leaf", err)
	}
	// Keep the leaf well formed and change one byte of its value.
	body := bytes.Clone(raw)
	body[len(body)-1] ^= 1
	if err := s.db.write([]entry{{hashColumn, h[:], body}}); err != nil {
		t.Fatal(err)
	}
	v, err := s.ProofViewAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	p, stats, err := v.ProveWithStats(0, fixture.root, []byte{0x30})
	if !errors.Is(err, ErrHashMismatch) || p != nil || stats.HashReads != 1 {
		t.Fatalf("accepted corrupt hash target, stats=%+v err=%v", stats, err)
	}
	key := []byte{0x30}
	served, _, err := v.AssembleValueProofWithStats(0, fixture.root, key)
	if err != nil || !served.Exists {
		t.Fatalf("expected assembly of structurally valid local bytes: %v", err)
	}
	if _, _, err := mpt.VerifyCompactProof(fixture.root, key, served.Nodes); err == nil {
		t.Fatal("client accepted a proof with tampered node content")
	}
}
