//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

// Count the actual snapshot calls as well as the reported metrics. Prefix reads
// are logical SeekForPrev operations, not necessarily physical disk reads.
type summarySnapshot struct {
	diskSnapshot
	prefix     map[string]int
	hashes     int
	denyPrefix func(NodeKey) error
	denyHash   bool
}

func (s *summarySnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	if cf == prefixColumn {
		ctx, err := DecodeNodeKey(key)
		if err != nil {
			return nil, nil, err
		}
		s.prefix[string(ctx.Path)]++
		if s.denyPrefix != nil {
			if err = s.denyPrefix(ctx); err != nil {
				return nil, nil, err
			}
		}
	}
	return s.diskSnapshot.prev(cf, key)
}
func (s *summarySnapshot) get(cf column, key []byte) ([]byte, error) {
	if cf == hashColumn {
		s.hashes++
		if s.denyHash {
			return nil, ErrNotFound
		}
	}
	return s.diskSnapshot.get(cf, key)
}
func (s *summarySnapshot) prefixCount() int {
	n := 0
	for _, count := range s.prefix {
		n += count
	}
	return n
}
func wrapSummaryReader(r *trieReader) *summarySnapshot {
	s := &summarySnapshot{diskSnapshot: r.snapshot, prefix: map[string]int{}}
	r.snapshot = s
	return s
}
func summaryFixture(t *testing.T, mask uint16) (*Store, *CachedTrie, *mpt.Trie, map[string][]byte) {
	t.Helper()
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "summary-fast-path"), true, false)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	want := map[string][]byte{}
	for i := 0; i < 16; i++ {
		key := []byte{byte(i << 4)}
		value := bytes.Repeat([]byte{byte(i + 1)}, 64)
		if err := tree.Update(key, value); err != nil {
			t.Fatal(err)
		}
		want[string(key)] = value
	}
	if _, _, err := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: mask}}); err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Hash()
	c, err := OpenCachedTrie(s, 0, root, DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if c.cache.current.n != 1 || c.cache.current.h != 16 {
		t.Fatal("expected a pure H frontier")
	}
	return s, c, tree, want
}

func TestSummaryFastPathAvoidsSiblingPrefixReads(t *testing.T) {
	s, c, tree, want := summaryFixture(t, 0xffff)
	leafHash, _, found, err := tree.CanonicalNodeAtPath([]byte{3})
	if err != nil || !found {
		t.Fatal(err)
	}
	v, err := s.ViewAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, err := v.TrieReader(0, c.root)
	if err != nil {
		t.Fatal(err)
	}
	baseline := wrapSummaryReader(base.(*trieReader))
	if _, err = base.(mpt.PathStore).GetNode([]byte{3}, leafHash); err != nil {
		t.Fatal(err)
	}
	if baseline.prefixCount() != 17 {
		t.Fatal("baseline reads", baseline.prefixCount())
	}
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	spy := wrapSummaryReader(b.reader.base)
	spy.denyPrefix = func(ctx NodeKey) error {
		if len(ctx.Path) > 0 && !bytes.Equal(ctx.Path, []byte{3}) {
			return errors.New("unexpected sibling body read")
		}
		return nil
	}
	b.reader.base.cfg.MaxReadNodes = 2
	value, err := b.Get([]byte{0x30})
	if err != nil || !bytes.Equal(value, want[string([]byte{0x30})]) {
		t.Fatal(value, err)
	}
	stats, _ := c.Stats()
	reads := stats.ExecutionResolution
	if spy.prefixCount() != 2 || reads.PrefixReads != 2 || reads.HashOnlyHits != 16 || reads.HashReads != 0 || reads.HashMisses != 0 {
		t.Fatal("summary fast path", reads, spy.prefix)
	}
	for i := 0; i < 16; i++ {
		if i == 3 {
			continue
		}
		if c.cache.policy.heat.find([]byte{byte(i)}) != nil {
			t.Fatal("hash use changed body heat", i)
		}
	}
	if c.cache.observed.n != 1 {
		t.Fatal("hash-only siblings became body candidates")
	}
	t.Logf("same H body: Prefix SeekForPrev calls %d -> %d; H digest hits=%d", baseline.prefixCount(), spy.prefixCount(), reads.HashOnlyHits)
}

func TestSummaryNodeDigestDoesNotTouchBodyHeat(t *testing.T) {
	_, c, _, want := summaryFixture(t, 0xffff)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Get([]byte{0}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	i, ok := c.cache.current.nodePosition([]byte{0})
	if !ok {
		t.Fatal("warm body not admitted")
	}
	before := c.cache.current.nodes[i]
	b, err = c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	value, err := b.Get([]byte{0x30})
	if err != nil || !bytes.Equal(value, want[string([]byte{0x30})]) {
		t.Fatal(err)
	}
	after := c.cache.current.nodes[i]
	if before.A != after.A || before.U != after.U || before.Tick != after.Tick || c.cache.policy.heat.find([]byte{0}) != nil {
		t.Fatal("N digest use counted as body access")
	}
	stats, _ := c.Stats()
	if stats.ExecutionResolution.NodeHashHits != 1 || stats.ExecutionResolution.HashOnlyHits != 31 {
		t.Fatal("N/H digest counters", stats.ExecutionResolution)
	}
}

func TestSummaryRetainsDeclaredPrefixHashAndEmbedSources(t *testing.T) {
	for _, mask := range []uint16{0, 0x00ff, 0xffff} {
		for _, slot := range []byte{3, 10} {
			t.Run(fmt.Sprintf("%04x/%d", mask, slot), func(t *testing.T) {
				_, c, _, want := summaryFixture(t, mask)
				b, err := c.BeginBlock(1)
				if err != nil {
					t.Fatal(err)
				}
				defer b.Abort()
				spy := wrapSummaryReader(b.reader.base)
				value, err := b.Get([]byte{slot << 4})
				if err != nil || !bytes.Equal(value, want[string([]byte{slot << 4})]) {
					t.Fatal(err)
				}
				prefix, hash := 1, 1
				if mask&(1<<slot) != 0 {
					prefix, hash = 2, 0
				}
				stats, _ := c.Stats()
				reads := stats.ExecutionResolution
				if spy.prefixCount() != prefix || spy.hashes != hash || reads.PrefixReads != uint64(prefix) || reads.HashReads != uint64(hash) {
					t.Fatal("physical source changed", reads, spy.prefix, spy.hashes)
				}
			})
		}
	}
	// A compressed root must target p||fragment, while inline nodes never get
	// independent summary lookups or records.
	for _, size := range []int{1, 64} {
		t.Run(fmt.Sprint("extension-inline-size-", size), func(t *testing.T) {
			s, c, native, want := cachedFixture(t, DefaultCacheConfig(), 0)
			v := bytes.Repeat([]byte{1}, size)
			snapshot := identityCommit(t, c, native, want, 1, []identityWrite{{[]byte{0x10}, v}, {[]byte{0x11}, v}})
			c.Close()
			var err error
			c, err = OpenCachedTrie(s, 0, snapshot.root, DefaultCacheConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			b, err := c.BeginBlock(2)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Abort()
			spy := wrapSummaryReader(b.reader.base)
			value, err := b.Get([]byte{0x10})
			if err != nil || !bytes.Equal(value, v) {
				t.Fatal(err)
			}
			stats, _ := c.Stats()
			if size == 1 {
				if spy.prefixCount() != 0 || stats.ExecutionResolution.HashOnlyHits != 0 {
					t.Fatal("inline child resolved separately")
				}
			} else {
				if stats.ExecutionResolution.HashOnlyHits != 2 || spy.prefix[string([]byte{1})] != 2 || spy.prefix[string([]byte{0})] != 0 {
					t.Fatal("extension slot zero became nibble zero", stats.ExecutionResolution, spy.prefix)
				}
			}
		})
	}
}

func TestSummaryMissingBodyAndBadDigestAreErrors(t *testing.T) {
	for _, mode := range []string{"missing-prefix-body", "missing-hash-body", "corrupt-prefix-body", "corrupt-prefix-value", "bad-H-digest", "spoof-parent-and-H"} {
		t.Run(mode, func(t *testing.T) {
			mask := uint16(0xffff)
			if mode == "missing-hash-body" {
				mask = 0
			}
			_, c, tree, _ := summaryFixture(t, mask)
			b, err := c.BeginBlock(1)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Abort()
			spy := wrapSummaryReader(b.reader.base)
			spy.denyHash = mode == "missing-hash-body"
			if mode == "missing-prefix-body" {
				spy.denyPrefix = func(ctx NodeKey) error {
					if bytes.Equal(ctx.Path, []byte{3}) {
						return ErrNotFound
					}
					return nil
				}
			}
			if mode == "bad-H-digest" || mode == "spoof-parent-and-H" {
				i, _ := c.cache.current.hashPosition([]byte{2})
				old := c.cache.current.hashes[i].Hash
				c.cache.current.hashes[i].Hash = mpt.Hash{99}
				defer func() { c.cache.current.hashes[i].Hash = old }()
			}
			if mode == "corrupt-prefix-body" || mode == "corrupt-prefix-value" {
				// Keep the local record well formed but substitute a different self
				// digest, which must be checked against the cached authenticated edge.
				b.reader.base.snapshot = summaryCorruptTarget{diskSnapshot: spy, path: []byte{3}, corruptValue: mode == "corrupt-prefix-value"}
			}
			if mode == "spoof-parent-and-H" {
				_, raw, _, err := tree.CanonicalNodeAtPath(nil)
				if err != nil {
					t.Fatal(err)
				}
				desc, err := mpt.InspectCanonicalNode(raw)
				if err != nil {
					t.Fatal(err)
				}
				desc.Children[2].Hash = mpt.Hash{99}
				_, hash, err := mpt.RebuildCanonicalNode(desc)
				if err != nil {
					t.Fatal(err)
				}
				b.reader.base.snapshot = summarySpoofRoot{spy, hash}
			}
			h, _, _, _ := tree.CanonicalNodeAtPath([]byte{3})
			_, err = b.reader.GetNode([]byte{3}, h)
			if err == nil {
				t.Fatal("digest hit hid missing/corrupt body")
			}
			if mode != "missing-prefix-body" && mode != "missing-hash-body" && !errors.Is(err, ErrHashMismatch) {
				t.Fatal("authentication error lost", err)
			}
			if mode == "missing-hash-body" && spy.prefix[string([]byte{3})] != 0 {
				t.Fatal("Hash miss fell back to Prefix")
			}
		})
	}
}

type summaryCorruptTarget struct {
	diskSnapshot
	path         []byte
	corruptValue bool
}

func (s summaryCorruptTarget) prev(cf column, key []byte) ([]byte, []byte, error) {
	k, v, err := s.diskSnapshot.prev(cf, key)
	if err != nil || cf != prefixColumn {
		return k, v, err
	}
	ctx, err := DecodeNodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(ctx.Path, s.path) {
		return k, v, nil
	}
	r, err := DecodeRecord(v)
	if err != nil {
		return nil, nil, err
	}
	if s.corruptValue {
		r.Value = bytes.Repeat([]byte{99}, len(r.Value))
	} else {
		r.Hash = mpt.Hash{77}
	}
	v, err = EncodeRecord(r)
	return k, v, err
}

type summarySpoofRoot struct {
	diskSnapshot
	hash mpt.Hash
}

func (s summarySpoofRoot) prev(cf column, key []byte) ([]byte, []byte, error) {
	k, v, err := s.diskSnapshot.prev(cf, key)
	if err != nil || cf != prefixColumn {
		return k, v, err
	}
	ctx, err := DecodeNodeKey(key)
	if err != nil {
		return nil, nil, err
	}
	if len(ctx.Path) != 0 {
		return k, v, nil
	}
	r, err := DecodeRecord(v)
	if err != nil {
		return nil, nil, err
	}
	r.Hash = s.hash
	v, err = EncodeRecord(r)
	return k, v, err
}

func TestSummaryScopeGenerationAndHistoricalIsolation(t *testing.T) {
	s, c, tree, want := summaryFixture(t, 0xffff)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	ctx := NodeKey{TrieID: 0, Path: []byte{3}, Height: 0}
	if _, state, hit := b.reader.lookupHash(ctx); !hit || state != hashState {
		t.Fatal("valid H not found")
	}
	for _, bad := range []NodeKey{{TrieID: 1, Path: ctx.Path, Height: 0}, {TrieID: 0, Path: ctx.Path, Height: 1}} {
		if _, _, hit := b.reader.lookupHash(bad); hit {
			t.Fatal("cross-context digest reused")
		}
	}
	beforeHead := b.reader.head
	b.reader.head.CommitID[0] ^= 1
	if _, _, hit := b.reader.lookupHash(ctx); hit {
		t.Fatal("different Head reused cache")
	}
	b.reader.head = beforeHead
	store := c.store
	c.store = &Store{}
	if _, _, hit := b.reader.lookupHash(ctx); hit {
		t.Fatal("different Store reused cache")
	}
	c.store = store
	readerGeneration := b.reader.generation
	b.reader.generation--
	if _, _, hit := b.reader.lookupHash(ctx); hit {
		t.Fatal("stale reader generation reused cache")
	}
	b.reader.generation = readerGeneration
	i, _ := c.cache.current.hashPosition([]byte{2})
	generation := c.cache.current.hashes[i].Generation
	c.cache.current.hashes[i].Generation--
	spy := wrapSummaryReader(b.reader.base)
	value, err := b.Get([]byte{0x30})
	if err != nil || !bytes.Equal(value, want[string([]byte{0x30})]) {
		t.Fatal(err)
	}
	if spy.prefixCount() != 3 {
		t.Fatal("stale sibling digest did not use disk", spy.prefix)
	}
	c.cache.current.hashes[i].Generation = generation
	// Even a corrupt live H digest cannot affect a separately opened historical
	// proof, which must neither consult nor modify this execution cache.
	i, _ = c.cache.current.hashPosition([]byte{2})
	old := c.cache.current.hashes[i].Hash
	c.cache.current.hashes[i].Hash = mpt.Hash{88}
	before, _ := c.Stats()
	root, _ := tree.Hash()
	assertProofs(t, s, 0, root, want, [][]byte{{0x20}, {0x30}})
	after, _ := c.Stats()
	if !reflect.DeepEqual(before, after) {
		t.Fatal("history changed execution summary statistics")
	}
	c.cache.current.hashes[i].Hash = old
	reader := b.reader
	b.Abort()
	if _, _, hit := reader.lookupHash(ctx); hit {
		t.Fatal("released reader retained cache access")
	}
	if _, err := reader.GetNode(ctx.Path, old); !errors.Is(err, ErrClosed) {
		t.Fatal("released reader usable", err)
	}
}

func TestSummaryDirtyWorkingStateKeepsPinnedBaseDigests(t *testing.T) {
	s, c, native, want := summaryFixture(t, 0xffff)
	oldRoot := c.root
	oldWant := map[string][]byte{}
	for k, v := range want {
		oldWant[k] = v
	}
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	changed := bytes.Repeat([]byte{9}, 64)
	if err = b.Update([]byte{0x30}, changed); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Get([]byte{0x30}); err != nil || !bytes.Equal(got, changed) {
		t.Fatal("cached old digest overrode dirty body", err)
	}
	if got, err := b.Get([]byte{0x40}); err != nil || !bytes.Equal(got, want[string([]byte{0x40})]) {
		t.Fatal("dirty sibling contaminated base parent", err)
	}
	native.Update([]byte{0x30}, changed)
	expected, _ := native.Hash()
	want[string([]byte{0x30})] = changed
	if root, err := b.Root(); err != nil || root != expected {
		t.Fatal("working root", err)
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	assertCache(t, c.cache, expected)
	assertProofs(t, s, 0, oldRoot, oldWant, [][]byte{{0x30}, {0x40}})
	assertProofs(t, s, 1, expected, want, [][]byte{{0x30}, {0x40}})
	b, err = c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Abort()
	if _, _, hit := b.reader.lookupHash(NodeKey{Path: []byte{3}, Height: 0}); hit {
		t.Fatal("new Head accepted old-height summary request")
	}
}
