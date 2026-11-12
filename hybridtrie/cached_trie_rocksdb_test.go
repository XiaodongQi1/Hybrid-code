//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
	"testing"
)

func cachedFixture(t *testing.T, cfg CacheConfig, keys int) (*Store, *CachedTrie, *mpt.Trie, map[string][]byte) {
	t.Helper()
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "bounded-cache"), true, false)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	want := map[string][]byte{}
	for i := 0; i < keys; i++ {
		key := sha256.Sum256([]byte(fmt.Sprint(i)))
		value := bytes.Repeat([]byte{byte(i)}, 64)
		if err := tree.Update(key[:], value); err != nil {
			t.Fatal(err)
		}
		want[string(key[:])] = value
	}
	if _, _, err := s.Commit(0, [32]byte{}, []TrieUpdate{{TrieID: 0, Tree: tree, PrefixMask: 0xffff}}); err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Hash()
	c, err := OpenCachedTrie(s, 0, root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return s, c, tree, want
}

func TestCachedTrieBoundedExecutionAndColdHistory(t *testing.T) {
	for _, mode := range []string{"tiny-no-H", "tiny", "default"} {
		t.Run(mode, func(t *testing.T) {
			cfg := DefaultCacheConfig()
			if mode != "default" {
				cfg.NodeBytes, cfg.HashBytes, cfg.CandidateBytes = 16<<10, 8<<10, 32<<10
			}
			if mode == "tiny-no-H" {
				cfg.HashBytes, cfg.CandidateBytes = 0, 0
			}
			s, c, native, want := cachedFixture(t, cfg, 256)
			assertCache(t, c.cache, c.root)
			if c.cache.current.n != 1 || c.cache.current.h > 16 {
				t.Fatal("open prefetched descendants")
			}
			type history struct {
				height uint64
				root   mpt.Hash
				state  map[string][]byte
			}
			var snapshots []history
			var queryKeys [][]byte
			for height := uint64(1); height <= 40; height++ {
				b, err := c.BeginBlock(height)
				if err != nil {
					t.Fatal(height, err)
				}
				for j := 0; j < 8; j++ {
					i := (int(height)*19 + j*31) % 272
					key := sha256.Sum256([]byte(fmt.Sprint(i)))
					got, err := b.Get(key[:])
					if err != nil || !bytes.Equal(got, want[string(key[:])]) {
						t.Fatal("execution Get", height, err)
					}
					value := bytes.Repeat([]byte{byte(height), byte(j)}, 24+int(height%9))
					if (int(height)+j)%7 == 0 {
						value = nil
					}
					if err := b.Update(key[:], value); err != nil {
						t.Fatal("execution Update", height, err)
					}
					native.Update(key[:], value)
					if len(value) == 0 {
						delete(want, string(key[:]))
					} else {
						want[string(key[:])] = value
					}
					if j == 0 {
						queryKeys = append(queryKeys, append([]byte(nil), key[:]...))
					}
				}
				root, err := b.Root()
				expected, _ := native.Hash()
				if err != nil || root != expected {
					t.Fatal("root before commit", height, err)
				}
				head, _, err := b.Commit()
				if err != nil || head.Height != height {
					t.Fatal("commit", height, err)
				}
				if b.tree != nil || b.reader != nil || b.view != nil || c.active != nil || s.views != 0 {
					t.Fatal("retained working tree/view")
				}
				if _, err := b.Get(nil); !errors.Is(err, ErrClosed) {
					t.Fatal("spent block usable", err)
				}
				assertCache(t, c.cache, root)
				if height%10 == 0 {
					state := map[string][]byte{}
					for k, v := range want {
						state[k] = append([]byte(nil), v...)
					}
					snapshots = append(snapshots, history{height, root, state})
				}
			}
			stats, _ := c.Stats()
			if stats.NodeBodyHits+stats.PreparationHits == 0 || stats.NodeBodyMisses == 0 || stats.NodeEvictions == 0 {
				t.Fatal("cache not exercised", stats)
			}
			if mode != "tiny-no-H" && (stats.HashAdmissions == 0 || stats.NodeEntries <= 1) {
				t.Fatal("N/H not used", stats)
			}
			for _, snapshot := range snapshots {
				assertProofs(t, s, snapshot.height, snapshot.root, snapshot.state, queryKeys[:4])
			}
			after, _ := c.Stats()
			if !reflect.DeepEqual(stats, after) {
				t.Fatal("historical proof polluted execution cache")
			}
			t.Logf("N/H reserved=%d/%d KiB; candidates=%d KiB; plan=%d KiB; live=%d/%d bytes; entries=%d/%d; hits/misses=%d/%d; max block charge=%d bytes", stats.NodeAllocatedBytes>>10, stats.HashAllocatedBytes>>10, stats.CandidateAllocatedBytes>>10, stats.PlanAllocatedBytes>>10, stats.NodeLiveBytes, stats.HashLiveBytes, stats.NodeEntries, stats.HashEntries, stats.NodeBodyHits, stats.NodeBodyMisses, stats.MaxBlockChargedBytes)
		})
	}
}

func TestCachedTrieColdReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cfgDB := DefaultConfig("raw-bytes/v1", "cache-reopen")
	s := openTestStore(t, path, cfgDB, true, false)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte{1}, []byte{2})
	if _, _, err := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}}); err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Hash()
	c, err := OpenCachedTrie(s, 0, root, DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Update([]byte{2}, []byte{3}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	root = c.root
	c.Close()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTestStore(t, path, cfgDB, false, false)
	defer s.Close()
	c, err = OpenCachedTrie(s, 0, root, DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assertCache(t, c.cache, root)
	b, err = c.BeginBlock(2)
	if err != nil {
		t.Fatal(err)
	}
	value, err := b.Get([]byte{2})
	if err != nil || !bytes.Equal(value, []byte{3}) {
		t.Fatal(value, err)
	}
	b.Abort()
}

func TestCachedTrieInlineTransitionsEmptyAndReinsert(t *testing.T) {
	_, c, native, _ := cachedFixture(t, DefaultCacheConfig(), 0)
	keys := [][]byte{{0x10}, {0x11}, {0x10, 0x12}, {}, {0x22}}
	for height := uint64(1); height <= 5; height++ {
		b, err := c.BeginBlock(height)
		if err != nil {
			t.Fatal(err)
		}
		for j, key := range keys {
			value := []byte{byte(j + 1)}
			if height == 2 {
				value = bytes.Repeat(value, 40)
			}
			if height == 4 {
				value = nil
			}
			if err := b.Update(key, value); err != nil {
				t.Fatal(err)
			}
			native.Update(key, value)
		}
		expected, _ := native.Hash()
		if _, _, err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		if c.root != expected {
			t.Fatal("inline transition root")
		}
		assertCache(t, c.cache, expected)
	}
}

func TestCachedTrieAbortLimitsAndStaleWriter(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.MaxOperations = 2
	s, c, _, _ := cachedFixture(t, cfg, 0)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginBlock(1); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err := b.Update([]byte{1}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := b.Update([]byte{2}, []byte{3}); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Root()
	if err := b.Update([]byte{3}, []byte{4}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	after, _ := b.Root()
	if before != after {
		t.Fatal("operation limit changed root")
	}
	generation := c.cache.generation
	b.Abort()
	b.Abort()
	if c.cache.generation != generation || c.root != mpt.EmptyRootHash || s.views != 0 {
		t.Fatal("abort installed state")
	}
	head, _ := s.Head()
	if _, _, err := s.Commit(1, head.CommitID, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginBlock(2); !errors.Is(err, ErrConflict) {
		t.Fatal("external writer accepted", err)
	}
}

func TestCachedTrieWorkLimitAndConcurrentReads(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.MaxBlockCharge = 1 << 20
	_, c, _, _ := cachedFixture(t, cfg, 0)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := b.Update([]byte{byte(i)}, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := b.Root()
	if err := b.Update([]byte{4}, []byte{1}); !errors.Is(err, ErrLimit) {
		t.Fatal("work guard", err)
	}
	after, _ := b.Root()
	if before != after {
		t.Fatal("work limit mutated state")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 4; j++ {
				if _, err := b.Get([]byte{1}); err != nil {
					t.Error(err)
				}
				c.Stats()
			}
		}()
	}
	wg.Wait()
	if _, _, err := b.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestCachedTrieCommitFailureKeepsOldLayout(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			s, c, _, _ := cachedFixture(t, DefaultCacheConfig(), 16)
			oldRoot, generation := c.root, c.cache.generation
			b, err := c.BeginBlock(1)
			if err != nil {
				t.Fatal(err)
			}
			if err = b.Update([]byte{0x42}, []byte{4}); err != nil {
				t.Fatal(err)
			}
			write := s.write
			s.write = func(entries []entry) error {
				if applied {
					if err := write(entries); err != nil {
						return err
					}
				}
				return errors.New("injected write outcome")
			}
			if _, _, err = b.Commit(); !errors.Is(err, ErrUncertain) {
				t.Fatal(err)
			}
			if c.root != oldRoot || c.cache.generation != generation || b.tree != nil || s.views != 0 {
				t.Fatal("installed speculative layout or retained view")
			}
			assertCache(t, c.cache, oldRoot)
			if _, err = c.BeginBlock(1); !errors.Is(err, ErrUncertain) {
				t.Fatal("not fail-stop", err)
			}
		})
	}
}

func TestCachedTriePrewriteFailureAndPlanNoIO(t *testing.T) {
	s, c, _, _ := cachedFixture(t, DefaultCacheConfig(), 0)
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		key := sha256.Sum256([]byte(fmt.Sprint(i)))
		if err = b.Update(key[:], bytes.Repeat([]byte{1}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	generation := c.cache.generation
	s.mu.Lock()
	original := s.cfg.MaxBatchBytes
	s.cfg.MaxBatchBytes = 4096
	s.mu.Unlock()
	if _, _, err = b.Commit(); !errors.Is(err, ErrLimit) {
		t.Fatal("prewrite limit", err)
	}
	if c.cache.generation != generation || c.active != b || c.root != mpt.EmptyRootHash {
		t.Fatal("prewrite error installed cache")
	}
	s.mu.Lock()
	s.cfg.MaxBatchBytes = original
	s.mu.Unlock()
	if _, _, err = b.Commit(); err != nil {
		t.Fatal("retry", err)
	}
	stats, _ := c.Stats()
	if stats.PreparationMisses != 0 || stats.NodeBodyMisses != 0 {
		t.Fatal("new materialized tree required prefetch", stats)
	}
}

func TestCachedTriePinnedRootOverflowAndRootBinding(t *testing.T) {
	cfg := DefaultCacheConfig()
	cfg.NodeBytes = 16 << 10
	s, c, _, _ := cachedFixture(t, cfg, 0)
	if _, err := OpenCachedTrie(s, 0, mpt.Hash{1}, cfg); !errors.Is(err, ErrHashMismatch) {
		t.Fatal("untrusted root", err)
	}
	b, err := c.BeginBlock(1)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Update([]byte{1}, bytes.Repeat([]byte{1}, 9<<10)); err != nil {
		t.Fatal(err)
	}
	gen := c.cache.generation
	if _, _, err = b.Commit(); !errors.Is(err, ErrLimit) {
		t.Fatal("oversize pinned root accepted", err)
	}
	if c.cache.generation != gen || c.root != mpt.EmptyRootHash {
		t.Fatal("overflow installed plan")
	}
	if err = b.Update([]byte{1}, []byte{2}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = b.Commit(); err != nil {
		t.Fatal("repair and retry", err)
	}
}
