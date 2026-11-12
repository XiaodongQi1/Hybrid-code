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

func TestCachedForestAtomicEVMRecovery(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			cfg := DefaultConfig("ethereum-mpt", "forest-evm-fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, cfg, true, false)
			f, err := OpenCachedForest(s, DefaultForestCacheConfig())
			if err != nil {
				t.Fatal(err)
			}
			account, storage := forestTree(t, 1), forestTree(t, 2)
			u, err := f.BeginBlock(0, [32]byte{})
			if err != nil {
				t.Fatal(err)
			}
			if err = u.SetTrie(0, account); err != nil {
				t.Fatal(err)
			}
			if err = u.SetBlockID([32]byte{1}); err != nil {
				t.Fatal(err)
			}
			h, _, err := u.Commit()
			if err != nil {
				t.Fatal(err)
			}
			u, err = f.BeginBlock(1, h.CommitID)
			if err != nil {
				t.Fatal(err)
			}
			root, _ := account.Hash()
			r, err := u.TrieReader(0, root)
			if err != nil {
				t.Fatal(err)
			}
			at, err := mpt.Open(root, r)
			if err != nil {
				t.Fatal(err)
			}
			if err = at.Update([]byte{0x12}, bytes.Repeat([]byte{3}, 64)); err != nil {
				t.Fatal(err)
			}
			id, err := u.CreateStorageTrie([20]byte{1})
			if err != nil {
				t.Fatal(err)
			}
			if err = u.SetTrie(0, at); err != nil {
				t.Fatal(err)
			}
			if err = u.SetTrie(id, storage); err != nil {
				t.Fatal(err)
			}
			code := []byte{0x60, 0x01, 0x00}
			hash := codeDigest(code)
			if err = u.PutCode(hash, code); err != nil {
				t.Fatal(err)
			}
			if err = u.SetBlockID([32]byte{2}); err != nil {
				t.Fatal(err)
			}
			before := f.sessions[0].trie.cache.generation
			real := s.write
			s.write = func(entries []entry) error {
				seen := map[byte]int{}
				for _, e := range entries {
					if e.cf == metaColumn {
						seen[e.key[0]]++
					}
				}
				if seen['E'] != 1 || seen['C'] != 1 || seen['L'] != 1 || seen['O'] != 1 || seen['R'] != 2 || seen['M'] != 1 {
					t.Fatal("incomplete atomic EVM batch", seen)
				}
				if f.sessions[0].trie.cache.generation != before {
					t.Fatal("cache published before durable write")
				}
				if after {
					if err := real(entries); err != nil {
						return err
					}
				}
				return errors.New("injected EVM write outcome")
			}
			if _, _, err = u.Commit(); !errors.Is(err, ErrUncertain) {
				t.Fatal(err)
			}
			if f.sessions[0].trie.cache.generation != before {
				t.Fatal("uncertain write installed shadow cache")
			}
			f.Close()
			s.Close()
			s = openTestStore(t, path, cfg, false, false)
			defer s.Close()
			head, err := s.Head()
			if err != nil {
				t.Fatal(err)
			}
			expectHeight := uint64(0)
			expectID := [32]byte{1}
			if after {
				expectHeight = 1
				expectID = [32]byte{2}
			}
			if head.Height != expectHeight || head.NextTrieID != 1+expectHeight {
				t.Fatal(head)
			}
			v, err := s.ViewAt(expectHeight)
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			blockID, err := v.BlockID()
			if err != nil || blockID != expectID {
				t.Fatal(blockID, err)
			}
			got, err := v.Code(hash)
			if after {
				if err != nil || !bytes.Equal(got, code) {
					t.Fatal("durable code missing", err)
				}
				forestMapping(t, s, 1, [20]byte{1}, id, true)
				forestProof(t, s, 1, 0, forestTree(t, 3), 3)
				forestProof(t, s, 1, id, storage, 2)
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatal("uncommitted code leaked", err)
			}
		})
	}
}

func TestCachedForestAbortLimitHistoricalIsolationAndEviction(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "forest-cache-bounds")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	pool := DefaultForestCacheConfig()
	pool.Slots = 2
	f, err := OpenCachedForest(s, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	u, _ := f.BeginBlock(0, [32]byte{})
	trees := map[uint64]*mpt.Trie{0: forestTree(t, 1)}
	u.SetTrie(0, trees[0])
	for n := byte(1); n <= 4; n++ {
		id, err := u.CreateStorageTrie([20]byte{n})
		if err != nil {
			t.Fatal(err)
		}
		trees[id] = forestTree(t, n+1)
		if err = u.SetTrie(id, trees[id]); err != nil {
			t.Fatal(err)
		}
	}
	h, _, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	for height := uint64(1); height <= 4; height++ {
		u, err = f.BeginBlock(height, h.CommitID)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []uint64{0, height} {
			root, _ := trees[id].Hash()
			r, err := u.TrieReader(id, root)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := mpt.Open(root, r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = loaded.Get([]byte{0x12}); err != nil {
				t.Fatal(err)
			}
		}
		if err = u.SetTrie(0, trees[0]); err != nil {
			t.Fatal(err)
		}
		if height == 1 {
			root, _ := trees[0].Hash()
			reader, _ := u.TrieReader(0, root)
			changed, err := mpt.Open(root, reader)
			if err != nil {
				t.Fatal(err)
			}
			if err = changed.Update([]byte{0x13}, make([]byte, 8192)); err != nil {
				t.Fatal(err)
			}
			u.SetTrie(0, changed)
			budget := s.cfg.MaxBatchBytes
			s.cfg.MaxBatchBytes = 4096
			generation := f.sessions[0].trie.cache.generation
			if _, _, err = u.Commit(); !errors.Is(err, ErrLimit) {
				t.Fatal("pre-write failure", err)
			}
			if got, _ := s.Head(); got != h || f.sessions[0].trie.cache.generation != generation {
				t.Fatal("failed cache plan published")
			}
			s.cfg.MaxBatchBytes = budget
			u.Abort()
			u, err = f.BeginBlock(height, h.CommitID)
			if err != nil {
				t.Fatal(err)
			}
			u.SetTrie(0, trees[0])
		}
		h, _, err = u.Commit()
		if err != nil {
			t.Fatal(err)
		}
		before, _ := f.Stats()
		forestProof(t, s, 0, 1, trees[1], 2)
		after, _ := f.Stats()
		if !reflect.DeepEqual(before, after) {
			t.Fatal("historical query changed execution cache")
		}
		if before.Partitions > 2 || before.NodeAllocatedBytes > 2*pool.PerTrie.NodeBytes {
			t.Fatal(before)
		}
	}
	stats, _ := f.Stats()
	if stats.Evictions == 0 {
		t.Fatal("partition eviction was not exercised", stats)
	}
	if f.active != nil || s.views != 0 {
		t.Fatal("retained working trie or snapshot", s.views)
	}
}

func TestForestEVMMetadataBoundsOwnershipAndCorruption(t *testing.T) {
	cfg := DefaultConfig("ethereum-mpt", "evm-metadata")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), cfg, true, false)
	defer s.Close()
	u := forestBegin(t, s, 0, [32]byte{})
	if err := u.SetBlockID([32]byte{}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := u.SetBlockID([32]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := u.SetBlockID([32]byte{2}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	code := []byte{0x60, 1, 0}
	hash := codeDigest(code)
	if err := u.PutCode(mpt.Hash{1}, code); !errors.Is(err, ErrCorrupt) {
		t.Fatal("unbound code accepted", err)
	}
	if err := u.PutCode(hash, code); err != nil {
		t.Fatal(err)
	}
	code[0] = 0xff
	if _, _, err := u.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal("EVM block published without account root", err)
	}
	forestSet(t, u, 0, forestTree(t, 1))
	h, _ := forestCommit(t, u)
	v, err := s.ViewAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if got, err := v.Code(hash); err != nil || !bytes.Equal(got, []byte{0x60, 1, 0}) {
		t.Fatal("PutCode retained caller memory", got, err)
	}
	u = forestBegin(t, s, 1, h.CommitID)
	if err = u.PutCode(hash, []byte{0x60, 1, 0}); err != nil {
		t.Fatal(err)
	}
	forestSet(t, u, 0, forestTree(t, 1))
	if err = u.SetBlockID([32]byte{2}); err != nil {
		t.Fatal(err)
	}
	forestCommit(t, u)
	if id, err := v.BlockID(); err != nil || id != [32]byte{1} {
		t.Fatal("pinned block ID changed", err)
	}
	if err = s.write([]entry{{metaColumn, codeKey(hash), []byte{0xff}}}); err != nil {
		t.Fatal(err)
	}
	current, err := s.ViewAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer current.Close()
	if _, err = current.Code(hash); !errors.Is(err, ErrHashMismatch) {
		t.Fatal("corrupt bytecode accepted", err)
	}
}

func TestCachedForestReaderWithConcurrentStats(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), DefaultConfig("raw-bytes/v1", "forest-stats"), true, false)
	defer s.Close()
	f, err := OpenCachedForest(s, DefaultForestCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	u, _ := f.BeginBlock(0, [32]byte{})
	tree := forestTree(t, 1)
	u.SetTrie(0, tree)
	h, _, err := u.Commit()
	if err != nil {
		t.Fatal(err)
	}
	u, err = f.BeginBlock(1, h.CommitID)
	if err != nil {
		t.Fatal(err)
	}
	root, _ := tree.Hash()
	reader, err := u.TrieReader(0, root)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := mpt.Open(root, reader)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := f.Stats(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		if _, err := loaded.Get([]byte{0x12}); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	u.Abort()
}
