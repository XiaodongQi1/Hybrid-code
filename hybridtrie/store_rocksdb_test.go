//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
	"testing"
)

func openTestStore(t *testing.T, path string, c Config, create, ro bool) *Store {
	t.Helper()
	s, e := Open(path, c, create, ro)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func assertProofs(t *testing.T, s *Store, height uint64, root mpt.Hash, want map[string][]byte, keys [][]byte) {
	t.Helper()
	v, e := s.ViewAt(height)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	got, e := v.Root(0)
	if e != nil || got != root {
		t.Fatal("root directory", e)
	}
	reader, e := v.TrieReader(0, root)
	if e != nil {
		t.Fatal(e)
	}
	tree, e := mpt.Open(root, reader)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range keys {
		value, e := tree.Get(key)
		if e != nil || !bytes.Equal(value, want[string(key)]) {
			t.Fatalf("height %d Get %x: %x %v", height, key, value, e)
		}
		proof, e := v.Prove(0, root, key)
		if e != nil {
			t.Fatal("prove", e)
		}
		value, exists, e := mpt.VerifyProof(root, key, proof)
		_, expected := want[string(key)]
		if e != nil || exists != expected || !bytes.Equal(value, want[string(key)]) {
			t.Fatal("proof", height, exists, e)
		}
	}
	if _, e = v.TrieReader(0, mpt.Hash{9}); !errors.Is(e, ErrHashMismatch) {
		t.Fatal("untrusted root accepted", e)
	}
}

func TestRocksDBColdHistoryAndLayoutTransitions(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "cold-history")
	c.InitialHeight = (1 << 44) + 17
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, c, true, false)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	want := map[string][]byte{}
	var head Head
	var roots []mpt.Hash
	var states []map[string][]byte
	keys := [][]byte{bytes.Repeat([]byte{0x11}, 32), bytes.Repeat([]byte{0x12}, 32), bytes.Repeat([]byte{0x21}, 32), bytes.Repeat([]byte{0x99}, 32)}
	var pinned *View
	for block := 0; block < 9; block++ {
		value := bytes.Repeat([]byte{byte(block%3 + 1)}, 40+block)
		if e := tree.Update(keys[block%3], value); e != nil {
			t.Fatal(e)
		}
		want[string(keys[block%3])] = value
		if block == 3 || block == 7 {
			tree.Delete(keys[0])
			delete(want, string(keys[0]))
		}
		mask := []uint16{0xffff, 0, 0xaaaa}[block%3]
		var stats CommitStats
		var e error
		head, stats, e = s.Commit(c.InitialHeight+uint64(block), head.CommitID, []TrieUpdate{{0, tree, mask}})
		if e != nil {
			t.Fatal(block, e)
		}
		if stats.PrefixRecords == 0 {
			t.Fatal("no final node versions")
		}
		root, _ := tree.Hash()
		roots = append(roots, root)
		state := map[string][]byte{}
		for k, v := range want {
			state[k] = append([]byte(nil), v...)
		}
		states = append(states, state)
		if block == 0 {
			pinned, e = s.ViewAt(head.Height)
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	if e := s.Close(); !errors.Is(e, ErrBusy) {
		t.Fatal("closed with pinned snapshot", e)
	}
	proof, e := pinned.Prove(0, roots[0], keys[0])
	if e != nil {
		t.Fatal(e)
	}
	if _, ok, e := mpt.VerifyProof(roots[0], keys[0], proof); e != nil || !ok {
		t.Fatal(e)
	}
	pinned.Close()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTestStore(t, path, c, false, true)
	defer s.Close()
	for i, root := range roots {
		assertProofs(t, s, c.InitialHeight+uint64(i), root, states[i], keys)
	}
	if _, _, e = s.Commit(head.Height+1, head.CommitID, nil); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
	v, _ := s.ViewAt(head.Height)
	reader, _ := v.TrieReader(0, roots[len(roots)-1])
	v.Close()
	if _, e = reader.(mpt.PathStore).GetNode(nil, roots[len(roots)-1]); !errors.Is(e, ErrClosed) {
		t.Fatal("closed reader", e)
	}
}

func TestRocksDBEmptyBlocksReinsertionAndInstances(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "reinsert")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	key := bytes.Repeat([]byte{1}, 32)
	tree.Update(key, []byte{7})
	root, _ := tree.Hash()
	h, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}, {1, tree, 0}})
	if e != nil {
		t.Fatal(e)
	}
	tree.Delete(key)
	h, _, e = s.Commit(1, h.CommitID, []TrieUpdate{{0, tree, 0}})
	if e != nil {
		t.Fatal(e)
	}
	h, _, e = s.Commit(2, h.CommitID, nil)
	if e != nil {
		t.Fatal(e)
	}
	tree.Update(key, []byte{7})
	h, _, e = s.Commit(3, h.CommitID, []TrieUpdate{{0, tree, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	v, _ := s.ViewAt(3)
	defer v.Close()
	r, e := v.Root(1)
	if e != nil || r != root {
		t.Fatal("namespace changed", e)
	}
	snap := s.db.snapshot()
	defer snap.close()
	k, _ := EncodeNodeKey(NodeKey{Height: 3})
	found, _, e := snap.prev(prefixColumn, k)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := DecodeNodeKey(found)
	if e != nil || decoded.Height != 3 {
		t.Fatal("reactivation did not write new occurrence", decoded, e)
	}
	assertProofs(t, s, 2, mpt.EmptyRootHash, map[string][]byte{}, [][]byte{key})
	assertProofs(t, s, 3, root, map[string][]byte{string(key): {7}}, [][]byte{key})
	if _, _, e = s.Commit(3, h.CommitID, nil); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
}

func TestRocksDBFailureRecovery(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			c := DefaultConfig("raw-bytes/v1", "fault")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, c, true, false)
			tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
			h, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0}})
			if e != nil {
				t.Fatal(e)
			}
			oldRoot, _ := tree.Hash()
			tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
			newRoot, _ := tree.Hash()
			realWrite := s.write
			s.write = func(entries []entry) error {
				if after {
					if e := realWrite(entries); e != nil {
						return e
					}
				}
				return errors.New("injected write error")
			}
			if _, _, e = s.Commit(1, h.CommitID, []TrieUpdate{{0, tree, 0xffff}}); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			if _, e = s.Head(); !errors.Is(e, ErrUncertain) {
				t.Fatal("write handle reused", e)
			}
			s.Close()
			s = openTestStore(t, path, c, false, false)
			defer s.Close()
			head, e := s.Head()
			if e != nil {
				t.Fatal(e)
			}
			expected := uint64(0)
			root := oldRoot
			state := map[string][]byte{"a": bytes.Repeat([]byte{1}, 40)}
			if after {
				expected = 1
				root = newRoot
				state["b"] = bytes.Repeat([]byte{2}, 40)
			}
			if head.Height != expected {
				t.Fatal(head)
			}
			assertProofs(t, s, expected, root, state, [][]byte{[]byte("a"), []byte("b")})
		})
	}
}

func TestRocksDBLimitsManifestAndCorruption(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "limits")
	c.MaxBatchBytes = 4096
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, c, true, false)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("large"), make([]byte, 5000))
	if _, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0}}); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if h, e := s.Head(); e != nil || h.Initialized {
		t.Fatal("failed plan published", h, e)
	}
	tree, _ = mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
	h, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	root, _ := tree.Hash()
	snap := s.db.snapshot()
	key, _ := EncodeNodeKey(NodeKey{})
	_, raw, e := snap.prev(prefixColumn, key)
	snap.close()
	if e != nil {
		t.Fatal(e)
	}
	r, e := DecodeRecord(raw)
	if e != nil {
		t.Fatal(e)
	}
	// Tamper the persisted root's claimed digest. Proving absence must fail.
	r.Hash[0] ^= 1
	raw, _ = EncodeRecord(r)
	if e = s.db.write([]entry{{prefixColumn, key, raw}}); e != nil {
		t.Fatal(e)
	}
	v, _ := s.ViewAt(h.Height)
	if _, e = v.Prove(0, root, []byte("missing")); e == nil {
		t.Fatal("corrupt record accepted as absence")
	}
	v.Close()
	s.Close()
	changed := c
	changed.WorkloadID = "wrong"
	if opened, e := Open(path, changed, false, true); e == nil {
		opened.Close()
		t.Fatal("mismatched manifest accepted")
	}
	if opened, e := Open(path, c, true, false); e == nil {
		opened.Close()
		t.Fatal("existing db overwritten")
	}
}

func TestRocksDBProcessExitRecovery(t *testing.T) {
	if mode := os.Getenv("HYBRID_EXIT_MODE"); mode != "" {
		c := DefaultConfig("raw-bytes/v1", "process-exit")
		s := openTestStore(t, os.Getenv("HYBRID_EXIT_PATH"), c, false, false)
		tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
		tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
		head, _ := s.Head()
		write := s.write
		s.write = func(entries []entry) error {
			if mode == "after" {
				if e := write(entries); e != nil {
					t.Fatal(e)
				}
			}
			os.Exit(37)
			return nil
		}
		s.Commit(1, head.CommitID, []TrieUpdate{{0, tree, 0}})
		t.Fatal("exit hook not reached")
	}
	for _, mode := range []string{"before", "after"} {
		t.Run(mode, func(t *testing.T) {
			c := DefaultConfig("raw-bytes/v1", "process-exit")
			path := filepath.Join(t.TempDir(), "db")
			s := openTestStore(t, path, c, true, false)
			tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
			oldRoot, _ := tree.Hash()
			if _, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}}); e != nil {
				t.Fatal(e)
			}
			s.Close()
			exe, e := os.Executable()
			if e != nil {
				t.Fatal(e)
			}
			cmd := exec.Command(exe, "-test.run=^TestRocksDBProcessExitRecovery$")
			cmd.Env = append(os.Environ(), "HYBRID_EXIT_MODE="+mode, "HYBRID_EXIT_PATH="+path)
			output, e := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(e, &exit) || exit.ExitCode() != 37 {
				t.Fatalf("child exit: %v %s", e, output)
			}
			s = openTestStore(t, path, c, false, true)
			defer s.Close()
			head, e := s.Head()
			if e != nil {
				t.Fatal(e)
			}
			root := oldRoot
			state := map[string][]byte{"a": bytes.Repeat([]byte{1}, 40)}
			height := uint64(0)
			if mode == "after" {
				tree, _ = mpt.NewEmpty(mpt.NewMemoryStore())
				tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
				root, _ = tree.Hash()
				height = 1
				state = map[string][]byte{"b": bytes.Repeat([]byte{2}, 40)}
			}
			if head.Height != height {
				t.Fatal(head)
			}
			assertProofs(t, s, height, root, state, [][]byte{[]byte("a"), []byte("b")})
		})
	}
}

func TestRocksDBConcurrentHistoricalReads(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "concurrent")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
	root, _ := tree.Hash()
	head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	v, _ := s.ViewAt(0)
	defer v.Close()
	var wg sync.WaitGroup
	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				proof, e := v.Prove(0, root, []byte("a"))
				if e != nil {
					t.Error(e)
					return
				}
				value, exists, e := mpt.VerifyProof(root, []byte("a"), proof)
				if e != nil || !exists || !bytes.Equal(value, bytes.Repeat([]byte{1}, 40)) {
					t.Error("historical view changed", e)
					return
				}
			}
		}()
	}
	for i := 1; i < 8; i++ {
		tree.Update([]byte("a"), bytes.Repeat([]byte{byte(i + 1)}, 40))
		head, _, e = s.Commit(uint64(i), head.CommitID, []TrieUpdate{{0, tree, uint16(i%2) * 0xffff}})
		if e != nil {
			t.Fatal(e)
		}
	}
	wg.Wait()
}

type noHashSnapshot struct{ diskSnapshot }

func (s noHashSnapshot) get(cf column, key []byte) ([]byte, error) {
	if cf == hashColumn {
		return nil, ErrNotFound
	}
	return s.diskSnapshot.get(cf, key)
}
func TestRocksDBMissingDataAndReadBudget(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "missing-data")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
	root, _ := tree.Hash()
	if _, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0}}); e != nil {
		t.Fatal(e)
	}
	v, _ := s.ViewAt(0)
	v.snapshot = noHashSnapshot{v.snapshot}
	if _, e := v.Prove(0, root, []byte("a")); e == nil {
		t.Fatal("missing Hash child became absence")
	}
	v.Close()
	v, _ = s.ViewAt(0)
	defer v.Close()
	reader, e := v.TrieReader(0, root)
	if e != nil {
		t.Fatal(e)
	}
	r := reader.(*trieReader)
	r.cfg.MaxReadNodes = 1
	if _, e = r.GetNode([]byte{6, 1}, mpt.Hash{}); !errors.Is(e, ErrLimit) {
		t.Fatal("read budget ignored", e)
	}
}

func TestRocksDBMaxHeightExhaustion(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "height-end")
	c.InitialHeight = MaxHeight
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), []byte{1})
	root, _ := tree.Hash()
	h, _, e := s.Commit(MaxHeight, [32]byte{}, []TrieUpdate{{0, tree, 0}})
	if e != nil {
		t.Fatal(e)
	}
	assertProofs(t, s, MaxHeight, root, map[string][]byte{"a": {1}}, [][]byte{[]byte("a")})
	if _, _, e = s.Commit(MaxHeight+1, h.CommitID, nil); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if _, _, e = s.Commit(0, h.CommitID, nil); !errors.Is(e, ErrLimit) {
		t.Fatal("height wrapped", e)
	}
}

func TestRocksDBUpdateFromColdHistoricalReader(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "cold-update")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	oracle, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	oracle.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	oracle.Update([]byte("ab"), bytes.Repeat([]byte{2}, 40))
	head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, oracle, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	want := map[string][]byte{"a": bytes.Repeat([]byte{1}, 40), "ab": bytes.Repeat([]byte{2}, 40)}
	for h := uint64(1); h <= 4; h++ {
		root, _ := oracle.Hash()
		view, e := s.ViewAt(h - 1)
		if e != nil {
			t.Fatal(e)
		}
		reader, e := view.TrieReader(0, root)
		if e != nil {
			t.Fatal(e)
		}
		tree, e := mpt.Open(root, reader)
		if e != nil {
			t.Fatal(e)
		}
		key := []byte("ab")
		value := bytes.Repeat([]byte{byte(h + 2)}, 40)
		if h == 2 {
			value = nil
		}
		if h == 3 {
			key = []byte("ac")
		}
		if e = tree.Update(key, value); e != nil {
			t.Fatal(e)
		}
		oracle.Update(key, value)
		if len(value) == 0 {
			delete(want, string(key))
		} else {
			want[string(key)] = value
		}
		got, _ := tree.Hash()
		expected, _ := oracle.Hash()
		if got != expected {
			t.Fatal("cold update changed root")
		}
		head, _, e = s.Commit(h, head.CommitID, []TrieUpdate{{0, tree, uint16(h%2) * 0xffff}})
		if e != nil {
			t.Fatal(e)
		}
		view.Close()
		assertProofs(t, s, h, expected, want, [][]byte{[]byte("a"), []byte("ab"), []byte("ac"), []byte("missing")})
	}
}

func databaseFingerprint(t *testing.T, path string) string {
	t.Helper()
	sum := sha256.New()
	e := filepath.Walk(path, func(name string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.IsDir() {
			return nil
		}
		relative, e := filepath.Rel(path, name)
		if e != nil {
			return e
		}
		raw, e := os.ReadFile(name)
		if e != nil {
			return e
		}
		fmt.Fprintf(sum, "%s:%d:", relative, len(raw))
		sum.Write(raw)
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return fmt.Sprintf("%x", sum.Sum(nil))
}
func TestRocksDBReadOnlyFilesUnchanged(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "readonly-bytes")
	path := filepath.Join(t.TempDir(), "db")
	s := openTestStore(t, path, c, true, false)
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	root, _ := tree.Hash()
	if _, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0}}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	before := databaseFingerprint(t, path)
	s = openTestStore(t, path, c, false, true)
	assertProofs(t, s, 0, root, map[string][]byte{"a": bytes.Repeat([]byte{1}, 40)}, [][]byte{[]byte("a"), []byte("missing")})
	s.Close()
	if after := databaseFingerprint(t, path); after != before {
		t.Fatalf("read-only files changed: %s -> %s", before, after)
	}
}

func TestRocksDBPreparedLayoutAndBackfillCounters(t *testing.T) {
	c := DefaultConfig("raw-bytes/v1", "prepared-layout")
	s := openTestStore(t, filepath.Join(t.TempDir(), "db"), c, true, false)
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	tree.Update([]byte("a"), bytes.Repeat([]byte{1}, 40))
	tree.Update([]byte("b"), bytes.Repeat([]byte{2}, 40))
	head, stats, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}})
	if e != nil || stats.HashRecords != 0 {
		t.Fatal(stats, e)
	}
	tree.Update([]byte("a"), bytes.Repeat([]byte{3}, 40))
	root, _ := tree.Hash()
	_, _, e = s.CommitWithLayout(1, head.CommitID, []TrieUpdate{{0, tree, 0xffff}}, func(NodeKey, uint16) (uint16, error) { return 0, errors.New("rejected layout") })
	if e == nil {
		t.Fatal("layout error ignored")
	}
	if actual, e := s.Head(); e != nil || actual != head {
		t.Fatal("failed layout published", e)
	}
	head, stats, e = s.CommitWithLayout(1, head.CommitID, []TrieUpdate{{0, tree, 0xffff}}, func(ctx NodeKey, eligible uint16) (uint16, error) { ctx.Path = append(ctx.Path, 9); return 0, nil })
	if e != nil || stats.HashRecords == 0 || stats.ClosureBackfillRecords != 0 || stats.ClosureBackfillBytes != 0 {
		t.Fatal(stats, e)
	}
	assertProofs(t, s, head.Height, root, map[string][]byte{"a": bytes.Repeat([]byte{3}, 40), "b": bytes.Repeat([]byte{2}, 40)}, [][]byte{[]byte("a"), []byte("b")})
}
