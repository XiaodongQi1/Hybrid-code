package versionedtrie

import (
	"bytes"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
	"testing"
)

func testStore(t *testing.T) (*Store, string, Config) {
	t.Helper()
	p := t.TempDir() + "/db"
	c := DefaultConfig("ethereum-mpt", "fixture")
	s, e := Open(p, c, true, false)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s, p, c
}
func begin(t *testing.T, s *Store, h uint64) *Update {
	t.Helper()
	id := [32]byte{1}
	root := mpt.Hash{1}
	u, e := s.Begin(h, root, id)
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func record(v byte) Record {
	return Record{Kind: Branch, Bitmap: 1, Hash: mpt.Hash{v}, Value: []byte{v}}
}
func view(t *testing.T, s *Store, h uint64) *View {
	t.Helper()
	v, e := s.ViewAt(h)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { v.Close() })
	return v
}
func commit(t *testing.T, u *Update) {
	t.Helper()
	if e := u.Commit(); e != nil {
		t.Fatal(e)
	}
}
func TestAtomicLifecycleHistoryAndReopen(t *testing.T) {
	s, path, cfg := testStore(t)
	owner := [32]byte{1}
	p := []byte{1, 2, 3}
	u := begin(t, s, 0)
	q, e := u.AllocateTrieID()
	if e != nil || q != 1 {
		t.Fatal(q, e)
	}
	if e = u.PutNode(uint64(q), p, record(7)); e != nil {
		t.Fatal(e)
	}
	if e = u.PutNode(0, nil, record(1)); e != nil {
		t.Fatal(e)
	}
	if e = u.PutLifecycle(owner, Lifecycle{true, q}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ViewAt(0); !errors.Is(e, ErrNotFound) {
		t.Fatal("uncommitted head visible", e)
	}
	commit(t, u)
	old := view(t, s, 0)
	u = begin(t, s, 1)
	if e = u.PutLifecycle(owner, Lifecycle{}); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	u = begin(t, s, 2)
	q2, e := u.AllocateTrieID()
	if e != nil || q2 != 2 {
		t.Fatal(q2, e)
	}
	u.PutLifecycle(owner, Lifecycle{true, q2})
	u.PutNode(uint64(q2), p, record(8))
	code := []byte("contract code")
	hash := codeHash(code)
	if e = u.PutCode(hash, code); e != nil {
		t.Fatal(e)
	}
	code[0] = 0
	commit(t, u)
	for h, want := range []Lifecycle{{true, 1}, {}, {true, 2}} {
		v := view(t, s, uint64(h))
		_, life, e := v.Lifecycle(owner)
		if e != nil || life != want {
			t.Fatal(h, life, e)
		}
		if want.Active {
			height, node, e := v.Node(uint64(want.TrieID), p)
			if e != nil || height != uint32(h) || node.Value[0] != byte(7+h/2) {
				t.Fatal(h, height, node, e)
			}
		}
		v.Close()
	}
	if _, _, e = old.Node(2, p); !errors.Is(e, ErrNotFound) {
		t.Fatal("new instance leaked", e)
	}
	if _, e = old.Code(hash); !errors.Is(e, ErrNotFound) {
		t.Fatal("new code leaked into snapshot", e)
	}
	if _, e = old.snap.Get(rootKey(2), nil); !errors.Is(e, leveldb.ErrNotFound) {
		t.Fatal("snapshot moved", e)
	}
	if e = s.Close(); !errors.Is(e, ErrBusy) {
		t.Fatal("closed active snapshot", e)
	}
	old.Close()
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s2, e := Open(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	v := view(t, s2, 2)
	defer v.Close()
	if b, e := v.Code(hash); e != nil || string(b) != "contract code" {
		t.Fatal(e)
	}
	u = begin(t, s2, 3)
	next, _ := u.AllocateTrieID()
	if next != 3 {
		t.Fatal("allocator not recovered")
	}
	u.Abort()
	u = begin(t, s2, 3)
	again, _ := u.AllocateTrieID()
	if again != 3 {
		t.Fatal("aborted reservation published")
	}
	commit(t, u)
	if _, e = u.AllocateTrieID(); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}
func TestAllocatorConflictAndLifecycleNonReuse(t *testing.T) {
	s, _, _ := testStore(t)
	a := [32]byte{1}
	b := [32]byte{2}
	u := begin(t, s, 0)
	stale := begin(t, s, 0)
	q, _ := u.AllocateTrieID()
	u.PutLifecycle(a, Lifecycle{true, q})
	u.PutLifecycle(b, Lifecycle{true, q})
	if e := u.Commit(); !errors.Is(e, ErrConflict) {
		t.Fatal("ID assigned twice", e)
	}
	q2, _ := u.AllocateTrieID()
	u.PutLifecycle(b, Lifecycle{true, q2})
	commit(t, u)
	if e := stale.Commit(); !errors.Is(e, ErrConflict) {
		t.Fatal("stale plan accepted", e)
	}
	stale.Abort()
	u = begin(t, s, 1)
	u.PutLifecycle(a, Lifecycle{})
	commit(t, u)
	u = begin(t, s, 2)
	u.PutLifecycle(a, Lifecycle{true, q})
	if e := u.Commit(); !errors.Is(e, ErrConflict) {
		t.Fatal("old ID reactivated", e)
	}
	fresh, _ := u.AllocateTrieID()
	u.PutLifecycle(a, Lifecycle{true, fresh})
	commit(t, u)
	u = begin(t, s, 3)
	u.PutLifecycle(a, Lifecycle{true, fresh})
	commit(t, u)
	u = begin(t, s, 4)
	u.PutLifecycle([32]byte{3}, Lifecycle{true, q2})
	if e := u.Commit(); !errors.Is(e, ErrConflict) {
		t.Fatal("old ID transferred", e)
	}
	u.Abort()
}
func TestCommitFailureRequiresReopen(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[after], func(t *testing.T) {
			s, path, cfg := testStore(t)
			u := begin(t, s, 0)
			q, _ := u.AllocateTrieID()
			owner := [32]byte{1}
			u.PutNode(uint64(q), nil, record(3))
			u.PutLifecycle(owner, Lifecycle{true, q})
			realWrite := s.write
			s.write = func(b *leveldb.Batch) error {
				if after {
					if e := realWrite(b); e != nil {
						return e
					}
				}
				return errors.New("injected write error")
			}
			if e := u.Commit(); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			if _, e := s.Head(); !errors.Is(e, ErrUncertain) {
				t.Fatal("uncertain head accepted", e)
			}
			if _, e := s.Begin(0, mpt.Hash{1}, [32]byte{1}); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			if _, e := s.ViewAt(0); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			recovered, e := Open(path, cfg, false, false)
			if e != nil {
				t.Fatal(e)
			}
			defer recovered.Close()
			h, e := recovered.Head()
			if e != nil || h.Initialized != after {
				t.Fatal(h, e)
			}
			if after {
				v := view(t, recovered, 0)
				defer v.Close()
				_, life, e := v.Lifecycle(owner)
				if e != nil || life.TrieID != q {
					t.Fatal(e)
				}
				if _, _, e = v.Node(uint64(q), nil); e != nil {
					t.Fatal(e)
				}
				if h.NextTrieID != 2 {
					t.Fatal("allocator atomicity")
				}
			} else if h.NextTrieID != 1 {
				t.Fatal("allocator advanced without commit")
			}
		})
	}
}
func TestManifestReadOnlyAndLimits(t *testing.T) {
	s, path, cfg := testStore(t)
	u := begin(t, s, 0)
	commit(t, u)
	s.Close()
	mismatch := cfg
	mismatch.CodecID = "legacy-raw105"
	if db, e := Open(path, mismatch, false, true); e == nil {
		db.Close()
		t.Fatal("codec mismatch")
	}
	mismatch = cfg
	mismatch.Layout = "fake-cf"
	if _, e := Open(path, mismatch, false, true); e == nil {
		t.Fatal("layout accepted")
	}
	before := databaseFiles(t, path)
	ro, e := Open(path, cfg, false, true)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ro.Begin(1, mpt.Hash{1}, [32]byte{1}); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
	v := view(t, ro, 0)
	if v.Root() != (mpt.Hash{1}) {
		t.Fatal("root")
	}
	v.Close()
	ro.Close()
	after := databaseFiles(t, path)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("readonly changed DB bytes")
	}
	if _, e = Open(path, cfg, true, false); e == nil {
		t.Fatal("create overwrote existing")
	}
	s2, e := Open(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s2.Close()
	if _, e = s2.Begin(1<<32, mpt.Hash{1}, [32]byte{1}); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if _, e = s2.Begin(3, mpt.Hash{1}, [32]byte{1}); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e = s2.ViewAt(1 << 32); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	u = begin(t, s2, 1)
	if e = u.PutNode(1<<32, nil, record(1)); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if e = u.PutNode(0, []byte{16}, record(1)); e == nil {
		t.Fatal("bad nibble")
	}
	if e = u.PutNode(0, make([]byte, 64), Record{Kind: Extension, Path: []byte{0x11}}); e == nil {
		t.Fatal("path beyond 64")
	}
	if e = u.PutCode(mpt.Hash{}, []byte("wrong")); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	u.Abort()
}
func databaseFiles(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	entries, e := os.ReadDir(path)
	if e != nil {
		t.Fatal(e)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		b, e := os.ReadFile(filepath.Join(path, entry.Name()))
		if e != nil {
			t.Fatal(e)
		}
		out[entry.Name()] = string(b)
	}
	return out
}
func TestBatchBudgetAndCounterExhaustion(t *testing.T) {
	cfg := DefaultConfig("ethereum-mpt", "budget")
	cfg.MaxBatchBytes = 4096
	s, e := Open(t.TempDir()+"/db", cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	u := begin(t, s, 0)
	large := record(1)
	large.Value = make([]byte, 4096)
	if e = u.PutNode(0, nil, large); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	// Replacing a pending record coalesces it, rather than charging both versions.
	for i := 0; i < 100; i++ {
		if e = u.PutNode(0, nil, record(byte(i))); e != nil {
			t.Fatal(e)
		}
	}
	commit(t, u)
	v := view(t, s, 0)
	_, r, e := v.Node(0, nil)
	if e != nil || r.Value[0] != 99 {
		t.Fatal(r, e)
	}
	v.Close()
	// Seed the allocator boundary without billions of allocations.
	s.mu.Lock()
	s.head.NextTrieID = math.MaxUint32
	s.mu.Unlock()
	u = begin(t, s, 1)
	q, e := u.AllocateTrieID()
	if e != nil || q != math.MaxUint32 {
		t.Fatal(q, e)
	}
	if _, e = u.AllocateTrieID(); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	commit(t, u)
	if h, e := s.Head(); e != nil || h.NextTrieID != uint64(math.MaxUint32)+1 {
		t.Fatal(h, e)
	}
}
func TestConcurrentViewsAreOwnerAndHeightScoped(t *testing.T) {
	s, _, _ := testStore(t)
	var owners [2][32]byte
	owners[0][0] = 1
	owners[1][0] = 2
	for h := uint64(0); h < 3; h++ {
		u := begin(t, s, h)
		for i, owner := range owners {
			q, _ := u.AllocateTrieID()
			u.PutLifecycle(owner, Lifecycle{true, q})
			u.PutNode(uint64(q), []byte{1}, record(byte(h*2+uint64(i)+1)))
		}
		commit(t, u)
	}
	views := []*View{view(t, s, 0), view(t, s, 1), view(t, s, 2)}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for repeat := 0; repeat < 100; repeat++ {
				for h, v := range views {
					for i, owner := range owners {
						_, life, e := v.Lifecycle(owner)
						if e != nil || life.TrieID != uint32(h*2+i+1) {
							t.Error("namespace", life, e)
							return
						}
						_, r, e := v.Node(uint64(life.TrieID), []byte{1})
						if e != nil || !bytes.Equal(r.Value, []byte{byte(h*2 + i + 1)}) {
							t.Error("record namespace", r, e)
							return
						}
					}
				}
			}
		}()
	}
	wg.Wait()
	for _, v := range views {
		v.Close()
		if _, _, e := v.Node(0, nil); !errors.Is(e, ErrClosed) {
			t.Fatal(e)
		}
	}
}

func TestPinnedReadersDuringConcurrentCommits(t *testing.T) {
	s, _, _ := testStore(t)
	u := begin(t, s, 0)
	if e := u.PutNode(0, nil, record(1)); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	pinned := view(t, s, 0)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h, r, e := pinned.Node(0, nil)
				if e != nil || h != 0 || len(r.Value) != 1 || r.Value[0] != 1 {
					t.Error("pinned snapshot changed", h, r, e)
					return
				}
			}
		}()
	}
	for h := uint64(1); h <= 16; h++ {
		u := begin(t, s, h)
		if e := u.PutNode(0, nil, record(byte(h+1))); e != nil {
			t.Fatal(e)
		}
		commit(t, u)
		v := view(t, s, h)
		height, r, e := v.Node(0, nil)
		if e != nil || height != uint32(h) || r.Value[0] != byte(h+1) {
			t.Fatal(height, r, e)
		}
		v.Close()
	}
	wg.Wait()
	pinned.Close()
	stats, e := s.LogicalWrites()
	if e != nil || stats.Total().Puts == 0 {
		t.Fatal("successful writes not counted", e)
	}
}
