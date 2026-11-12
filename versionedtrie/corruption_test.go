package versionedtrie

import (
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"math"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func TestCorruptDataIsNotAbsence(t *testing.T) {
	s, _, _ := testStore(t)
	u := begin(t, s, 0)
	commit(t, u)
	key, _ := EncodeNodeKey(0, []byte{1}, 0)
	header, _ := nodeHeader(0, []byte{1})
	code := []byte("code")
	hash := codeHash(code)
	b := new(leveldb.Batch)
	b.Put(key, []byte{1})
	b.Put(lifecycleKey([32]byte{1}, 0), []byte{1})
	b.Put(append([]byte{'C'}, hash[:]...), []byte("tampered"))
	if e := s.db.Write(b, nil); e != nil {
		t.Fatal(e)
	}
	v := view(t, s, 0)
	if _, _, e := v.Node(0, []byte{1}); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	if _, _, e := v.Lifecycle([32]byte{1}); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	if _, e := v.Code(hash); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	v.Close()
	b = new(leveldb.Batch)
	b.Delete(key)
	b.Put(append(header, 0), []byte{1})
	if e := s.db.Write(b, nil); e != nil {
		t.Fatal(e)
	}
	v = view(t, s, 0)
	defer v.Close()
	if _, _, e := v.Node(0, []byte{1}); !errors.Is(e, ErrCorrupt) {
		t.Fatal("malformed key accepted", e)
	}
}
func TestHeadRootMismatchRejectedOnOpen(t *testing.T) {
	s, path, cfg := testStore(t)
	u := begin(t, s, 0)
	commit(t, u)
	if e := s.db.Put(rootKey(0), make([]byte, 64), nil); e != nil {
		t.Fatal(e)
	}
	s.Close()
	if db, e := Open(path, cfg, false, false); e == nil {
		db.Close()
		t.Fatal("head root mismatch accepted")
	}
}
func TestHeightAndAllocatorBoundaryReopen(t *testing.T) {
	cfg := DefaultConfig("ethereum-mpt", "uint32-boundary")
	cfg.InitialHeight = math.MaxUint32
	path := t.TempDir() + "/db"
	s, e := Open(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	// Explicitly seed allocator exhaustion as an internal boundary fixture.
	s.head.NextTrieID = math.MaxUint32
	u := begin(t, s, math.MaxUint32)
	q, e := u.AllocateTrieID()
	if e != nil || q != math.MaxUint32 {
		t.Fatal(q, e)
	}
	u.PutLifecycle([32]byte{1}, Lifecycle{true, q})
	u.PutNode(uint64(q), nil, record(9))
	commit(t, u)
	s.Close()
	s, e = Open(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	v := view(t, s, math.MaxUint32)
	defer v.Close()
	if _, _, e = v.Node(math.MaxUint32, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Begin(uint64(math.MaxUint32)+1, mpt.Hash{1}, [32]byte{1}); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
}

var injectedReadError = errors.New("injected read failure")

type failedReader struct{ versionReader }

func (f failedReader) NewIterator(*util.Range, *opt.ReadOptions) iterator.Iterator {
	return iterator.NewEmptyIterator(injectedReadError)
}
func TestIteratorErrorNotConvertedToNotFound(t *testing.T) {
	if _, _, e := predecessor(failedReader{}, []byte{'N'}, 0); !errors.Is(e, injectedReadError) {
		t.Fatal(e)
	}
}
