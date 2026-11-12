package versionedtrie

import (
	"bytes"
	"errors"
	"reflect"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func canonicalTree(t *testing.T, store mpt.Store, values map[string][]byte) *mpt.Trie {
	t.Helper()
	tr, e := mpt.NewEmpty(store)
	if e != nil {
		t.Fatal(e)
	}
	for key, value := range values {
		if e = tr.Update([]byte(key), value); e != nil {
			t.Fatal(e)
		}
	}
	return tr
}
func TestHistoricalTrieRoundtripAcrossCompressionChanges(t *testing.T) {
	s, path, cfg := testStore(t)
	full := mpt.NewMemoryStore()
	var roots []mpt.Hash
	var rows []map[string][]byte
	states := []map[string][]byte{
		{"\x10": {1}, "\x11": {2}}, {"\x10": {3}, "\x11": {2}}, {"\x10": {3}},
		{"\x10": {1}, "\x11": {2}}, {"\x10": {1}, "\x11": {2}}, {}, {"\x11": bytes.Repeat([]byte{5}, 32)},
	}
	before := mpt.EmptyRootHash
	for i, values := range states {
		tr := canonicalTree(t, full, values)
		root, e := tr.Commit()
		if e != nil {
			t.Fatal(e)
		}
		u := beginWithRoot(t, s, uint64(i), root)
		stats, e := u.ProjectTrie(0, before, tr)
		if e != nil {
			t.Fatal("project", i, e)
		}
		if i == 4 && (stats.Emitted != 0 || stats.Pruned != 1) {
			t.Fatal("unchanged tree not pruned", stats)
		}
		if i == 5 && stats.Emitted != 0 {
			t.Fatal("empty emitted")
		}
		commit(t, u)
		roots = append(roots, root)
		rows = append(rows, values)
		before = root
	}
	s.Close()
	reopened, e := Open(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	for i, root := range roots {
		v := view(t, reopened, uint64(i))
		reader, e := v.TrieReader(0)
		if e != nil {
			t.Fatal(e)
		}
		tree, e := mpt.Open(root, reader)
		if e != nil {
			t.Fatal(i, e)
		}
		ref, _ := mpt.Open(root, full)
		for _, key := range []string{"\x10", "\x11", "\x12"} {
			got, e := tree.Get([]byte(key))
			if e != nil || !bytes.Equal(got, rows[i][key]) {
				t.Fatal("get", i, key, e)
			}
			proof, e := tree.Prove([]byte(key))
			if e != nil {
				t.Fatal(e)
			}
			expected, e := ref.Prove([]byte(key))
			if e != nil || !reflect.DeepEqual(proof, expected) {
				t.Fatal("proof bytes changed", i, e)
			}
			value, exists, e := mpt.VerifyProof(root, []byte(key), proof)
			if e != nil || exists != (len(rows[i][key]) != 0) || !bytes.Equal(value, rows[i][key]) {
				t.Fatal("proof", i, e)
			}
		}
		e = mpt.WalkPaths(root, full, func(p []byte, h mpt.Hash, blob []byte) (bool, error) {
			raw, e := reader.GetNode(p, h)
			if e != nil || !bytes.Equal(raw, blob) {
				t.Fatal("RLP mismatch", i, p, e)
			}
			return true, nil
		}, nil)
		if e != nil {
			t.Fatal(e)
		}
		v.Close()
	}
}
func beginWithRoot(t *testing.T, s *Store, height uint64, root mpt.Hash) *Update {
	t.Helper()
	u, e := s.Begin(height, root, [32]byte{1})
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func TestIncrementalUpdateFromVersionedReader(t *testing.T) {
	s, _, _ := testStore(t)
	full := mpt.NewMemoryStore()
	values := map[string][]byte{"abc": []byte("initial"), "abcd": []byte("child")}
	tr := canonicalTree(t, full, values)
	root, _ := tr.Hash()
	u := beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	v := view(t, s, 0)
	reader, _ := v.TrieReader(0)
	tr, e := mpt.Open(root, reader)
	if e != nil {
		t.Fatal(e)
	}
	tr.Update([]byte("abc"), []byte("updated"))
	newRoot, _ := tr.Hash()
	u = beginWithRoot(t, s, 1, newRoot)
	if _, e = u.ProjectTrie(0, root, tr); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	v.Close()
	latest := view(t, s, 1)
	defer latest.Close()
	r, _ := latest.TrieReader(0)
	tree, e := mpt.Open(newRoot, r)
	if e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"abc", "abcd"} {
		if _, e = tree.Prove([]byte(key)); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = r.Get(root[:]); !errors.Is(e, ErrPathRequired) {
		t.Fatal(e)
	}
	if e = r.NewBatch().Write(); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
}
func TestNewNamespaceMaterializesIdenticalTree(t *testing.T) {
	s, _, _ := testStore(t)
	full := mpt.NewMemoryStore()
	tr := canonicalTree(t, full, map[string][]byte{"abc": []byte("one"), "abcd": []byte("two")})
	root, _ := tr.Commit()
	u := beginWithRoot(t, s, 0, mpt.EmptyRootHash)
	q, _ := u.AllocateTrieID()
	owner := [32]byte{1}
	u.PutLifecycle(owner, Lifecycle{true, q})
	stats, e := u.ProjectTrie(uint64(q), mpt.EmptyRootHash, tr)
	if e != nil || stats.Emitted == 0 {
		t.Fatal(e)
	}
	commit(t, u)
	u = beginWithRoot(t, s, 1, mpt.EmptyRootHash)
	fresh, _ := u.AllocateTrieID()
	u.PutLifecycle(owner, Lifecycle{true, fresh})
	stats, e = u.ProjectTrie(uint64(fresh), mpt.EmptyRootHash, tr)
	if e != nil || stats.Emitted == 0 {
		t.Fatal("identical new instance omitted", stats, e)
	}
	commit(t, u)
	v := view(t, s, 1)
	defer v.Close()
	reader, _ := v.TrieReader(uint64(fresh))
	if _, e = mpt.Open(root, reader); e != nil {
		t.Fatal(e)
	}
}
func TestProjectionErrorsCannotCommitPartialRows(t *testing.T) {
	s, _, _ := testStore(t)
	full := mpt.NewMemoryStore()
	tr := canonicalTree(t, full, map[string][]byte{"abc": bytes.Repeat([]byte{1}, MaxRecordBytes)})
	root, _ := tr.Hash()
	u := beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e == nil {
		t.Fatal("oversized projection accepted")
	}
	if e := u.Commit(); e == nil {
		t.Fatal("partial projection committed")
	}
	u.Abort()
	if _, e := s.ViewAt(0); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	tr = canonicalTree(t, full, map[string][]byte{"abc": []byte("valid")})
	root, _ = tr.Hash()
	u = beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e != nil {
		t.Fatal(e)
	}
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e := u.Commit(); e == nil {
		t.Fatal("duplicate final tree accepted")
	}
	u.Abort()
}

func TestTrieReaderBudgetsCorruptionAndClose(t *testing.T) {
	s, _, _ := testStore(t)
	full := mpt.NewMemoryStore()
	tr := canonicalTree(t, full, map[string][]byte{"\x10": {1}, "\x11": {2}})
	root, _ := tr.Hash()
	u := beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	v := view(t, s, 0)
	r, _ := v.TrieReader(0)
	raw, stats, e := r.ReadNodeWithStats(nil, root)
	if e != nil || len(raw) == 0 || stats.Records < 2 || stats.InlineReferences == 0 {
		t.Fatal(stats, e)
	}
	limited, _ := r.WithLimits(ReadLimits{MaxRecords: 1, MaxRecordBytes: 1 << 20})
	if _, e = limited.GetNode(nil, root); !errors.Is(e, ErrLimit) {
		t.Fatal("record budget", e)
	}
	limited, _ = r.WithLimits(ReadLimits{MaxRecords: 100, MaxRecordBytes: 1})
	if _, e = limited.GetNode(nil, root); !errors.Is(e, ErrLimit) {
		t.Fatal("byte budget", e)
	}
	if _, e = r.WithLimits(ReadLimits{}); !errors.Is(e, ErrLimit) {
		t.Fatal(e)
	}
	if _, e = r.GetNode(nil, mpt.Hash{9}); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	// No complete RLP copies are stored under hash-addressed archive keys.
	if _, e = s.db.Get(root[:], nil); e == nil {
		t.Fatal("full archive copy persisted")
	}
	v.Close()
	if _, e = r.GetNode(nil, root); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	key, _ := EncodeNodeKey(0, nil, 0)
	original, e := s.db.Get(key, nil)
	if e != nil {
		t.Fatal(e)
	}
	rec, e := DecodeRecord(original)
	if e != nil {
		t.Fatal(e)
	}
	modified := rec
	modified.Hash = mpt.Hash{7}
	if b, e := EncodeRecord(modified); e != nil {
		t.Fatal(e)
	} else {
		s.db.Put(key, b, nil)
	}
	bad := view(t, s, 0)
	reader, _ := bad.TrieReader(0)
	if _, e = reader.GetNode(nil, root); !errors.Is(e, ErrCorrupt) {
		t.Fatal("hash corruption accepted", e)
	}
	bad.Close()
	modified = rec
	modified.Inline = !rec.Inline
	b, _ := EncodeRecord(modified)
	s.db.Put(key, b, nil)
	bad = view(t, s, 0)
	reader, _ = bad.TrieReader(0)
	if _, e = reader.GetNode(nil, root); !errors.Is(e, ErrCorrupt) {
		t.Fatal("reference corruption accepted", e)
	}
	bad.Close()
	s.db.Put(key, original, nil)
	// A missing physical inline child is an error, never an absent logical key.
	missing, _ := EncodeNodeKey(0, []byte{1, 0}, 0)
	s.db.Delete(missing, nil)
	bad = view(t, s, 0)
	reader, _ = bad.TrieReader(0)
	if _, e = reader.GetNode(nil, root); !errors.Is(e, ErrCorrupt) {
		t.Fatal("missing child accepted", e)
	}
	bad.Close()
}

func TestTrieReaderRejectsChangedLeafValue(t *testing.T) {
	s, _, _ := testStore(t)
	tr := canonicalTree(t, mpt.NewMemoryStore(), map[string][]byte{"a": bytes.Repeat([]byte{1}, 64)})
	root, _ := tr.Hash()
	u := beginWithRoot(t, s, 0, root)
	if _, e := u.ProjectTrie(0, mpt.EmptyRootHash, tr); e != nil {
		t.Fatal(e)
	}
	commit(t, u)
	key, _ := EncodeNodeKey(0, nil, 0)
	raw, e := s.db.Get(key, nil)
	if e != nil {
		t.Fatal(e)
	}
	r, e := DecodeRecord(raw)
	if e != nil {
		t.Fatal(e)
	}
	r.Value[0] ^= 1
	raw, e = EncodeRecord(r)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.db.Put(key, raw, nil); e != nil {
		t.Fatal(e)
	}
	v := view(t, s, 0)
	defer v.Close()
	reader, _ := v.TrieReader(0)
	if _, e = reader.GetNode(nil, root); !errors.Is(e, ErrCorrupt) {
		t.Fatal("changed leaf accepted", e)
	}
}
