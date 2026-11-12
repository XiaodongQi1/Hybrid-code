package hybridtrie

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestLevelDBHistoryAndSharedPrefixCursor(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "leveldb-history")
	opts := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opts
	path := filepath.Join(t.TempDir(), "db")
	db, e := Open(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { db.Close() }()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for n := 0; n < 32; n++ {
		if e = tree.Update([]byte{byte(n), 0}, bytes.Repeat([]byte{byte(n + 1)}, 40)); e != nil {
			t.Fatal(e)
		}
	}
	var head Head
	var roots []mpt.Hash
	var versions []map[string][]byte
	for height := uint64(0); height < 5; height++ {
		if height > 0 {
			key := []byte{byte(height), 0}
			if height == 2 {
				e = tree.Delete(key)
			} else {
				e = tree.Update(key, bytes.Repeat([]byte{99}, 48))
			}
			if e != nil {
				t.Fatal(e)
			}
		}
		root, e := tree.Hash()
		if e != nil {
			t.Fatal(e)
		}
		head, _, e = db.Commit(height, head.CommitID, []TrieUpdate{{Tree: tree, PrefixMask: 0xffff}})
		if e != nil {
			t.Fatal(e)
		}
		state := map[string][]byte{}
		for n := 0; n < 32; n++ {
			key := []byte{byte(n), 0}
			v, e := tree.Get(key)
			if e != nil {
				t.Fatal(e)
			}
			state[string(key)] = v
		}
		roots = append(roots, root)
		versions = append(versions, state)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e = Open(path, cfg, false, true)
	if e != nil {
		t.Fatal(e)
	}
	for h, root := range roots {
		v, e := db.ViewAt(uint64(h))
		if e != nil {
			t.Fatal(e)
		}
		for _, key := range [][]byte{{1, 0}, {2, 0}, {5, 0}, {40, 0}} {
			p, stats, e := v.ProveWithStats(0, root, key)
			if e != nil {
				t.Fatal(e)
			}
			value, found, e := mpt.VerifyCompactProof(root, key, p)
			want := versions[h][string(key)]
			if e != nil || found != (len(want) > 0) || !bytes.Equal(value, want) {
				t.Fatal("historical value", h, key, e)
			}
			if stats.PrefixReads > 0 && (!stats.IteratorReuseEnabled || stats.PrefixIteratorCreates != 1) {
				t.Fatal("LevelDB did not reuse one cursor", stats)
			}
		}
		if e = v.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e = db.Commit(5, head.CommitID, nil); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
}
