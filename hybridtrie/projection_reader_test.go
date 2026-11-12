package hybridtrie

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// A parent authenticates its child's digest and locator. The child still has
// to agree with that digest before its source may be reused by a later parent.
func TestProjectionRejectsCorruptRetainedPrefixBody(t *testing.T) {
	cfg := DefaultConfig("raw-bytes/v1", "projection")
	opts := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg.Storage = &opts
	s, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for _, k := range []byte{0, 16, 32} {
		if e = tree.Update([]byte{k}, bytes.Repeat([]byte{k}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{0, tree, 0xffff}})
	if e != nil {
		t.Fatal(e)
	}
	key := NodeKey{Path: []byte{0}}
	snap := s.db.snapshot()
	rec, e := readPrefix(snap, key)
	snap.close()
	if e != nil {
		t.Fatal(e)
	}
	rec.Value[0] ^= 255 // retain its claimed self hash so the parent still rebuilds
	encoded, e := EncodeRecord(rec)
	if e != nil {
		t.Fatal(e)
	}
	rawKey, e := EncodeNodeKey(key)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.db.write([]entry{{prefixColumn, rawKey, encoded}}); e != nil {
		t.Fatal(e)
	}
	if e = tree.Update([]byte{16}, bytes.Repeat([]byte{7}, 64)); e != nil {
		t.Fatal(e)
	}
	got, _, e := s.Commit(1, head.CommitID, []TrieUpdate{{0, tree, 0xffff}})
	if !errors.Is(e, ErrHashMismatch) || got != head {
		t.Fatalf("retained invalid body: head=%v err=%v", got, e)
	}
}
