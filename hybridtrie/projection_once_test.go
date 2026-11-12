package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func TestProjectionEncodesFinalPrefixRecordsOnce(t *testing.T) {
	s := pendingStore(t)
	tr, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for _, k := range []byte{0x10, 0x20, 0x30} {
		if e := tr.Update([]byte{k}, bytes.Repeat([]byte{k}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	head, stats, e := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: tr}})
	if e != nil || stats.PrefixRecords != stats.PrefixEncodes {
		t.Fatal(stats, e)
	}
	old, _ := tr.Hash()
	if e = tr.Update([]byte{0x10}, bytes.Repeat([]byte{9}, 64)); e != nil {
		t.Fatal(e)
	}
	_, stats, e = s.Commit(1, head.CommitID, []TrieUpdate{{Tree: tr, PrefixMask: 0xffff}})
	if e != nil || stats.PrefixEncodes == 0 || stats.PrefixEncodes != stats.PrefixRecords || stats.RetainedHashEdges == 0 {
		t.Fatal("re-encoded a parent or lost retained Hash edges", stats, e)
	}
	root, _ := tr.Hash()
	for height, h := range []mpt.Hash{old, root} {
		v, e := s.ViewAt(uint64(height))
		if e != nil {
			t.Fatal(e)
		}
		for _, k := range []byte{0x10, 0x20, 0x30} {
			p, _, e := v.ProveWithStats(0, h, []byte{k})
			if e != nil {
				t.Fatal(e)
			}
			value, ok, e := mpt.VerifyProof(h, []byte{k}, p)
			want := k
			if height == 1 && k == 0x10 {
				want = 9
			}
			if e != nil || !ok || !bytes.Equal(value, bytes.Repeat([]byte{want}, 64)) {
				t.Fatal("changed historical result", e)
			}
		}
		v.Close()
	}
}
