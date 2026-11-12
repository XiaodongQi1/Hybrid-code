package hybridtrie

import (
	"bytes"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// Repeated execution reads must reuse a canonical body already staged in this
// block, whether the parent's physical reference is Prefix or Hash.
func TestExecutionReusesStagedBody(t *testing.T) {
	for _, prefix := range []bool{false, true} {
		name := "hash"
		if prefix {
			name = "prefix"
		}
		t.Run(name, func(t *testing.T) {
			s := pendingStore(t)
			original, _ := mpt.NewEmpty(mpt.NewMemoryStore())
			for _, k := range []byte{0x10, 0x20, 0x30} {
				if e := original.Update([]byte{k}, bytes.Repeat([]byte{k}, 64)); e != nil {
					t.Fatal(e)
				}
			}
			mask := uint16(0)
			if prefix {
				mask = 0xffff
			}
			head, _, e := s.Commit(0, [32]byte{}, []TrieUpdate{{Tree: original, PrefixMask: mask}})
			if e != nil {
				t.Fatal(e)
			}
			counted := &pendingReadEngine{diskEngine: s.db}
			s.db = counted
			f, e := OpenCachedForest(s, DefaultPrefixBoundaryForestCacheConfig())
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			b, e := f.BeginBlock(1, head.CommitID)
			if e != nil {
				t.Fatal(e)
			}
			root, _ := original.Hash()
			store, e := b.TrieReader(0, root)
			if e != nil {
				t.Fatal(e)
			}
			r := store.(*boundaryReader)
			path := []byte{1}
			h, _, ok, e := original.CanonicalNodeAtPath(path)
			if e != nil || !ok {
				t.Fatal("missing child", e)
			}
			loc := mpt.NodeLocation{Kind: uint8(Hash)}
			if prefix {
				loc = mpt.NodeLocation{Kind: uint8(Prefix), Exact: true}
				// Materializing a Prefix parent stages compact sibling bodies.
				if _, e = r.GetMaterializedNode(nil, root); e != nil {
					t.Fatal(e)
				}
				counted.count = true
				value, handled, err := r.LocatedLocalValue(path, h, []byte{0}, loc, nil)
				if err != nil || !handled || !bytes.Equal(value, bytes.Repeat([]byte{0x10}, 64)) {
					t.Fatal("compact staged point read", handled, err)
				}
				if counted.nodeReads != 0 {
					t.Fatalf("compact child reread %d node records", counted.nodeReads)
				}
				counted.count = false
			}
			if _, e = r.GetLocatedMaterializedNode(path, h, loc); e != nil {
				t.Fatal(e)
			}
			counted.count = true
			if _, e = r.GetLocatedMaterializedNode(path, h, loc); e != nil {
				t.Fatal(e)
			}
			// A point read with a locator must also defer to the staged body.
			if _, handled, e := r.LocatedLocalValue(path, h, []byte{0}, loc, nil); e != nil || handled {
				t.Fatal("point read bypassed staged materialized body", handled, e)
			}
			if counted.nodeReads != 0 {
				t.Fatalf("staged child reread %d node records", counted.nodeReads)
			}
			if f.boundary.stats.TransientBodyHits == 0 {
				t.Fatal("missing reuse statistic")
			}
			counted.count = false
			// Exact version mismatches must never reuse a same-hash body from
			// a different physical version, and closed readers remain invalid.
			if prefix {
				bad := loc
				bad.Height = 1
				if _, e = r.GetLocatedMaterializedNode(path, h, bad); e == nil {
					t.Fatal("accepted future version")
				}
			}
			b.Abort()
			if _, e = r.GetLocatedMaterializedNode(path, h, loc); e == nil {
				t.Fatal("reused closed reader")
			}
		})
	}
}
