package hybridtrie

import (
	"bytes"
	"fmt"
	"math/rand"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

// A test-only hash store rebuilt entirely from physical records exercises codecs
// independently of the persistent backend's reference-closure protocol.
func rebuildTree(t *testing.T, tree *mpt.Trie, mask uint16) (*mpt.Trie, int, int) {
	t.Helper()
	empty, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	nodes := map[string]mpt.LogicalChange{}
	_, e := tree.DiffLogicalNodes(empty, 10000, func(c mpt.LogicalChange) error { nodes[string(c.Path)] = c; return nil })
	if e != nil {
		t.Fatal(e)
	}
	store := mpt.NewMemoryStore()
	batch := store.NewBatch()
	embedded, independent := 0, 0
	for _, c := range nodes {
		if len(c.Path) > 0 && len(c.RLP) < 32 {
			embedded++
			continue
		}
		ctx := NodeKey{8, c.Path, 17}
		desc := mustInspect(t, c.RLP)
		var eligible uint16
		for i, r := range desc.Children {
			if r.Present && len(r.Inline) == 0 {
				eligible |= 1 << i
			}
		}
		record, e := FromCanonical(ctx, c.RLP, eligible&mask)
		if e != nil {
			t.Fatal(e)
		}
		record = roundtrip(t, record)
		raw, h, e := RebuildCanonical(ctx, record, func(loc ChildLocator) (mpt.Hash, error) {
			child, ok := nodes[string(loc.Target.Path)]
			if !ok {
				return mpt.Hash{}, fmt.Errorf("missing child at %x", loc.Target.Path)
			}
			return child.Hash, nil
		})
		if e != nil || !bytes.Equal(raw, c.RLP) || h != c.Hash {
			t.Fatalf("node %x: %v", c.Path, e)
		}
		if e = batch.Put(h[:], raw); e != nil {
			t.Fatal(e)
		}
		independent++
	}
	if e = batch.Write(); e != nil {
		t.Fatal(e)
	}
	root, _ := tree.Hash()
	out, e := mpt.Open(root, store)
	if e != nil {
		t.Fatal(e)
	}
	return out, independent, embedded
}

func checkQueries(t *testing.T, tree *mpt.Trie, expected map[string][]byte, queries [][]byte) {
	t.Helper()
	root, _ := tree.Hash()
	for _, key := range queries {
		got, e := tree.Get(key)
		if e != nil || !bytes.Equal(got, expected[string(key)]) {
			t.Fatalf("Get %x: %x %v", key, got, e)
		}
		proof, e := tree.Prove(key)
		if e != nil {
			t.Fatal(e)
		}
		value, exists, e := mpt.VerifyProof(root, key, proof)
		_, want := expected[string(key)]
		if e != nil || exists != want || !bytes.Equal(value, expected[string(key)]) {
			t.Fatalf("proof %x: %v %v", key, exists, e)
		}
	}
}

func TestSplitMergeReinsertAndPublishedRoot(t *testing.T) {
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	expected := map[string][]byte{}
	changes := [][2]string{{"abc", "123"}, {"abcd", "abcd"}, {"abc", "abc"}, {"abcd", ""}, {"abc", ""}, {"abcd", "abcd"}, {"abc", "abc"}}
	for step, kv := range changes {
		if e := tree.Update([]byte(kv[0]), []byte(kv[1])); e != nil {
			t.Fatal(e)
		}
		if kv[1] == "" {
			delete(expected, kv[0])
		} else {
			expected[kv[0]] = []byte(kv[1])
		}
		if step == 2 {
			root, _ := tree.Hash()
			if fmt.Sprintf("%x", root) != "7a320748f780ad9ad5b0837302075ce0eeba6c26e3d8562c67ccc0f1b273298a" {
				t.Fatal("published Ethereum vector", root)
			}
		}
		for _, mask := range []uint16{0, 0xffff, 0xaaaa} {
			out, _, _ := rebuildTree(t, tree, mask)
			checkQueries(t, out, expected, [][]byte{[]byte("abc"), []byte("abcd"), []byte("abce")})
		}
	}
}

func TestDeterministicBlockUpdatesPreserveRootsAndProofs(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	expected := map[string][]byte{}
	keys := make([][]byte, 24)
	for i := range keys {
		keys[i] = make([]byte, 32)
		keys[i][31] = byte(i)
	}
	oldRoot, _ := tree.Hash()
	for block := 0; block < 20; block++ {
		before := tree.Copy()
		for j := 0; j < 6; j++ {
			key := keys[rng.Intn(len(keys))]
			var value []byte
			if rng.Intn(4) != 0 {
				value = bytes.Repeat([]byte{byte(1 + rng.Intn(250))}, 1+rng.Intn(65))
			}
			if e := tree.Update(key, value); e != nil {
				t.Fatal(e)
			}
			if len(value) == 0 {
				delete(expected, string(key))
			} else {
				expected[string(key)] = value
			}
		}
		if h, _ := before.Hash(); h != oldRoot {
			t.Fatal("COW root changed")
		}
		for _, mask := range []uint16{0, 0xffff, 0x5555} {
			out, _, _ := rebuildTree(t, tree, mask)
			checkQueries(t, out, expected, keys)
		}
		oldRoot, _ = tree.Hash()
	}
}
