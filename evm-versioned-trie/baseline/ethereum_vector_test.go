package baseline

import (
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

// Published independent vector: ethereum/tests, TrieTests/trietest.json,
// branch-value-update. https://github.com/ethereum/tests/blob/develop/TrieTests/trietest.json
// Includes short embedded children, a branch value and replacement.
func TestPublishedEthereumTrieVector(t *testing.T) {
	s := mpt.NewMemoryStore()
	tree, e := mpt.NewEmpty(s)
	if e != nil {
		t.Fatal(e)
	}
	for _, kv := range [][2]string{{"abc", "123"}, {"abcd", "abcd"}, {"abc", "abc"}} {
		if e = tree.Update([]byte(kv[0]), []byte(kv[1])); e != nil {
			t.Fatal(e)
		}
	}
	h, e := tree.Commit()
	if e != nil {
		t.Fatal(e)
	}
	if fmt.Sprintf("%x", h) != "7a320748f780ad9ad5b0837302075ce0eeba6c26e3d8562c67ccc0f1b273298a" {
		t.Fatalf("published root mismatch: %x", h)
	}
	for _, key := range []string{"abc", "abcd", "abce"} {
		p, e := tree.Prove([]byte(key))
		if e != nil {
			t.Fatal(e)
		}
		v, found, e := mpt.VerifyProof(h, []byte(key), p)
		if e != nil {
			t.Fatal(e)
		}
		if (key == "abce") == found {
			t.Fatal("membership", key)
		}
		if found && string(v) != key {
			t.Fatal("value", key)
		}
	}
}
