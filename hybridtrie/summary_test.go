package hybridtrie

import (
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func TestSummaryLookupSeparatesBodyAndDigest(t *testing.T) {
	c, err := newResidentCache(DefaultCacheConfig())
	if err != nil {
		t.Fatal(err)
	}
	generation := c.generation
	c.current.n = 1
	c.current.nodes[0] = nodeEntry{Length: 1, Hash: mpt.Hash{1}, Generation: generation}
	c.current.nodes[0].Path[0] = 1
	c.current.h = 1
	c.current.hashes[0] = hashEntry{Length: 1, Hash: mpt.Hash{2}, Generation: generation}
	c.current.hashes[0].Path[0] = 2
	if _, ok := c.lookupBody([]byte{2}, mpt.Hash{2}, generation); ok {
		t.Fatal("H satisfied a body request")
	}
	for _, tc := range []struct {
		path  byte
		hash  mpt.Hash
		state uint8
	}{
		{1, mpt.Hash{1}, nodeState}, {2, mpt.Hash{2}, hashState},
	} {
		h, state, hit := c.lookupHash([]byte{tc.path}, generation)
		if !hit || h != tc.hash || state != tc.state {
			t.Fatal(h, state, hit)
		}
	}
	if _, hit := c.lookupBody([]byte{1}, mpt.Hash{9}, generation); hit {
		t.Fatal("wrong expected hash hit N")
	}
	if _, _, hit := c.lookupHash([]byte{3}, generation); hit {
		t.Fatal("missing position hit")
	}
	c.current.hashes[0].Generation--
	if _, _, hit := c.lookupHash([]byte{2}, generation); hit {
		t.Fatal("stale H hit")
	}
	if _, hit := c.lookupBody([]byte{1}, mpt.Hash{1}, generation+1); hit {
		t.Fatal("stale reader hit N")
	}
}
