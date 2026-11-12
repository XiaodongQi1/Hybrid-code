package blockhashcache

import (
	"errors"
	"math/big"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"testing"
)

func TestWindowReopenEvictionAndFailure(t *testing.T) {
	c := Cache{}
	reads := 0
	load := func(h uint64) (common.Hash, error) { reads++; return common.BigToHash(new(big.Int).SetUint64(h)), nil }
	w, e := c.Window(301, 1, load)
	if e != nil || reads != 256 || len(w) != 256 || w[45] != common.BigToHash(new(big.Int).SetUint64(45)) {
		t.Fatal(reads, len(w), e)
	}
	c.Remember(301, common.BigToHash(new(big.Int).SetUint64(301)))
	_, e = c.Window(302, 1, load)
	if e != nil || reads != 256 || len(c) != 256 {
		t.Fatal(reads, len(c), e)
	}
	_, e = (Cache{}).Window(2, 0, func(uint64) (common.Hash, error) { return common.Hash{}, errors.New("read failure") })
	if e == nil {
		t.Fatal("lost error")
	}
}
