// Package blockhashcache caches only immutable committed block identities.
package blockhashcache

import "github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"

type Cache map[uint64]common.Hash

// Window warms once on reopen and thereafter reads only missing identities.
// The caller serializes access and calls Remember only after a successful commit.
func (c Cache) Window(next, initial uint64, load func(uint64) (common.Hash, error)) (map[uint64]common.Hash, error) {
	start := initial
	if next > 256 && next-256 > start {
		start = next - 256
	}
	for h := range c {
		if h < start || h >= next {
			delete(c, h)
		}
	}
	for h := start; h < next; h++ {
		if _, ok := c[h]; !ok {
			v, e := load(h)
			if e != nil {
				return nil, e
			}
			c[h] = v
		}
	}
	return c, nil
}
func (c Cache) Remember(h uint64, id common.Hash) {
	c[h] = id
	for n := range c {
		if h >= 256 && n <= h-256 {
			delete(c, n)
		}
	}
}
