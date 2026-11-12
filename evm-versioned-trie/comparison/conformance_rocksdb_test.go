//go:build rocksdb && cgo

package comparison

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func TestHybridNativeComparisonConformance(t *testing.T) {
	options := kvstore.DefaultOptions(kvstore.RocksDB)
	runComparisonConformance(t, func(t *testing.T) conformanceTarget {
		cfg := backend.DefaultHybridConfig("comparison-native-conformance")
		cfg.Store.Storage = &options
		cfg.Initialization = backend.InitializationHashCold
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		cfg.Cache.Boundary.PolicyIntervalBlocks = 2
		path := filepath.Join(t.TempDir(), "hybrid")
		db, e := backend.OpenHybrid(path, cfg, true, false)
		if e != nil {
			t.Fatal(e)
		}
		return conformanceTarget{name: "HybridTrie",
			initial: func(b block.InputBlock) (mpt.Hash, error) {
				r, e := db.Apply(b, nil)
				if e != nil {
					return mpt.Hash{}, e
				}
				return r.Root, nil
			},
			importBlock: func(b block.InputBlock, trace block.LogicalBlock) (mpt.Hash, error) {
				r, e := db.ApplyLogicalBlock(b, trace)
				if e != nil {
					return mpt.Hash{}, e
				}
				return r.Root, nil
			},
			query: func(req q.Request) (q.Response, error) {
				return db.Query(context.Background(), req)
			},
			close: func() error { return db.Close() },
			reopen: func() error {
				if e := db.Close(); e != nil {
					return e
				}
				var e error
				db, e = backend.OpenHybrid(path, cfg, false, true)
				return e
			},
		}
	}, &options)
}
