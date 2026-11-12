//go:build rocksdb && cgo

package comparison

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestCommonResourcesRocksDB(t *testing.T) { runCommonResources(t, kvstore.RocksDB) }
