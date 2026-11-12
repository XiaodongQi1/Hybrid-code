//go:build rocksdb && cgo

package backend

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestHybridExecutionReadsRocksDB(t *testing.T) { testHybridExecutionReads(t, kvstore.RocksDB) }
