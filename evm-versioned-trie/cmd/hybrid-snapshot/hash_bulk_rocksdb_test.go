//go:build rocksdb && cgo

package main

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestHybridSnapshotCLIHashBulkRocksDB(t *testing.T) { runHashBulkCLI(t, kvstore.RocksDB) }
