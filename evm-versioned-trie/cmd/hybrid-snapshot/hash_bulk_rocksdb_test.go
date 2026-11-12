//go:build rocksdb && cgo

package main

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestHybridSnapshotCLIHashBulkRocksDB(t *testing.T) { runHashBulkCLI(t, kvstore.RocksDB) }
