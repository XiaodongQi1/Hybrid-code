//go:build rocksdb && cgo

package main

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestCrashRecoveryRocksDB(t *testing.T) { testRecovery(t, kvstore.RocksDB) }
