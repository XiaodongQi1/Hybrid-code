//go:build rocksdb && cgo

package main

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestCrashRecoveryRocksDB(t *testing.T) { testRecovery(t, kvstore.RocksDB) }
