//go:build rocksdb && cgo

package runner

import (
	"testing"

	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

func TestHybridReadSwitchesRocksDB(t *testing.T) { testHybridReadSwitches(t, kvstore.RocksDB) }
