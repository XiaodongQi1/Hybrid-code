//go:build rocksdb && cgo

package runner

import (
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestHybridReadSwitchesRocksDB(t *testing.T) { testHybridReadSwitches(t, kvstore.RocksDB) }
