//go:build rocksdb && cgo

package backend

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestHybridAdaptiveRocksDBConformance(t *testing.T) { runAdaptiveConformance(t, kvstore.RocksDB) }
