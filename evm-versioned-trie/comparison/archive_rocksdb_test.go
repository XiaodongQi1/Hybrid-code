//go:build rocksdb && cgo

package comparison

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestArchiveRocksDBLiveEVM(t *testing.T) { runArchiveLiveEVM(t, kvstore.RocksDB) }
