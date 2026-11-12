//go:build rocksdb && cgo

package comparison

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestCommonBulkSnapshotRocksDB(t *testing.T) { runCommonBulkSnapshot(t, kvstore.RocksDB) }
