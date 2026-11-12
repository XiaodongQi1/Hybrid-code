//go:build rocksdb && cgo

package hybridtrie

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestHashBulkRocksDBPublicationRecovery(t *testing.T) { runHashBulkRecovery(t, kvstore.RocksDB) }
