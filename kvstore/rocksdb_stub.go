//go:build !rocksdb || !cgo

package kvstore

import "fmt"

func openRocksDB(string, Options, bool, bool) (driver, error) {
	return nil, fmt.Errorf("%w: RocksDB requires -tags rocksdb, CGO_ENABLED=1 and native RocksDB 9.10.0", ErrUnavailable)
}
