//go:build rocksdb && cgo

#include <rocksdb/version.h>
#include <rocksdb/statistics.h>
#include <cstdint>
#if ROCKSDB_MAJOR != 9 || ROCKSDB_MINOR != 10 || ROCKSDB_PATCH != 0
#error "kvstore requires RocksDB 9.10.0 headers"
#endif
extern "C" int kvstore_rocksdb_version_ok() {
  return ROCKSDB_NAMESPACE::GetRocksVersionAsString(true) == "9.10.0";
}
extern "C" uint32_t kvstore_rocksdb_ticker(int index) {
  switch(index) {
    case 0: return ROCKSDB_NAMESPACE::WAL_FILE_BYTES;
    case 1: return ROCKSDB_NAMESPACE::FLUSH_WRITE_BYTES;
    case 2: return ROCKSDB_NAMESPACE::COMPACT_WRITE_BYTES;
    default: return ROCKSDB_NAMESPACE::TICKER_ENUM_MAX;
  }
}
