#!/bin/sh
# Builds only the pinned native dependency under ignored .local/native.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
native_dir="$project_root/.local/native"
mkdir -p "$native_dir"
archive="$native_dir/rocksdb-9.10.0.tar.gz"
if [ ! -f "$archive" ]; then
  curl -fL --connect-timeout 20 https://codeload.github.com/facebook/rocksdb/tar.gz/refs/tags/v9.10.0 -o "$archive"
fi
expected=fdccab16133c9d927a183c2648bcea8d956fb41eb1df2aacaa73eb0b95e43724
actual=$(shasum -a 256 "$archive" | cut -d ' ' -f 1)
[ "$actual" = "$expected" ] || { echo 'RocksDB archive checksum mismatch' >&2; exit 1; }
if [ ! -d "$native_dir/rocksdb-9.10.0" ]; then
  tar -xzf "$archive" -C "$native_dir"
fi
(cd "$native_dir/rocksdb-9.10.0" && env PORTABLE=1 USE_RTTI=1 DISABLE_WARNING_AS_ERROR=1 ROCKSDB_DISABLE_ZSTD=1 ROCKSDB_DISABLE_LZ4=1 ROCKSDB_DISABLE_SNAPPY=1 ROCKSDB_DISABLE_BZIP=1 ROCKSDB_DISABLE_ZLIB=1 make -j"${HYBRID_BUILD_JOBS:-4}" shared_lib)
if [ "$(uname -s)" = Darwin ]; then
  install_name_tool -id '@rpath/librocksdb.9.10.dylib' "$native_dir/rocksdb-9.10.0/librocksdb.9.10.0.dylib"
fi
