#!/bin/sh
# Configuration/output paths are relative to the caller's directory.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
rocks_prefix=${ROCKSDB_PREFIX:-"$project_root/.local/native/rocksdb-9.10.0"}
case "$rocks_prefix" in *[[:space:]]*) echo "ROCKSDB_PREFIX must not contain whitespace" >&2; exit 1;; esac
[ -f "$rocks_prefix/include/rocksdb/c.h" ] || { echo 'Run build-hybrid-rocksdb.sh or set ROCKSDB_PREFIX to RocksDB 9.10.0.' >&2; exit 1; }
export CGO_ENABLED=1
export CGO_CPPFLAGS="${CGO_CPPFLAGS:-} -I$rocks_prefix/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$rocks_prefix -Wl,-rpath,$rocks_prefix"
exec go -C "$project_root/evm-versioned-trie" run -mod=readonly -tags rocksdb ./cmd/hybrid-snapshot -base-dir "$PWD" "$@"
