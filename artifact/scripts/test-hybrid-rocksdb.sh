#!/bin/sh
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
rocks_prefix=${ROCKSDB_PREFIX:-"$project_root/.local/native/rocksdb-9.10.0"}
case "$rocks_prefix" in *[[:space:]]*) echo "ROCKSDB_PREFIX must not contain whitespace" >&2; exit 1;; esac
[ -f "$rocks_prefix/include/rocksdb/c.h" ] || { echo 'Run build-hybrid-rocksdb.sh or set ROCKSDB_PREFIX to a built RocksDB 9.10.0 source tree.' >&2; exit 1; }
export CGO_ENABLED=1
export CGO_CPPFLAGS="${CGO_CPPFLAGS:-} -I$rocks_prefix/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$rocks_prefix -Wl,-rpath,$rocks_prefix"
(cd "$project_root" && go test -mod=readonly -tags rocksdb ./kvstore ./hybridtrie ./mpt)
(cd "$project_root/evm-state" && go test -mod=readonly -tags rocksdb ./artifact ./snapshot)
(cd "$project_root/evm-path-archive" && go test -mod=readonly -tags rocksdb ./... -count=1)
(cd "$project_root/evm-versioned-trie" && go test -mod=readonly -tags rocksdb ./baseline ./backend ./comparison ./runner ./cmd/hybrid-snapshot -run 'Test(Hybrid|Archive|ParallelProof|CommonResources|CommonBulk|DefaultResource)' -count=1 -v)
