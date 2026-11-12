#!/bin/sh
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
rocks_prefix=${ROCKSDB_PREFIX:-"$project_root/.local/native/rocksdb-9.10.0"}
case "$rocks_prefix" in *[[:space:]]*) echo 'ROCKSDB_PREFIX must not contain whitespace' >&2; exit 1;; esac
[ -f "$rocks_prefix/include/rocksdb/c.h" ] || { echo 'Run build-hybrid-rocksdb.sh first.' >&2; exit 1; }
config=${1:-"$project_root/artifact/configs/hybrid-storage.example.json"}
case "$config" in /*) ;; *) config="$PWD/$config";; esac
export CGO_ENABLED=1
export CGO_CPPFLAGS="${CGO_CPPFLAGS:-} -I$rocks_prefix/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$rocks_prefix -Wl,-rpath,$rocks_prefix"
export HYBRID_STORAGE_CONFIG="$config"
export HYBRID_STORAGE_OUTPUT_BASE="$project_root"
cd "$project_root"
go test -mod=readonly -tags rocksdb ./hybridtrie -run '^TestHybridStorageExperiment$' -count=1 -timeout=30m -v
