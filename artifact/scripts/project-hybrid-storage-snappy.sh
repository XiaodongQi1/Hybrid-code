#!/bin/sh
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
rocks_prefix=${ROCKSDB_PREFIX:-"$project_root/.local/native/rocksdb-9.10.0"}
results=${1:?Usage: project-hybrid-storage-snappy.sh RESULTS_DIRECTORY}
case "$results" in /*) ;; *) results="$PWD/$results";; esac
case "$rocks_prefix" in *[[:space:]]*) echo 'ROCKSDB_PREFIX must not contain whitespace' >&2; exit 1;; esac
export CGO_ENABLED=1
export CGO_CPPFLAGS="${CGO_CPPFLAGS:-} -I$rocks_prefix/include"
export CGO_LDFLAGS="${CGO_LDFLAGS:-} -L$rocks_prefix -Wl,-rpath,$rocks_prefix"
export HYBRID_STORAGE_RESULTS="$results"
cd "$project_root"
go test -mod=readonly -tags rocksdb ./hybridtrie -run '^TestHybridStorageSnappyProjection$' -count=1 -timeout=30m -v
