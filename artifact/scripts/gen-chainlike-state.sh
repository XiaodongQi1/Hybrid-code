#!/bin/sh
# Writes a deterministic, chain-shaped synthetic workload to artifact/data/chainlike
# (git-ignored). Extra flags are passed to hybrid-genstate; see its -h output.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
out=${CHAINLIKE_OUT:-"$project_root/artifact/data/chainlike"}
cd "$project_root/evm-versioned-trie"
exec go run ./cmd/hybrid-genstate -out "$out" "$@"
