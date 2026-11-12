#!/bin/sh
# Configuration and output paths are resolved from the caller's directory.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
exec go -C "$project_root/evm-versioned-trie" run -mod=readonly ./cmd/versioned-evm -base-dir "$PWD" "$@"
