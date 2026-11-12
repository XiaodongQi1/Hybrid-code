#!/bin/sh
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
exec go -C "$project_root/evm-versioned-trie" run -mod=readonly ./cmd/versioned-evm-storage -base-dir "$PWD" "$@"
