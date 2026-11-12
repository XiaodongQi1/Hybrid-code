#!/bin/sh
# Run from any directory. No experiment datasets or network services required.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
(cd "$project_root" && go test -mod=readonly ./...)
(cd "$project_root/evm-runtime" && go test -mod=readonly ./...)
(cd "$project_root/evm-state" && go test -mod=readonly ./...)
(cd "$project_root/evm-path-archive" && go test -mod=readonly ./...)
(cd "$project_root/evm-versioned-trie" && go test -mod=readonly ./...)
