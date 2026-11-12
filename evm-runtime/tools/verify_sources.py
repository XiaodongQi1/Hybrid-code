#!/usr/bin/env python3
"""Verify the slim local fork and enforce its dependency/storage boundary."""
import hashlib
import json
from pathlib import Path
root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "UPSTREAM.json").read_text())
expected = manifest["slim_source_sha256"]
actual = {str(p.relative_to(root)) for folder in ["geth", "state", "internal", "examples", "testdata"] for p in (root / folder).rglob("*") if p.is_file()}
actual |= {str(p.relative_to(root)) for p in root.glob("*.go")}
assert actual == set(expected), f"source file set changed: {actual.symmetric_difference(expected)}"
for rel, sha in expected.items():
    assert hashlib.sha256((root / rel).read_bytes()).hexdigest() == sha, rel
for rel in ["geth/trie", "geth/triedb", "geth/core/state", "geth/core/rawdb", "geth/core/stateless", "geth/ethdb", "geth/metrics", "geth/node", "geth/log", "geth/core/tracing", "geth/consensus", "geth/params/bootnodes.go", "geth/params/dao.go", "geth/params/network_params.go", "geth/params/verkle_params.go", "geth/params/forks", "geth/core/types/block.go", "geth/core/types/state_account.go", "geth/core/types/log.go", "geth/core/types/bloom9.go", "geth/core/types/withdrawal.go", "geth/core/types/tx_setcode.go", "geth/core/types/transaction_marshalling.go"]:
    assert not (root / rel).exists(), rel
assert "github.com/ethereum/go-ethereum" not in (root / "go.mod").read_text()
print(f"Verified {len(expected)} slim local source/fixture files; no original trie/database/state/node backend.")
