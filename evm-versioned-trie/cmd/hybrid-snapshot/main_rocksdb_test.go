//go:build rocksdb && cgo

package main

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	"testing"
)

func TestHybridSnapshotCLIImportResumeAndSourceBinding(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "accounts.jsonl")
	f, e := os.Create(source)
	if e != nil {
		t.Fatal(e)
	}
	encoder := json.NewEncoder(f)
	for i := 1; i <= 500; i++ {
		a := block.Allocation{Address: common.BigToAddress(big.NewInt(int64(i))), Balance: big.NewInt(int64(i))}
		if i%17 == 0 {
			a.Code = []byte{0}
			a.Storage = map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}
		}
		if e = encoder.Encode(a); e != nil {
			t.Fatal(e)
		}
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	cfg := config{Schema: "hybrid-snapshot/v1", SourceFile: "accounts.jsonl", Header: block.InputBlock{Height: 7000, ID: common.HexToHash("0x1234"), Context: block.Context{GasLimit: 30_000_000}}, Snapshot: backend.DefaultSnapshotConfig(), MaxBatchBytes: 4096}
	cfg.Snapshot.BatchBytes = 4096
	cfg.Snapshot.ChunkEntries = 16
	cfg.Snapshot.CheckpointAccounts = 16
	configPath := filepath.Join(dir, "config.json")
	if e = writeJSON(configPath, cfg); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(dir, "out")
	first, e := run(context.Background(), configPath, output, false)
	if e != nil || first.Snapshot.Accounts != 500 || first.Snapshot.Import.Batches < 2 || first.Snapshot.Import.PeakBatchBytes > 4096 {
		t.Fatal(first, e)
	}
	second, e := run(context.Background(), configPath, output, true)
	if e != nil || !second.Snapshot.AlreadyComplete || second.Snapshot.Accounts != 500 || second.Snapshot.Root != first.Snapshot.Root || second.Snapshot.InputDigest != first.Snapshot.InputDigest {
		t.Fatal(second, e)
	}
	if _, e = run(context.Background(), configPath, output, false); e == nil {
		t.Fatal("existing output overwritten")
	}
	changed, e := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	changed.WriteString("\n")
	changed.Close()
	if _, e = run(context.Background(), configPath, output, true); e == nil {
		t.Fatal("changed source bytes accepted")
	}
	if _, e = os.Stat(filepath.Join(output, "db.snapshot-build")); !os.IsNotExist(e) {
		t.Fatal("successful scratch retained", e)
	}
}
