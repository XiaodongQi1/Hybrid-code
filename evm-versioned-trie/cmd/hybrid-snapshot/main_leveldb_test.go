package main

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestHybridSnapshotCLILevelDBImportAndResume(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "accounts.jsonl")
	f, err := os.Create(source)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(f)
	for i := int64(1); i <= 64; i++ {
		a := block.Allocation{Address: common.BigToAddress(big.NewInt(i)), Balance: big.NewInt(i)}
		if i%8 == 0 {
			a.Code = []byte{0}
			a.Storage = map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}
		}
		if err = encoder.Encode(a); err != nil {
			f.Close()
			t.Fatal(err)
		}
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	options := kvstore.DefaultOptions(kvstore.LevelDB)
	cfg := config{Schema: "hybrid-snapshot/v1", SourceFile: "accounts.jsonl", Header: block.InputBlock{Height: 7000, ID: common.HexToHash("0x1234"), Context: block.Context{GasLimit: 30_000_000}}, Snapshot: backend.DefaultSnapshotConfig(), MaxBatchBytes: 4096, HybridStorage: &options}
	cfg.Snapshot.BatchBytes = 4096
	cfg.Snapshot.ChunkEntries = 8
	cfg.Snapshot.CheckpointAccounts = 8
	configPath := filepath.Join(dir, "config.json")
	if err = writeJSON(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "out")
	first, err := run(context.Background(), configPath, output, false)
	if err != nil || first.Snapshot.Accounts != 64 || first.Snapshot.Import.Batches < 2 {
		t.Fatal(first, err)
	}
	second, err := run(context.Background(), configPath, output, true)
	if err != nil || !second.Snapshot.AlreadyComplete || second.Snapshot.Root != first.Snapshot.Root || second.Snapshot.InputDigest != first.Snapshot.InputDigest {
		t.Fatal(second, err)
	}
	before, err := os.ReadFile(filepath.Join(output, "db", "KVSTORE.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker struct{ Engine kvstore.Engine }
	if err = json.Unmarshal(before, &marker); err != nil || marker.Engine != kvstore.LevelDB {
		t.Fatal("snapshot importer used the wrong engine", marker, err)
	}
	options.Engine = kvstore.RocksDB
	if err = writeJSON(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = run(context.Background(), configPath, output, true); err == nil {
		t.Fatal("changed engine accepted on resume")
	}
	after, err := os.ReadFile(filepath.Join(output, "db", "KVSTORE.json"))
	if err != nil || string(after) != string(before) {
		t.Fatal("engine mismatch changed the database marker", err)
	}
}
