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

func TestHybridSnapshotCLIHashBulkLevelDB(t *testing.T) { runHashBulkCLI(t, kvstore.LevelDB) }
func runHashBulkCLI(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	dir := t.TempDir()
	source := filepath.Join(dir, "accounts.jsonl")
	f, e := os.Create(source)
	if e != nil {
		t.Fatal(e)
	}
	encoder := json.NewEncoder(f)
	for i := 1; i <= 80; i++ {
		a := block.Allocation{Address: common.BigToAddress(big.NewInt(int64(i))), Balance: big.NewInt(int64(i))}
		if i%11 == 0 {
			a.Code = []byte{0}
			a.Storage = map[common.Hash]common.Hash{{31: 1}: {31: 2}}
		}
		if e = encoder.Encode(a); e != nil {
			t.Fatal(e)
		}
	}
	f.Close()
	o := kvstore.DefaultOptions(engine)
	o.BlockCacheBytes, o.WriteBufferBytes = 1<<20, 1<<20
	c := config{Initialization: backend.InitializationHashColdBulk, Schema: "hybrid-snapshot/v1", SourceFile: "accounts.jsonl", Header: block.InputBlock{Height: 100, ID: common.Hash{31: 1}, Context: block.Context{GasLimit: 1000000, BaseFee: new(big.Int), BlobBaseFee: big.NewInt(1)}}, Snapshot: backend.DefaultHashColdBulkSnapshotConfig(), MaxBatchBytes: 4096, HybridStorage: &o}
	c.Snapshot.BatchBytes = 4096
	c.Snapshot.ChunkEntries = 8
	c.Snapshot.CheckpointAccounts = 8
	p := filepath.Join(dir, "config.json")
	if e = writeJSON(p, c); e != nil {
		t.Fatal(e)
	}
	output := filepath.Join(dir, "out")
	r, e := run(context.Background(), p, output, false)
	if e != nil {
		t.Fatal(e)
	}
	if r.Snapshot.Mode != backend.InitializationHashColdBulk || r.Snapshot.ResumedBuild || r.Snapshot.Accounts != 80 || r.Snapshot.Import.Batches < 2 {
		t.Fatal(r)
	}
	_, e = os.Stat(filepath.Join(output, "db.snapshot-build"))
	if e != nil {
		t.Fatal("sealed source was not retained", e)
	}
	again, e := run(context.Background(), p, output, true)
	if e != nil || !again.Snapshot.AlreadyComplete || again.Snapshot.Root != r.Snapshot.Root {
		t.Fatal(again, e)
	}
	c.Initialization = backend.InitializationPrefix
	if e = writeJSON(p, c); e != nil {
		t.Fatal(e)
	}
	if _, e = run(context.Background(), p, output, true); e == nil {
		t.Fatal("changed representation resumed")
	}
}
