package snapshot

import (
	"context"
	"errors"
	"io"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func sourceFixture(engine kvstore.Engine) (Config, []block.Allocation) {
	o := kvstore.DefaultOptions(engine)
	o.BlockCacheBytes, o.WriteBufferBytes = 1<<20, 1<<20
	c := Config{SourceID: "snapshot-source-fixture", Header: block.InputBlock{Height: 0, ID: common.Hash{31: 9}, Context: block.Context{GasLimit: 1000000, BaseFee: new(big.Int), BlobBaseFee: big.NewInt(1)}}, BatchBytes: 4096, ChunkEntries: 4, CheckpointAccounts: 4, Storage: &o}
	var rows []block.Allocation
	for i := 1; i <= 40; i++ {
		a := block.Allocation{Address: common.BigToAddress(big.NewInt(int64(i))), Balance: big.NewInt(int64(i))}
		if i%13 == 0 {
			a.Code = []byte{0}
			a.Storage = map[common.Hash]common.Hash{{31: 1}: {31: 2}, {31: 3}: {31: 4}}
		}
		rows = append(rows, a)
	}
	return c, rows
}
func allocationIterator(rows []block.Allocation) func() (block.Allocation, error) {
	i := 0
	return func() (block.Allocation, error) {
		if i == len(rows) {
			return block.Allocation{}, io.EOF
		}
		a := rows[i]
		i++
		return a, nil
	}
}
func TestSourceLevelDBResumeAndBoundInputs(t *testing.T) { runSourceResume(t, kvstore.LevelDB) }
func runSourceResume(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	c, rows := sourceFixture(engine)
	path := filepath.Join(t.TempDir(), "source")
	ctx, cancel := context.WithCancel(context.Background())
	i := 0
	_, e := Build(ctx, path, c, func() (block.Allocation, error) {
		a := rows[i]
		i++
		if i == 17 {
			cancel()
		}
		return a, nil
	})
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	bc, e := BuildConfig(c)
	if e != nil {
		t.Fatal(e)
	}
	partial, e := ht.OpenSnapshotBuilder(path, bc)
	if e != nil {
		t.Fatal(e)
	}
	if partial.Progress().Ready || partial.Progress().Cursor != 16 {
		t.Fatal(partial.Progress())
	}
	if _, e = partial.OpenSource(); !errors.Is(e, ht.ErrSnapshotPending) {
		t.Fatal("partial source exposed", e)
	}
	partial.Close()
	bad := append([]block.Allocation(nil), rows...)
	bad[0].Balance = big.NewInt(999)
	if _, e = Build(context.Background(), path, c, allocationIterator(bad)); !errors.Is(e, ht.ErrConflict) {
		t.Fatal("changed consumed input accepted", e)
	}
	if _, e = Build(context.Background(), path, c, allocationIterator(rows[:3])); !errors.Is(e, ht.ErrConflict) {
		t.Fatal("truncated consumed input accepted", e)
	}
	b, e := Build(context.Background(), path, c, allocationIterator(rows))
	if e != nil {
		t.Fatal(e)
	}
	s, e := b.OpenSource()
	if e != nil {
		t.Fatal(e)
	}
	m, e := Verify(context.Background(), s, c.Header)
	if e != nil || m.Accounts != 40 || m.NextTrieID != 4 {
		t.Fatal(m, e)
	}
	if e = b.Close(); !errors.Is(e, ht.ErrBusy) {
		t.Fatal("closed a pinned source", e)
	}
	s.Close()
	if e = b.Close(); e != nil {
		t.Fatal(e)
	}
	ready, e := Build(context.Background(), path, c, nil)
	if e != nil {
		t.Fatal(e)
	}
	ready.Close()
	changed := c
	changed.Header.Context.Time = 10
	if _, e = Build(context.Background(), path, changed, nil); !errors.Is(e, ht.ErrConflict) {
		t.Fatal("changed header reopened source", e)
	}
	// Corrupt original input independently of canonical state: provenance must
	// still be verified before a destination is allowed to publish its Head.
	o := *c.Storage
	o.Columns = []string{"default", "prefix", "hash"}
	db, e := kvstore.Open(path, o, false, false)
	if e != nil {
		t.Fatal(e)
	}
	batch := kvstore.NewBatch()
	key := append([]byte{'A'}, rows[0].Address[:]...)
	batch.Put("default", key, []byte("corrupt"))
	if e = db.Write(batch); e != nil {
		t.Fatal(e)
	}
	db.Close()
	b, e = Build(context.Background(), path, c, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	s, e = b.OpenSource()
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if _, e = Verify(context.Background(), s, c.Header); e == nil {
		t.Fatal("corrupted account source was trusted")
	}
}
