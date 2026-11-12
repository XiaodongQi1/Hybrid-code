package patharchive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	block "github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/internal/logicalio"
	"testing"
	"time"

	ds "github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestCompactValuesAndOrderedHeights(t *testing.T) {
	accountKey := latestKey('F', []byte{0}, make([]byte, 32))
	slotKey := latestKey('F', append([]byte{1}, make([]byte, 32)...), make([]byte, 32))
	for _, n := range []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, math.MaxUint64} {
		a := ds.EmptyAccount()
		a.Nonce = n
		a.Balance.SetUint64(n)
		for _, roots := range []bool{false, true} {
			if roots {
				a.StorageRoot = mpt.Hash(word(n))
				a.CodeHash = word(n)
			}
			raw := ds.Encode(a)
			packed, e := encodeValue(accountKey, raw)
			must(t, e)
			decoded, e := decodeValue(accountKey, packed)
			must(t, e)
			if !bytes.Equal(raw, decoded) {
				t.Fatal("account roundtrip")
			}
		}
		if n > 0 {
			raw := word(n).Bytes()
			packed, e := encodeValue(slotKey, raw)
			must(t, e)
			decoded, e := decodeValue(slotKey, packed)
			must(t, e)
			if !bytes.Equal(raw, decoded) {
				t.Fatal("slot roundtrip")
			}
		}
	}
	// Exercise the full 256-bit balance and every truncation of an account
	// with both optional hashes, not only the small balances of the fixture.
	max := ds.EmptyAccount()
	max.Balance.SetBytes(bytes.Repeat([]byte{0xff}, 32))
	max.Nonce = math.MaxUint64
	max.StorageRoot = mpt.Hash(word(1))
	max.CodeHash = word(2)
	encoded, e := encodeValue(accountKey, ds.Encode(max))
	must(t, e)
	if encoded[0]>>2 != 32 {
		t.Fatal("balance length does not fit flag byte")
	}
	decoded, e := decodeValue(accountKey, encoded)
	must(t, e)
	if !bytes.Equal(decoded, ds.Encode(max)) {
		t.Fatal("256-bit balance")
	}
	for n := 1; n < len(encoded); n++ {
		if _, e := decodeValue(accountKey, encoded[:n]); e == nil {
			t.Fatalf("accepted truncated account %d", n)
		}
	}
	for _, bad := range [][]byte{{4, 0, 0}, {0, 128, 0, 0}, {0, 0, 1, 0}, {0, 0, 33}, {1, 0, 0}, {0, 0, 0, 0}} {
		if _, e := decodeValue(accountKey, bad); e == nil {
			t.Fatalf("invalid account accepted: %x", bad)
		}
	}
	for _, bad := range [][]byte{{0}, {0, 1}, make([]byte, 33)} {
		if _, e := decodeValue(slotKey, bad); e == nil {
			t.Fatal("invalid slot accepted")
		}
	}
	r := reader{format: Format}
	heights := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 65535, 65536, 1 << 32, math.MaxUint64}
	for i, h := range heights {
		b := r.heightBytes(h)
		if !r.validHeight(b) {
			t.Fatal(h)
		}
		if i > 0 && bytes.Compare(r.heightBytes(heights[i-1]), b) >= 0 {
			t.Fatal("height ordering")
		}
	}
	for _, b := range [][]byte{nil, {0, 1}, {1, 0}, {2, 1}, {9, 1, 2, 3, 4, 5, 6, 7, 8, 9}} {
		if r.validHeight(b) {
			t.Fatalf("bad height %x", b)
		}
	}
	// Odd nibble padding and explicit lengths prevent path-prefix collisions.
	keys := map[string]bool{}
	for n := 0; n <= 64; n++ {
		p := make([]byte, n)
		for i := range p {
			p[i] = byte(i % 16)
		}
		k := r.key('P', []byte{0}, p)
		if keys[string(k)] {
			t.Fatal("path collision")
		}
		keys[string(k)] = true
	}
}

func TestCompactHistoricalSeekBoundaries(t *testing.T) {
	for _, format := range []string{DifferentialFormat, Format} {
		cfg := DefaultConfig("seek")
		cfg.Format = format
		a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		must(t, e)
		defer a.Close()
		r := a.s.live(context.Background())
		r.current = false // This test installs synthetic historical records, not a head.
		k := latestKey('F', []byte{0}, make([]byte, 32))
		heights := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, math.MaxUint64}
		values := make([][]byte, len(heights))
		for i, h := range heights {
			if i%3 != 0 {
				ac := ds.EmptyAccount()
				ac.Nonce = uint64(i)
				values[i] = ds.Encode(ac)
			}
			v, e := encodeValue(k, values[i])
			must(t, e)
			if format == Format {
				batch := newDiskBatch()
				must(t, a.s.writeGroups(context.Background(), map[string][]byte{string(k): v}, h, func(k, v []byte) error { batch.Put(k, v); return nil }, &Changes{}))
				must(t, a.s.write(batch))
			} else {
				must(t, a.s.db.Put(append(r.historyPrefix(k), r.heightBytes(h)...), v))
			}
		}
		latest := ds.Encode(ds.EmptyAccount())
		v, e := encodeValue(k, latest)
		must(t, e)
		must(t, a.s.db.Put(k, v))
		for _, h := range []uint64{0, 1, 126, 127, 128, 254, 255, 256, 16382, 16383, 16384, math.MaxUint64 - 1, math.MaxUint64} {
			want := latest
			for i, n := range heights {
				if n > h {
					want = values[i]
					break
				}
			}
			got, e := r.value(k, h)
			must(t, e)
			if !bytes.Equal(got, want) {
				t.Fatalf("seek h=%d", h)
			}
		}
	}
}

// Same EVM input, canonical roots and proofs; only the durable representation
// differs. Writes are counted before WAL/compression, including delete keys.
func TestP01LogicalComparison(t *testing.T) {
	w, addresses, slots := dataset(t)
	compareFormats(t, w, addresses, slots, "PATH_COMPARISON_OUTPUT")
}
func TestP01ContractScaleComparison(t *testing.T) {
	key, e := crypto.HexToECDSA("0000000000000000000000000000000000000000000000000000000000000001")
	must(t, e)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	coinbase := common.HexToAddress("0xcafe")
	addresses := []common.Address{sender, coinbase}
	contracts := []common.Address{}
	alloc := []block.Allocation{{Address: sender, Balance: big.NewInt(1000000000000)}}
	for i := 0; i < 255; i++ {
		addr := common.HexToAddress(fmt.Sprintf("%040x", 0x10000+i))
		addresses = append(addresses, addr)
		a := block.Allocation{Address: addr, Balance: big.NewInt(int64(i + 1))}
		if i < 32 {
			a.Code = common.FromHex("60003560005560005460005260206000f3")
			contracts = append(contracts, addr)
		}
		alloc = append(alloc, a)
	}
	w := block.Workload{Schema: block.WorkloadSchema}
	nonce := uint64(0)
	for h := uint64(1); h <= 65; h++ {
		b := block.InputBlock{Height: h, ID: word(10000 + h), Context: block.Context{Time: 1000 + h, GasLimit: 10000000, Coinbase: coinbase, BaseFee: big.NewInt(1), BlobBaseFee: big.NewInt(1)}}
		if h == 1 {
			b.Allocations = alloc
		} else {
			b.ParentID = w.Blocks[len(w.Blocks)-1].ID
			for j := 0; j < 8; j++ {
				to := contracts[(int(h)*8+j)%len(contracts)]
				tx, e := types.SignTx(types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(1337), Nonce: nonce, Gas: 100000, GasFeeCap: big.NewInt(3), GasTipCap: big.NewInt(1), To: &to, Value: big.NewInt(0), Data: word(h % 7).Bytes()}), types.LatestSignerForChainID(big.NewInt(1337)), key)
				must(t, e)
				nonce++
				wire, e := tx.MarshalBinary()
				must(t, e)
				b.Transactions = append(b.Transactions, wire)
			}
		}
		w.Blocks = append(w.Blocks, b)
	}
	compareFormats(t, w, addresses, []common.Hash{{}, word(1)}, "PATH_SCALE_COMPARISON_OUTPUT")
}
func compareFormats(t *testing.T, w block.Workload, addresses []common.Address, slots []common.Hash, output string) {
	roots, outcomes := runOracle(t, w, addresses, slots, DefaultConfig("compare").Chain)
	type result struct {
		HistoryClasses map[string]HistoryClass

		Format                         string
		Retained                       map[string]int64
		InitialRetained                map[string]int64
		NetStateBytes                  int64
		QueryVerifyNS                  int64
		Encoding                       HistoryEncoding
		Recovery                       HistoryRecovery
		InitialWrites, ExecutionWrites logicalio.Stats
		Verified                       bool
	}
	reports := []result{}
	for _, format := range []string{LegacyFormat, CompactFormat, FullHistoryFormat, DifferentialFormat, Format} {
		cfg := DefaultConfig("compare")
		cfg.Format = format
		path := filepath.Join(t.TempDir(), "db")
		a, e := Open(path, cfg, true, false)
		must(t, e)
		var initial logicalio.Stats
		for i, b := range w.Blocks {
			rec, e := a.ExecuteBlock(context.Background(), b)
			must(t, e)
			equal(t, rec.Root, roots[i+1])
			equal(t, rec.Outcomes, outcomes[i])
			if i == 0 {
				initial = a.LogicalWrites()
				initialKV, err := a.Breakdown()
				must(t, err)
				reports = append(reports, result{Format: format, InitialWrites: initial, InitialRetained: initialKV})
			}
		}
		rep := &reports[len(reports)-1]
		rep.Retained, e = a.Breakdown()
		must(t, e)
		rep.ExecutionWrites = logicalio.Difference(initial, a.LogicalWrites())
		for _, key := range []string{"current-trie", "current-flat", "reverse-history", "code", "history-checkpoints", "history-index", "hash-location-index"} {
			rep.NetStateBytes += rep.Retained[key] - rep.InitialRetained[key]
		}
		rep.HistoryClasses, e = a.HistoryInventory()
		must(t, e)
		rep.Encoding, e = a.EncodingMetrics()
		must(t, e)
		start := time.Now()
		verifyHistory(t, a, roots, addresses, slots)
		rep.QueryVerifyNS = time.Since(start).Nanoseconds()
		rep.Recovery = a.RecoveryMetrics()
		must(t, a.Audit(context.Background()))
		if format == Format {
			it := a.s.db.NewIterator(prefixRange([]byte{'I'}))
			found := it.First()
			it.Release()
			if found {
				t.Fatal("v2 has I records")
			}
		}
		must(t, a.Close())
		a, e = Open(path, cfg, false, true)
		must(t, e)
		verifyHistory(t, a, roots, addresses, slots)
		must(t, a.Audit(context.Background()))
		must(t, a.Close())
		wrong := cfg
		if format == Format {
			wrong.Format = LegacyFormat
		} else {
			wrong.Format = Format
		}
		if bad, e := Open(path, wrong, false, true); e == nil {
			bad.Close()
			t.Fatal("wrong format accepted")
		}
		rep.Verified = true
	}
	v3, v4 := reports[2], reports[3]
	if v4.NetStateBytes >= v3.NetStateBytes {
		t.Fatal("fixture differential storage did not improve")
	}
	v4 = reports[len(reports)-1]
	var nk, nv uint64
	for _, c := range v4.HistoryClasses {
		nk += c.KeyBytes
		nv += c.ValueBytes
	}
	if nk != v4.Encoding.KeyBytes || nv != v4.Encoding.ValueBytes {
		t.Fatal("history accounting does not close")
	}
	if v4.Recovery.MaxDeltas >= 8 {
		t.Fatal("recovery bound exceeded")
	}
	b, e := json.MarshalIndent(struct {
		Blocks  int
		Results []result
	}{len(w.Blocks), reports}, "", "  ")
	must(t, e)
	t.Log(string(b))
	if path := os.Getenv(output); path != "" {
		must(t, os.WriteFile(path, b, 0600))
	}
	if reports[1].Retained["hash-location-index"] != 0 || reports[1].Retained["current-flat"] >= reports[0].Retained["current-flat"] || reports[1].Retained["reverse-history"] >= reports[0].Retained["reverse-history"] {
		t.Fatal("compact representation did not remove expected overhead")
	}
}
