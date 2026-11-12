package patharchive

import (
	"bytes"
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
)

func TestNativeCodecFlatValues(t *testing.T) {
	accountKey := latestKey('F', []byte{0}, make([]byte, 32))
	slotKey := latestKey('F', append([]byte{1}, make([]byte, 32)...), make([]byte, 32))
	for _, n := range []uint64{0, 1, 127, 128, 255, 256, math.MaxUint64} {
		a := state.EmptyAccount()
		a.Nonce = n
		a.Balance.SetUint64(n)
		raw, e := codec.Ethereum.EncodeAccount(a)
		must(t, e)
		packed, e := encodeValueWithCodec(accountKey, raw, codec.Ethereum)
		must(t, e)
		back, e := decodeValueWithCodec(accountKey, packed, codec.Ethereum)
		must(t, e)
		if !bytes.Equal(raw, back) {
			t.Fatal("native flat account")
		}
		if n > 0 {
			raw, e := codec.Ethereum.EncodeStorage(word(n))
			must(t, e)
			packed, e := encodeValueWithCodec(slotKey, raw, codec.Ethereum)
			must(t, e)
			back, e := decodeValueWithCodec(slotKey, packed, codec.Ethereum)
			must(t, e)
			if !bytes.Equal(raw, back) {
				t.Fatal("native flat slot")
			}
		}
	}
}

func TestNativeCodecExecutionRootsHistoryAndReopen(t *testing.T) {
	w, addresses, slots := dataset(t)
	for _, format := range []string{LegacyFormat, CompactFormat, FullHistoryFormat, DifferentialFormat, Format, IndexedFormat, PackedFormat} {
		t.Run(format, func(t *testing.T) {
			cfg := DefaultConfig("native-codec-execution")
			cfg.Codec = codec.Ethereum
			cfg.Format = format
			if IsIndexedFormat(format) {
				l := DefaultHistoryLayout().ForFormat(format)
				cfg.HistoryLayout = &l
			}
			roots, outcomes := runOracleWithCodec(t, w, addresses, slots, cfg.Chain, codec.Ethereum)
			path := filepath.Join(t.TempDir(), "db")
			db, e := Open(path, cfg, true, false)
			must(t, e)
			defer func() { db.Close() }()
			for i, input := range w.Blocks {
				r, e := db.ExecuteBlock(context.Background(), input)
				must(t, e)
				if r.Root != roots[i+1] {
					t.Fatal("native root differs from map-state oracle", input.Height)
				}
				equal(t, r.Outcomes, outcomes[i])
			}
			must(t, db.Audit(context.Background()))
			must(t, db.Close())
			db, e = Open(path, cfg, false, true)
			must(t, e)
			must(t, db.PrepareQueryHeaders(context.Background()))
			for i, input := range w.Blocks {
				for _, address := range addresses {
					requests := []q.Request{{Height: input.Height, Kind: q.Account, Address: address}, {Height: input.Height, Kind: q.Code, Address: address}}
					for _, slot := range slots {
						requests = append(requests, q.Request{Height: input.Height, Kind: q.Storage, Address: address, Slot: slot})
					}
					for _, req := range requests {
						r, e := db.Query(context.Background(), req)
						must(t, e)
						if r.ProofFormat != q.CompactProof {
							t.Fatal("expanded native proof")
						}
						_, e = q.VerifyWithCodec(context.Background(), roots[i+1], req, r, q.DefaultLimits(), codec.Ethereum)
						must(t, e)
					}
				}
			}
			// Default openings remain legacy, and cannot reinterpret native data.
			legacy := cfg
			legacy.Codec = ""
			if wrong, e := Open(path, legacy, false, true); e == nil {
				wrong.Close()
				t.Fatal("codec mismatch accepted")
			}
		})
	}
}
