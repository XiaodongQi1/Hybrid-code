package backend

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
)

func TestLogicalTraceImportMatchesEVM(t *testing.T) {
	runLogicalTraceImport(t, legacyTestOpener)
}

func runLogicalTraceImport(t *testing.T, open testOpener) {
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 32, Blocks: 5, TransactionsPerBlock: 16, Contracts: 3, Seed: 31})
			if e != nil {
				t.Fatal(e)
			}
			cfg := smallConfig(mode)
			source, e := Open(filepath.Join(t.TempDir(), "source"), cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer source.Close()
			path := filepath.Join(t.TempDir(), "target")
			target, e := open(t, path, cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer func() { target.Close() }()
			initial, e := source.Apply(w.Blocks[0], nil)
			if e != nil {
				t.Fatal(e)
			}
			got, e := target.Apply(w.Blocks[0], nil)
			if e != nil || got.Root != initial.Root {
				t.Fatal("initialization", e)
			}
			for _, input := range w.Blocks[1:] {
				r, trace, e := source.ApplyWithTrace(input, nil)
				if e != nil {
					t.Fatal("capture", input.Height, e)
				}
				id, raw, e := trace.Freeze(input, mode)
				if e != nil {
					t.Fatal(e)
				}
				trace, e = block.ParseLogicalBlock(raw, id, input, mode)
				if e != nil {
					t.Fatal(e)
				}
				before, e := target.Head()
				if e != nil {
					t.Fatal(e)
				}
				wrong := trace
				wrong.Root[0] ^= 1
				if _, e = target.ApplyLogicalBlock(input, wrong); e == nil {
					t.Fatal("wrong expected root accepted")
				}
				after, e := target.Head()
				if e != nil || before != after {
					t.Fatal("root mismatch changed durable Head", e)
				}
				changedInput := input
				changedInput.Context.Time++
				if _, e = target.ApplyLogicalBlock(changedInput, trace); e == nil {
					t.Fatal("unbound input accepted")
				}
				got, e := target.ApplyLogicalBlock(input, trace)
				if e != nil || got.Root != r.Root || !reflect.DeepEqual(got.Outcomes, r.Outcomes) {
					t.Fatal("logical import differs from EVM", input.Height, e)
				}
			}
			if e = target.Close(); e != nil {
				t.Fatal(e)
			}
			target, e = open(t, path, cfg, false, true)
			if e != nil {
				t.Fatal(e)
			}
			for _, input := range w.Blocks {
				v, e := target.ViewAt(input.Height)
				if e != nil {
					t.Fatal(e)
				}
				sv, e := source.ViewAt(input.Height)
				if e != nil {
					t.Fatal(e)
				}
				for _, address := range []common.Address{common.HexToAddress("0x1000"), common.HexToAddress("0x2000"), common.HexToAddress("0x2001"), common.HexToAddress("0xdead")} {
					for _, kind := range []q.Kind{q.Account, q.Storage, q.Code} {
						req := q.Request{Height: input.Height, Kind: kind, Address: address}
						want := traceResponse(t, sv, req)
						actual := traceResponse(t, v, req)
						av, e := q.VerifyWithCodec(context.Background(), sv.Root(), req, actual, q.DefaultLimits(), mode)
						if e != nil {
							t.Fatal("imported historical proof", input.Height, address, kind, e)
						}
						wv, e := q.VerifyWithCodec(context.Background(), sv.Root(), req, want, q.DefaultLimits(), mode)
						if e != nil || !reflect.DeepEqual(av, wv) {
							t.Fatal("historical value changed", e)
						}
						// The identical wire representation is independent of the
						// source's path-versioned or target's hybrid physical layout.
						x, _ := json.Marshal(actual)
						y, _ := json.Marshal(want)
						if string(x) != string(y) {
							t.Fatal("compact proof wire differs across stores")
						}
					}
				}
				v.Close()
				sv.Close()
			}
		})
	}
}

func traceResponse(t *testing.T, v *View, req q.Request) q.Response {
	t.Helper()
	r, e := q.Read(context.Background(), req, v)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
