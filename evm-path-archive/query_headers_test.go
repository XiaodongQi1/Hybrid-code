package patharchive

import (
	"context"
	"path/filepath"
	dq "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func TestPreparedQueryHeadersPreserveHistoricalProofs(t *testing.T) {
	ctx := context.Background()
	w, addresses, slots := dataset(t)
	cfg := DefaultConfig("prepared-query-headers")
	cfg.Format = PackedFormat
	path := filepath.Join(t.TempDir(), "db")
	a, e := Open(path, cfg, true, false)
	must(t, e)
	if e = a.PrepareQueryHeaders(ctx); e == nil {
		t.Fatal("mutable archive accepted immutable header cache")
	}
	roots := map[uint64]mpt.Hash{}
	for _, b := range w.Blocks {
		r, e := a.ExecuteBlock(ctx, b)
		must(t, e)
		roots[b.Height] = r.Root
	}
	must(t, a.Close())
	a, e = Open(path, cfg, false, true)
	must(t, e)
	defer a.Close()
	var requests []dq.Request
	var before []dq.Response
	for h := uint64(1); h <= uint64(len(w.Blocks)); h++ {
		for _, kind := range []dq.Kind{dq.Account, dq.Storage} {
			q := dq.Request{Height: h, Kind: kind, Address: addresses[1]}
			if kind == dq.Storage {
				q.Slot = slots[0]
			}
			r, e := a.Query(ctx, q)
			must(t, e)
			requests = append(requests, q)
			before = append(before, r)
		}
	}
	must(t, a.PrepareQueryHeaders(ctx))
	if len(a.queryHeaders) != len(w.Blocks) {
		t.Fatal("header coverage")
	}
	for i, q := range requests {
		r, e := a.Query(ctx, q)
		must(t, e)
		equal(t, r, before[i])
		_, e = dq.Verify(ctx, roots[q.Height], q, r, dq.DefaultLimits())
		must(t, e)
	}
}
