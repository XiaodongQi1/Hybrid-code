package patharchive

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	dq "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	ds "github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
)

func TestP2InitializationBoundaryAndRestart(t *testing.T) {
	for _, zero := range []bool{false, true} {
		t.Run(fmt.Sprint(zero), func(t *testing.T) {
			w, addresses, slots := dataset(t)
			cfg := DefaultConfig("p2-boundary")
			cfg.ZeroGenesis = zero
			if zero {
				for i := range w.Blocks {
					w.Blocks[i].Height--
				}
			}
			path := filepath.Join(t.TempDir(), "db")
			a, e := Open(path, cfg, true, false)
			must(t, e)
			roots := make([]mpt.Hash, len(w.Blocks)+1)
			for i, b := range w.Blocks {
				rec, e := a.ExecuteBlock(context.Background(), b)
				must(t, e)
				roots[b.Height] = rec.Root
				if i == 0 {
					it := a.s.db.NewIterator(prefixRange([]byte{'D'}))
					found := it.First()
					err := it.Error()
					it.Release()
					must(t, err)
					if found {
						t.Fatal("initialization created unreachable absence history")
					}
					_, e = a.Query(context.Background(), dq.Request{Height: uint64(len(w.Blocks) + 1), Address: addresses[0], Kind: dq.Account})
					if e == nil {
						t.Fatal("future query accepted")
					}
					if !zero {
						_, e = a.Query(context.Background(), dq.Request{Height: 0, Address: addresses[0], Kind: dq.Account})
						if e == nil {
							t.Fatal("pre-initialization query accepted")
						}
					}
				}
			}
			// Contract is created after initialization; its absence at initialization
			// must remain reconstructible, even though initial D records were omitted.
			r := a.s.live(context.Background())
			k := latestKey('F', []byte{0}, ds.AccountKey(addresses[1]))
			value, e := r.value(k, cfg.initial())
			must(t, e)
			if value != nil {
				t.Fatal("later creation leaked into initial state")
			}
			it := r.historyIterator(k)
			if !it.First() {
				t.Fatal("later creation absence omitted")
			}
			if len(it.Value()) != 0 {
				t.Fatal("expected creation absence")
			}
			it.Release()
			verifyHistory(t, a, roots, addresses, slots)
			must(t, a.Audit(context.Background()))
			must(t, a.Close())
			for _, ro := range []bool{false, true} {
				a, e = Open(path, cfg, false, ro)
				must(t, e)
				verifyHistory(t, a, roots, addresses, slots)
				must(t, a.Audit(context.Background()))
				must(t, a.Close())
			}
		})
	}
}

func TestP2HeadFlatReadSkipsHistory(t *testing.T) {
	w, addresses, _ := dataset(t)
	a, e := Open(filepath.Join(t.TempDir(), "db"), DefaultConfig("p2-head"), true, false)
	must(t, e)
	defer a.Close()
	for _, b := range w.Blocks[:3] {
		_, e = a.ExecuteBlock(context.Background(), b)
		must(t, e)
	}
	r := a.s.live(context.Background())
	iters := 0
	original := r.iter
	r.iter = func(x *kvstore.Bounds) iterator { iters++; return original(x) }
	key := latestKey('F', []byte{0}, ds.AccountKey(addresses[0]))
	v, e := r.value(key, r.height)
	must(t, e)
	if v == nil || iters != 0 {
		t.Fatal("latest flat read sought history")
	}
	_, e = r.value(key, 1)
	must(t, e)
	if iters != 1 {
		t.Fatal("historical request bypassed history")
	}
	// Queries at the same historical height on a pinned snapshot must still seek.
	snap, e := a.s.db.GetSnapshot()
	must(t, e)
	defer snap.Release()
	historical := snapshotReader(context.Background(), snap, a.s.cfg.Format, 1)
	historicalValue, e := historical.value(key, 1)
	must(t, e)
	liveHistorical, e := r.value(key, 1)
	must(t, e)
	if !bytes.Equal(historicalValue, liveHistorical) {
		t.Fatal("historical reader mismatch")
	}
}

// P2 changes only unreachable initialization history in retained state. All
// later before-images (including absence) and current bytes must be identical.
func TestP2RetainedStateDiff(t *testing.T) {
	w, _, _ := dataset(t)
	dumps := []map[string][]byte{}
	for _, format := range []string{CompactFormat, FullHistoryFormat} {
		cfg := DefaultConfig("p2-diff")
		cfg.Format = format
		a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		must(t, e)
		for _, b := range w.Blocks {
			_, e = a.ExecuteBlock(context.Background(), b)
			must(t, e)
		}
		dump := map[string][]byte{}
		it := a.s.db.NewIterator(nil)
		for it.Next() {
			k := it.Key()
			if bytes.ContainsRune([]byte("PFDIC"), rune(k[0])) {
				dump[string(k)] = bytes.Clone(it.Value())
			}
		}
		must(t, it.Error())
		it.Release()
		must(t, a.Close())
		dumps = append(dumps, dump)
	}
	removed := 0
	for key, v := range dumps[0] {
		if got, ok := dumps[1][key]; ok {
			if !bytes.Equal(got, v) {
				t.Fatal("retained state value changed")
			}
			continue
		}
		// Decode the self-delimiting key to locate its exact ordered height suffix.
		k := []byte(key)
		if k[0] != 'D' || len(v) != 0 {
			t.Fatal("removed non-absence history")
		}
		ownerLen := 1
		if k[2] == 1 {
			ownerLen = 33
		}
		end := 2 + ownerLen
		if k[1] == 'P' {
			n := int(k[end])
			end += 1 + (n+1)/2
		} else if k[1] == 'F' {
			end += 32
		} else {
			t.Fatal("bad history key")
		}
		if !bytes.Equal(k[end:], orderedHeight(1)) {
			t.Fatal("removed post-initialization history")
		}
		removed++
	}
	if removed == 0 {
		t.Fatal("no initialization history removed")
	}
	for k := range dumps[1] {
		if _, ok := dumps[0][k]; !ok {
			t.Fatal("unexpected new state record")
		}
	}
}
