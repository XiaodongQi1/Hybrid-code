package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sort"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	snap "github.com/XiaodongQi1/Hybrid-code/evm-state/snapshot"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type truthBlock struct {
	Height                        uint64
	Root                          mpt.Hash
	LogicalObject                 common.Hash
	LogicalSHA256, OutcomesSHA256 string
	GasUsed                       uint64
	ChangedAccounts, ChangedSlots int
}
type queryCase struct {
	HeightDistribution string
	Request            q.Request
	AnswerSHA256       string
}
type slotKey struct {
	addr common.Address
	slot common.Hash
}

func (x *experiment) buildSnapshot(ctx context.Context) error {
	h := x.w.Blocks[0]
	rows := append([]block.Allocation(nil), h.Allocations...)
	h.Allocations = nil
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].Address[:], rows[j].Address[:]) < 0 })
	i := 0
	opt := x.plan.Archive.Storage
	opt.Columns = nil
	opt.ApplicationCacheBytes = 0
	opt.ExecutionCacheOnly = false
	config := snap.Config{
		SourceID: x.id, Header: h, BatchBytes: x.cfg.Resources.MaxBatchBytes,
		ChunkEntries: 256, CheckpointAccounts: 256, Storage: &opt,
	}
	path := filepath.Join(x.dir, "source")
	build, e := snap.BuildConfig(config)
	if e != nil {
		return e
	}
	if e = prepareDatabase(path, func(temp string) (io.Closer, error) { return ht.OpenSnapshotBuilder(temp, build) }); e != nil {
		return e
	}
	b, e := snap.Build(ctx, path, config, func() (block.Allocation, error) {
		if i == len(rows) {
			return block.Allocation{}, io.EOF
		}
		r := rows[i]
		i++
		return r, nil
	})
	if e != nil {
		return e
	}
	s, e := b.OpenSource()
	if e != nil {
		b.Close()
		return e
	}
	m, e := snap.Verify(ctx, s, h)
	if e == nil {
		e = save(filepath.Join(x.dir, "snapshot.json"), m)
	}
	if e != nil {
		s.Close()
		b.Close()
		return e
	}
	x.snap = snapshot{h, m.Root, b, s}
	return nil
}
func (x *experiment) reference(ctx context.Context, art *artifact.Store) (err error) {
	path := filepath.Join(x.dir, "reference")
	if e := prepareDatabase(path, func(temp string) (io.Closer, error) {
		return comparison.OpenArchive(temp, x.archiveConfig(), true, false, art)
	}); e != nil {
		return e
	}
	a, e := comparison.OpenArchive(path, x.archiveConfig(), false, false, art)
	if e != nil {
		return e
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	_, _, e = a.ImportSnapshot(ctx, x.snap.header, x.snap.source, &x.snap.root)
	if e != nil {
		return e
	}
	x.truth = append(x.truth, truthBlock{Height: x.snap.header.Height, Root: x.snap.root})
	var slots []slotKey
	seen := map[slotKey]bool{}
	addSlots := func(addr common.Address, values map[common.Hash]common.Hash) {
		for slot := range values {
			k := slotKey{addr, slot}
			if !seen[k] {
				seen[k] = true
				slots = append(slots, k)
			}
		}
	}
	for _, a := range x.w.Blocks[0].Allocations {
		addSlots(a.Address, a.Storage)
	}
	last, _, _, e := a.CommittedHead()
	if e != nil {
		return e
	}
	for _, b := range x.w.Blocks[1:] {
		var r comparison.ArchiveResult
		var t block.LogicalBlock
		if b.Height <= last {
			r, e = a.Result(b.Height)
			if e != nil {
				return e
			}
			raw, err := art.Get("logical-block", r.Logical)
			if err != nil {
				return err
			}
			if e = json.Unmarshal(raw, &t); e != nil {
				return e
			}
			if r.ID != b.ID || r.Root != t.Root {
				return fmt.Errorf("invalid reference height %d", b.Height)
			}
		} else {
			r, t, e = a.ExecuteWithTrace(ctx, b)
			if e == nil {
				e = signalBoundary(ctx, "reference", "archive", int(b.Height))
			}
		}
		if e != nil {
			return fmt.Errorf("block %d: %w", b.Height, e)
		}
		d, _, e := t.Freeze(b, codec.Ethereum)
		if e != nil {
			return e
		}
		od, e := digest(r.OutcomesData)
		if e != nil {
			return e
		}
		info := truthBlock{Height: b.Height, Root: r.Root, LogicalObject: r.Logical, LogicalSHA256: d, OutcomesSHA256: od, ChangedAccounts: len(t.Accounts)}
		for _, ch := range t.Accounts {
			info.ChangedSlots += len(ch.Slots)
			addSlots(ch.Address, ch.Slots)
		}
		for _, o := range t.Outcomes {
			info.GasUsed += o.Receipt.GasUsed
		}
		x.truth = append(x.truth, info)
	}
	if e = save(filepath.Join(x.dir, "blocks.json"), x.truth); e != nil {
		return e
	}
	if !x.cfg.has("historical-query") {
		return nil
	}
	var reqs []q.Request
	if x.cfg.QueryFile != "" {
		reqs = x.requestsFile
		if len(reqs) == 0 || len(reqs) > 1_000_000 {
			return fmt.Errorf("query file must contain 1 to 1000000 requests")
		}
	} else {
		sort.Slice(slots, func(i, j int) bool {
			if slots[i].addr != slots[j].addr {
				return bytes.Compare(slots[i].addr[:], slots[j].addr[:]) < 0
			}
			return bytes.Compare(slots[i].slot[:], slots[j].slot[:]) < 0
		})
		reqs = x.requests(slots)
	}
	for i, req := range reqs {
		if req.Height < x.snap.header.Height || req.Height > x.truth[len(x.truth)-1].Height {
			return fmt.Errorf("query %d outside frozen height range", i)
		}
		r, e := a.Query(ctx, req)
		if e != nil {
			return e
		}
		v, e := q.VerifyWithCodec(ctx, x.truth[req.Height-x.snap.header.Height].Root, req, r, q.DefaultLimits(), codec.Ethereum)
		if e != nil {
			return e
		}
		d, e := digest(v)
		if e != nil {
			return e
		}
		dist := "explicit"
		if x.cfg.QueryFile == "" {
			dist = "uniform"
			if i%4 >= 2 {
				dist = "recent-max-of-two"
			}
		}
		x.corpus = append(x.corpus, queryCase{dist, req, d})
	}
	corpus := struct {
		WorkloadID string
		Cases      []queryCase
	}{x.id, x.corpus}
	x.corpusID, e = digest(corpus)
	if e != nil {
		return e
	}
	if e = save(filepath.Join(x.dir, "queries.json"), reqs); e != nil {
		return e
	}
	return save(filepath.Join(x.dir, "corpus.json"), corpus)
}
func (x *experiment) requests(slots []slotKey) []q.Request {
	rng := rand.New(rand.NewSource(x.cfg.QuerySeed))
	alloc := x.w.Blocks[0].Allocations
	reqs := make([]q.Request, x.cfg.Queries)
	for i := range reqs {
		h := rng.Intn(len(x.truth))
		if i%4 >= 2 {
			h = max(h, rng.Intn(len(x.truth)))
		}
		r := q.Request{Height: x.truth[h].Height, Kind: q.Account}
		if len(alloc) > 0 {
			r.Address = alloc[rng.Intn(len(alloc))].Address
		}
		if i%2 == 1 {
			r.Kind = q.Storage
			if len(slots) > 0 {
				k := slots[rng.Intn(len(slots))]
				r.Address, r.Slot = k.addr, k.slot
			}
			if i%8 == 7 {
				rng.Read(r.Slot[:])
			}
		} else if i%8 == 6 {
			rng.Read(r.Address[:])
		}
		reqs[i] = r
	}
	return reqs
}
func (x *experiment) check(i int, r result) error {
	d, e := digest(r.outcomes)
	if e != nil {
		return e
	}
	t := x.truth[i]
	if r.root != t.Root || d != t.OutcomesSHA256 {
		return fmt.Errorf("root or transaction outcomes differ at height %d", t.Height)
	}
	return nil
}
