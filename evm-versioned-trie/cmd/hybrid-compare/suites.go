package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"time"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

type storageResult struct {
	EngineCountersSingleScope bool
	Method                    string
	Initial, Final            kvstore.Measurement
	Increment                 kvstore.InventoryDelta
	StateBytes, SSTBytes      int64
	Writes                    kvstore.WriteCounters
}
type blockSample struct {
	Height, GasUsed                                     uint64
	Transactions, ChangedAccounts, ChangedSlots         int
	TotalNS, PrepareNS, EVMNS, MaterializeNS, PublishNS int64
}
type querySample struct {
	HeightDistribution           string
	Index                        int
	Height                       uint64
	Kind                         q.Kind
	AccountExists, SlotExists    bool
	ProofNodes, ProofBytes       int
	ServiceNS, VerifyNS, TotalNS int64
}
type distribution struct {
	Count                      int
	MeanNS                     float64
	P50NS, P70NS, P90NS, P95NS int64
}

func summarize(values []int64) distribution {
	v := slices.Clone(values)
	slices.Sort(v)
	var sum float64
	for _, n := range v {
		sum += float64(n)
	}
	rank := func(p int) int64 { return v[(len(v)*p+99)/100-1] }
	return distribution{len(v), sum / float64(len(v)), rank(50), rank(70), rank(90), rank(95)}
}

type executionCacheSample struct {
	Policy                                           string
	AllocatedBytes, Entries, Hits, Misses, Evictions uint64
}

func (a adapter) executionCache() (executionCacheSample, error) {
	if h, ok := a.database.(interface {
		CacheStats() (ht.ForestCacheStats, error)
	}); ok {
		s, err := h.CacheStats()
		return executionCacheSample{s.Mode, s.AllocatedBytes(), s.Boundary.NodeEntries, s.NodeBodyHits, s.NodeBodyMisses, s.Evictions}, err
	}
	s, err := a.EngineStats()
	c := s.ReadCache
	return executionCacheSample{s.ReadCachePolicy, c.AllocatedBytes, c.Entries, c.Hits, c.Misses, c.Evictions}, err
}

type trialResult struct {
	CacheAfterWarmup, CacheAfterExecution                     *executionCacheSample `json:",omitempty"`
	QueryStatsBefore, QueryStatsAfter                         *kvstore.Statistics   `json:",omitempty"`
	QueryOpenNS                                               int64                 `json:",omitempty"`
	SamplesComplete                                           bool
	Suites                                                    []string
	Method, CaseID                                            string
	Trial                                                     int
	Blocks                                                    []blockSample
	Queries                                                   []querySample
	Execution, QueryService, QueryVerification, QueryEndToEnd distribution
	Verified, ReadOnlyUnchanged                               bool
}
type reportData struct {
	Schema, WorkloadID, CorpusID string
	Engine                       kvstore.Engine
	Storage                      []storageResult
	Trials                       []trialResult
}

func (x *experiment) storage(ctx context.Context, m string, art *artifact.Store) (s storageResult, err error) {
	s.Method = m
	path := filepath.Join(x.dir, "state-"+m)
	initialName := "initial-" + m + ".json"
	ok, e := x.readSealed(initialName, &s.Initial)
	if e != nil {
		return s, e
	}
	present, e := exists(path)
	if e != nil {
		return s, e
	}
	if ok && !present {
		return s, fmt.Errorf("missing checkpoint database %s", m)
	}
	// This hook is installed only on the dedicated storage-import handle.
	local := *x
	var prepare func(kvstore.WriteCounters) error
	observer := func(w kvstore.WriteCounters) error {
		if prepare == nil {
			return nil
		}
		return prepare(w)
	}
	local.plan.Archive.Storage.BeforeWrite = observer
	local.plan.Path.Storage.BeforeWrite = observer
	local.plan.Hybrid.Storage.BeforeWrite = observer
	if !present {
		if e = prepareDatabase(path, func(temp string) (io.Closer, error) { return local.open(m, temp, true, false, art) }); e != nil {
			return s, e
		}
	}
	a, e := local.open(m, path, false, false, art)
	if e != nil {
		return s, e
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	if !ok {
		if e = a.initialize(ctx); e != nil {
			return s, e
		}
		i, e := x.headIndex(a)
		if e != nil || i != 0 {
			return s, fmt.Errorf("missing initial inventory at head index %d: %v", i, e)
		}
		if s.Initial, e = a.MeasureStorage(ctx, true); e != nil {
			return s, e
		}
		if e = save(filepath.Join(x.dir, initialName), s.Initial); e != nil {
			return s, e
		}
		if e = x.sealFile(initialName); e != nil {
			return s, e
		}
	}
	head, e := x.headIndex(a)
	if e != nil {
		return s, e
	}
	journal := filepath.Join(x.dir, "writes-"+m+".json")
	var intent writeIntent
	s.Writes = kvstore.WriteCounters{Columns: map[string]kvstore.WriteCount{}, BatchBytesComplete: true}
	if e = readIntent(journal, &intent); e == nil {
		if intent.Run != x.runID || intent.Method != m || intent.Index < 1 || intent.Index >= len(x.truth) || intent.Before.Commits != uint64(intent.Index-1) || intent.After.Commits != uint64(intent.Index) {
			return s, fmt.Errorf("invalid write checkpoint for %s", m)
		}
		if t := x.truth[intent.Index]; t.Height != intent.Height || t.Root != intent.Root || x.w.Blocks[intent.Index].ID != intent.BlockID {
			return s, fmt.Errorf("write intent identity mismatch")
		}
		switch head {
		case intent.Index:
			s.Writes = intent.After
		case intent.Index - 1:
			s.Writes = intent.Before
		default:
			return s, fmt.Errorf("write checkpoint %d disagrees with database index %d", intent.Index, head)
		}
	} else if !errors.Is(e, os.ErrNotExist) || head != 0 {
		return s, fmt.Errorf("missing or unreadable write checkpoint at %d: %w", head, e)
	}
	prior := s.Writes
	start, e := a.EngineStats()
	if e != nil {
		return s, e
	}
	for i := head + 1; i < len(x.truth); i++ {
		if e = ctx.Err(); e != nil {
			return s, e
		}
		b, t := x.w.Blocks[i], x.truth[i]
		raw, e := art.Get("logical-block", t.LogicalObject)
		if e != nil {
			return s, e
		}
		trace, e := block.ParseLogicalBlock(raw, t.LogicalSHA256, b, codec.Ethereum)
		if e != nil {
			return s, e
		}
		prepared := false
		prepare = func(w kvstore.WriteCounters) error {
			if prepared || w.Commits != 1 {
				return fmt.Errorf("expected a single atomic block commit")
			}
			intent = writeIntent{Height: b.Height, Root: t.Root, BlockID: b.ID, Run: x.runID, Method: m, Index: i, Before: s.Writes, After: s.Writes.Plus(w)}
			if e := saveIntent(journal, intent); e != nil {
				return e
			}
			prepared = true
			return signalBoundary(ctx, "prepared", m, i)
		}
		r, e := a.apply(ctx, b, trace)
		if e != nil {
			return s, fmt.Errorf("import block %d: %w", b.Height, e)
		}
		if !prepared {
			return s, fmt.Errorf("block committed without write checkpoint")
		}
		prepare = nil
		s.Writes = intent.After
		if e = signalBoundary(ctx, "committed", m, i); e != nil {
			return s, e
		}
		if e = x.check(i, r); e != nil {
			return s, e
		}
	}
	if s.Final, e = a.MeasureStorage(ctx, true); e != nil {
		return s, e
	}
	if s.Increment, e = kvstore.Delta(s.Initial, s.Final); e != nil {
		return s, e
	}
	delta, e := kvstore.WriteDelta(start, s.Final.BeforeMaintenance)
	if e != nil {
		return s, e
	}
	// WriteDelta can include zero-count columns inherited from initialization.
	total := prior.Plus(delta)
	for col, v := range total.Columns {
		if v == (kvstore.WriteCount{}) {
			delete(total.Columns, col)
		}
	}
	if !reflect.DeepEqual(total, s.Writes) {
		return s, fmt.Errorf("write journal disagrees with observed engine writes")
	}
	s.EngineCountersSingleScope = s.Initial.AfterMaintenance.Scope.ID == s.Final.BeforeMaintenance.Scope.ID
	s.StateBytes = s.Increment.State.KeyBytes + s.Increment.State.ValueBytes
	s.SSTBytes = int64(s.Final.Files.SSTBytes) - int64(s.Initial.Files.SSTBytes)
	return s, save(filepath.Join(x.dir, "storage-"+m+".json"), s)
}
func (x *experiment) execution(ctx context.Context, m, path string, art *artifact.Store, trial *trialResult) (samples []blockSample, err error) {
	a, e := x.open(m, path, true, false, art)
	if e != nil {
		return nil, e
	}
	e = errors.Join(a.initialize(ctx), a.Close())
	if e != nil {
		return nil, e
	}
	a, e = x.open(m, path, false, false, art)
	if e != nil {
		return nil, e
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	for j, b := range x.w.Blocks[1:] {
		if j == x.cfg.WarmupBlocks {
			s, e := a.executionCache()
			if e != nil {
				return nil, e
			}
			if s.AllocatedBytes != x.plan.CommonApplicationTargetBytes {
				return nil, fmt.Errorf("%s execution cache has %d bytes, expected %d", m, s.AllocatedBytes, x.plan.CommonApplicationTargetBytes)
			}
			trial.CacheAfterWarmup = &s
		}
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		var p block.ExecutionTiming
		start := time.Now()
		r, e := a.execute(ctx, b, &p)
		end := time.Now()
		if e != nil {
			return nil, fmt.Errorf("execute block %d: %w", b.Height, e)
		}
		if e = x.check(j+1, r); e != nil {
			return nil, e
		}
		if e = signalBoundary(ctx, "execution", m, j+1); e != nil {
			return nil, e
		}
		if j < x.cfg.WarmupBlocks {
			continue
		}
		if p.Transactions.Before(start) || p.Materialize.Before(p.Transactions) || p.Publish.Before(p.Materialize) || end.Before(p.Publish) {
			return nil, fmt.Errorf("invalid phase boundaries at %d", b.Height)
		}
		t := x.truth[j+1]
		samples = append(samples, blockSample{
			Height:          b.Height,
			GasUsed:         t.GasUsed,
			Transactions:    len(b.Transactions),
			ChangedAccounts: t.ChangedAccounts,
			ChangedSlots:    t.ChangedSlots,
			TotalNS:         end.Sub(start).Nanoseconds(),
			PrepareNS:       p.Transactions.Sub(start).Nanoseconds(),
			EVMNS:           p.Materialize.Sub(p.Transactions).Nanoseconds(),
			MaterializeNS:   p.Publish.Sub(p.Materialize).Nanoseconds(),
			PublishNS:       end.Sub(p.Publish).Nanoseconds(),
		})
	}
	s, e := a.executionCache()
	if e != nil {
		return nil, e
	}
	trial.CacheAfterExecution = &s
	return samples, nil
}
func (x *experiment) queries(ctx context.Context, m, original string, g *trialResult) (samples []querySample, err error) {
	startOpen := time.Now()
	a, e := x.open(m, filepath.Join(x.dir, "state-"+m), false, true, nil)
	g.QueryOpenNS = time.Since(startOpen).Nanoseconds()
	if e != nil {
		return nil, e
	}
	defer func() { err = errors.Join(err, a.Close()) }()
	before, e := a.EngineStats()
	if e != nil {
		return nil, e
	}
	g.QueryStatsBefore = &before
	limits := q.DefaultLimits()
	for i, c := range x.corpus {
		if e = signalBoundary(ctx, "query", m, i+1); e != nil {
			return nil, e
		}
		req := c.Request
		root := x.truth[req.Height-x.snap.header.Height].Root
		start := time.Now()
		r, e := a.Query(ctx, req)
		served := time.Now()
		if e != nil {
			return nil, e
		}
		v, e := q.VerifyWithCodec(ctx, root, req, r, limits, codec.Ethereum)
		end := time.Now()
		if e != nil {
			return nil, e
		}
		d, e := digest(v)
		if e != nil {
			return nil, e
		}
		if d != c.AnswerSHA256 || r.ProofFormat != q.CompactProof {
			return nil, fmt.Errorf("query %d answer or proof format mismatch", i)
		}
		n := 0
		for _, group := range [][][]byte{r.AccountProof, r.StorageProof} {
			for _, node := range group {
				n += len(node)
			}
		}
		samples = append(samples, querySample{
			HeightDistribution: c.HeightDistribution,
			Index:              i,
			Height:             req.Height,
			Kind:               req.Kind,
			AccountExists:      v.Account != nil,
			SlotExists:         v.SlotExists,
			ProofNodes:         len(r.AccountProof) + len(r.StorageProof),
			ProofBytes:         n,
			ServiceNS:          served.Sub(start).Nanoseconds(),
			VerifyNS:           end.Sub(served).Nanoseconds(),
			TotalNS:            end.Sub(start).Nanoseconds(),
		})
	}
	after, e := a.EngineStats()
	if e != nil {
		return nil, e
	}
	g.QueryStatsAfter = &after
	final, e := a.MeasureStorage(ctx, false)
	if e != nil {
		return nil, e
	}
	if !final.Complete || final.Inventory.SHA256 != original {
		return nil, fmt.Errorf("%s inventory changed during read-only queries", m)
	}
	return samples, nil
}
func (x *experiment) measureTrial(ctx context.Context, m string, trial int, art *artifact.Store, inventory string) (g trialResult, err error) {
	g.Method, g.Trial = m, trial
	g.Suites = x.cfg.Suites
	g.CaseID, err = digest(struct {
		Schema, Workload, Corpus, Method string
		Trial, Warmup                    int
		Plan                             comparison.ResourcePlan
		Suites                           []string
	}{"hybrid-compare/v1", x.id, x.corpusID, m, trial, x.cfg.WarmupBlocks, x.plan, x.cfg.Suites})
	if err != nil {
		return g, err
	}
	name := fmt.Sprintf("trial-%02d-%s", trial, m)
	wantID := g.CaseID
	sealed, e := x.readSealed(name+".json", &g)
	if e != nil {
		return g, e
	}
	if sealed {
		if g.Method != m || g.Trial != trial || g.CaseID != wantID || !g.Verified || !g.SamplesComplete || x.cfg.has("execution") && len(g.Blocks) != len(x.truth)-1-x.cfg.WarmupBlocks || x.cfg.has("historical-query") && (!g.ReadOnlyUnchanged || len(g.Queries) != len(x.corpus)) {
			return g, fmt.Errorf("invalid sealed trial %s", name)
		}
		if x.cfg.has("execution") {
			return g, os.RemoveAll(filepath.Join(x.dir, name+"-db"))
		}
		return g, nil
	}
	db := filepath.Join(x.dir, name+"-db")
	if x.cfg.has("execution") {
		present, e := exists(db)
		if e != nil {
			return g, e
		}
		if present {
			dir, e := os.MkdirTemp(x.dir, name+"-interrupted-*")
			if e != nil {
				return g, e
			}
			if e = os.Rename(db, filepath.Join(dir, "db")); e != nil {
				return g, e
			}
		}
		if g.Blocks, err = x.execution(ctx, m, db, art, &g); err != nil {
			return g, err
		}
		var times []int64
		for _, s := range g.Blocks {
			times = append(times, s.TotalNS)
		}
		g.Execution = summarize(times)
	}
	if x.cfg.has("historical-query") {
		if g.Queries, err = x.queries(ctx, m, inventory, &g); err != nil {
			return g, err
		}
		var service, verify, total []int64
		for _, s := range g.Queries {
			service = append(service, s.ServiceNS)
			verify = append(verify, s.VerifyNS)
			total = append(total, s.TotalNS)
		}
		g.QueryService, g.QueryVerification, g.QueryEndToEnd = summarize(service), summarize(verify), summarize(total)
		g.ReadOnlyUnchanged = true
	}
	g.Verified, g.SamplesComplete = true, true
	if err = save(filepath.Join(x.dir, name+".json"), g); err != nil {
		return g, err
	}
	if err = x.sealFile(name + ".json"); err != nil {
		return g, err
	}
	if x.cfg.has("execution") {
		return g, os.RemoveAll(db)
	}
	return g, nil
}
