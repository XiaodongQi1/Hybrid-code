package main

import (
	"context"
	"fmt"

	pa "github.com/XiaodongQi1/Hybrid-code/evm-path-archive"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type snapshot struct {
	header  block.InputBlock
	root    mpt.Hash
	builder *ht.SnapshotBuilder
	source  *ht.SnapshotSource
}
type database interface {
	CommittedHead() (uint64, mpt.Hash, [32]byte, error)
	EngineStats() (kvstore.Statistics, error)
	Close() error
	Query(context.Context, q.Request) (q.Response, error)
	MeasureStorage(context.Context, bool) (kvstore.Measurement, error)
}
type result struct {
	root     mpt.Hash
	outcomes []block.Outcome
}
type adapter struct {
	database
	initialize func(context.Context) error
	apply      func(context.Context, block.InputBlock, block.LogicalBlock) (result, error)
	execute    func(context.Context, block.InputBlock, *block.ExecutionTiming) (result, error)
}

func (x *experiment) archiveConfig() comparison.ArchiveConfig {
	c := x.plan.ArchiveConfig(x.id)
	c.InitialHeight = x.snap.header.Height
	return c
}
func (x *experiment) open(method, path string, create, ro bool, art *artifact.Store) (adapter, error) {
	s := x.snap
	switch method {
	case "archive":
		a, e := comparison.OpenArchive(path, x.archiveConfig(), create, ro, art)
		if e != nil {
			return adapter{}, e
		}
		return adapter{a, func(ctx context.Context) error {
			_, _, e := a.ImportSnapshot(ctx, s.header, s.source, &s.root)
			return e
		},
			func(ctx context.Context, b block.InputBlock, t block.LogicalBlock) (result, error) {
				r, e := a.ApplyLogicalBlock(ctx, b, t)
				return result{r.Root, r.OutcomesData}, e
			},
			func(ctx context.Context, b block.InputBlock, p *block.ExecutionTiming) (result, error) {
				r, e := a.ExecuteMeasured(ctx, b, p)
				return result{r.Root, r.OutcomesData}, e
			}}, nil
	case "path":
		c := x.plan.PathConfig(x.id)
		c.ZeroGenesis = s.header.Height == 0
		a, e := pa.OpenWithArtifacts(path, c, create, ro, art)
		if e != nil {
			return adapter{}, e
		}
		return adapter{a, func(ctx context.Context) error {
			_, _, e := a.ImportSnapshot(ctx, s.header, s.source, &s.root)
			return e
		},
			func(ctx context.Context, b block.InputBlock, t block.LogicalBlock) (result, error) {
				r, e := a.ApplyLogicalBlock(ctx, b, t)
				return result{r.Root, r.Outcomes}, e
			},
			func(ctx context.Context, b block.InputBlock, p *block.ExecutionTiming) (result, error) {
				r, e := a.ExecuteMeasured(ctx, b, p)
				return result{r.Root, r.Outcomes}, e
			}}, nil
	case "hybrid":
		c := x.plan.HybridConfig(x.id)
		c.ParallelProof = x.cfg.ParallelProof
		c.Initialization = backend.InitializationHashColdBulk
		if x.cfg.PrefixSnapshot {
			c.Initialization = backend.InitializationPrefix
			sc := backend.DefaultSnapshotConfig()
			c.Snapshot = &sc
		}
		c.Store.InitialHeight = s.header.Height
		a, e := backend.OpenHybrid(path, c, create, ro)
		if e != nil {
			return adapter{}, e
		}
		convert := func(r *backend.Result, e error) (result, error) {
			if e != nil {
				return result{}, e
			}
			return result{r.Root, r.Outcomes}, nil
		}
		return adapter{a, func(ctx context.Context) error {
			_, _, e := a.ImportPreparedSnapshot(ctx, s.header, s.builder, &s.root)
			return e
		},
			func(_ context.Context, b block.InputBlock, t block.LogicalBlock) (result, error) {
				return convert(a.ApplyLogicalBlock(b, t))
			},
			func(_ context.Context, b block.InputBlock, p *block.ExecutionTiming) (result, error) {
				return convert(a.ApplyMeasured(b, nil, p))
			}}, nil
	}
	return adapter{}, fmt.Errorf("unknown method %q", method)
}
