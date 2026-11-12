package storagebench

import (
	"context"
	"fmt"
	pa "github.com/XiaodongQi1/Hybrid-code/evm-path-archive"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type method interface {
	close() error
	apply(block.InputBlock, executionRecord) (mpt.Hash, error)
	root(uint64) (rootRow, error)
	query(q.Request) (q.Response, error)
}
type pathMethod struct{ archive *pa.Archive }

func pathConfig(c Config, id string) pa.Config {
	p := pa.DefaultConfig(id)
	p.Format = pa.PackedFormat
	p.ZeroGenesis = true
	p.MaxBatchBytes = c.MaxBatchBytes
	p.MaxStagingBytes = c.MaxBatchBytes
	l := pa.DefaultHistoryLayout().ForFormat(p.Format)
	p.HistoryLayout = &l
	return p
}
func openPath(path string, c Config, id string, create, ro bool) (*pathMethod, error) {
	a, e := pa.OpenWithOptions(path, pathConfig(c, id), create, ro, c.Storage)
	if e != nil {
		return nil, e
	}
	if ro {
		if e = a.PrepareQueryHeaders(context.Background()); e != nil {
			a.Close()
			return nil, e
		}
	}
	return &pathMethod{a}, nil
}
func (p *pathMethod) close() error { return p.archive.Close() }
func (p *pathMethod) apply(b block.InputBlock, r executionRecord) (mpt.Hash, error) {
	var got pa.Record
	var e error
	if b.Height == 0 {
		got, e = p.archive.ExecuteBlock(context.Background(), b)
	} else {
		got, e = p.archive.ApplyEvaluatedBlock(context.Background(), b, r.Evaluated)
	}
	if e == nil && got.Root != r.Root {
		e = fmt.Errorf("Path block root mismatch")
	}
	return got.Root, e
}
func (p *pathMethod) root(h uint64) (rootRow, error) {
	r, e := p.archive.Result(h)
	return rootRow{r.Height, r.ID, r.Root}, e
}
func (p *pathMethod) query(req q.Request) (q.Response, error) {
	return p.archive.Query(context.Background(), req)
}

type compactMethod struct {
	db     *backend.Backend
	view   *backend.View
	height uint64
}

func openCompact(path string, c Config, id string, create, ro bool) (*compactMethod, error) {
	b, e := backend.Open(path, c.versionConfig(id), create, ro)
	if e != nil {
		return nil, e
	}
	return &compactMethod{db: b}, nil
}
func (p *compactMethod) close() error {
	if p.view != nil {
		p.view.Close()
		p.view = nil
	}
	return p.db.Close()
}
func (p *compactMethod) apply(b block.InputBlock, r executionRecord) (mpt.Hash, error) {
	got, e := p.db.ApplyEvaluated(b, r.Root, r.Evaluated)
	if e != nil {
		return mpt.Hash{}, e
	}
	return got.Root, nil
}
func (p *compactMethod) at(h uint64) (*backend.View, error) {
	if p.view != nil && p.height == h {
		return p.view, nil
	}
	if p.view != nil {
		p.view.Close()
		p.view = nil
	}
	v, e := p.db.ViewAt(h)
	if e == nil {
		p.view = v
		p.height = h
	}
	return v, e
}
func (p *compactMethod) root(h uint64) (rootRow, error) {
	v, e := p.at(h)
	if e != nil {
		return rootRow{}, e
	}
	return rootRow{Height: h, Root: v.Root()}, nil
}
func (p *compactMethod) query(req q.Request) (q.Response, error) {
	v, e := p.at(req.Height)
	if e != nil {
		return q.Response{}, e
	}
	return v.ReadWithProof(context.Background(), req)
}

type archiveMethod struct {
	s      *hashStore
	view   *forest.View
	height uint64
}

func (p *archiveMethod) close() error { return p.s.db.Close() }
func (p *archiveMethod) apply(block.InputBlock, executionRecord) (mpt.Hash, error) {
	return mpt.Hash{}, fmt.Errorf("source executes EVM directly")
}
func (p *archiveMethod) root(h uint64) (rootRow, error) { return p.s.storedRoot(h) }
func (p *archiveMethod) query(req q.Request) (q.Response, error) {
	if p.view == nil || p.height != req.Height {
		r, e := p.root(req.Height)
		if e != nil {
			return q.Response{}, e
		}
		p.view, e = forest.Open(r.Root, p.s)
		if e != nil {
			return q.Response{}, e
		}
		p.height = req.Height
	}
	return forestQuery(p.view, req)
}
func openMethod(path, name string, c Config, id string, create, ro bool) (method, error) {
	switch name {
	case "PathV7":
		return openPath(path, c, id, create, ro)
	case "HashVersioned":
		return openCompact(path, c, id, create, ro)
	case "HashArchive":
		s, e := openHash(path, c, create, ro)
		if e != nil {
			return nil, e
		}
		return &archiveMethod{s: s}, nil
	}
	return nil, fmt.Errorf("unknown method")
}
