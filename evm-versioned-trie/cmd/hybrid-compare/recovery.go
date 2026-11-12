package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// sealed JSON keeps the public result format intact. The small sidecar is the
// completion record; interruption between the two publications leaves an
// unsealed result, which can be rebuilt.
type seal struct{ Run, Name, SHA256 string }

func (x *experiment) sealFile(name string) error {
	path := filepath.Join(x.dir, name)
	data, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return save(path+".seal", seal{x.runID, name, fmt.Sprintf("%x", sha256.Sum256(data))})
}
func (x *experiment) readSealed(name string, v any) (bool, error) {
	path := filepath.Join(x.dir, name)
	var s seal
	if e := strictFile(path+".seal", &s); errors.Is(e, os.ErrNotExist) {
		return false, nil
	} else if e != nil {
		return false, e
	}
	b, e := os.ReadFile(path)
	if e != nil {
		return false, e
	}
	if s != (seal{x.runID, name, fmt.Sprintf("%x", sha256.Sum256(b))}) {
		return false, fmt.Errorf("invalid seal: %s", name)
	}
	return true, json.Unmarshal(b, v)
}
func exists(path string) (bool, error) {
	_, e := os.Stat(path)
	if errors.Is(e, os.ErrNotExist) {
		return false, nil
	}
	return e == nil, e
}

// Publish a fully initialized, closed engine directory before importing any
// data. A crash during initial schema creation leaves only an attempt directory.
func prepareDatabase(path string, create func(string) (io.Closer, error)) error {
	present, e := exists(path)
	if e != nil || present {
		return e
	}
	parent := filepath.Dir(path)
	attempt, e := os.MkdirTemp(parent, ".create-"+filepath.Base(path)+"-*")
	if e != nil {
		return e
	}
	temp := filepath.Join(attempt, "db")
	db, e := create(temp)
	if e != nil {
		return e
	}
	if e = db.Close(); e != nil {
		return e
	}
	if e = os.Rename(temp, path); e != nil {
		return e
	}
	if e = os.Remove(attempt); e != nil {
		return e
	}
	return syncDirectory(parent)
}
func syncDirectory(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func atomicBytes(path string, b []byte) error {
	f, e := os.CreateTemp(filepath.Dir(path), ".publish-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	return syncDirectory(filepath.Dir(path))
}
func (x *experiment) prepare(configPath string, wire []byte, resume bool) (*os.File, error) {
	var queryID string
	if x.cfg.QueryFile != "" && x.cfg.has("historical-query") {
		var reqs []q.Request
		if e := strictFile(resolved(filepath.Dir(configPath), x.cfg.QueryFile), &reqs); e != nil {
			return nil, e
		}
		x.requestsFile = reqs
		var e error
		queryID, e = digest(reqs)
		if e != nil {
			return nil, e
		}
	}
	id, e := digest([]any{"hybrid-compare/resume-v1", x.id, queryID, x.cfg, x.plan, rt.DefaultChainConfig()})
	if e != nil {
		return nil, e
	}
	x.runID = id
	if !resume {
		if e = os.MkdirAll(filepath.Dir(x.dir), 0755); e != nil {
			return nil, e
		}
		if e = os.Mkdir(x.dir, 0700); e != nil {
			return nil, fmt.Errorf("create fresh output: %w", e)
		}
	}
	lock, e := os.Open(filepath.Clean(x.dir))
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, fmt.Errorf("run directory is busy: %w", e)
	}
	success := false
	defer func() {
		if !success {
			lock.Close()
		}
	}()
	manifest := filepath.Join(x.dir, "run-id.json")
	if resume {
		var old string
		if e = strictFile(manifest, &old); e != nil {
			return nil, fmt.Errorf("missing or invalid resume manifest: %w", e)
		}
		if old != id {
			return nil, fmt.Errorf("workload, query input, configuration or chain rules changed")
		}
	} else if e = save(manifest, id); e != nil {
		return nil, e
	}
	for _, f := range []struct {
		name string
		data []byte
	}{{"workload.json", wire}, {"config.json", nil}, {"resources.json", nil}} {
		b := f.data
		if b == nil {
			var v any = x.cfg
			if f.name == "resources.json" {
				v = x.plan
			}
			b, e = json.MarshalIndent(v, "", "  ")
			if e != nil {
				return nil, e
			}
			b = append(b, '\n')
		}
		path := filepath.Join(x.dir, f.name)
		if resume {
			old, err := os.ReadFile(path)
			if err == nil {
				if !bytes.Equal(old, b) {
					return nil, fmt.Errorf("changed %s", f.name)
				}
				continue
			}
			if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
		if e = atomicBytes(path, b); e != nil {
			return nil, e
		}
	}
	success = true
	return lock, nil
}

type frozenReference struct{ Blocks, Corpus string }

func (x *experiment) prepareReference(ctx context.Context, a *artifact.Store) error {
	var ref frozenReference
	ok, e := x.readSealed("reference-ready.json", &ref)
	if e != nil {
		return e
	}
	if ok {
		if e = strictFile(filepath.Join(x.dir, "blocks.json"), &x.truth); e != nil {
			return e
		}
		sum, e := digest(x.truth)
		if e != nil || sum != ref.Blocks || len(x.truth) != len(x.w.Blocks) || x.truth[0].Root != x.snap.root {
			return fmt.Errorf("invalid frozen block index")
		}
		if x.cfg.has("historical-query") {
			var corpus struct {
				WorkloadID string
				Cases      []queryCase
			}
			if e = strictFile(filepath.Join(x.dir, "corpus.json"), &corpus); e != nil {
				return e
			}
			sum, e = digest(corpus)
			if e != nil || sum != ref.Corpus || corpus.WorkloadID != x.id || len(corpus.Cases) == 0 {
				return fmt.Errorf("invalid frozen query corpus")
			}
			var requests []q.Request
			if e = strictFile(filepath.Join(x.dir, "queries.json"), &requests); e != nil {
				return e
			}
			if len(requests) != len(corpus.Cases) {
				return fmt.Errorf("query list and sealed corpus differ")
			}
			for i, c := range corpus.Cases {
				if requests[i] != c.Request {
					return fmt.Errorf("query list differs at index %d", i)
				}
			}
			x.corpus, x.corpusID = corpus.Cases, sum
		}
	} else {
		if e = x.reference(ctx, a); e != nil {
			return e
		}
		ref.Blocks, e = digest(x.truth)
		if e != nil {
			return e
		}
		ref.Corpus = x.corpusID
		if e = save(filepath.Join(x.dir, "reference-ready.json"), ref); e != nil {
			return e
		}
		if e = x.sealFile("reference-ready.json"); e != nil {
			return e
		}
	}
	return os.RemoveAll(filepath.Join(x.dir, "reference"))
}

// A single durable intent describes both possible outcomes of one atomic
// write. The database head decides which cumulative total to carry forward.
type writeIntent struct {
	Height        uint64
	Root          mpt.Hash
	BlockID       [32]byte
	Run, Method   string
	Index         int
	Before, After kvstore.WriteCounters
}

func (x *experiment) headIndex(a database) (int, error) {
	h, r, id, e := a.CommittedHead()
	if e != nil {
		return 0, e
	}
	first := x.w.Blocks[0].Height
	if h < first || h-first >= uint64(len(x.truth)) {
		return 0, fmt.Errorf("database head %d outside workload", h)
	}
	i := int(h - first)
	if x.truth[i].Height != h || x.truth[i].Root != r || x.w.Blocks[i].ID != id {
		return 0, fmt.Errorf("database head identity mismatch at %d", h)
	}
	return i, nil
}

func (x *experiment) storedResult(ctx context.Context, m string, art *artifact.Store) (storageResult, error) {
	name := "storage-" + m + ".json"
	var s storageResult
	ok, e := x.readSealed(name, &s)
	if e != nil {
		return s, e
	}
	if !ok {
		s, e = x.storage(ctx, m, art)
		if e != nil {
			return s, e
		}
		return s, x.sealFile(name)
	}
	if s.Method != m || !s.Final.Complete || !s.Initial.Complete || s.Writes.Commits != uint64(len(x.truth)-1) {
		return s, fmt.Errorf("invalid storage result for %s", m)
	}
	a, e := x.open(m, filepath.Join(x.dir, "state-"+m), false, true, nil)
	if e != nil {
		return s, e
	}
	i, e := x.headIndex(a)
	if e == nil && i != len(x.truth)-1 {
		e = fmt.Errorf("incomplete sealed database: %s", m)
	}
	if e == nil {
		var current kvstore.Measurement
		current, e = a.MeasureStorage(ctx, false)
		if e == nil && current.Inventory.SHA256 != s.Final.Inventory.SHA256 {
			e = fmt.Errorf("sealed inventory changed: %s", m)
		}
	}
	return s, errors.Join(e, a.Close())
}

type intentFile struct {
	Intent writeIntent
	Digest string
}

func saveIntent(path string, w writeIntent) error {
	sum, e := digest(w)
	if e != nil {
		return e
	}
	return save(path, intentFile{w, sum})
}
func readIntent(path string, out *writeIntent) error {
	var f intentFile
	if e := strictFile(path, &f); e != nil {
		return e
	}
	sum, e := digest(f.Intent)
	if e != nil || sum != f.Digest {
		return fmt.Errorf("corrupt write intent: %s", path)
	}
	*out = f.Intent
	return nil
}

// Allows the crash tests to terminate a subprocess at an exact commit boundary.
type boundaryKey struct{}

func signalBoundary(ctx context.Context, stage, method string, index int) error {
	if f, ok := ctx.Value(boundaryKey{}).(func(string, string, int) error); ok {
		return f(stage, method, index)
	}
	return nil
}
