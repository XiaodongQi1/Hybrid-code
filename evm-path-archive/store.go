// Package patharchive implements a path-based archive algorithm on Statera MPT.
// It does not import the upstream go-ethereum module.
package patharchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	ds "github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/internal/kvcache"
	"github.com/XiaodongQi1/Hybrid-code/internal/logicalio"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
)

const LegacyFormat = "statera-path-archive/v1"
const CompactFormat = "statera-path-archive/v2"
const FullHistoryFormat = "statera-path-archive/v3"
const DifferentialFormat = "statera-path-archive/v4"
const Format = "statera-path-archive/v5"

func isDifferential(f string) bool {
	return IsIndexedFormat(f) || f == Format || f == DifferentialFormat
}

var ErrCorrupt = errors.New("path archive: corrupt data")
var ErrReadOnly = errors.New("path archive: read-only")
var ErrUnknown = errors.New("path archive: commit outcome unknown, reopen required")

type Store struct {
	nodeCache                   *forest.ExecutionNodeCache
	importPending               bool
	artifacts                   *artifact.Store
	recovery                    recoveryCounters
	logical                     logicalio.Stats
	mu                          sync.RWMutex
	db                          diskEngine
	cfg                         Config
	head                        Record
	readOnly, closed, uncertain bool
	write                       func(*diskBatch) error
}

// Engine iterators are wrapped below through a function-based reader.
type iterator interface {
	First() bool
	Seek([]byte) bool
	Next() bool
	Key() []byte
	Value() []byte
	Error() error
	Release()
}
type reader struct {
	servingProof            bool // Query uses local node bytes; the client checks their digests.
	targetID, targetHeight  uint64
	targetKnown             bool
	layout                  HistoryLayout
	get                     func([]byte) ([]byte, error)
	iter                    func(*kvstore.Bounds) iterator
	ctx                     context.Context
	format                  string
	mode                    codec.Mode
	owner                   []byte
	height                  uint64
	current                 bool
	limit, byteLimit        uint64
	metrics                 *recoveryCounters
	groupEntries, indexSpan uint64
}
type denied struct{}

func (denied) Put([]byte, []byte) error { return ErrReadOnly }
func (denied) Write() error             { return ErrReadOnly }
func (r reader) NewBatch() mpt.Batch    { return denied{} }
func (r reader) codec() codec.Mode {
	if r.mode == "" {
		return codec.Legacy
	}
	return r.mode
}
func (s *Store) live(ctx context.Context) reader {
	r := reader{get: func(k []byte) ([]byte, error) { return s.db.Get(k) }, iter: func(x *kvstore.Bounds) iterator { return s.db.NewIterator(x) }, ctx: ctx, format: s.cfg.Format, owner: []byte{0}, height: s.head.Height, current: true, limit: s.cfg.HistoryNodeCheckpointInterval, byteLimit: s.cfg.MaxHistoryDecodeBytes, metrics: &s.recovery, groupEntries: s.cfg.HistoryGroupEntries, indexSpan: s.cfg.HistoryIndexSpan}
	if s.cfg.HistoryLayout != nil {
		r.layout = *s.cfg.HistoryLayout
	}
	r.mode = s.cfg.codec()
	return r
}
func snapshotReader(ctx context.Context, s diskSnapshot, format string, height uint64) reader {
	return reader{get: func(k []byte) ([]byte, error) { return s.Get(k) }, iter: func(x *kvstore.Bounds) iterator { return s.NewIterator(x) }, ctx: ctx, format: format, owner: []byte{0}, height: height, limit: 8, byteLimit: 64 << 20, groupEntries: 16, indexSpan: 64, layout: DefaultHistoryLayout().ForFormat(format)}
}
func num(h uint64) []byte                 { b := make([]byte, 8); binary.BigEndian.PutUint64(b, h); return b }
func heightKey(tag byte, h uint64) []byte { return append([]byte{tag}, num(h)...) }
func historyPrefix(k []byte) []byte {
	p := make([]byte, 3)
	p[0] = 'D'
	binary.BigEndian.PutUint16(p[1:], uint16(len(k)))
	return append(p, k...)
}
func latestKey(tag byte, owner, path []byte) []byte {
	return append(append([]byte{tag}, owner...), path...)
}
func unpack(v []byte) ([]byte, error) {
	if len(v) == 1 && v[0] == 0 {
		return nil, nil
	}
	if len(v) > 1 && v[0] == 1 {
		return bytes.Clone(v[1:]), nil
	}
	return nil, ErrCorrupt
}
func (r reader) value(k []byte, h uint64) ([]byte, error) {
	if e := r.ctx.Err(); e != nil {
		return nil, e
	}
	if (isDifferential(r.format) || r.format == FullHistoryFormat) && r.current && h == r.height {
		return r.currentValue(k)
	}
	if IsIndexedFormat(r.format) {
		return r.valueIndexed(k, h)
	}
	if isDifferential(r.format) && k[0] == 'P' {
		return r.nodeHistory(k, h)
	}
	prefix := r.historyPrefix(k)
	it := r.historyIterator(k)
	defer it.Release()
	if h != ^uint64(0) && it.Seek(append(bytes.Clone(prefix), r.heightBytes(h+1)...)) {
		if !r.validHeight(it.Key()[len(prefix):]) {
			return nil, ErrCorrupt
		}
		if r.format != LegacyFormat {
			return decodeValueWithCodec(k, it.Value(), r.codec())
		}
		return unpack(it.Value())
	}
	if e := it.Error(); e != nil {
		return nil, e
	}
	return r.currentValue(k)
}
func (r reader) currentValue(k []byte) ([]byte, error) {
	v, e := r.get(k)
	if errors.Is(e, kvstore.ErrNotFound) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	if r.format != LegacyFormat {
		return decodeValueWithCodec(k, v, r.codec())
	}
	return v, nil
}

// A compatibility index maps a hash to one path/version that contained it.
// Blobs live ONLY in the current path table or reverse history, never under hashes.
func (r reader) Get(k []byte) ([]byte, error) {
	if r.format != LegacyFormat {
		return nil, errors.New("path archive: owner/path required")
	}
	if e := r.ctx.Err(); e != nil {
		return nil, e
	}
	if len(k) != 32 {
		return nil, ErrCorrupt
	}
	loc, e := r.get(append([]byte{'I'}, k...))
	if e != nil {
		return nil, e
	}
	if len(loc) < 10 {
		return nil, ErrCorrupt
	}
	key, h := loc[:len(loc)-8], binary.BigEndian.Uint64(loc[len(loc)-8:])
	if key[0] != 'P' {
		return nil, ErrCorrupt
	}
	v, e := r.value(key, h)
	if e != nil {
		return nil, e
	}
	if !r.servingProof && !bytes.Equal(crypto.Keccak256(v), k) {
		return nil, ErrCorrupt
	}
	return v, nil
}
func (r reader) Code(h common.Hash) ([]byte, error) {
	if h == ds.EmptyCode {
		return nil, nil
	}
	if e := r.ctx.Err(); e != nil {
		return nil, e
	}
	b, e := r.get(append([]byte{'C'}, h[:]...))
	if e == nil && crypto.Keccak256Hash(b) != h {
		return nil, ErrCorrupt
	}
	return b, e
}
func openStore(path string, c Config, create, ro bool) (*Store, error) {
	return openStoreWithOptions(path, c, create, ro, kvcache.LegacyOptions())
}
func openStoreWithOptions(path string, c Config, create, ro bool, tuning kvcache.Options) (*Store, error) {
	return openStoreExternal(path, c, create, ro, tuning, nil)
}
func openStoreExternal(path string, c Config, create, ro bool, tuning kvcache.Options, artifacts *artifact.Store) (*Store, error) {
	if e := tuning.Validate(); e != nil {
		return nil, e
	}
	if create && ro {
		return nil, ErrReadOnly
	}
	if create && c.Storage == nil {
		if e := os.Mkdir(path, 0700); e != nil {
			return nil, e
		}
	}
	if c.ExecutionNodeCacheBytes != 0 && (c.Storage == nil || c.Storage.ApplicationCacheBytes != 0) {
		return nil, kvstore.ErrInvalid
	}
	nodeCache, err := forest.NewExecutionNodeCache(c.ExecutionNodeCacheBytes, ro)
	if err != nil {
		return nil, err
	}
	var db diskEngine
	var e error
	if c.Storage == nil {
		db, e = openLegacyEngine(path, create, ro, tuning)
	} else {
		if tuning != kvcache.LegacyOptions() {
			return nil, errors.New("configure shared engine buffers in Config.Storage; legacy tuning cannot be combined")
		}
		db, e = openSharedEngine(path, *c.Storage, create, ro)
	}
	if e != nil {
		return nil, e
	}
	s := &Store{nodeCache: nodeCache, db: db, cfg: c, readOnly: ro, head: Record{Root: mpt.EmptyRootHash}}
	s.artifacts = artifacts
	s.logical = logicalio.Stats{}
	s.write = func(b *diskBatch) error {
		e := db.Write(b)
		if e == nil {
			s.logical.AddKVBatch(b.raw)
		}
		return e
	}
	fail := func(e error) (*Store, error) { db.Close(); return nil, e }
	wire, _ := json.Marshal(c)
	if create {
		b := newDiskBatch()
		b.Put([]byte("M/config"), wire)
		h, _ := json.Marshal(s.head)
		if c.StateOnly {
			h, _ = json.Marshal(stateHeader{Root: s.head.Root})
		}
		b.Put([]byte("M/head"), h)
		if e = s.write(b); e != nil {
			return fail(e)
		}
	} else {
		old, e := db.Get([]byte("M/config"))
		if e != nil || !bytes.Equal(wire, old) {
			return fail(errors.New("path archive config mismatch"))
		}
		old, e = db.Get([]byte("M/head"))
		if e != nil {
			return fail(e)
		}
		if e = json.Unmarshal(old, &s.head); e != nil {
			return fail(e)
		}
		if s.head.ID != (common.Hash{}) {
			r, e := s.stateRecord(s.head.Height)
			if e != nil || !equalJSON(r, s.head) {
				return fail(ErrCorrupt)
			}
			if s.head.Root != mpt.EmptyRootHash {
				if _, e = mpt.Open(s.head.Root, s.live(context.Background())); e != nil {
					return fail(e)
				}
			}
		}
	}
	if shared, ok := s.db.(*sharedEngine); ok {
		m, e := shared.db.BulkStatus([]byte("M/initial-import"))
		if e == nil {
			if m.Complete != (s.head.ID != (common.Hash{})) {
				return fail(ErrCorrupt)
			}
			s.importPending = !m.Complete
		} else if !errors.Is(e, kvstore.ErrNotFound) {
			return fail(e)
		}
	}
	return s, nil
}
func equalJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}
func (s *Store) check() error {
	if s.closed {
		return errors.New("path archive closed")
	}
	if s.uncertain {
		return ErrUnknown
	}
	if s.importPending {
		return kvstore.ErrImportPending
	}
	return nil
}
func (s *Store) stateRecord(h uint64) (Record, error) {
	var r Record
	b, e := s.db.Get(heightKey('B', h))
	if e != nil {
		return r, e
	}
	e = json.Unmarshal(b, &r)
	if e == nil && r.Height != h {
		e = ErrCorrupt
	}
	if e == nil && s.cfg.StateOnly && (r.Artifacts == nil || r.ID == (common.Hash{}) || r.Artifacts.Input == (common.Hash{}) || r.Artifacts.Outcomes == (common.Hash{}) || r.Artifacts.Changes == (common.Hash{})) {
		e = ErrCorrupt
	}
	return r, e
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if e := s.db.Close(); e != nil {
		return e
	}
	s.nodeCache = nil
	s.closed = true
	return nil
}

type staged struct {
	reader
	p *forest.Plan
}

func (v staged) Get(k []byte) ([]byte, error) {
	var h mpt.Hash
	if len(k) != 32 {
		return nil, ErrCorrupt
	}
	copy(h[:], k)
	if b, ok := v.p.Nodes[h]; ok {
		return bytes.Clone(b), nil
	}
	return v.reader.Get(k)
}

type Changes struct {
	Nodes, Values, ReverseBytes, IndexBytes uint64
	History                                 *HistoryEncoding `json:",omitempty"`
}

func (s *Store) commit(ctx context.Context, p *forest.Plan, r Record) (Record, error) {
	// Caller holds archive/store mutex. All old reads precede the atomic batch.
	old := s.live(ctx)
	next := staged{old, p}
	writes := map[string][]byte{}
	priors := map[string][]byte{}
	locations := map[mpt.Hash][]byte{}
	var stats Changes
	visits := uint64(0)
	check := func() error {
		visits++
		if visits > s.cfg.MaxWalkNodes {
			return errors.New("path walk budget exceeded")
		}
		return ctx.Err()
	}
	diff := func(owner []byte, before, after mpt.Hash) (map[string][]byte, map[string][]byte, error) {
		known := map[string]mpt.Hash{}
		oldLeaves, newLeaves := map[string][]byte{}, map[string][]byte{}
		scopedOld := old
		scopedOld.owner = bytes.Clone(owner)
		scopedNext := next
		scopedNext.reader = scopedOld
		e := mpt.WalkPaths(after, scopedNext, func(path []byte, h mpt.Hash, blob []byte) (bool, error) {
			if e := check(); e != nil {
				return false, e
			}
			k := old.key('P', owner, path)
			known[string(path)] = h
			prior, e := old.get(k)
			if e != nil && !errors.Is(e, kvstore.ErrNotFound) {
				return false, e
			}
			if bytes.Equal(prior, blob) {
				return false, nil
			}
			if isDifferential(s.cfg.Format) || s.cfg.Format == FullHistoryFormat {
				priors[string(k)] = bytes.Clone(prior)
			}
			writes[string(k)] = blob
			if _, ok := locations[h]; !ok && s.cfg.Format == LegacyFormat {
				locations[h] = k
			}
			return true, nil
		}, func(k, v []byte) error { newLeaves[string(k)] = v; return nil })
		if e != nil {
			return nil, nil, e
		}
		e = mpt.WalkPaths(before, scopedOld, func(path []byte, h mpt.Hash, blob []byte) (bool, error) {
			if e := check(); e != nil {
				return false, e
			}
			if known[string(path)] == h {
				return false, nil
			}
			if _, ok := known[string(path)]; !ok {
				writes[string(old.key('P', owner, path))] = nil
			}
			return true, nil
		}, func(k, v []byte) error { oldLeaves[string(k)] = v; return nil })
		if e != nil {
			return nil, nil, e
		}
		for k := range oldLeaves {
			writes[string(latestKey('F', owner, []byte(k)))] = nil
		}
		for k, v := range newLeaves {
			writes[string(latestKey('F', owner, []byte(k)))] = v
		}
		return oldLeaves, newLeaves, nil
	}
	prevAccounts, newAccounts, e := diff([]byte{0}, s.head.Root, p.Root)
	if e != nil {
		return r, e
	}
	keys := map[string]bool{}
	for k := range prevAccounts {
		keys[k] = true
	}
	for k := range newAccounts {
		keys[k] = true
	}
	for key := range keys {
		before, after := mpt.EmptyRootHash, mpt.EmptyRootHash
		if b := prevAccounts[key]; b != nil {
			a, e := s.cfg.codec().DecodeAccount(b)
			if e != nil {
				return r, e
			}
			before = a.StorageRoot
		}
		if b := newAccounts[key]; b != nil {
			a, e := s.cfg.codec().DecodeAccount(b)
			if e != nil {
				return r, e
			}
			after = a.StorageRoot
		}
		if before != after {
			if _, _, e = diff(append([]byte{1}, []byte(key)...), before, after); e != nil {
				return r, e
			}
		}
	}
	pendingHistory := map[string][]byte{}
	batch := newDiskBatch()
	used := uint64(0)
	put := func(k, v []byte) error {
		n := uint64(len(k) + len(v) + 16)
		if n > s.cfg.MaxBatchBytes-used {
			return errors.New("path batch budget exceeded")
		}
		used += n
		batch.Put(k, v)
		return nil
	}
	for key, value := range writes {
		if s.cfg.Format != LegacyFormat {
			var err error
			value, err = encodeValueWithCodec([]byte(key), value, s.cfg.codec())
			if err != nil {
				return r, err
			}
		}
		prior, cached := priors[key]
		var e error
		if !cached {
			prior, e = old.get([]byte(key))
		}
		if errors.Is(e, kvstore.ErrNotFound) {
			prior = nil
		} else if e != nil {
			return r, e
		}
		if bytes.Equal(prior, value) {
			continue
		}
		undo := []byte{0}
		if prior != nil {
			undo = append([]byte{1}, prior...)
		}
		if s.cfg.Format != LegacyFormat {
			undo = bytes.Clone(prior)
		}
		// No API query can precede the initialization block. Only this
		// first commit may omit absence; later creations need their boundary.
		if !((isDifferential(s.cfg.Format) || s.cfg.Format == FullHistoryFormat) && s.head.ID == (common.Hash{}) && r.Height == s.cfg.initial() && prior == nil) {
			if isDifferential(s.cfg.Format) && !IsIndexedFormat(s.cfg.Format) && key[0] == 'P' {
				var err error
				undo, err = s.historyWrite(put, func(k []byte) error {
					n := uint64(len(k) + 16)
					if n > s.cfg.MaxBatchBytes-used {
						return errors.New("path batch budget exceeded")
					}
					used += n
					batch.Delete(k)
					return nil
				}, []byte(key), prior, value, &stats)
				if err != nil {
					return r, err
				}
			}
			if s.cfg.Format == Format || IsIndexedFormat(s.cfg.Format) {
				pendingHistory[key] = bytes.Clone(undo)
			} else {
				hk := append(old.historyPrefix([]byte(key)), old.heightBytes(r.Height)...)
				if e = put(hk, undo); e != nil {
					return r, e
				}
				stats.ReverseBytes += uint64(len(hk) + len(undo))
				if isDifferential(s.cfg.Format) && key[0] == 'P' {
					stats.History.KeyBytes += uint64(len(hk))
				}
			}
		}
		if isDifferential(s.cfg.Format) && !IsIndexedFormat(s.cfg.Format) && key[0] == 'P' && s.head.ID == (common.Hash{}) && r.Height == s.cfg.initial() && prior == nil {
			if e = put(counterKey([]byte(key)), []byte{0}); e != nil {
				return r, e
			}
		}
		if key[0] == 'P' {
			stats.Nodes++
		} else {
			stats.Values++
		}
		if value == nil {
			used += uint64(len(key) + 16)
			if used > s.cfg.MaxBatchBytes {
				return r, errors.New("path batch budget exceeded")
			}
			batch.Delete([]byte(key))
		} else if e = put([]byte(key), value); e != nil {
			return r, e
		}
	}
	if IsIndexedFormat(s.cfg.Format) {
		if e = s.indexedRecordID(s.head.ID == (common.Hash{}), pendingHistory, &r); e != nil {
			return r, e
		}
		for key, prior := range pendingHistory {
			ordinal, err := old.appendVersion([]byte(key), r.StateID, put)
			if err != nil {
				return r, err
			}
			if key[0] == 'P' {
				prior, err = indexedEncoding(prior, writes[key], ordinal, s.cfg.HistoryNodeCheckpointInterval, &stats)
				if err != nil {
					return r, err
				}
				pendingHistory[key] = prior
			}
		}
		if len(pendingHistory) > 0 {
			if e = s.writeSections(pendingHistory, r.StateID, put, &stats); e != nil {
				return r, e
			}
		}
	}
	if s.cfg.Format == Format {
		if e = s.writeGroups(ctx, pendingHistory, r.Height, put, &stats); e != nil {
			return r, e
		}
	}
	for h, key := range locations {
		ik := append([]byte{'I'}, h[:]...)
		_, e := old.get(ik)
		if e == nil {
			continue
		}
		if !errors.Is(e, kvstore.ErrNotFound) {
			return r, e
		}
		loc := append(bytes.Clone(key), num(r.Height)...)
		if e = put(ik, loc); e != nil {
			return r, e
		}
		stats.IndexBytes += uint64(len(ik) + len(loc))
	}
	for h, code := range p.Codes {
		if crypto.Keccak256Hash(code) != h {
			return r, ErrCorrupt
		}
		k := append([]byte{'C'}, h[:]...)
		prior, e := old.get(k)
		if e == nil {
			if !bytes.Equal(prior, code) {
				return r, ErrCorrupt
			}
			continue
		}
		if !errors.Is(e, kvstore.ErrNotFound) {
			return r, e
		}
		if e = put(k, code); e != nil {
			return r, e
		}
	}
	r.Changes = stats
	var wire []byte
	if s.cfg.StateOnly {
		var e error
		r, wire, e = s.externalRecord(r)
		if e != nil {
			return r, e
		}
	} else {
		wire, _ = json.Marshal(r)
	}
	if e = put(heightKey('B', r.Height), wire); e != nil {
		return r, e
	}
	if e = put([]byte("M/head"), wire); e != nil {
		return r, e
	}
	if !s.cfg.StateOnly {
		if e = put(heightKey('L', r.Height), r.Input); e != nil {
			return r, e
		}
	}
	if e = ctx.Err(); e != nil {
		return r, e
	}
	if e = s.write(batch); e != nil {
		s.uncertain = true
		return r, fmt.Errorf("%w: %v", ErrUnknown, e)
	}
	s.head = r
	return r, nil
}

// LogicalWrites returns counters since this handle was opened.
func (s *Store) LogicalWrites() logicalio.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.logical.Clone()
}
