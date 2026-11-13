package hybridtrie

import (
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
	"time"
	"unsafe"
)

// ExecutionReadConfig controls a block-local read table, independently of the
// retained node cache. Workers=0 uses the same lazy representation synchronously.
// BodyBytes bounds retained raw/parsed results; working trie objects and native
// DB buffers are not this cache. Queue and table backing are separately billed.
type ExecutionReadConfig struct {
	Enabled                 bool
	Workers, Queue, Entries uint32
	BodyBytes               uint64
}

func DefaultExecutionReadConfig() ExecutionReadConfig {
	return ExecutionReadConfig{true, 1, 256, 8192, 8 << 20}
}
func (c ExecutionReadConfig) valid() bool {
	if !c.Enabled {
		return true
	}
	return c.Workers <= 8 && c.Queue > 0 && c.Queue <= 65536 && c.Entries >= 16 && c.Entries <= 1<<20 && c.BodyBytes >= 4096 && c.BodyBytes <= 128<<20
}

type ExecutionReadStats struct {
	MaxReadBufferBytes, PeakDecodeScratchBytes                                                       uint64
	HeaderOnlyReads, LogicalRecordBytes                                                              uint64
	RecordReads, PrefixSeeks, PrefixExactGets, HashGets, ReadBytes                                   uint64
	BackgroundReads, ForegroundReads, Prefetches, QueueSkips, TableFallbacks                         uint64
	ForegroundClaims, Waits, WaitNanos, IONanos, DrainNanos                                          uint64
	HashHeaderHits, RawReuseHits, ParsedReuseHits, NodeDecodeCount, NodeDecodeBytes, NodeDecodeNanos uint64
	BodyUpgradeReads, BodyDrops, ReadErrors                                                          uint64
	PeakRawBytes, PeakNodeBytes, PeakBodyBytes, PeakEntries, MetadataBytes, QueueBytes               uint64
}

// readKey contains the original physical position, never a COW destination.
type executionReadKey struct {
	ID     uint64
	Path   [32]byte
	Length uint8
}

func executionKey(id uint64, path []byte) executionReadKey {
	k := executionReadKey{ID: id, Length: uint8(len(path))}
	for i, n := range path {
		k.Path[i/2] |= n << uint(4*(1-i%2))
	}
	return k
}
func (k executionReadKey) path() []byte {
	p := make([]byte, int(k.Length))
	for i := range p {
		p[i] = (k.Path[i/2] >> uint(4*(1-i%2))) & 15
	}
	return p
}

// Execution entries already own the packed path. Do not unpack, repack and
// decode it again just to locate the same immutable physical record.
func (k executionReadKey) appendNodeKey(dst []byte, height uint64) ([]byte, error) {
	if k.ID > MaxTrieID || k.Length > MaxDepth || height > MaxHeight {
		return nil, ErrLimit
	}
	n := (int(k.Length) + 1) / 2
	if k.Length%2 != 0 && k.Path[n-1]&15 != 0 {
		return nil, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, nodePositionHeaderBytes+n+HeightBytes)...)
	out := dst[start:]
	put48(out[:TrieIDBytes], k.ID)
	out[TrieIDBytes] = k.Length
	copy(out[nodePositionHeaderBytes:], k.Path[:n])
	put48(out[len(out)-HeightBytes:], height)
	return dst, nil
}
func (k executionReadKey) bucket(mask uint64) int {
	h := uint64(1469598103934665603) ^ k.ID
	for _, v := range k.Path {
		h = (h ^ uint64(v)) * 1099511628211
	}
	h = (h ^ uint64(k.Length)) * 1099511628211
	return int(h & mask)
}

const (
	readIdle uint8 = iota
	readQueued
	readRunning
	readReady
)

type executionReadEntry struct {
	hasSummary bool
	bodyErr    error
	indexSlot  uint32
	key        executionReadKey
	loc        mpt.NodeLocation
	hash       mpt.Hash
	known      bool
	tracked    bool
	rawCharge  uint64
	state      uint8
	raw        []byte
	node       mpt.MaterializedNode
	nodeBytes  uint64
	parsed     bool
	err        error
}
type executionReadBacking struct {
	entries []executionReadEntry
	index   []uint32
}

type executionReads struct {
	foregroundAdmission     *admissionReadBudget // execution-thread scoped, never read by workers
	drainOnce               sync.Once
	mu                      sync.Mutex
	cond                    *sync.Cond
	cfg                     ExecutionReadConfig
	snapshot                diskSnapshot
	height                  uint64
	entries                 []executionReadEntry
	index                   []uint32
	count                   int
	queue                   chan *executionReadEntry
	workers                 sync.WaitGroup
	started, sealed, closed bool
	main                    diskPredecessor
	rawBytes, nodeBytes     uint64
	stats                   ExecutionReadStats
}

func newExecutionReads(snapshot diskSnapshot, height uint64, cfg ExecutionReadConfig, reuse ...*executionReadBacking) *executionReads {
	s := &executionReads{cfg: cfg, snapshot: snapshot, height: height}
	if len(reuse) > 0 && reuse[0] != nil {
		s.entries = reuse[0].entries
		s.index = reuse[0].index
	}
	s.cond = sync.NewCond(&s.mu)
	s.stats.MetadataBytes = uint64(unsafe.Sizeof(*s)) + uint64(len(s.entries))*uint64(unsafe.Sizeof(executionReadEntry{})) + uint64(len(s.index))*4
	return s
}

// Caller holds mu. Registered entries are stable until all readers have joined.
func (s *executionReads) entry(key executionReadKey, loc mpt.NodeLocation, hash mpt.Hash, known, optional bool) *executionReadEntry {
	if s.entries == nil {
		size := 1
		for size < int(s.cfg.Entries)*2 {
			size *= 2
		}
		s.entries = make([]executionReadEntry, s.cfg.Entries)
		s.index = make([]uint32, size)
		s.stats.MetadataBytes = uint64(unsafe.Sizeof(*s)) + uint64(len(s.entries))*uint64(unsafe.Sizeof(executionReadEntry{})) + uint64(size)*4
	}
	mask := uint64(len(s.index) - 1)
	at := key.bucket(mask)
	for s.index[at] != 0 {
		e := &s.entries[s.index[at]-1]
		if e.key == key {
			return e
		}
		at = (at + 1) & int(mask)
	}
	if s.count == len(s.entries) {
		if optional {
			s.stats.QueueSkips++
			return nil
		}
		s.stats.TableFallbacks++
		// Foreground is serialized with trie mutation. No optional task can be
		// registered for this key once the fixed table is full.
		return &executionReadEntry{key: key, loc: loc, hash: hash, known: known}
	}
	e := &s.entries[s.count]
	*e = executionReadEntry{key: key, loc: loc, hash: hash, known: known, tracked: true, indexSlot: uint32(at)}
	s.count++
	s.index[at] = uint32(s.count)
	s.stats.PeakEntries = uint64(s.count)
	return e
}
func (s *executionReads) prefetch(k executionReadKey, loc mpt.NodeLocation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.sealed || !s.cfg.Enabled || s.cfg.Workers == 0 {
		return
	}
	e := s.entry(k, loc, mpt.Hash{}, false, true)
	if e == nil || e.state != readIdle {
		return
	}
	if s.queue == nil {
		s.queue = make(chan *executionReadEntry, s.cfg.Queue)
		s.stats.QueueBytes = uint64(s.cfg.Queue) * uint64(unsafe.Sizeof((*executionReadEntry)(nil)))
	}
	e.state = readQueued
	select {
	case s.queue <- e:
		s.stats.Prefetches++
		if !s.started {
			s.started = true
			for i := uint32(0); i < s.cfg.Workers; i++ {
				s.workers.Add(1)
				go s.worker()
			}
		}
	default:
		e.state = readIdle
		s.stats.QueueSkips++
	}
}
func (s *executionReads) worker() {
	defer s.workers.Done()
	var cursor diskPredecessor
	defer func() {
		if cursor != nil {
			cursor.close()
		}
	}()
	for e := range s.queue {
		s.mu.Lock()
		if e.state != readQueued {
			s.mu.Unlock()
			continue
		}
		e.state = readRunning
		s.mu.Unlock()
		s.read(e, &cursor, true)
	}
}

// No cache lock or shared cursor is held during I/O. Worker only reads bytes.
func (s *executionReads) read(e *executionReadEntry, cursor *diskPredecessor, background bool) {
	started := time.Now()
	var raw []byte
	var err error
	var seek, exact, hashed uint64
	var headerOnly bool
	var logicalSize int
	loc := e.loc
	h := e.hash
	if ReferenceKind(loc.Kind) == Hash {
		hashed = 1
		raw, err = s.snapshot.get(hashColumn, h[:])
	} else {
		height := s.height
		if loc.Exact {
			height = loc.Height
		}
		var key []byte
		key, err = e.key.appendNodeKey(nil, height)
		if err == nil && loc.Exact {
			exact = 1
			raw, err = s.snapshot.get(prefixColumn, key)
		} else if err == nil {
			seek = 1
			if *cursor == nil {
				*cursor, _, err = openPredecessor(s.snapshot, prefixColumn)
			}
			if err == nil {
				var found []byte
				if background {
					s.mu.Lock()
					free := s.cfg.BodyBytes - s.rawBytes - s.nodeBytes
					s.mu.Unlock()
					if free < recordHeaderBytes {
						free = recordHeaderBytes
					}
					if free > MaxRecordBytes {
						free = MaxRecordBytes
					}
					found, raw, logicalSize, err = readLimited(*cursor, key, int(free))
					headerOnly = logicalSize > len(raw)
				} else {
					found, raw, err = (*cursor).prev(key)
				}
				if err == nil {
					err = matchEncodedNodeKey(found, key)
					if err == nil {
						loc.Height = read48(found[len(found)-HeightBytes:])
						loc.Exact = true
					}
				}
			}
		}
		if err == nil && !headerOnly && isDelta(raw) {
			// Cached bodies and parsers see complete records only.
			raw, err = normalizePrefix(s.snapshot, NodeKey{e.key.ID, e.key.path(), loc.Height}, raw, nil)
		}
		if err == nil {
			if len(raw) < recordHeaderBytes || len(raw) > MaxRecordBytes || logicalSize > MaxRecordBytes {
				err = ErrCorrupt
			} else {
				_, err = recordKind(raw[0])
				copy(h[:], raw[1:recordHeaderBytes])
				if err == nil && e.known && h != e.hash {
					err = ErrHashMismatch
				}
			}
		}
	}
	if logicalSize == 0 {
		logicalSize = len(raw)
	}
	elapsed := uint64(time.Since(started))
	s.mu.Lock()
	defer s.mu.Unlock()
	if uint64(cap(raw)) > s.stats.MaxReadBufferBytes {
		s.stats.MaxReadBufferBytes = uint64(cap(raw))
	}
	s.stats.RecordReads++
	s.stats.LogicalRecordBytes += uint64(logicalSize)
	if headerOnly {
		s.stats.HeaderOnlyReads++
	}
	s.stats.PrefixSeeks += seek
	s.stats.PrefixExactGets += exact
	s.stats.HashGets += hashed
	s.stats.ReadBytes += uint64(len(raw))
	s.stats.IONanos += elapsed
	if background {
		s.stats.BackgroundReads++
	} else {
		s.stats.ForegroundReads++
	}
	if err != nil && e.hasSummary {
		e.bodyErr = err
	} else {
		e.loc, e.hash, e.known, e.err = loc, h, err == nil, err
		e.hasSummary = err == nil
	}
	if err != nil {
		s.stats.ReadErrors++
	}
	// Retain bytes only within the table budget. A foreground caller may still
	// consume its single temporary body; materialization releases that result.
	if err == nil && !headerOnly && e.tracked && uint64(cap(raw))+s.rawBytes+s.nodeBytes <= s.cfg.BodyBytes {
		e.raw = raw
		e.rawCharge = uint64(cap(raw))
		s.rawBytes += e.rawCharge
	} else if err == nil && !background {
		e.raw = raw
	} else if err == nil {
		s.stats.BodyDrops++
	}
	e.state = readReady
	s.peaks()
	s.cond.Broadcast()
}
func (s *executionReads) peaks() {
	if s.rawBytes > s.stats.PeakRawBytes {
		s.stats.PeakRawBytes = s.rawBytes
	}
	if s.nodeBytes > s.stats.PeakNodeBytes {
		s.stats.PeakNodeBytes = s.nodeBytes
	}
	if s.rawBytes+s.nodeBytes > s.stats.PeakBodyBytes {
		s.stats.PeakBodyBytes = s.rawBytes + s.nodeBytes
	}
}

// Foreground only; at most one main-thread body request/decoder is active.
func (s *executionReads) need(k executionReadKey, loc mpt.NodeLocation, h mpt.Hash, known, body bool, admission ...*admissionReadBudget) (*executionReadEntry, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if ReferenceKind(loc.Kind) != Prefix && ReferenceKind(loc.Kind) != Hash || loc.Exact && loc.Height > s.height {
		s.mu.Unlock()
		return nil, ErrCorrupt
	}
	e := s.entry(k, loc, h, known, false)
	readHere := false
	for {
		if loc.Kind != e.loc.Kind || loc.Exact && e.loc.Exact && loc.Height != e.loc.Height {
			s.mu.Unlock()
			return nil, ErrPositionMismatch
		}
		if known && e.hasSummary && e.hash != h {
			s.mu.Unlock()
			return nil, ErrHashMismatch
		}
		if !body && e.hasSummary {
			s.mu.Unlock()
			return e, nil
		}
		if e.state == readRunning {
			t := time.Now()
			s.stats.Waits++
			s.cond.Broadcast()
			s.cond.Wait()
			s.stats.WaitNanos += uint64(time.Since(t))
			continue
		}
		if e.state == readReady {
			if body && e.bodyErr != nil {
				s.mu.Unlock()
				return nil, e.bodyErr
			}
			if e.err != nil {
				s.mu.Unlock()
				return nil, e.err
			}
			if known && e.hash != h {
				s.mu.Unlock()
				return nil, ErrHashMismatch
			}
			if !body || e.raw != nil || e.parsed {
				if body && !e.parsed && e.raw != nil && !readHere {
					s.stats.RawReuseHits++
				}
				s.mu.Unlock()
				return e, nil
			}
			s.stats.BodyUpgradeReads++ // exact physical version retained in e.loc
		} else if e.state == readQueued {
			s.stats.ForegroundClaims++
		}
		var budget *readBudget
		a := s.foregroundAdmission
		if len(admission) > 0 && admission[0] != nil {
			a = admission[0]
		}
		if a != nil {
			budget = &readBudget{admission: a}
			if err := budget.beforeRead(); err != nil {
				s.mu.Unlock()
				return nil, err
			}
		}
		e.state = readRunning
		s.mu.Unlock()
		s.read(e, &s.main, false)
		readHere = true
		if budget != nil {
			// Only main-thread optional admission I/O is billed here. Background work
			// was already done as part of execution, never to force cache admission.
			if err := budget.admission.afterRead(uint64(len(e.raw) + nodePositionHeaderBytes + (int(k.Length)+1)/2 + HeightBytes)); err != nil {
				return nil, err
			}
		}
		s.mu.Lock()
	}
}
func (s *executionReads) sealAndDrain() {
	s.drainOnce.Do(func() {
		t := time.Now()
		s.mu.Lock()
		if !s.sealed {
			s.sealed = true
			if s.queue != nil {
				close(s.queue)
			}
		}
		s.mu.Unlock()
		s.workers.Wait()
		s.mu.Lock()
		s.stats.DrainNanos += uint64(time.Since(t))
		s.mu.Unlock()
	})
}
func (s *executionReads) close() *executionReadBacking {
	s.sealAndDrain()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.main != nil {
		s.main.close()
		s.main = nil
	}
	// Transfer backing arrays only after workers join. Clear just occupied
	// slots; neither cache contents nor whole-capacity arrays are copied/scanned.
	backing := &executionReadBacking{s.entries, s.index}
	for i := 0; i < s.count; i++ {
		s.index[s.entries[i].indexSlot] = 0
		s.entries[i] = executionReadEntry{}
	}
	s.entries = nil
	s.index = nil
	s.rawBytes = 0
	s.nodeBytes = 0
	return backing
}
func (s *executionReads) snapshotStats() ExecutionReadStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func (s *ExecutionReadStats) add(v ExecutionReadStats) {
	if v.MaxReadBufferBytes > s.MaxReadBufferBytes {
		s.MaxReadBufferBytes = v.MaxReadBufferBytes
	}
	if v.PeakDecodeScratchBytes > s.PeakDecodeScratchBytes {
		s.PeakDecodeScratchBytes = v.PeakDecodeScratchBytes
	}
	s.HeaderOnlyReads += v.HeaderOnlyReads
	s.LogicalRecordBytes += v.LogicalRecordBytes
	s.RecordReads += v.RecordReads
	s.PrefixSeeks += v.PrefixSeeks
	s.PrefixExactGets += v.PrefixExactGets
	s.HashGets += v.HashGets
	s.ReadBytes += v.ReadBytes
	s.BackgroundReads += v.BackgroundReads
	s.ForegroundReads += v.ForegroundReads
	s.Prefetches += v.Prefetches
	s.QueueSkips += v.QueueSkips
	s.TableFallbacks += v.TableFallbacks
	s.ForegroundClaims += v.ForegroundClaims
	s.Waits += v.Waits
	s.WaitNanos += v.WaitNanos
	s.IONanos += v.IONanos
	s.DrainNanos += v.DrainNanos
	s.HashHeaderHits += v.HashHeaderHits
	s.RawReuseHits += v.RawReuseHits
	s.ParsedReuseHits += v.ParsedReuseHits
	s.NodeDecodeCount += v.NodeDecodeCount
	s.NodeDecodeBytes += v.NodeDecodeBytes
	s.NodeDecodeNanos += v.NodeDecodeNanos
	s.BodyUpgradeReads += v.BodyUpgradeReads
	s.BodyDrops += v.BodyDrops
	s.ReadErrors += v.ReadErrors
	if v.PeakRawBytes > s.PeakRawBytes {
		s.PeakRawBytes = v.PeakRawBytes
	}
	if v.PeakNodeBytes > s.PeakNodeBytes {
		s.PeakNodeBytes = v.PeakNodeBytes
	}
	if v.PeakBodyBytes > s.PeakBodyBytes {
		s.PeakBodyBytes = v.PeakBodyBytes
	}
	if v.PeakEntries > s.PeakEntries {
		s.PeakEntries = v.PeakEntries
	}
	if v.MetadataBytes > s.MetadataBytes {
		s.MetadataBytes = v.MetadataBytes
	}
	if v.QueueBytes > s.QueueBytes {
		s.QueueBytes = v.QueueBytes
	}
}

func executionReadBudget(c ExecutionReadConfig) uint64 {
	if !c.Enabled {
		return 0
	}
	size := uint64(1)
	for size < uint64(c.Entries)*2 {
		size *= 2
	}
	return c.BodyBytes + uint64(c.Entries)*uint64(unsafe.Sizeof(executionReadEntry{})) + size*4 + uint64(c.Queue)*uint64(unsafe.Sizeof((*executionReadEntry)(nil))) + uint64(unsafe.Sizeof(executionReads{}))
}
