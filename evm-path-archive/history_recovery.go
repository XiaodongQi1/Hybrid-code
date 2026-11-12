package patharchive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
)

type HistoryEncoding struct {
	CheckpointValueBytes                      uint64 `json:",omitempty"`
	StructureValueBytes                       uint64 `json:",omitempty"`
	NoGainValueBytes                          uint64 `json:",omitempty"`
	Full, Absent, Delta, KeyBytes, ValueBytes uint64
	Checkpoint, Structure, NoGain             uint64
}
type recoveryCounters struct {
	indexReads, groupReads                atomic.Uint64
	reads, bytes, decoded, ns, calls, max atomic.Uint64
	histogram                             [65]atomic.Uint64
}
type HistoryRecovery struct {
	IndexReads, GroupReads                                      uint64
	Calls, Records, Bytes, DecodedBytes, Nanoseconds, MaxDeltas uint64
	DeltaHistogram                                              [65]uint64
}

func (a *Archive) RecoveryMetrics() HistoryRecovery {
	c := &a.s.recovery
	r := HistoryRecovery{IndexReads: c.indexReads.Load(), GroupReads: c.groupReads.Load(), Calls: c.calls.Load(), Records: c.reads.Load(), Bytes: c.bytes.Load(), DecodedBytes: c.decoded.Load(), Nanoseconds: c.ns.Load(), MaxDeltas: c.max.Load()}
	for i := range r.DeltaHistogram {
		r.DeltaHistogram[i] = c.histogram[i].Load()
	}
	return r
}
func (c *recoveryCounters) record(start time.Time, reads, n, decoded, d uint64) {
	if c == nil {
		return
	}
	c.calls.Add(1)
	c.reads.Add(reads)
	c.bytes.Add(n)
	c.decoded.Add(decoded)
	c.ns.Add(uint64(time.Since(start)))
	for old := c.max.Load(); d > old; old = c.max.Load() {
		if c.max.CompareAndSwap(old, d) {
			break
		}
	}
	if d > 64 {
		d = 64
	}
	c.histogram[d].Add(1)
}
func counterKey(k []byte) []byte { q := bytes.Clone(k); q[0] = 'Q'; return q }
func (s *Store) historyWrite(put func([]byte, []byte) error, del func([]byte) error, k, prior, value []byte, stats *Changes) ([]byte, error) {
	q := counterKey(k)
	count := uint64(0)
	if len(prior) > 0 {
		b, e := s.db.Get(q)
		if e != nil {
			return nil, errors.Join(ErrCorrupt, e)
		}
		n, used := binary.Uvarint(b)
		if used <= 0 || used != len(b) || used != len(binary.AppendUvarint(nil, n)) || n >= s.cfg.HistoryNodeCheckpointInterval {
			return nil, ErrCorrupt
		}
		count = n
	}
	undo, why, e := encodeNodeHistory(prior, value, count >= s.cfg.HistoryNodeCheckpointInterval-1)
	if e != nil {
		return nil, e
	}
	if why == "delta" {
		count++
	} else {
		count = 0
	}
	if len(value) == 0 {
		if e = del(q); e != nil {
			return nil, e
		}
	} else if e = put(q, binary.AppendUvarint(nil, count)); e != nil {
		return nil, e
	}
	if stats.History == nil {
		stats.History = &HistoryEncoding{}
	}
	h := stats.History
	switch why {
	case "absent":
		h.Absent++
	case "delta":
		h.Delta++
	default:
		h.Full++
	}
	switch why {
	case "checkpoint":
		h.Checkpoint++
		h.CheckpointValueBytes += uint64(len(undo))
	case "structure":
		h.Structure++
		h.StructureValueBytes += uint64(len(undo))
	case "no-gain":
		h.NoGain++
		h.NoGainValueBytes += uint64(len(undo))
	}
	h.ValueBytes += uint64(len(undo))
	return undo, nil
}
func (r reader) nodeHistory(k []byte, h uint64) (out []byte, err error) {
	start := time.Now()
	var reads, size, decoded uint64
	var deltas [][]byte
	defer func() { r.metrics.record(start, reads, size-decoded, decoded, uint64(len(deltas))) }()
	if r.limit == 0 {
		return nil, ErrHistoryLimit
	}
	prefix := r.historyPrefix(k)
	it := r.historyIterator(k)
	defer it.Release()
	found := false
	if h != ^uint64(0) {
		found = it.Seek(append(bytes.Clone(prefix), r.heightBytes(h+1)...))
	}
	var base []byte
	for found {
		if e := r.ctx.Err(); e != nil {
			return nil, e
		}
		if len(it.Key()) <= len(prefix) || !r.validHeight(it.Key()[len(prefix):]) {
			return nil, ErrCorrupt
		}
		rec := it.Value()
		reads++
		if c, ok := it.(interface{ ReadCost() uint64 }); ok {
			size += c.ReadCost()
		} else {
			size += uint64(len(it.Key()) + len(rec))
		}
		if size > r.byteLimit {
			return nil, ErrHistoryLimit
		}
		if len(rec) == 0 {
			return nil, ErrCorrupt
		}
		if rec[0]&3 != deltaNode {
			base, err = restoreNodeHistory(rec, nil)
			if err != nil {
				return nil, err
			}
			break
		}
		if uint64(len(deltas)) >= r.limit-1 {
			return nil, ErrHistoryLimit
		}
		deltas = append(deltas, bytes.Clone(rec))
		found = it.Next()
	}
	if e := it.Error(); e != nil {
		return nil, e
	}
	if !found {
		base, err = r.currentValue(k)
		if err != nil {
			return nil, err
		}
		reads++
		size += uint64(len(k) + len(base))
		if size > r.byteLimit {
			return nil, ErrHistoryLimit
		}
	}
	for i := len(deltas) - 1; i >= 0; i-- {
		if e := r.ctx.Err(); e != nil {
			return nil, e
		}
		base, err = restoreNodeHistory(deltas[i], base)
		if err != nil {
			return nil, err
		}
		size += uint64(len(base))
		decoded += uint64(len(base))
		if size > r.byteLimit {
			return nil, ErrHistoryLimit
		}
	}
	return base, nil
}
func (a *Archive) EncodingMetrics() (HistoryEncoding, error) {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total HistoryEncoding
	if e := s.check(); e != nil {
		return total, e
	}
	for h := s.cfg.initial(); h <= s.head.Height; h++ {
		r, e := s.record(h)
		if e != nil {
			return total, e
		}
		x := r.Changes.History
		if x == nil {
			continue
		}
		total.Full += x.Full
		total.Absent += x.Absent
		total.Delta += x.Delta
		total.KeyBytes += x.KeyBytes
		total.ValueBytes += x.ValueBytes
		total.Checkpoint += x.Checkpoint
		total.Structure += x.Structure
		total.NoGain += x.NoGain
		total.CheckpointValueBytes += x.CheckpointValueBytes
		total.StructureValueBytes += x.StructureValueBytes
		total.NoGainValueBytes += x.NoGainValueBytes
	}
	return total, nil
}

// validateCheckpointCounters checks the auxiliary table without changing it.
// Normal commits validate the touched counter; an explicit Audit checks all.
func (s *Store) validateCheckpointCounters() error {
	if !isDifferential(s.cfg.Format) || IsIndexedFormat(s.cfg.Format) {
		return nil
	}
	for _, tag := range []byte{'P', 'Q'} {
		it := s.db.NewIterator(prefixRange([]byte{tag}))
		for it.Next() {
			k := bytes.Clone(it.Key())
			if tag == 'P' {
				k[0] = 'Q'
			} else {
				k[0] = 'P'
			}
			b, e := s.db.Get(k)
			if e != nil {
				it.Release()
				return errors.Join(ErrCorrupt, e)
			}
			if tag == 'Q' {
				b = it.Value()
			}
			n, used := binary.Uvarint(b)
			if used <= 0 || used != len(b) || used != len(binary.AppendUvarint(nil, n)) || n >= s.cfg.HistoryNodeCheckpointInterval {
				it.Release()
				return ErrCorrupt
			}
		}
		e := it.Error()
		it.Release()
		if e != nil {
			return e
		}
	}
	return nil
}
