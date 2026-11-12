package kvstore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

type MemoryConfiguration struct {
	BlockCacheBytes                          int64
	PerMemtableBytes, GlobalWriteBufferBytes int64
	WriteBufferPolicy                        string
	ApplicationCache                         ReadCachePlan
	ExecutionCacheOnly                       bool
	BackgroundJobs                           int
}

func memoryConfiguration(o Options) MemoryConfiguration {
	p, _ := PlanReadCache(o.ApplicationCacheBytes)
	m := MemoryConfiguration{BlockCacheBytes: o.BlockCacheBytes, PerMemtableBytes: o.WriteBufferBytes, GlobalWriteBufferBytes: o.GlobalWriteBufferBytes, ApplicationCache: p, ExecutionCacheOnly: o.ExecutionCacheOnly, BackgroundJobs: o.BackgroundJobs, WriteBufferPolicy: "legacy-per-memtable"}
	if o.GlobalWriteBufferBytes > 0 {
		if o.Engine == RocksDB {
			m.WriteBufferPolicy = "shared-write-buffer-manager-with-stall"
		} else {
			m.WriteBufferPolicy = "single-db-active-and-immutable-target"
		}
	}
	return m
}

// Scope IDs change at every open. Counters from a reopen cannot silently stand
// in for a full run. A controller must checkpoint and merge explicit segments.
type CounterScope struct {
	ID                  string
	CreatedWithDatabase bool
	Coverage            string
}

func newCounterScope(create bool) (CounterScope, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return CounterScope{}, e
	}
	coverage := "since-current-open"
	if create {
		coverage = "since-database-creation"
	}
	return CounterScope{hex.EncodeToString(b[:]), create, coverage}, nil
}

type WriteCount struct{ Puts, Deletes, KeyBytes, ValueBytes, DeleteKeyBytes, NamespaceBytes uint64 }

func (c WriteCount) ApplicationBytes() uint64 { return c.KeyBytes + c.ValueBytes + c.DeleteKeyBytes }
func (c WriteCount) add(b WriteCount) WriteCount {
	return WriteCount{c.Puts + b.Puts, c.Deletes + b.Deletes, c.KeyBytes + b.KeyBytes, c.ValueBytes + b.ValueBytes, c.DeleteKeyBytes + b.DeleteKeyBytes, c.NamespaceBytes + b.NamespaceBytes}
}

type WriteCounters struct {
	BatchBytes         uint64
	BatchBytesComplete bool
	Commits            uint64
	Total              WriteCount
	Columns            map[string]WriteCount
}

func (w *WriteCounters) add(entries []operation, engine Engine) {
	w.Commits++
	for _, op := range entries {
		c := WriteCount{}
		if op.deleted {
			c.Deletes = 1
			c.DeleteKeyBytes = uint64(len(op.key))
		} else {
			c.Puts = 1
			c.KeyBytes = uint64(len(op.key))
			c.ValueBytes = uint64(len(op.value))
		}
		if engine == LevelDB {
			c.NamespaceBytes = uint64(1 + len(op.column))
		}
		w.Total = w.Total.add(c)
		w.Columns[op.column] = w.Columns[op.column].add(c)
	}
}
func (w WriteCounters) clone() WriteCounters {
	out := w
	out.Columns = map[string]WriteCount{}
	for k, v := range w.Columns {
		out.Columns[k] = v
	}
	return out
}
func countDelta(a, b WriteCount) (WriteCount, error) {
	x := []uint64{a.Puts, a.Deletes, a.KeyBytes, a.ValueBytes, a.DeleteKeyBytes, a.NamespaceBytes}
	y := []uint64{b.Puts, b.Deletes, b.KeyBytes, b.ValueBytes, b.DeleteKeyBytes, b.NamespaceBytes}
	d := make([]uint64, len(x))
	for i := range x {
		if y[i] < x[i] {
			return WriteCount{}, ErrCorrupt
		}
		d[i] = y[i] - x[i]
	}
	return WriteCount{d[0], d[1], d[2], d[3], d[4], d[5]}, nil
}

type EngineStatistics struct {
	BlockCacheCapacityBytes, WriteBufferCapacityBytes *uint64
	// Nil is unsupported or disabled, never a fabricated zero. File byte counts
	// are engine-observed bytes, not physical device IO or write amplification.
	BlockCacheBytes, BlockCachePinnedBytes, MemtableBytes, MutableMemtableBytes, LiveSSTBytes *uint64
	FileWriteBytes, WALWriteBytes, FlushWriteBytes, CompactionWriteBytes                      *uint64
}
type statisticsSource interface {
	statistics() (EngineStatistics, error)
}
type Statistics struct {
	ReadCachePolicy string
	Info            Info
	Scope           CounterScope
	Writes          WriteCounters
	ReadCache       ReadCacheStats
	Engine          EngineStatistics
}

func (d *DB) Statistics() (Statistics, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.check(); e != nil {
		return Statistics{}, e
	}
	return d.statsLocked()
}

// WriteDelta refuses a different open scope, including accidental comparisons
// after a restart. Persisted controller checkpoints can carry prior segments.
func WriteDelta(before, after Statistics) (WriteCounters, error) {
	if before.Scope.ID == "" || before.Scope.ID != after.Scope.ID {
		return WriteCounters{}, fmt.Errorf("%w: counter scope changed; checkpointed segments required", ErrInvalid)
	}
	if after.Writes.Commits < before.Writes.Commits {
		return WriteCounters{}, ErrCorrupt
	}
	if !before.Writes.BatchBytesComplete || !after.Writes.BatchBytesComplete || after.Writes.BatchBytes < before.Writes.BatchBytes {
		return WriteCounters{}, ErrCorrupt
	}
	out := WriteCounters{Commits: after.Writes.Commits - before.Writes.Commits, Columns: map[string]WriteCount{}, BatchBytes: after.Writes.BatchBytes - before.Writes.BatchBytes, BatchBytesComplete: true}
	var e error
	out.Total, e = countDelta(before.Writes.Total, after.Writes.Total)
	if e != nil {
		return out, e
	}
	for k, v := range after.Writes.Columns {
		out.Columns[k], e = countDelta(before.Writes.Columns[k], v)
		if e != nil {
			return out, e
		}
	}
	return out, nil
}

// Plus joins disjoint committed write counts, including counts from earlier opens.
func (w WriteCounters) Plus(next WriteCounters) WriteCounters {
	out := w.clone()
	out.Total = out.Total.add(next.Total)
	out.Commits += next.Commits
	out.BatchBytes += next.BatchBytes
	out.BatchBytesComplete = w.BatchBytesComplete && next.BatchBytesComplete
	for name, c := range next.Columns {
		out.Columns[name] = out.Columns[name].add(c)
	}
	return out
}
func prepareWrite(observer func(WriteCounters) error, es []operation, engine Engine, size uint64) error {
	if observer == nil {
		return nil
	}
	counts := WriteCounters{Columns: map[string]WriteCount{}, BatchBytes: size, BatchBytesComplete: true}
	counts.add(es, engine)
	return observer(counts)
}
