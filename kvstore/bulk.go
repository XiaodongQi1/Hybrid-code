package kvstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrImportPending = errors.New("kvstore: initial import incomplete")

type BulkStats struct {
	Batches, Records, Bytes, PeakBatchBytes, VerifiedRecords uint64
	Resumed                                                  bool
}
type BulkMarker struct {
	Schema, Identity string
	Complete         bool
	Stats            BulkStats
}
type BulkPlan struct {
	Identity   string
	MarkerKey  []byte
	BatchBytes uint64
	// Final publishes only the final root/Head; all other rows stay unreachable
	// until this batch. It must be deterministic for the same Identity.
	Final     *Batch
	Committed func(*Batch)
}
type BulkEmit func(column string, key, value []byte) error
type BulkWalk func(BulkEmit) error

func (d *DB) BulkStatus(key []byte) (BulkMarker, error) {
	var out BulkMarker
	raw, e := d.Get(DefaultColumn, key)
	if e != nil {
		return out, e
	}
	if json.Unmarshal(raw, &out) != nil || out.Schema != "kvstore-initial-import/v1" || out.Identity == "" {
		return out, ErrCorrupt
	}
	canonical, _ := json.Marshal(out)
	if !bytes.Equal(raw, canonical) {
		return out, ErrCorrupt
	}
	return out, nil
}

// ImportInitial reuses an immutable deterministic stream. The owner must block
// ordinary reads/writes while the durable marker is incomplete. Progress and
// every data batch share one synchronous write; completion and Head share one.
// Already synced prefixes are skipped on resume but checked in the final pass.
func (d *DB) ImportInitial(ctx context.Context, p BulkPlan, walk BulkWalk) (BulkStats, error) {
	var stats BulkStats
	if p.Identity == "" || len(p.MarkerKey) == 0 || p.BatchBytes < 4096 || p.BatchBytes > 128<<20 || p.Final == nil || walk == nil {
		return stats, ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return stats, e
	}
	m, e := d.BulkStatus(p.MarkerKey)
	if e == nil {
		if m.Identity != p.Identity {
			return stats, ErrCorrupt
		}
		if m.Complete {
			return m.Stats, nil
		}
		m.Stats.Resumed = true
	} else if errors.Is(e, ErrNotFound) {
		m = BulkMarker{Schema: "kvstore-initial-import/v1", Identity: p.Identity}
	} else {
		return stats, e
	}
	write := func(b *Batch, marker BulkMarker) (BulkMarker, error) {
		var payload uint64
		for _, op := range b.entries {
			if op.deleted {
				return marker, ErrInvalid
			}
			if op.column == DefaultColumn && bytes.Equal(op.key, p.MarkerKey) {
				return marker, ErrInvalid
			}
			payload += uint64(len(op.key) + len(op.value) + 16)
		}
		base := marker.Stats.Bytes
		marker.Stats.Batches++
		for tries := 0; tries < 16; tries++ {
			raw, _ := json.Marshal(marker)
			size := payload + uint64(len(p.MarkerKey)+len(raw)+16)
			if size > p.BatchBytes {
				return marker, ErrLimit
			}
			peak := marker.Stats.PeakBatchBytes
			if size > peak {
				peak = size
			}
			if marker.Stats.Bytes == base+size && marker.Stats.PeakBatchBytes == peak {
				batch := NewBatch()
				batch.entries = append(batch.entries, b.entries...)
				batch.Put(DefaultColumn, p.MarkerKey, raw)
				if e := ctx.Err(); e != nil {
					return marker, e
				}
				if e := d.Write(batch); e != nil {
					return marker, e
				}
				if p.Committed != nil {
					p.Committed(batch)
				}
				return marker, nil
			}
			marker.Stats.Bytes = base + size
			marker.Stats.PeakBatchBytes = peak
		}
		return marker, ErrCorrupt
	}
	if e != nil {
		m, e = write(NewBatch(), m)
		if e != nil {
			return stats, e
		}
	}
	stats = m.Stats
	batch := NewBatch()
	var used, seq uint64
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		pending := m
		pending.Stats = stats
		pending.Stats.Records = seq
		var e error
		pending, e = write(batch, pending)
		if e != nil {
			return e
		}
		m = pending
		stats = m.Stats
		batch = NewBatch()
		used = 0
		return nil
	}
	e = walk(func(column string, key, value []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		seq++
		if seq <= m.Stats.Records {
			return nil
		}
		size := uint64(len(key) + len(value) + 16)
		if size+2048 > p.BatchBytes {
			return ErrLimit
		}
		if used+size+2048 > p.BatchBytes {
			seq--
			if e := flush(); e != nil {
				return e
			}
			seq++
		}
		batch.Put(column, key, value)
		used += size
		return nil
	})
	if e != nil {
		return stats, e
	}
	if seq < m.Stats.Records {
		return stats, ErrCorrupt
	}
	if e = flush(); e != nil {
		return stats, e
	}
	var verified uint64
	e = walk(func(column string, key, value []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		actual, e := d.Get(column, key)
		if e != nil {
			return e
		}
		if !bytes.Equal(actual, value) {
			return fmt.Errorf("%w: initial-import verification", ErrCorrupt)
		}
		verified++
		return nil
	})
	if e != nil {
		return stats, e
	}
	if verified != seq {
		return stats, ErrCorrupt
	}
	m.Stats = stats
	m.Stats.VerifiedRecords = verified
	m.Complete = true
	m, e = write(p.Final, m)
	if e != nil {
		return stats, e
	}
	return m.Stats, nil
}
