package patharchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// valueIndexed maps an execution height to a state conversion ID. Empty blocks
// reuse their predecessor's ID, and therefore do not create history versions.
func (r reader) valueIndexed(k []byte, h uint64) ([]byte, error) {
	if r.current && h == r.height {
		return r.currentValue(k)
	}
	// Charge actual point-read bytes, including index/directory framing.
	var encoded, decoded uint64
	get := r.get
	r.get = func(key []byte) ([]byte, error) {
		if e := r.ctx.Err(); e != nil {
			return nil, e
		}
		v, e := get(key)
		if e != nil {
			return nil, e
		}
		n := uint64(len(key) + len(v))
		if n > r.byteLimit-encoded-decoded {
			return nil, ErrHistoryLimit
		}
		encoded += n
		if r.metrics != nil {
			switch key[0] {
			case 'X', 'Y':
				r.metrics.indexReads.Add(1)
			case 'H', 'R':
				r.metrics.groupReads.Add(1)
			}
		}
		return v, nil
	}
	// Query and Audit already resolved this record under the archive lock.
	// Carry its mapping through owner-scoped reader copies, rather than loading
	// and decoding the large B/outcomes value for every node in a proof.
	target := r.targetID
	if !r.targetKnown || r.targetHeight != h {
		wire, e := r.get(heightKey('B', h))
		if e != nil {
			return nil, e
		}
		var rec Record
		if json.Unmarshal(wire, &rec) != nil || rec.Height != h {
			return nil, ErrCorrupt
		}
		target = rec.StateID
	}
	cursor, e := r.versionCursor(k)
	if e != nil {
		return nil, e
	}
	id, found, e := cursor.seekGT(target)
	if e != nil {
		return nil, e
	}
	if !found {
		return r.currentValue(k)
	}
	if k[0] == 'F' {
		v, e := r.historyEntry(k, id)
		if e != nil {
			return nil, e
		}
		return decodeValueWithCodec(k, v, r.codec())
	}
	start := time.Now()
	var deltas [][]byte
	var reads uint64
	defer func() { r.metrics.record(start, reads, encoded, decoded, uint64(len(deltas))) }()
	var base []byte
	for found {
		if e = r.ctx.Err(); e != nil {
			return nil, e
		}
		v, e := r.historyEntry(k, id)
		if e != nil {
			return nil, e
		}
		reads++
		if encoded+decoded > r.byteLimit {
			return nil, ErrHistoryLimit
		}
		if len(v) == 0 {
			return nil, ErrCorrupt
		}
		if v[0]&3 != deltaNode {
			base, e = restoreNodeHistory(v, nil)
			if e != nil {
				return nil, e
			}
			break
		}
		if r.limit == 0 || uint64(len(deltas)) >= r.limit-1 {
			return nil, ErrHistoryLimit
		}
		deltas = append(deltas, v)
		id, found, e = cursor.seekGT(id)
		if e != nil {
			return nil, e
		}
	}
	if !found {
		base, e = r.currentValue(k)
		if e != nil {
			return nil, e
		}
		reads++
	}
	for i := len(deltas) - 1; i >= 0; i-- {
		if e = r.ctx.Err(); e != nil {
			return nil, e
		}
		base, e = restoreNodeHistory(deltas[i], base)
		if e != nil {
			return nil, e
		}
		decoded += uint64(len(base))
		if encoded+decoded > r.byteLimit {
			return nil, ErrHistoryLimit
		}
	}
	return base, nil
}
func indexedEncoding(old, next []byte, ordinal, k uint64, stats *Changes) ([]byte, error) {
	v, why, e := encodeNodeHistory(old, next, ordinal%k == 0)
	if e != nil {
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
		h.CheckpointValueBytes += uint64(len(v))
	case "structure":
		h.Structure++
		h.StructureValueBytes += uint64(len(v))
	case "no-gain":
		h.NoGain++
		h.NoGainValueBytes += uint64(len(v))
	}
	h.ValueBytes += uint64(len(v))
	return v, nil
}
func (s *Store) indexedRecordID(initial bool, pending map[string][]byte, r *Record) error {
	r.StateID = s.head.StateID
	if initial {
		r.StateID = 0
		return nil
	}
	if len(pending) > 0 {
		if r.StateID == math.MaxUint64 {
			return ErrCorrupt
		}
		r.StateID++
	}
	return nil
}

// This checks both directions of the immutable history/index relation. The full
// forest audit then validates reconstructed nodes against each canonical root.
func (s *Store) auditIndexed(ctx context.Context) error {
	if !IsIndexedFormat(s.cfg.Format) {
		return nil
	}
	r := s.live(ctx)
	var last uint64
	var previousRecord Record
	expected := map[string][]uint64{}
	var historyEntries uint64
	for h := s.cfg.initial(); h <= s.head.Height; h++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		rec, e := s.record(h)
		if e != nil {
			return e
		}
		if rec.StateID < last || rec.StateID-last > 1 || (h == s.cfg.initial() && rec.StateID != 0) {
			return ErrCorrupt
		}
		if h > s.cfg.initial() && ((rec.Root == previousRecord.Root) != (rec.StateID == last)) {
			return ErrCorrupt
		}
		if rec.StateID > last {
			entries := 0
			for kind := byte(0); kind < 4; kind++ {
				wire, e := r.get(sectionKey('R', rec.StateID, kind))
				if e != nil {
					return errors.Join(ErrCorrupt, e)
				}
				bounds, e := decodeBounds(wire, r.layout)
				if e != nil {
					return e
				}
				var previous []byte
				for i, bound := range bounds {
					data, e := r.get(chunkKey(rec.StateID, kind, uint64(i)))
					if e != nil || uint64(len(data)) != bound.size {
						return errors.Join(ErrCorrupt, e)
					}
					rows, e := decodeChunk(data, r.layout)
					if e != nil {
						return e
					}
					if bound.key != nil && !bytes.Equal(rows[len(rows)-1].key, bound.key) {
						return ErrCorrupt
					}
					for _, row := range rows {
						if kind != section(row.key) || previous != nil && bytes.Compare(previous, row.key) >= 0 {
							return ErrCorrupt
						}
						previous = row.key
						entries++
						historyEntries++
						if historyEntries > s.cfg.MaxWalkNodes {
							return ErrHistoryLimit
						}
						expected[string(row.key)] = append(expected[string(row.key)], rec.StateID)

					}
				}
			}
			if entries == 0 {
				return ErrCorrupt
			}
		}
		last = rec.StateID
		previousRecord = rec
		if h == math.MaxUint64 {
			break
		}
	}
	it := s.db.NewIterator(nil)
	defer it.Release()
	for it.Next() {
		if e := ctx.Err(); e != nil {
			return e
		}
		key := it.Key()
		if len(key) == 0 {
			return ErrCorrupt
		}
		switch key[0] {
		case 'D', 'G', 'J', 'Q', 'I':
			return ErrCorrupt
		case 'X':
			k := key[1:]
			if validHistoryKey(k) != nil {
				return ErrCorrupt
			}
			ids, e := r.allVersions(k)
			if e != nil || len(ids) == 0 {
				return errors.Join(ErrCorrupt, e)
			}
			want := expected[string(k)]
			if len(want) != len(ids) {
				return ErrCorrupt
			}
			for i := range ids {
				if ids[i] != want[i] {
					return ErrCorrupt
				}
			}
			delete(expected, string(k))
			for i, id := range ids {
				if id > s.head.StateID {
					return ErrCorrupt
				}
				v, e := r.historyEntry(k, id)
				if e != nil {
					return e
				}
				if k[0] == 'P' {
					if len(v) == 0 {
						return ErrCorrupt
					}
					if uint64(i+1)%s.cfg.HistoryNodeCheckpointInterval == 0 && v[0]&3 == deltaNode {
						return ErrCorrupt
					}
				}
			}
		case 'Y':
			// The variable-size entity key is self-delimiting; reject orphan blocks.
			n, e := indexedIdentEnd(key[1:])
			if e != nil {
				return e
			}
			k := key[1 : 1+n]
			tail := key[1+n:]
			block, e := takeVar(&tail)
			if e != nil || len(tail) != 0 {
				return ErrCorrupt
			}
			x, ok, e := r.readIndex(k)
			if e != nil || !ok || x.inline != nil || block >= uint64(len(x.desc)) {
				return ErrCorrupt
			}
		case 'H', 'R':
			if e := s.auditHistoryObject(key, it.Value()); e != nil {
				return e
			}
		}
	}
	if len(expected) != 0 {
		return ErrCorrupt
	}
	return it.Error()
}

func indexedIdentEnd(k []byte) (int, error) {
	n, e := ownerEnd(k)
	if e != nil {
		return 0, e
	}
	end := n + 32
	if k[0] == 'P' {
		if len(k) <= n || k[n] > 64 {
			return 0, ErrCorrupt
		}
		end = n + 1 + (int(k[n])+1)/2
	}
	if len(k) < end || validHistoryKey(k[:end]) != nil {
		return 0, ErrCorrupt
	}
	return end, nil
}
func (s *Store) auditHistoryObject(k, v []byte) error {
	if (len(v) > 1 && v[0] == 0) != (s.cfg.Format == PackedFormat) {
		return ErrCorrupt
	}
	if len(k) < 10 {
		return ErrCorrupt
	}
	id := binary.BigEndian.Uint64(k[1:9])
	kind := k[9]
	if id == 0 || id > s.head.StateID || kind > 3 {
		return ErrCorrupt
	}
	if k[0] == 'R' {
		if len(k) != 10 {
			return ErrCorrupt
		}
		_, e := decodeBounds(v, *s.cfg.HistoryLayout)
		return e
	}
	tail := k[10:]
	n, e := takeVar(&tail)
	if e != nil || len(tail) != 0 {
		return ErrCorrupt
	}
	wire, e := s.db.Get(sectionKey('R', id, kind))
	if e != nil {
		return errors.Join(ErrCorrupt, e)
	}
	bounds, e := decodeBounds(wire, *s.cfg.HistoryLayout)
	if e != nil || n >= uint64(len(bounds)) || uint64(len(v)) != bounds[n].size {
		return ErrCorrupt
	}
	_, e = decodeChunk(v, *s.cfg.HistoryLayout)
	return e
}
