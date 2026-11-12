package patharchive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

// Each state conversion has four sections. Paths and flat keys share their
// owner prefix within independently restartable runs; small owners share chunks.
func section(k []byte) byte {
	var n byte
	if k[0] == 'P' {
		n = 2
	}
	return n + k[1]
}
func sectionKey(tag byte, id uint64, kind byte) []byte { return append(heightKey(tag, id), kind) }
func chunkKey(id uint64, kind byte, n uint64) []byte {
	return binary.AppendUvarint(sectionKey('H', id, kind), n)
}

type boundary struct {
	key  []byte
	size uint64
}

func encodeBounds(bs []boundary) []byte {
	b := varBytes(nil, uint64(len(bs)))
	for _, d := range bs {
		b = putBytes(b, d.key)
		b = varBytes(b, d.size)
	}
	return b
}
func decodeBounds(b []byte, l HistoryLayout) ([]boundary, error) {
	if len(b) > 1 && b[0] == 0 {
		return decodePackedBounds(b, l)
	}
	if uint64(len(b)) > l.MaxIndexMetaBytes {
		return nil, ErrHistoryLimit
	}
	n, e := takeVar(&b)
	if e != nil || n > uint64(len(b))/3 {
		return nil, ErrCorrupt
	}
	out := make([]boundary, 0, int(n))
	var prev []byte
	for i := uint64(0); i < n; i++ {
		k, e := takeBytes(&b, 99)
		if e != nil || validHistoryKey(k) != nil || (i > 0 && bytes.Compare(prev, k) >= 0) {
			return nil, ErrCorrupt
		}
		size, e := takeVar(&b)
		if e != nil || size == 0 || size > l.MaxRecordBytes+1024 {
			return nil, ErrCorrupt
		}
		out = append(out, boundary{bytes.Clone(k), size})
		prev = k
	}
	if len(b) != 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}
func encodeRun(entries []historyEntry) []byte {
	var b, prev []byte
	for _, e := range entries {
		shared := 0
		for shared < len(prev) && shared < len(e.key) && prev[shared] == e.key[shared] {
			shared++
		}
		b = varBytes(b, uint64(shared))
		b = putBytes(b, e.key[shared:])
		b = putBytes(b, e.value)
		prev = e.key
	}
	return b
}
func decodeRun(b []byte, l HistoryLayout) ([]historyEntry, error) {
	var rows []historyEntry
	var prev []byte
	for len(b) > 0 {
		shared, e := takeVar(&b)
		if e != nil || shared > uint64(len(prev)) {
			return nil, ErrCorrupt
		}
		suffix, e := takeBytes(&b, 99)
		if e != nil || shared+uint64(len(suffix)) > 99 {
			return nil, ErrCorrupt
		}
		v, e := takeBytes(&b, l.MaxRecordBytes)
		if e != nil {
			return nil, e
		}
		k := append(bytes.Clone(prev[:int(shared)]), suffix...)
		if validHistoryKey(k) != nil || (len(rows) > 0 && bytes.Compare(prev, k) >= 0) {
			return nil, ErrCorrupt
		}
		rows = append(rows, historyEntry{k, bytes.Clone(v)})
		prev = k
	}
	if len(rows) == 0 {
		return nil, ErrCorrupt
	}
	return rows, nil
}
func encodeChunk(entries []historyEntry, l HistoryLayout) []byte {
	var runs [][]byte
	var bounds []boundary
	start := 0
	for i := range entries {
		if i > start && uint64(len(encodeRun(entries[start:i+1]))) > l.RestartBytes {
			wire := encodeRun(entries[start:i])
			runs = append(runs, wire)
			bounds = append(bounds, boundary{entries[i-1].key, uint64(len(wire))})
			start = i
		}
	}
	if start < len(entries) {
		wire := encodeRun(entries[start:])
		runs = append(runs, wire)
		bounds = append(bounds, boundary{entries[len(entries)-1].key, uint64(len(wire))})
	}
	b := putBytes(nil, encodeBounds(bounds))
	for _, run := range runs {
		b = append(b, run...)
	}
	return b
}
func chunkRuns(b []byte, l HistoryLayout) ([]boundary, []byte, error) {
	if uint64(len(b)) > l.MaxRecordBytes+1024 {
		return nil, nil, ErrHistoryLimit
	}
	wire, e := takeBytes(&b, l.MaxIndexMetaBytes)
	if e != nil {
		return nil, nil, e
	}
	bounds, e := decodeBounds(wire, l)
	if e != nil || len(bounds) == 0 {
		return nil, nil, ErrCorrupt
	}
	var total uint64
	for _, d := range bounds {
		if d.size > uint64(len(b))-total {
			return nil, nil, ErrCorrupt
		}
		total += d.size
	}
	if total != uint64(len(b)) {
		return nil, nil, ErrCorrupt
	}
	return bounds, b, nil
}
func findChunk(b, k []byte, l HistoryLayout) ([]byte, error) {
	if len(b) > 0 && b[0] == 0 {
		return findPackedChunk(b, k, l)
	}
	bounds, data, e := chunkRuns(b, l)
	if e != nil {
		return nil, e
	}
	i := sort.Search(len(bounds), func(i int) bool { return bytes.Compare(bounds[i].key, k) >= 0 })
	if i == len(bounds) {
		return nil, ErrCorrupt
	}
	var offset uint64
	for _, d := range bounds[:i] {
		offset += d.size
	}
	rows, e := decodeRun(data[offset:offset+bounds[i].size], l)
	if e != nil || !bytes.Equal(rows[len(rows)-1].key, bounds[i].key) {
		return nil, ErrCorrupt
	}
	if i > 0 && bytes.Compare(rows[0].key, bounds[i-1].key) <= 0 {
		return nil, ErrCorrupt
	}
	j := sort.Search(len(rows), func(i int) bool { return bytes.Compare(rows[i].key, k) >= 0 })
	if j == len(rows) || !bytes.Equal(rows[j].key, k) {
		return nil, ErrCorrupt
	}
	return rows[j].value, nil
}
func decodeChunk(b []byte, l HistoryLayout) ([]historyEntry, error) {
	if len(b) > 0 && b[0] == 0 {
		return decodePackedChunk(b, l)
	}
	bounds, data, e := chunkRuns(b, l)
	if e != nil {
		return nil, e
	}
	var out []historyEntry
	for _, d := range bounds {
		rows, e := decodeRun(data[:d.size], l)
		data = data[d.size:]
		if e != nil || !bytes.Equal(rows[len(rows)-1].key, d.key) {
			return nil, ErrCorrupt
		}
		if len(out) > 0 && bytes.Compare(out[len(out)-1].key, rows[0].key) >= 0 {
			return nil, ErrCorrupt
		}
		out = append(out, rows...)
	}
	return out, nil
}
func (s *Store) writeSections(pending map[string][]byte, id uint64, put func([]byte, []byte) error, stats *Changes) error {
	var sections [4][]historyEntry
	for k, v := range pending {
		key := []byte(k)
		if validHistoryKey(key) != nil || uint64(len(v)) > s.cfg.HistoryLayout.MaxRecordBytes {
			return ErrHistoryLimit
		}
		sections[section(key)] = append(sections[section(key)], historyEntry{key, v})
	}
	l := *s.cfg.HistoryLayout
	encode := encodeChunk
	encodeDirectory := encodeBounds
	if s.cfg.Format == PackedFormat {
		encode = encodePackedChunk
		encodeDirectory = encodePackedBounds
	}
	for kind, entries := range sections {
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		var bounds []boundary
		for start := 0; start < len(entries); {
			// Exponential growth and binary search avoid encoding the whole
			// chunk once per appended entry.
			end := start + 1
			wire := encode(entries[start:end], l)
			if uint64(len(wire)) <= l.ChunkBytes {
				hi := end
				for hi < len(entries) {
					candidate := start + 2*(hi-start)
					if candidate > len(entries) {
						candidate = len(entries)
					}
					next := encode(entries[start:candidate], l)
					if uint64(len(next)) > l.ChunkBytes {
						hi = candidate
						break
					}
					end = candidate
					wire = next
					hi = candidate
				}
				lo := end
				for lo+1 < hi {
					mid := lo + (hi-lo)/2
					next := encode(entries[start:mid], l)
					if uint64(len(next)) <= l.ChunkBytes {
						lo = mid
						wire = next
					} else {
						hi = mid
					}
				}
				end = lo
			}

			if uint64(len(wire)) > l.MaxRecordBytes+1024 {
				return ErrHistoryLimit
			}
			if s.cfg.Format == PackedFormat {
				// Refuse an object exceeding the configured directory budget
				// before the enclosing atomic batch is committed.
				if _, _, e := parsePackedChunk(wire, l); e != nil {
					return e
				}
			}
			if e := put(chunkKey(id, byte(kind), uint64(len(bounds))), wire); e != nil {
				return e
			}
			bounds = append(bounds, boundary{entries[end-1].key, uint64(len(wire))})
			if kind >= 2 {
				if stats.History == nil {
					stats.History = &HistoryEncoding{}
				}
				n := uint64(len(chunkKey(id, byte(kind), uint64(len(bounds)-1))) + len(wire))
				for _, row := range entries[start:end] {
					n -= uint64(len(row.value))
				}
				stats.History.KeyBytes += n
			}
			stats.ReverseBytes += uint64(len(chunkKey(id, byte(kind), uint64(len(bounds)-1))) + len(wire))
			start = end
		}
		directory := encodeDirectory(bounds)
		if uint64(len(directory)) > l.MaxIndexMetaBytes {
			return ErrHistoryLimit
		}
		if kind >= 2 && len(entries) > 0 {
			stats.History.KeyBytes += uint64(len(sectionKey('R', id, byte(kind))) + len(directory))
		}
		if e := put(sectionKey('R', id, byte(kind)), directory); e != nil {
			return e
		}
	}
	return nil
}
func (r reader) historyEntry(k []byte, id uint64) ([]byte, error) {
	if r.format == PackedFormat {
		return r.packedHistoryEntry(k, id)
	}
	if e := r.ctx.Err(); e != nil {
		return nil, e
	}
	wire, e := r.get(sectionKey('R', id, section(k)))
	if e != nil {
		return nil, errors.Join(ErrCorrupt, e)
	}
	if len(wire) > 1 && wire[0] == 0 {
		return nil, ErrCorrupt
	}
	bounds, e := decodeBounds(wire, r.layout)
	if e != nil {
		return nil, e
	}
	i := sort.Search(len(bounds), func(i int) bool { return bytes.Compare(bounds[i].key, k) >= 0 })
	if i == len(bounds) {
		return nil, ErrCorrupt
	}
	data, e := r.get(chunkKey(id, section(k), uint64(i)))
	if e != nil {
		return nil, errors.Join(ErrCorrupt, e)
	}
	if uint64(len(data)) != bounds[i].size {
		return nil, ErrCorrupt
	}
	return findChunk(data, k, r.layout)
}
