package patharchive

// v7 H objects contain owner headers, a key stream and a separate value stream.
// Restart directories contain offsets only. R stores chunk sizes, never keys;
// multi-chunk lookup binary-searches the last key already present in H.
import (
	"bytes"
	"errors"
	"sort"
)

type packedGroup struct {
	owner                    []byte
	count                    uint64
	keys, values             []byte
	keyOffsets, valueOffsets []uint64
}

func encodePackedChunk(rows []historyEntry, l HistoryLayout) []byte {
	var header, keys, values []byte
	kind := section(rows[0].key)
	groups := 0
	for start := 0; start < len(rows); {
		n, _ := ownerEnd(rows[start].key)
		owner := rows[start].key[:n]
		end := start + 1
		for end < len(rows) && bytes.HasPrefix(rows[end].key, owner) {
			end++
		}
		var kb, vb, offsets, prev []byte
		var lastK, lastV uint64
		for i, row := range rows[start:end] {
			tail := row.key[n:]
			shared := 0
			if uint64(i)%l.RestartEntries == 0 {
				if i > 0 {
					offsets = varBytes(offsets, uint64(len(kb))-lastK)
					offsets = varBytes(offsets, uint64(len(vb))-lastV)
				}
				lastK, lastV = uint64(len(kb)), uint64(len(vb))
			} else {
				for shared < len(prev) && shared < len(tail) && prev[shared] == tail[shared] {
					shared++
				}
			}
			kb = varBytes(kb, uint64(shared))
			kb = varBytes(kb, uint64(len(tail)-shared))
			kb = varBytes(kb, uint64(len(row.value)))
			kb = append(kb, tail[shared:]...)
			vb = append(vb, row.value...)
			prev = tail
		}
		if kind%2 == 1 {
			header = append(header, owner[2:]...)
		}
		header = varBytes(header, uint64(end-start))
		header = varBytes(header, uint64(len(kb)))
		header = varBytes(header, uint64(len(vb)))
		header = append(header, offsets...)
		keys = append(keys, kb...)
		values = append(values, vb...)
		groups++
		start = end
	}
	h := varBytes([]byte{kind}, uint64(groups))
	h = append(h, header...)
	out := putBytes([]byte{0}, h)
	out = append(out, keys...)
	return append(out, values...)
}

func parsePackedChunk(b []byte, l HistoryLayout) ([]packedGroup, uint64, error) {
	if len(b) < 2 || b[0] != 0 || l.RestartEntries == 0 || l.RestartEntries > 256 || uint64(len(b)) > l.MaxRecordBytes+1024 {
		return nil, 0, ErrCorrupt
	}
	original := len(b)
	b = b[1:]
	h, e := takeBytes(&b, l.MaxIndexMetaBytes)
	if e != nil || len(h) < 2 || h[0] > 3 {
		return nil, 0, ErrCorrupt
	}
	directory := uint64(original - len(b))
	kind := h[0]
	h = h[1:]
	count, e := takeVar(&h)
	if e != nil || count == 0 || count > uint64(len(h))/3 || (kind%2 == 0 && count != 1) {
		return nil, 0, ErrCorrupt
	}
	var groups []packedGroup
	var keyTotal, valueTotal uint64
	var lengths [][2]uint64
	for i := uint64(0); i < count; i++ {
		owner := []byte{'F', kind % 2}
		if kind >= 2 {
			owner[0] = 'P'
		}
		if kind%2 == 1 {
			if len(h) < 32 {
				return nil, 0, ErrCorrupt
			}
			owner = append(owner, h[:32]...)
			h = h[32:]
		}
		if i > 0 && bytes.Compare(groups[len(groups)-1].owner, owner) >= 0 {
			return nil, 0, ErrCorrupt
		}
		records, e := takeVar(&h)
		if e != nil || records == 0 {
			return nil, 0, ErrCorrupt
		}
		kl, e := takeVar(&h)
		if e != nil || kl > uint64(len(b))-keyTotal || records > kl/3 {
			return nil, 0, ErrCorrupt
		}
		vl, e := takeVar(&h)
		if e != nil || vl > uint64(len(b))-valueTotal {
			return nil, 0, ErrCorrupt
		}
		g := packedGroup{owner: owner, count: records, keyOffsets: []uint64{0}, valueOffsets: []uint64{0}}
		restarts := (records - 1) / l.RestartEntries
		if restarts > uint64(len(h))/2 {
			return nil, 0, ErrCorrupt
		}
		var ko, vo uint64
		for j := uint64(0); j < restarts; j++ {
			dk, e := takeVar(&h)
			if e != nil || dk == 0 || dk >= kl-ko {
				return nil, 0, ErrCorrupt
			}
			dv, e := takeVar(&h)
			if e != nil || dv > vl-vo {
				return nil, 0, ErrCorrupt
			}
			ko += dk
			vo += dv
			g.keyOffsets = append(g.keyOffsets, ko)
			g.valueOffsets = append(g.valueOffsets, vo)
		}
		keyTotal += kl
		valueTotal += vl
		lengths = append(lengths, [2]uint64{kl, vl})
		groups = append(groups, g)
	}
	if len(h) != 0 || keyTotal > uint64(len(b)) || valueTotal != uint64(len(b))-keyTotal {
		return nil, 0, ErrCorrupt
	}
	keys, values := b[:keyTotal], b[keyTotal:]
	for i, n := range lengths {
		groups[i].keys = keys[:n[0]]
		keys = keys[n[0]:]
		groups[i].values = values[:n[1]]
		values = values[n[1]:]
	}
	return groups, directory, nil
}

// Restart keys are already stored in the key stream. Read only that key when
// binary-searching offsets; node payloads do not participate in the search.
func packedRestartKey(g packedGroup, i int) ([]byte, error) {
	b := g.keys[g.keyOffsets[i]:]
	shared, e := takeVar(&b)
	if e != nil || shared != 0 {
		return nil, ErrCorrupt
	}
	n, e := takeVar(&b)
	if e != nil || n > uint64(len(b)) {
		return nil, ErrCorrupt
	}
	_, e = takeVar(&b)
	if e != nil || n > uint64(len(b)) {
		return nil, ErrCorrupt
	}
	key := append(bytes.Clone(g.owner), b[:n]...)
	if validHistoryKey(key) != nil {
		return nil, ErrCorrupt
	}
	return key, nil
}

func packedRun(g packedGroup, i int, l HistoryLayout) ([]historyEntry, error) {
	kend, vend := uint64(len(g.keys)), uint64(len(g.values))
	if i+1 < len(g.keyOffsets) {
		kend = g.keyOffsets[i+1]
		vend = g.valueOffsets[i+1]
	}
	b := g.keys[g.keyOffsets[i]:kend]
	values := g.values[g.valueOffsets[i]:vend]
	count := l.RestartEntries
	if remain := g.count - uint64(i)*l.RestartEntries; remain < count {
		count = remain
	}
	var out []historyEntry
	var prev []byte
	for j := uint64(0); j < count; j++ {
		shared, e := takeVar(&b)
		if e != nil || shared > uint64(len(prev)) {
			return nil, ErrCorrupt
		}
		suffix, e := takeVar(&b)
		if e != nil || suffix > uint64(len(b)) || shared+suffix > 33 {
			return nil, ErrCorrupt
		}
		size, e := takeVar(&b)
		if e != nil || size > uint64(len(values)) || suffix > uint64(len(b)) {
			return nil, ErrCorrupt
		}
		tail := append(bytes.Clone(prev[:shared]), b[:suffix]...)
		b = b[suffix:]
		key := append(bytes.Clone(g.owner), tail...)
		if validHistoryKey(key) != nil || (j > 0 && bytes.Compare(prev, tail) >= 0) {
			return nil, ErrCorrupt
		}
		out = append(out, historyEntry{key, bytes.Clone(values[:size])})
		values = values[size:]
		prev = tail
	}
	if len(b) != 0 || len(values) != 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}
func decodePackedChunk(b []byte, l HistoryLayout) ([]historyEntry, error) {
	groups, _, e := parsePackedChunk(b, l)
	if e != nil {
		return nil, e
	}
	var rows []historyEntry
	for _, g := range groups {
		for i := range g.keyOffsets {
			run, e := packedRun(g, i, l)
			if e != nil {
				return nil, e
			}
			if len(rows) > 0 && bytes.Compare(rows[len(rows)-1].key, run[0].key) >= 0 {
				return nil, ErrCorrupt
			}
			rows = append(rows, run...)
		}
	}
	return rows, nil
}
func findPackedChunk(b, key []byte, l HistoryLayout) ([]byte, error) {
	groups, _, e := parsePackedChunk(b, l)
	if e != nil {
		return nil, e
	}
	n, e := ownerEnd(key)
	if e != nil || validHistoryKey(key) != nil {
		return nil, ErrCorrupt
	}
	i := sort.Search(len(groups), func(i int) bool { return bytes.Compare(groups[i].owner, key[:n]) >= 0 })
	if i == len(groups) || !bytes.Equal(groups[i].owner, key[:n]) {
		return nil, ErrCorrupt
	}
	g := groups[i]
	var searchErr error
	j := sort.Search(len(g.keyOffsets), func(j int) bool {
		k, err := packedRestartKey(g, j)
		if err != nil {
			searchErr = err
			return true
		}
		return bytes.Compare(k, key) > 0
	}) - 1
	if searchErr != nil {
		return nil, searchErr
	}
	if j < 0 {
		return nil, ErrCorrupt
	}
	rows, e := packedRun(g, j, l)
	if e != nil {
		return nil, e
	}
	for _, row := range rows {
		if bytes.Equal(row.key, key) {
			return row.value, nil
		}
	}
	return nil, ErrCorrupt
}
func packedLastKey(b []byte, l HistoryLayout) ([]byte, error) {
	groups, _, e := parsePackedChunk(b, l)
	if e != nil {
		return nil, e
	}
	g := groups[len(groups)-1]
	rows, e := packedRun(g, len(g.keyOffsets)-1, l)
	if e != nil {
		return nil, e
	}
	return rows[len(rows)-1].key, nil
}
func encodePackedBounds(bs []boundary) []byte {
	b := varBytes([]byte{0}, uint64(len(bs)))
	for _, d := range bs {
		b = varBytes(b, d.size)
	}
	return b
}
func decodePackedBounds(b []byte, l HistoryLayout) ([]boundary, error) {
	if len(b) < 2 || b[0] != 0 || uint64(len(b)) > l.MaxIndexMetaBytes {
		return nil, ErrCorrupt
	}
	b = b[1:]
	n, e := takeVar(&b)
	if e != nil || n > uint64(len(b)) {
		return nil, ErrCorrupt
	}
	var out []boundary
	for i := uint64(0); i < n; i++ {
		size, e := takeVar(&b)
		if e != nil || size == 0 || size > l.MaxRecordBytes+1024 {
			return nil, ErrCorrupt
		}
		out = append(out, boundary{size: size})
	}
	if len(b) != 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}
func (r reader) packedHistoryEntry(k []byte, id uint64) ([]byte, error) {
	wire, e := r.get(sectionKey('R', id, section(k)))
	if e != nil {
		return nil, errors.Join(ErrCorrupt, e)
	}
	bounds, e := decodePackedBounds(wire, r.layout)
	if e != nil {
		return nil, e
	}
	// Cache only within this lookup so binary search does not reread its winner.
	chunks := map[int][]byte{}
	read := func(i int) ([]byte, error) {
		if b, ok := chunks[i]; ok {
			return b, nil
		}
		b, err := r.get(chunkKey(id, section(k), uint64(i)))
		if err != nil || uint64(len(b)) != bounds[i].size {
			return nil, errors.Join(ErrCorrupt, err)
		}
		if len(b) < 3 || b[0] != 0 {
			return nil, ErrCorrupt
		}
		groups, _, err := parsePackedChunk(b, r.layout)
		if err != nil || section(groups[0].owner) != section(k) {
			return nil, ErrCorrupt
		}
		chunks[i] = b
		return b, nil
	}
	var searchErr error
	i := 0
	if len(bounds) > 1 {
		i = sort.Search(len(bounds), func(i int) bool {
			b, err := read(i)
			if err != nil {
				searchErr = err
				return true
			}
			last, err := packedLastKey(b, r.layout)
			if err != nil {
				searchErr = err
				return true
			}
			return bytes.Compare(last, k) >= 0
		})
	}
	if searchErr != nil {
		return nil, searchErr
	}
	if i == len(bounds) {
		return nil, ErrCorrupt
	}
	b, e := read(i)
	if e != nil {
		return nil, e
	}
	return findPackedChunk(b, k, r.layout)
}
