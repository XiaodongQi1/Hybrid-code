package patharchive

// The v6 history index is an application-level ordered index. Online reads use
// point Get only; neither state IDs nor locations rely on KV iteration order.
import (
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

const IndexedFormat = "statera-path-archive/v6"
const PackedFormat = "statera-path-archive/v7"

func IsIndexedFormat(f string) bool { return f == IndexedFormat || f == PackedFormat }

type HistoryLayout struct {
	// v7 history keys restart by entry count; ID blocks still use RestartBytes.
	RestartEntries                                              uint64 `json:",omitempty"`
	ChunkBytes, RestartBytes, IndexBlockBytes, InlineIndexBytes uint64
	MaxIndexMetaBytes, MaxRecordBytes                           uint64
}

func DefaultHistoryLayout() HistoryLayout {
	return HistoryLayout{ChunkBytes: 32 << 10, RestartBytes: 256, IndexBlockBytes: 4096, InlineIndexBytes: 64, MaxIndexMetaBytes: 4 << 20, MaxRecordBytes: 4 << 20}
}
func (l HistoryLayout) ForFormat(format string) HistoryLayout {
	if format == PackedFormat && l.RestartEntries == 0 {
		l.RestartEntries = 16
	}
	return l
}
func (l HistoryLayout) Validate() error {
	if !l.valid() {
		return errors.New("invalid indexed history layout")
	}
	return nil
}
func (l HistoryLayout) valid() bool {
	return l.RestartEntries <= 256 && l.ChunkBytes >= 128 && l.ChunkBytes <= 1<<20 && l.RestartBytes >= 16 && l.RestartBytes <= l.ChunkBytes && l.RestartBytes <= l.IndexBlockBytes && l.IndexBlockBytes >= 64 && l.IndexBlockBytes <= 1<<20 && l.InlineIndexBytes <= l.IndexBlockBytes && l.MaxIndexMetaBytes >= 64 && l.MaxIndexMetaBytes <= 64<<20 && l.MaxRecordBytes >= l.ChunkBytes && l.MaxRecordBytes <= 64<<20
}
func indexKey(k []byte) []byte { return append([]byte{'X'}, k...) }
func indexBlockKey(k []byte, n uint64) []byte {
	return binary.AppendUvarint(append([]byte{'Y'}, k...), n)
}
func varBytes(b []byte, n uint64) []byte { return binary.AppendUvarint(b, n) }
func takeBytes(b *[]byte, max uint64) ([]byte, error) {
	n, e := takeVar(b)
	if e != nil || n > max || n > uint64(len(*b)) {
		return nil, ErrCorrupt
	}
	v := (*b)[:int(n)]
	*b = (*b)[int(n):]
	return v, nil
}
func putBytes(b, v []byte) []byte { b = varBytes(b, uint64(len(v))); return append(b, v...) }

type indexDesc struct{ max, count uint64 }
type versionIndex struct {
	inline []uint64
	desc   []indexDesc
}

func (x versionIndex) count() (n uint64) {
	if x.inline != nil {
		return uint64(len(x.inline))
	}
	for _, d := range x.desc {
		n += d.count
	}
	return
}
func (x versionIndex) last() uint64 {
	if x.inline != nil {
		return x.inline[len(x.inline)-1]
	}
	return x.desc[len(x.desc)-1].max
}

// ID blocks are split into independently decoded runs. A run starts with an
// absolute ID; the remaining IDs are positive base-128 deltas. The directory
// stores max/count/byte length once per run, enabling bounded restart search.
func encodeIDs(ids []uint64, restart uint64) []byte {
	type run struct {
		data       []byte
		max, count uint64
	}
	var runs []run
	var cur run
	var prev uint64
	for _, id := range ids {
		d := id
		if cur.count > 0 {
			d = id - prev
		}
		v := varBytes(nil, d)
		if cur.count > 0 && uint64(len(cur.data)+len(v)) > restart {
			runs = append(runs, cur)
			cur = run{}
			v = varBytes(nil, id)
		}
		cur.data = append(cur.data, v...)
		cur.count++
		cur.max = id
		prev = id
	}
	if cur.count > 0 {
		runs = append(runs, cur)
	}
	out := varBytes(nil, uint64(len(runs)))
	for _, r := range runs {
		out = varBytes(out, r.max)
		out = varBytes(out, r.count)
		out = varBytes(out, uint64(len(r.data)))
	}
	for _, r := range runs {
		out = append(out, r.data...)
	}
	return out
}

type idRun struct {
	max, count, length uint64
}

func decodeIDs(b []byte, capBytes uint64) ([]uint64, error) {
	if uint64(len(b)) > capBytes {
		return nil, ErrHistoryLimit
	}
	n, e := takeVar(&b)
	if e != nil || n == 0 || n > uint64(len(b))/3 {
		return nil, ErrCorrupt
	}
	runs := make([]idRun, 0, int(n))
	var total, last, count uint64
	for i := uint64(0); i < n; i++ {
		m, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		c, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		l, e := takeVar(&b)
		if e != nil || m <= last || c == 0 || c > l || l > capBytes-total || c > math.MaxUint64-count {
			return nil, ErrCorrupt
		}
		runs = append(runs, idRun{max: m, count: c, length: l})
		total += l
		count += c
		last = m
	}
	if total != uint64(len(b)) {
		return nil, ErrCorrupt
	}
	ids := make([]uint64, 0, int(count))
	var previous uint64
	for _, run := range runs {
		rest := b[:run.length]
		b = b[run.length:]
		var id uint64
		for j := uint64(0); j < run.count; j++ {
			d, e := takeVar(&rest)
			if e != nil || d == 0 {
				return nil, ErrCorrupt
			}
			if j == 0 {
				id = d
			} else {
				if d > math.MaxUint64-id {
					return nil, ErrCorrupt
				}
				id += d
			}
			if id <= previous {
				return nil, ErrCorrupt
			}
			ids = append(ids, id)
			previous = id
		}
		if len(rest) != 0 || id != run.max {
			return nil, ErrCorrupt
		}
	}
	return ids, nil
}
func seekIDs(b []byte, target, capBytes uint64) (uint64, bool, error) {
	ids, e := decodeIDs(b, capBytes)
	if e != nil {
		return 0, false, e
	}
	i := sort.Search(len(ids), func(i int) bool { return ids[i] > target })
	if i == len(ids) {
		return 0, false, nil
	}
	return ids[i], true, nil
}
func encodeIndex(x versionIndex, l HistoryLayout) []byte {
	if x.inline != nil {
		return append([]byte{0}, encodeIDs(x.inline, l.RestartBytes)...)
	}
	b := []byte{1}
	var prev uint64
	for _, d := range x.desc {
		b = varBytes(b, d.max-prev)
		b = varBytes(b, d.count)
		prev = d.max
	}
	return b
}
func decodeIndex(b []byte, l HistoryLayout) (versionIndex, error) {
	var x versionIndex
	if len(b) < 2 || uint64(len(b)) > l.MaxIndexMetaBytes {
		return x, ErrCorrupt
	}
	mode := b[0]
	b = b[1:]
	if mode == 0 {
		ids, e := decodeIDs(b, l.IndexBlockBytes)
		if e != nil {
			return x, e
		}
		x.inline = ids
		return x, nil
	}
	if mode != 1 {
		return x, ErrCorrupt
	}
	var max, count uint64
	for len(b) > 0 {
		d, e := takeVar(&b)
		if e != nil || d == 0 || d > math.MaxUint64-max {
			return x, ErrCorrupt
		}
		c, e := takeVar(&b)
		if e != nil || c == 0 || c > l.IndexBlockBytes || c > math.MaxUint64-count {
			return x, ErrCorrupt
		}
		max += d
		count += c
		x.desc = append(x.desc, indexDesc{max, c})
	}
	return x, nil
}
func (r reader) readIndex(k []byte) (versionIndex, bool, error) {
	b, e := r.get(indexKey(k))
	if errors.Is(e, kvstore.ErrNotFound) {
		return versionIndex{}, false, nil
	}
	if e != nil {
		return versionIndex{}, false, e
	}
	x, e := decodeIndex(b, r.layout)
	return x, true, e
}

// A recovery cursor decodes X once and retains only the current bounded ID
// block. Traversing a delta chain does not re-read/re-validate it at every step.
type versionCursor struct {
	r      reader
	key    []byte
	x      versionIndex
	exists bool
	block  int
	ids    []uint64
}

func (r reader) versionCursor(k []byte) (*versionCursor, error) {
	x, exists, e := r.readIndex(k)
	if e != nil {
		return nil, e
	}
	return &versionCursor{r: r, key: k, x: x, exists: exists, block: -1}, nil
}
func (c *versionCursor) seekGT(target uint64) (uint64, bool, error) {
	if e := c.r.ctx.Err(); e != nil {
		return 0, false, e
	}
	if !c.exists {
		return 0, false, nil
	}
	x := c.x
	if x.inline != nil {
		i := sort.Search(len(x.inline), func(i int) bool { return x.inline[i] > target })
		if i == len(x.inline) {
			return 0, false, nil
		}
		return x.inline[i], true, nil
	}
	i := sort.Search(len(x.desc), func(i int) bool { return x.desc[i].max > target })
	if i == len(x.desc) {
		return 0, false, nil
	}
	if c.block != i {
		b, e := c.r.get(indexBlockKey(c.key, uint64(i)))
		if e != nil {
			return 0, false, errors.Join(ErrCorrupt, e)
		}
		ids, e := decodeIDs(b, c.r.layout.IndexBlockBytes)
		if e != nil || uint64(len(ids)) != x.desc[i].count || ids[len(ids)-1] != x.desc[i].max {
			return 0, false, ErrCorrupt
		}
		if i > 0 && ids[0] <= x.desc[i-1].max {
			return 0, false, ErrCorrupt
		}
		c.ids = ids
		c.block = i
	}
	j := sort.Search(len(c.ids), func(i int) bool { return c.ids[i] > target })
	if j == len(c.ids) {
		return 0, false, ErrCorrupt
	}
	return c.ids[j], true, nil
}
func (r reader) nextVersion(k []byte, target uint64) (uint64, bool, error) {
	c, e := r.versionCursor(k)
	if e != nil {
		return 0, false, e
	}
	return c.seekGT(target)
}
func (r reader) appendVersion(k []byte, id uint64, put func([]byte, []byte) error) (uint64, error) {
	x, exists, e := r.readIndex(k)
	if e != nil {
		return 0, e
	}
	if id == 0 || (exists && id <= x.last()) || x.count() == math.MaxUint64 {
		return 0, ErrCorrupt
	}
	ordinal := x.count() + 1
	if !exists {
		x.inline = []uint64{id}
	} else if x.inline != nil {
		x.inline = append(x.inline, id)
	} else {
		i := len(x.desc) - 1
		b, e := r.get(indexBlockKey(k, uint64(i)))
		if e != nil {
			return 0, errors.Join(ErrCorrupt, e)
		}
		ids, e := decodeIDs(b, r.layout.IndexBlockBytes)
		if e != nil || uint64(len(ids)) != x.desc[i].count || ids[len(ids)-1] != x.desc[i].max {
			return 0, ErrCorrupt
		}
		if i > 0 && ids[0] <= x.desc[i-1].max {
			return 0, ErrCorrupt
		}
		ids = append(ids, id)
		wire := encodeIDs(ids, r.layout.RestartBytes)
		if uint64(len(wire)) > r.layout.IndexBlockBytes {
			i++
			ids = []uint64{id}
			wire = encodeIDs(ids, r.layout.RestartBytes)
			x.desc = append(x.desc, indexDesc{})
		}
		x.desc[i] = indexDesc{id, uint64(len(ids))}
		if e = put(indexBlockKey(k, uint64(i)), wire); e != nil {
			return 0, e
		}
	}
	if x.inline != nil && uint64(len(encodeIndex(x, r.layout))) > r.layout.InlineIndexBytes {
		ids := x.inline
		x.inline = nil
		x.desc = nil
		// A full inline block can overflow on the append which converts it.
		// Split at the last ID instead of rejecting an otherwise valid append.
		parts := [][]uint64{ids}
		if uint64(len(encodeIDs(ids, r.layout.RestartBytes))) > r.layout.IndexBlockBytes {
			if len(ids) < 2 {
				return 0, ErrHistoryLimit
			}
			parts = [][]uint64{ids[:len(ids)-1], ids[len(ids)-1:]}
		}
		for i, part := range parts {
			wire := encodeIDs(part, r.layout.RestartBytes)
			if uint64(len(wire)) > r.layout.IndexBlockBytes {
				return 0, ErrHistoryLimit
			}
			x.desc = append(x.desc, indexDesc{part[len(part)-1], uint64(len(part))})
			if e = put(indexBlockKey(k, uint64(i)), wire); e != nil {
				return 0, e
			}
		}

	}
	wire := encodeIndex(x, r.layout)
	if uint64(len(wire)) > r.layout.MaxIndexMetaBytes {
		return 0, ErrHistoryLimit
	}
	if e = put(indexKey(k), wire); e != nil {
		return 0, e
	}
	return ordinal, nil
}

// Exact verification is used by Audit; point readers need only one candidate.
func (r reader) allVersions(k []byte) ([]uint64, error) {
	x, ok, e := r.readIndex(k)
	if e != nil || !ok {
		return nil, e
	}
	if x.inline != nil {
		return x.inline, nil
	}
	var ids []uint64
	for i, d := range x.desc {
		b, e := r.get(indexBlockKey(k, uint64(i)))
		if e != nil {
			return nil, errors.Join(ErrCorrupt, e)
		}
		part, e := decodeIDs(b, r.layout.IndexBlockBytes)
		if e != nil || uint64(len(part)) != d.count || part[len(part)-1] != d.max {
			return nil, ErrCorrupt
		}
		if len(ids) > 0 && part[0] <= ids[len(ids)-1] {
			return nil, ErrCorrupt
		}
		ids = append(ids, part...)
	}
	return ids, nil
}
