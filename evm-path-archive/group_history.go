package patharchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

// Each immutable group is a restart point, with at most HistoryGroupEntries
// prefix-compressed entries. Indices are bounded height-window append lists.
type historyEntry struct{ key, value []byte }
type historyLocation struct{ height, chunk uint64 }

func takeVar(b *[]byte) (uint64, error) {
	n, u := binary.Uvarint(*b)
	if u <= 0 || u != len(binary.AppendUvarint(nil, n)) {
		return 0, ErrCorrupt
	}
	*b = (*b)[u:]
	return n, nil
}
func readOrdered(b []byte) (uint64, error) {
	if len(b) == 0 || b[0] > 8 || len(b) != int(b[0])+1 || (b[0] > 0 && b[1] == 0) {
		return 0, ErrCorrupt
	}
	var n uint64
	for _, v := range b[1:] {
		n = n<<8 | uint64(v)
	}
	return n, nil
}
func ownerEnd(k []byte) (int, error) {
	if len(k) < 2 || (k[0] != 'P' && k[0] != 'F') {
		return 0, ErrCorrupt
	}
	n := 2
	if k[1] == 1 {
		n = 34
	} else if k[1] != 0 {
		return 0, ErrCorrupt
	}
	if len(k) < n {
		return 0, ErrCorrupt
	}
	return n, nil
}
func validHistoryKey(k []byte) error {
	n, e := ownerEnd(k)
	if e != nil {
		return e
	}
	p := k[n:]
	if k[0] == 'F' {
		if len(p) != 32 {
			return ErrCorrupt
		}
		return nil
	}
	if len(p) < 1 || p[0] > 64 || len(p) != 1+(int(p[0])+1)/2 {
		return ErrCorrupt
	}
	if p[0]%2 == 1 && p[len(p)-1]&15 != 0 {
		return ErrCorrupt
	}
	return nil
}
func groupKey(owner []byte, h, chunk uint64) []byte {
	b := append([]byte{'G'}, owner...)
	b = append(b, orderedHeight(h)...)
	return binary.AppendUvarint(b, chunk)
}
func indexPrefix(k []byte) []byte { return append([]byte{'J'}, k...) }
func encodeGroup(entries []historyEntry, ownerLen int) []byte {
	var out, prev []byte
	for _, e := range entries {
		key := e.key[ownerLen:]
		shared := 0
		for shared < len(prev) && shared < len(key) && prev[shared] == key[shared] {
			shared++
		}
		out = binary.AppendUvarint(out, uint64(shared))
		out = binary.AppendUvarint(out, uint64(len(key)-shared))
		out = binary.AppendUvarint(out, uint64(len(e.value)))
		out = append(out, key[shared:]...)
		out = append(out, e.value...)
		prev = key
	}
	return out
}
func decodeGroup(owner, b []byte, limit uint64) ([]historyEntry, error) {
	var result []historyEntry
	var prev []byte
	for len(b) > 0 {
		if uint64(len(result)) >= limit {
			return nil, ErrHistoryLimit
		}
		shared, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		suffix, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		size, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		if shared > uint64(len(prev)) || suffix > 65 || shared+suffix > 65 || suffix > uint64(len(b)) || size > uint64(len(b))-suffix {
			return nil, ErrCorrupt
		}
		key := append(bytes.Clone(prev[:int(shared)]), b[:int(suffix)]...)
		b = b[int(suffix):]
		if len(result) > 0 && bytes.Compare(prev, key) >= 0 {
			return nil, ErrCorrupt
		}
		full := append(bytes.Clone(owner), key...)
		if e = validHistoryKey(full); e != nil {
			return nil, e
		}
		result = append(result, historyEntry{full, bytes.Clone(b[:int(size)])})
		b = b[int(size):]
		prev = key
	}
	if len(result) == 0 {
		return nil, ErrCorrupt
	}
	return result, nil
}
func decodeLocations(b []byte, bucket, span uint64) ([]historyLocation, error) {
	if span == 0 || bucket > ^uint64(0)/span {
		return nil, ErrCorrupt
	}
	base := bucket * span
	var out []historyLocation
	var off uint64
	for len(b) > 0 {
		if uint64(len(out)) >= span {
			return nil, ErrCorrupt
		}
		delta, e := takeVar(&b)
		if e != nil || delta == 0 || delta > span-off {
			return nil, ErrCorrupt
		}
		off += delta
		chunk, e := takeVar(&b)
		if e != nil {
			return nil, e
		}
		if base > ^uint64(0)-(off-1) {
			return nil, ErrCorrupt
		}
		out = append(out, historyLocation{base + off - 1, chunk})
	}
	if len(out) == 0 {
		return nil, ErrCorrupt
	}
	return out, nil
}
func (s *Store) writeGroups(ctx context.Context, pending map[string][]byte, h uint64, put func([]byte, []byte) error, stats *Changes) error {
	groups := map[string][]historyEntry{}
	for k, v := range pending {
		key := []byte(k)
		if e := validHistoryKey(key); e != nil {
			return e
		}
		n, _ := ownerEnd(key)
		groups[string(key[:n])] = append(groups[string(key[:n])], historyEntry{key, v})
	}
	owners := make([]string, 0, len(groups))
	for o := range groups {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	for _, owner := range owners {
		entries := groups[owner]
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		for start := 0; start < len(entries); start += int(s.cfg.HistoryGroupEntries) {
			if e := ctx.Err(); e != nil {
				return e
			}
			end := start + int(s.cfg.HistoryGroupEntries)
			if end > len(entries) {
				end = len(entries)
			}
			chunk := uint64(start) / s.cfg.HistoryGroupEntries
			part := entries[start:end]
			gk := groupKey([]byte(owner), h, chunk)
			value := encodeGroup(part, len(owner))
			if uint64(len(gk)+len(value)) > s.cfg.MaxHistoryDecodeBytes {
				return ErrHistoryLimit
			}
			if e := put(gk, value); e != nil {
				return e
			}
			stats.ReverseBytes += uint64(len(gk) + len(value))
			if owner[0] == 'P' {
				n := len(gk) + len(value)
				for _, entry := range part {
					n -= len(entry.value)
				}
				stats.History.KeyBytes += uint64(n)
			}
			for _, entry := range part {
				bucket := h / s.cfg.HistoryIndexSpan
				ik := append(indexPrefix(entry.key), orderedHeight(bucket)...)
				old, e := s.db.Get(ik)
				if e != nil && !errors.Is(e, kvstore.ErrNotFound) {
					return e
				}
				offset := uint64(0)
				if len(old) > 0 {
					loc, e := decodeLocations(old, bucket, s.cfg.HistoryIndexSpan)
					if e != nil {
						return e
					}
					last := loc[len(loc)-1].height
					if last >= h {
						return ErrCorrupt
					}
					offset = last%s.cfg.HistoryIndexSpan + 1
				}
				value := binary.AppendUvarint(bytes.Clone(old), h%s.cfg.HistoryIndexSpan+1-offset)
				value = binary.AppendUvarint(value, chunk)
				if e = put(ik, value); e != nil {
					return e
				}
				stats.IndexBytes += uint64(len(ik) + len(value))
			}
		}
	}
	return nil
}
func (r reader) historyIterator(k []byte) iterator {
	if r.format != Format {
		return r.iter(prefixRange(r.historyPrefix(k)))
	}
	return &groupCursor{r: r, key: bytes.Clone(k), prefix: indexPrefix(k)}
}

type groupCursor struct {
	r               reader
	key, prefix     []byte
	it              iterator
	locations       []historyLocation
	pos             int
	data, synthetic []byte
	err             error
	cost            uint64
}

func (c *groupCursor) First() bool { return c.seekHeight(0) }
func (c *groupCursor) Seek(key []byte) bool {
	prefix := c.r.historyPrefix(c.key)
	if !bytes.HasPrefix(key, prefix) {
		c.err = ErrCorrupt
		return false
	}
	h, e := readOrdered(key[len(prefix):])
	if e != nil {
		c.err = e
		return false
	}
	return c.seekHeight(h)
}
func (c *groupCursor) seekHeight(h uint64) bool {
	if c.it != nil {
		c.it.Release()
	}
	c.err = nil
	c.cost = 0
	if c.r.indexSpan == 0 {
		c.err = ErrCorrupt
		return false
	}
	c.it = c.r.iter(prefixRange(c.prefix))
	if !c.it.Seek(append(bytes.Clone(c.prefix), orderedHeight(h/c.r.indexSpan)...)) {
		c.err = c.it.Error()
		return false
	}
	for {
		if !c.loadIndex() {
			return false
		}
		c.pos = sort.Search(len(c.locations), func(i int) bool { return c.locations[i].height >= h })
		if c.pos < len(c.locations) {
			return c.loadRecord()
		}
		if !c.it.Next() {
			c.err = c.it.Error()
			return false
		}
	}
}
func (c *groupCursor) loadIndex() bool {
	if e := c.r.ctx.Err(); e != nil {
		c.err = e
		return false
	}
	if c.r.metrics != nil {
		c.r.metrics.indexReads.Add(1)
	}
	bucket, e := readOrdered(c.it.Key()[len(c.prefix):])
	if e != nil {
		c.err = e
		return false
	}
	c.cost += uint64(len(c.it.Key()) + len(c.it.Value()))
	if c.cost > c.r.byteLimit {
		c.err = ErrHistoryLimit
		return false
	}
	c.locations, e = decodeLocations(c.it.Value(), bucket, c.r.indexSpan)
	c.err = e
	return e == nil
}
func (c *groupCursor) loadRecord() bool {
	if e := c.r.ctx.Err(); e != nil {
		c.err = e
		return false
	}
	loc := c.locations[c.pos]
	n, e := ownerEnd(c.key)
	if e != nil {
		c.err = e
		return false
	}
	gk := groupKey(c.key[:n], loc.height, loc.chunk)
	if c.r.metrics != nil {
		c.r.metrics.groupReads.Add(1)
	}
	b, e := c.r.get(gk)
	if e != nil {
		c.err = errors.Join(ErrCorrupt, e)
		return false
	}
	c.cost += uint64(len(gk) + len(b))
	if c.cost > c.r.byteLimit {
		c.err = ErrHistoryLimit
		return false
	}
	entries, e := decodeGroup(c.key[:n], b, c.r.groupEntries)
	if e != nil {
		c.err = e
		return false
	}
	i := sort.Search(len(entries), func(i int) bool { return bytes.Compare(entries[i].key, c.key) >= 0 })
	if i == len(entries) || !bytes.Equal(entries[i].key, c.key) {
		c.err = ErrCorrupt
		return false
	}
	c.data = entries[i].value
	c.synthetic = append(c.r.historyPrefix(c.key), orderedHeight(loc.height)...)
	return true
}
func (c *groupCursor) Next() bool {
	if c.err != nil || c.it == nil {
		return false
	}
	c.pos++
	if c.pos < len(c.locations) {
		return c.loadRecord()
	}
	if !c.it.Next() {
		c.err = c.it.Error()
		return false
	}
	if !c.loadIndex() {
		return false
	}
	c.pos = 0
	return c.loadRecord()
}
func (c *groupCursor) Key() []byte   { return c.synthetic }
func (c *groupCursor) Value() []byte { return c.data }
func (c *groupCursor) Error() error  { return c.err }
func (c *groupCursor) Release() {
	if c.it != nil {
		c.it.Release()
		c.it = nil
	}
}
func (c *groupCursor) ReadCost() uint64 { n := c.cost; c.cost = 0; return n }

func parseGroupKey(k []byte) (owner []byte, h, chunk uint64, err error) {
	if len(k) < 2 || k[0] != 'G' {
		err = ErrCorrupt
		return
	}
	end, e := ownerEnd(k[1:])
	if e != nil {
		err = e
		return
	}
	end++
	owner = bytes.Clone(k[1:end])
	if len(k) <= end {
		err = ErrCorrupt
		return
	}
	n := int(k[end]) + 1
	if n > 9 || len(k) < end+n {
		err = ErrCorrupt
		return
	}
	h, err = readOrdered(k[end : end+n])
	if err != nil {
		return
	}
	b := k[end+n:]
	chunk, err = takeVar(&b)
	if err == nil && len(b) != 0 {
		err = ErrCorrupt
	}
	return
}
func parseIndexKey(k []byte) (key []byte, bucket uint64, err error) {
	if len(k) < 2 || k[0] != 'J' {
		err = ErrCorrupt
		return
	}
	end, e := ownerEnd(k[1:])
	if e != nil {
		err = e
		return
	}
	end++
	if k[1] == 'F' {
		end += 32
	} else {
		if len(k) <= end {
			err = ErrCorrupt
			return
		}
		end += 1 + (int(k[end])+1)/2
	}
	if len(k) <= end {
		err = ErrCorrupt
		return
	}
	key = bytes.Clone(k[1:end])
	err = validHistoryKey(key)
	if err != nil {
		return
	}
	bucket, err = readOrdered(k[end:])
	return
}
func (s *Store) auditGroups(ctx context.Context) error {
	if s.cfg.Format != Format {
		return nil
	}
	it := s.db.NewIterator(prefixRange([]byte{'G'}))
	defer it.Release()
	for it.Next() {
		if e := ctx.Err(); e != nil {
			return e
		}
		owner, h, chunk, e := parseGroupKey(it.Key())
		if e != nil {
			return e
		}
		if h <= s.cfg.initial() || h > s.head.Height {
			return ErrCorrupt
		}
		entries, e := decodeGroup(owner, it.Value(), s.cfg.HistoryGroupEntries)
		if e != nil {
			return e
		}
		for _, entry := range entries {
			ik := append(indexPrefix(entry.key), orderedHeight(h/s.cfg.HistoryIndexSpan)...)
			b, e := s.db.Get(ik)
			if e != nil {
				return errors.Join(ErrCorrupt, e)
			}
			locations, e := decodeLocations(b, h/s.cfg.HistoryIndexSpan, s.cfg.HistoryIndexSpan)
			if e != nil {
				return e
			}
			i := sort.Search(len(locations), func(i int) bool { return locations[i].height >= h })
			if i == len(locations) || locations[i] != (historyLocation{h, chunk}) {
				return ErrCorrupt
			}
		}
	}
	if e := it.Error(); e != nil {
		return e
	}
	index := s.db.NewIterator(prefixRange([]byte{'J'}))
	defer index.Release()
	for index.Next() {
		if e := ctx.Err(); e != nil {
			return e
		}
		key, bucket, e := parseIndexKey(index.Key())
		if e != nil {
			return e
		}
		locations, e := decodeLocations(index.Value(), bucket, s.cfg.HistoryIndexSpan)
		if e != nil {
			return e
		}
		r := s.live(ctx)
		cursor := &groupCursor{r: r, key: key, prefix: indexPrefix(key), locations: locations}
		for i := range locations {
			cursor.pos = i
			cursor.cost = 0
			if !cursor.loadRecord() {
				return cursor.err
			}
		}
	}
	return index.Error()
}
