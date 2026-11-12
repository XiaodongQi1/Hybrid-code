package patharchive

import (
	"fmt"
)

// HistoryClass accounts actual persisted D/P bytes, not put volume or disk size.
type HistoryClass struct{ Records, KeyBytes, ValueBytes uint64 }

func (a *Archive) HistoryInventory() (map[string]HistoryClass, error) {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	out := map[string]HistoryClass{}
	if IsIndexedFormat(s.cfg.Format) {
		return s.indexedInventory()
	}
	if s.cfg.Format == LegacyFormat {
		return out, nil
	}
	if s.cfg.Format == Format {
		return s.groupInventory()
	}
	it := s.db.NewIterator(prefixRange([]byte{'D', 'P'}))
	defer it.Release()
	for it.Next() {
		k, v := it.Key(), it.Value()
		if len(k) < 4 {
			return nil, ErrCorrupt
		}
		owner := "account"
		if k[2] == 1 {
			owner = "storage"
		} else if k[2] != 0 {
			return nil, ErrCorrupt
		}
		kind, record := "absent", "ABSENT"
		blob := v
		if isDifferential(s.cfg.Format) {
			if len(v) == 0 {
				return nil, ErrCorrupt
			}
			switch v[0] & 3 {
			case absentNode:
				if len(v) != 1 || v[0] != 0 {
					return nil, ErrCorrupt
				}
			case fullNode:
				record = "FULL"
				blob = v[1:]
			case deltaNode:
				record = "DELTA"
				kind = "short"
				if v[0]&4 != 0 {
					kind = "branch"
				}
			default:
				return nil, ErrCorrupt
			}
		} else if len(v) > 0 {
			record = "FULL"
		}
		if record == "FULL" {
			e, err := nodeElements(blob)
			if err != nil {
				return nil, err
			}
			kind = "short"
			if len(e) == 17 {
				kind = "branch"
			}
		}
		key := fmt.Sprintf("%s/%s/%s", owner, kind, record)
		n := out[key]
		n.Records++
		n.KeyBytes += uint64(len(k))
		n.ValueBytes += uint64(len(v))
		out[key] = n
	}
	return out, it.Error()
}

func (s *Store) groupInventory() (map[string]HistoryClass, error) {
	out := map[string]HistoryClass{}
	it := s.db.NewIterator(prefixRange([]byte{'G', 'P'}))
	defer it.Release()
	for it.Next() {
		owner, _, _, e := parseGroupKey(it.Key())
		if e != nil {
			return nil, e
		}
		entries, e := decodeGroup(owner, it.Value(), s.cfg.HistoryGroupEntries)
		if e != nil {
			return nil, e
		}
		overhead := len(it.Key()) + len(it.Value())
		for _, entry := range entries {
			overhead -= len(entry.value)
		}
		for i, entry := range entries {
			v := entry.value
			if len(v) == 0 {
				return nil, ErrCorrupt
			}
			scope := "account"
			if owner[1] == 1 {
				scope = "storage"
			}
			kind, record := "absent", "ABSENT"
			switch v[0] & 3 {
			case absentNode:
				if _, e = restoreNodeHistory(v, nil); e != nil {
					return nil, e
				}
			case fullNode:
				b, e := restoreNodeHistory(v, nil)
				if e != nil {
					return nil, e
				}
				elems, e := nodeElements(b)
				if e != nil {
					return nil, e
				}
				kind = "short"
				if len(elems) == 17 {
					kind = "branch"
				}
				record = "FULL"
			case deltaNode:
				record = "DELTA"
				kind = "short"
				if v[0]&4 != 0 {
					kind = "branch"
				}
			default:
				return nil, ErrCorrupt
			}
			key := fmt.Sprintf("%s/%s/%s", scope, kind, record)
			n := out[key]
			n.Records++
			n.ValueBytes += uint64(len(v))
			if i == 0 {
				n.KeyBytes += uint64(overhead)
			}
			out[key] = n
		}
	}
	return out, it.Error()
}

// Shared history framing is charged once to the first node in each chunk. The
// class totals include trie directories; index blocks have their own KV class.
func (s *Store) indexedInventory() (map[string]HistoryClass, error) {
	out := map[string]HistoryClass{}
	it := s.db.NewIterator(prefixRange([]byte{'H'}))
	defer it.Release()
	first := map[string]string{}
	for it.Next() {
		k, v := it.Key(), it.Value()
		if len(k) < 11 || k[9] > 3 {
			return nil, ErrCorrupt
		}
		if k[9] < 2 {
			continue
		}
		rows, e := decodeChunk(v, *s.cfg.HistoryLayout)
		if e != nil {
			return nil, e
		}
		overhead := uint64(len(k) + len(v))
		for _, row := range rows {
			overhead -= uint64(len(row.value))
		}
		for i, row := range rows {
			class, e := indexedNodeClass(row.key, row.value)
			if e != nil {
				return nil, e
			}
			n := out[class]
			n.Records++
			n.ValueBytes += uint64(len(row.value))
			if i == 0 {
				n.KeyBytes += overhead
				directory := string(append([]byte{'R'}, k[1:10]...))
				if _, ok := first[directory]; !ok {
					first[directory] = class
				}
			}
			out[class] = n
		}
	}
	if e := it.Error(); e != nil {
		return nil, e
	}
	for key, class := range first {
		v, e := s.db.Get([]byte(key))
		if e != nil {
			return nil, e
		}
		n := out[class]
		n.KeyBytes += uint64(len(key) + len(v))
		out[class] = n
	}
	return out, nil
}
func indexedNodeClass(k, v []byte) (string, error) {
	if len(v) == 0 {
		return "", ErrCorrupt
	}
	scope := "account"
	if k[1] == 1 {
		scope = "storage"
	}
	kind, record := "absent", "ABSENT"
	switch v[0] & 3 {
	case absentNode:
		if len(v) != 1 || v[0] != 0 {
			return "", ErrCorrupt
		}
	case fullNode:
		b, e := restoreNodeHistory(v, nil)
		if e != nil {
			return "", e
		}
		elems, e := nodeElements(b)
		if e != nil {
			return "", e
		}
		kind = "short"
		if len(elems) == 17 {
			kind = "branch"
		}
		record = "FULL"
	case deltaNode:
		kind = "short"
		if v[0]&4 != 0 {
			kind = "branch"
		}
		record = "DELTA"
	default:
		return "", ErrCorrupt
	}
	return fmt.Sprintf("%s/%s/%s", scope, kind, record), nil
}
