package patharchive

import (
	"encoding/binary"
	"fmt"
)

// StorageInventory is an additive decomposition of actual encoded KV state.
// Expanded nodes, Put traffic and physical LSM compression are not included.
// Node payload includes codec flags. Framing includes keys and record lengths;
// Directory includes both external R tables and the in-H restart directory.
type StorageInventory struct {
	Schema                             string
	Components                         map[string]uint64
	StateBytes, OtherBytes, AllKVBytes uint64
}

func (a *Archive) StorageInventory() (StorageInventory, error) {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := StorageInventory{Schema: "path-logical-components/v1", Components: map[string]uint64{}}
	if e := s.check(); e != nil {
		return out, e
	}
	it := s.db.NewIterator(nil)
	defer it.Release()
	add := func(name string, n uint64) { out.Components[name] += n }
	payload := func(k, v []byte, legacy bool) error {
		if k[0] == 'F' {
			add("flat-history-payload", uint64(len(v)))
			return nil
		}
		if len(v) == 0 && !isDifferential(s.cfg.Format) {
			add("node-absent", 0)
			return nil
		}
		if !isDifferential(s.cfg.Format) && !legacy {
			add("node-full", uint64(len(v)))
			return nil
		}
		if len(v) == 0 {
			return ErrCorrupt
		}
		switch v[0] & 3 {
		case absentNode:
			add("node-absent", uint64(len(v)))
		case fullNode:
			add("node-full", uint64(len(v)))
		case deltaNode:
			if legacy {
				return ErrCorrupt
			}
			add("node-delta", uint64(len(v)))
		default:
			return ErrCorrupt
		}
		return nil
	}
	for it.Next() {
		k, v := it.Key(), it.Value()
		if len(k) == 0 {
			return out, ErrCorrupt
		}
		size := uint64(len(k) + len(v))
		out.AllKVBytes += size
		switch k[0] {
		case 'P':
			add("current-trie", size)
		case 'F':
			add("current-flat", size)
		case 'C':
			add("code", size)
		case 'B':
			if s.cfg.StateOnly {
				add("root-headers", size)
			} else {
				out.OtherBytes += size
			}
		case 'M':
			if s.cfg.StateOnly && string(k) != "M/config" {
				add("coordination", size)
			} else {
				out.OtherBytes += size
			}
		case 'Q':
			add("checkpoint-counters", size)
		case 'I', 'J', 'X', 'Y':
			add("version-index", size)
		case 'R':
			add("history-directory", size)
		case 'D':
			var ident []byte
			if s.cfg.Format == LegacyFormat {
				if len(k) < 3 {
					return out, ErrCorrupt
				}
				n := int(binary.BigEndian.Uint16(k[1:3]))
				if n == 0 || len(k) != 3+n+8 {
					return out, ErrCorrupt
				}
				ident = k[3 : 3+n]
			} else {
				if len(k) < 2 {
					return out, ErrCorrupt
				}
				ident = k[1:]
				n, e := indexedIdentEnd(ident)
				if e != nil || !validEncodedHeight(ident[n:]) {
					return out, ErrCorrupt
				}
				ident = ident[:n]
			}
			if ident[0] != 'P' && ident[0] != 'F' {
				return out, ErrCorrupt
			}
			add("history-framing", uint64(len(k)))
			if e := payload(ident, v, s.cfg.Format == LegacyFormat); e != nil {
				return out, e
			}
		case 'G':
			owner, _, _, e := parseGroupKey(k)
			if e != nil {
				return out, e
			}
			rows, e := decodeGroup(owner, v, s.cfg.HistoryGroupEntries)
			if e != nil {
				return out, e
			}
			overhead := size
			for _, row := range rows {
				overhead -= uint64(len(row.value))
				if e := payload(row.key, row.value, false); e != nil {
					return out, e
				}
			}
			add("history-framing", overhead)
		case 'H':
			if len(k) < 11 || k[9] > 3 {
				return out, ErrCorrupt
			}
			rows, e := decodeChunk(v, *s.cfg.HistoryLayout)
			if e != nil {
				return out, e
			}
			var directory uint64
			if s.cfg.Format == PackedFormat {
				_, directory, e = parsePackedChunk(v, *s.cfg.HistoryLayout)
			} else {
				var data []byte
				_, data, e = chunkRuns(v, *s.cfg.HistoryLayout)
				directory = uint64(len(v) - len(data))
			}
			if e != nil {
				return out, e
			}
			add("history-directory", directory)
			overhead := size - directory
			for _, row := range rows {
				overhead -= uint64(len(row.value))
				if e := payload(row.key, row.value, false); e != nil {
					return out, e
				}
			}
			add("history-framing", overhead)
		default:
			out.OtherBytes += size
		}
	}
	if e := it.Error(); e != nil {
		return out, e
	}
	for _, n := range out.Components {
		out.StateBytes += n
	}
	if out.StateBytes+out.OtherBytes != out.AllKVBytes {
		return out, fmt.Errorf("%w: inventory does not close", ErrCorrupt)
	}
	return out, nil
}
func validEncodedHeight(b []byte) bool {
	return len(b) > 0 && b[0] <= 8 && len(b) == int(b[0])+1 && (b[0] == 0 || b[1] != 0)
}
