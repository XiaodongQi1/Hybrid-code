package hybridtrie

// The engine contract keeps column-family snapshots and batches indivisible.
// The shared kvstore supplies either RocksDB or GoLevelDB beneath this adapter.
type column uint8

const (
	metaColumn column = iota
	prefixColumn
	hashColumn
)

type entry struct {
	cf         column
	key, value []byte
}
type diskSnapshot interface {
	get(column, []byte) ([]byte, error)
	prev(column, []byte) ([]byte, []byte, error)
	close()
}

// Optional cursor capability. Wrappers that only implement diskSnapshot keep
// their prev hooks through the fallback adapter (important for fault injection).
// The native implementation owns one iterator; returned key/value bytes must
// remain valid after the next seek. The snapshot outlives every cursor.
type diskPredecessor interface {
	prev([]byte) ([]byte, []byte, error)
	close()
}

// Partial-value cursors return the full stored length for budget accounting.
type limitedPredecessor interface {
	prevLimited(key []byte, limit int) ([]byte, []byte, int, error)
}

func readLimited(cursor diskPredecessor, key []byte, limit int) ([]byte, []byte, int, error) {
	if p, ok := cursor.(limitedPredecessor); ok {
		return p.prevLimited(key, limit)
	}
	k, v, err := cursor.prev(key)
	size := len(v)
	if size > limit {
		v = v[:limit]
	}
	return k, v, size, err
}

type predecessorSource interface {
	newPredecessor(column) (diskPredecessor, error)
}
type fallbackPredecessor struct {
	snapshot diskSnapshot
	cf       column
	closed   bool
}

func (p *fallbackPredecessor) prev(key []byte) ([]byte, []byte, error) {
	if p.closed {
		return nil, nil, ErrClosed
	}
	return p.snapshot.prev(p.cf, key)
}
func (p *fallbackPredecessor) close() { p.closed = true }

func openPredecessor(s diskSnapshot, cf column) (diskPredecessor, bool, error) {
	if source, ok := s.(predecessorSource); ok {
		cursor, err := source.newPredecessor(cf)
		if err == nil && cursor == nil {
			return nil, true, ErrCorrupt
		}
		return cursor, true, err
	}
	return &fallbackPredecessor{snapshot: s, cf: cf}, false, nil
}

type diskEngine interface {
	snapshot() diskSnapshot
	write([]entry) error
	close() error
}

// rangeSource scans one native snapshot in comparator order with copied rows.
// Snapshot import uses it for immutable code/lifecycle metadata, never a map of
// the whole forest. The callback must not close the snapshot.
type rangeSource interface {
	scan(column, []byte, []byte, func([]byte, []byte) error) error
}
