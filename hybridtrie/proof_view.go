package hybridtrie

import "sync"

// ProofViewAt pins a historical snapshot for one logical query. Account and
// storage nodes both live in the Prefix column and share one lazy iterator.
// The root directory and lifecycle records use the separate metadata column.
func (s *Store) ProofViewAt(height uint64) (*View, error) {
	v, err := s.ViewAt(height)
	if err == nil {
		if source, ok := v.snapshot.(predecessorSource); ok {
			v.snapshot = &proofReadSnapshot{diskSnapshot: v.snapshot, source: source}
		}
	}
	return v, err
}

type proofReadSnapshot struct {
	diskSnapshot
	source  predecessorSource
	cursors [2]proofColumnCursor // metaColumn and prefixColumn
}

type proofColumnCursor struct {
	mu    sync.Mutex
	owned diskPredecessor
}

// Independent cursors for parallel local proof readers. Preserve any wrappers
// around the snapshot instead of extracting their underlying native database.
func (s *proofReadSnapshot) newPredecessor(cf column) (diskPredecessor, error) {
	return s.source.newPredecessor(cf)
}

func (s *proofReadSnapshot) borrow(cf column) (diskPredecessor, bool, error) {
	c := &s.cursors[cf]
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.owned != nil {
		return c, false, nil
	}
	it, err := s.source.newPredecessor(cf)
	if err != nil {
		return nil, false, err
	}
	if it == nil {
		return nil, false, ErrCorrupt
	}
	c.owned = it
	return c, true, nil
}

func (c *proofColumnCursor) prev(key []byte) ([]byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.owned.prev(key)
}

func (c *proofColumnCursor) prevLimited(key []byte, limit int) ([]byte, []byte, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return readLimited(c.owned, key, limit)
}

// A single trie walk releases only the borrow. View.Close owns the iterators.
func (*proofColumnCursor) close() {}

func (s *proofReadSnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	if cf != metaColumn && cf != prefixColumn {
		return s.diskSnapshot.prev(cf, key)
	}
	it, _, err := s.borrow(cf)
	if err != nil {
		return nil, nil, err
	}
	return it.prev(key)
}

func (s *proofReadSnapshot) close() {
	// View.Close holds the exclusive view lock: all reads have finished.
	for i := range s.cursors {
		if c := s.cursors[i].owned; c != nil {
			c.close()
		}
	}
	s.diskSnapshot.close()
}
