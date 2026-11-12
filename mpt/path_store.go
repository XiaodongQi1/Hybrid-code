package mpt

// PathStore optionally resolves a node by its canonical nibble path. The store
// scopes the trie owner and state version. Hash-only stores remain supported.
// Callers still authenticate returned bytes against expectedHash.
type PathStore interface {
	GetNode(path []byte, expectedHash Hash) ([]byte, error)
}

func ReadNode(store Store, path []byte, expectedHash Hash) ([]byte, error) {
	if s, ok := store.(PathStore); ok {
		return s.GetNode(path, expectedHash)
	}
	return store.Get(expectedHash[:])
}
