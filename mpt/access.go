package mpt

// TrieAccessObserver receives one event for each node visited by a logical
// Get/Update/Delete, including COW nodes already in memory. Hash, proof, diff
// and physical reads do not emit events. Paths are borrowed for the call.
// Observers follow Trie's single-owner mutation contract and must not reenter it.
type TrieAccessObserver interface{ ObserveTrieAccess(path []byte) }

func accessObserver(s Store) TrieAccessObserver { o, _ := s.(TrieAccessObserver); return o }

// SetAccessObserver also supports newly constructed memory tries. Copy carries
// the observer, while independently opened historical readers have none.
func (t *Trie) SetAccessObserver(o TrieAccessObserver) { t.observer = o }
