package mpt

// Stage adds this trie's new nodes to a caller-owned atomic batch without
// writing it or changing the trie. The caller must stage all referenced trees
// and metadata into the same store's batch before Write. A failed batch can be
// retried; no root has been accepted or overlay discarded by this method.
func (t *Trie) Stage(batch Batch) (Hash, error) {
	prepared, err := t.prepareCommit()
	if err != nil {
		return Hash{}, err
	}
	if err := prepared.putNodes(batch); err != nil {
		return Hash{}, err
	}
	return prepared.root, nil
}
