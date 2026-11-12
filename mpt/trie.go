package mpt

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrNilStore    = errors.New("mpt: nil store")
	ErrMissingNode = errors.New("mpt: missing trie node")
)

// Trie is a synchronous Merkle Patricia Trie. Updates use copy-on-write, so a
// Copy and every previously committed root remain valid after later updates.
// Trie is not safe for concurrent mutation.
type Trie struct {
	root     node
	store    Store
	observer TrieAccessObserver
	deferred bool
	// A materialized root loaded from this Store is already durable. Hashing
	// alone never sets this flag; a logical edit invalidates it immediately.
	materializedBase bool
	// Persistence dirtiness is independent of the digest cache. Watermarks are
	// per Trie, so acknowledging one COW copy cannot clean another copy.
	revision, persistedThrough uint64
	persistenceBase            Hash
}

func NewEmpty(store Store) (*Trie, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	return &Trie{store: store, observer: accessObserver(store), deferred: usesDeferredReads(store), persistenceBase: EmptyRootHash}, nil
}

// LocalValueReader is implemented by a pinned local store that can follow
// physical child locators for point reads. A store may decline a subtree to
// let the normal materialized cache service it. Paths are unpacked nibbles.
type LocalValueReader interface {
	DeferLocalRoot(root Hash) bool
	LocalValue(path []byte, expected Hash, remaining []byte, observer TrieAccessObserver) ([]byte, bool, error)
}

// Open creates a trie at a previously committed root.
func Open(root Hash, store Store) (*Trie, error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if root == EmptyRootHash {
		return &Trie{store: store, observer: accessObserver(store), deferred: usesDeferredReads(store), persistenceBase: root}, nil
	}
	if local, ok := store.(LocalValueReader); ok && local.DeferLocalRoot(root) {
		return &Trie{root: hashNode(root), store: store, observer: accessObserver(store), deferred: usesDeferredReads(store), persistenceBase: root}, nil
	}
	if source, ok := store.(MaterializedStore); ok {
		n, err := source.GetMaterializedNode(nil, root)
		if err != nil {
			return nil, fmt.Errorf("%w: %x: %w", ErrMissingNode, root, err)
		}
		if n.root == nil || n.hash != root {
			return nil, fmt.Errorf("mpt: materialized root mismatch")
		}
		return &Trie{root: n.root, store: store, observer: accessObserver(store), deferred: usesDeferredReads(store), persistenceBase: root, materializedBase: true}, nil
	}
	blob, err := ReadNode(store, nil, root)
	if err != nil {
		return nil, fmt.Errorf("%w: %x", ErrMissingNode, root)
	}
	if hashBytes(blob) != root {
		return nil, fmt.Errorf("mpt: root node hash mismatch")
	}
	decoded, err := decodeNode(blob)
	if err != nil {
		return nil, fmt.Errorf("mpt: invalid root node: %w", err)
	}
	publishDigest(decoded, root, uint32(len(blob)))
	return &Trie{root: decoded, store: store, observer: accessObserver(store), deferred: usesDeferredReads(store), persistenceBase: root, materializedBase: true}, nil
}

func (t *Trie) Copy() *Trie {
	return &Trie{root: t.root, store: t.store, observer: t.observer, deferred: t.deferred, materializedBase: t.materializedBase, revision: t.revision, persistedThrough: t.persistedThrough, persistenceBase: t.persistenceBase}
}

func (t *Trie) Get(key []byte) ([]byte, error) {
	nibbles := keyToNibbles(key)
	full := nibbles
	n := t.root
	for {
		path := full[:len(full)-len(nibbles)]
		var resolved node
		var err error
		if h, ok := referenceHash(n); ok {
			var loc *NodeLocation
			if located, yes := n.(*locatedHashNode); yes {
				loc = &located.location
			}
			if lookup, yes := t.store.(LocalLookupReader); yes {
				value, materialized, handled, e := lookup.LookupLocalValue(path, h, nibbles, loc, t.observer)
				if e != nil {
					return nil, e
				}
				resolved = materialized.root
				if resolved != nil && materialized.hash != h {
					return nil, fmt.Errorf("mpt: local lookup node mismatch")
				}
				if resolved == nil && handled {
					return value, nil
				}
			} else if local, yes := t.store.(LocatedValueReader); loc != nil && yes {
				if value, handled, e := local.LocatedLocalValue(path, h, nibbles, *loc, t.observer); handled || e != nil {
					return value, e
				}
			} else if local, yes := t.store.(LocalValueReader); yes {
				if value, handled, e := local.LocalValue(path, h, nibbles, t.observer); handled || e != nil {
					return value, e
				}
			}
		}
		if resolved == nil {
			resolved, err = t.resolve(n, path)
			if err != nil {
				return nil, err
			}
		}
		if resolved != nil && t.observer != nil {
			t.observer.ObserveTrieAccess(full[:len(full)-len(nibbles)])
		}
		switch current := resolved.(type) {
		case nil:
			return nil, nil
		case *leafNode:
			if equalPath(current.path, nibbles) {
				return cloneBytes(current.value), nil
			}
			return nil, nil
		case *extensionNode:
			if len(nibbles) < len(current.path) || !equalPath(current.path, nibbles[:len(current.path)]) {
				return nil, nil
			}
			nibbles = nibbles[len(current.path):]
			n = current.child
		case *branchNode:
			if len(nibbles) == 0 {
				return cloneBytes(current.value), nil
			}
			n = current.children[nibbles[0]]
			nibbles = nibbles[1:]
		default:
			return nil, fmt.Errorf("mpt: invalid node %T", resolved)
		}
	}
}

// Update inserts or replaces key. An empty value deletes the key.
func (t *Trie) Update(key, value []byte) error {
	if len(value) == 0 {
		return t.Delete(key)
	}
	t.revision = nextPersistenceRevision.Add(1)
	root, err := t.insert(t.root, keyToNibbles(key), cloneBytes(value), nil)
	if err != nil {
		return err
	}
	t.root = root
	t.materializedBase = false
	return nil
}

func (t *Trie) insert(n node, key, value, path []byte) (out node, err error) {
	original := n
	if t.deferred {
		defer func() {
			if err == nil && out != original {
				prefetchChildren(out)
			}
		}()
	}
	n, err = t.resolve(n, path)
	if err != nil {
		return nil, err
	}
	if n != nil && t.observer != nil {
		t.observer.ObserveTrieAccess(path)
	}
	switch current := n.(type) {
	case nil:
		return &leafNode{persistRevision: t.revision, path: cloneBytes(key), value: value}, nil
	case *leafNode:
		match := commonPrefix(current.path, key)
		if match == len(current.path) && match == len(key) {
			if equalPath(current.value, value) {
				return original, nil
			}
			return &leafNode{persistRevision: t.revision, path: cloneBytes(current.path), value: value}, nil
		}
		branch := &branchNode{persistRevision: t.revision}
		oldRest := current.path[match:]
		if len(oldRest) == 0 {
			branch.value = cloneBytes(current.value)
		} else {
			branch.children[oldRest[0]] = &leafNode{persistRevision: t.revision, path: cloneBytes(oldRest[1:]), value: cloneBytes(current.value)}
		}
		newRest := key[match:]
		if len(newRest) == 0 {
			branch.value = value
		} else {
			branch.children[newRest[0]] = &leafNode{persistRevision: t.revision, path: cloneBytes(newRest[1:]), value: value}
		}
		if match == 0 {
			return branch, nil
		}
		return &extensionNode{persistRevision: t.revision, path: cloneBytes(key[:match]), child: branch}, nil
	case *extensionNode:
		match := commonPrefix(current.path, key)
		if match == len(current.path) {
			child, err := t.insert(current.child, key[match:], value, append(cloneBytes(path), current.path...))
			if err != nil {
				return nil, err
			}
			if child == current.child {
				return original, nil
			}
			return &extensionNode{persistRevision: t.revision, path: cloneBytes(current.path), child: child}, nil
		}
		branch := &branchNode{persistRevision: t.revision}
		oldRest := current.path[match:]
		if len(oldRest) == 1 {
			branch.children[oldRest[0]] = current.child
		} else {
			branch.children[oldRest[0]] = &extensionNode{persistRevision: t.revision, path: cloneBytes(oldRest[1:]), child: current.child}
		}
		newRest := key[match:]
		if len(newRest) == 0 {
			branch.value = value
		} else {
			branch.children[newRest[0]] = &leafNode{persistRevision: t.revision, path: cloneBytes(newRest[1:]), value: value}
		}
		if match == 0 {
			return branch, nil
		}
		return &extensionNode{persistRevision: t.revision, path: cloneBytes(key[:match]), child: branch}, nil
	case *branchNode:
		copy := branchNode{persistRevision: t.revision, children: current.children}
		copy.value = cloneBytes(current.value)
		if len(key) == 0 {
			if equalPath(current.value, value) {
				return original, nil
			}
			copy.value = value
			return &copy, nil
		}
		child, err := t.insert(current.children[key[0]], key[1:], value, append(cloneBytes(path), key[0]))
		if err != nil {
			return nil, err
		}
		if child == current.children[key[0]] {
			return original, nil
		}
		copy.children[key[0]] = child
		return &copy, nil
	default:
		return nil, fmt.Errorf("mpt: invalid node %T", n)
	}
}

func (t *Trie) Delete(key []byte) error {
	t.revision = nextPersistenceRevision.Add(1)
	root, _, err := t.delete(t.root, keyToNibbles(key), nil)
	if err != nil {
		return err
	}
	t.root = root
	t.materializedBase = false
	return nil
}

func (t *Trie) delete(n node, key, path []byte) (out node, changed bool, err error) {
	if t.deferred {
		defer func() {
			if err == nil && changed {
				prefetchChildren(out)
			}
		}()
	}
	n, err = t.resolve(n, path)
	if err != nil {
		return nil, false, err
	}
	if n != nil && t.observer != nil {
		t.observer.ObserveTrieAccess(path)
	}
	switch current := n.(type) {
	case nil:
		return nil, false, nil
	case *leafNode:
		if equalPath(current.path, key) {
			return nil, true, nil
		}
		return current, false, nil
	case *extensionNode:
		if len(key) < len(current.path) || !equalPath(current.path, key[:len(current.path)]) {
			return current, false, nil
		}
		child, changed, err := t.delete(current.child, key[len(current.path):], append(cloneBytes(path), current.path...))
		if err != nil || !changed {
			return current, changed, err
		}
		if child == nil {
			return nil, true, nil
		}
		child, err = t.resolve(child, append(cloneBytes(path), current.path...))
		if err != nil {
			return nil, false, err
		}
		switch child := child.(type) {
		case *leafNode:
			return &leafNode{persistRevision: t.revision, path: append(cloneBytes(current.path), child.path...), value: cloneBytes(child.value)}, true, nil
		case *extensionNode:
			return &extensionNode{persistRevision: t.revision, path: append(cloneBytes(current.path), child.path...), child: child.child}, true, nil
		default:
			return &extensionNode{persistRevision: t.revision, path: cloneBytes(current.path), child: child}, true, nil
		}
	case *branchNode:
		copy := branchNode{persistRevision: t.revision, children: current.children}
		copy.value = cloneBytes(current.value)
		changed := false
		if len(key) == 0 {
			if len(current.value) == 0 {
				return current, false, nil
			}
			copy.value = nil
			changed = true
		} else {
			child, childChanged, err := t.delete(current.children[key[0]], key[1:], append(cloneBytes(path), key[0]))
			if err != nil {
				return nil, false, err
			}
			if !childChanged {
				return current, false, nil
			}
			copy.children[key[0]] = child
			changed = true
		}
		if !changed {
			return current, false, nil
		}
		collapsed, err := t.collapseBranch(&copy, path)
		return collapsed, true, err
	default:
		return nil, false, fmt.Errorf("mpt: invalid node %T", n)
	}
}

func (t *Trie) collapseBranch(branch *branchNode, path []byte) (node, error) {
	count, index := 0, -1
	for i, child := range branch.children {
		if child != nil {
			count++
			index = i
		}
	}
	if len(branch.value) != 0 {
		if count == 0 {
			return &leafNode{persistRevision: t.revision, value: cloneBytes(branch.value)}, nil
		}
		return branch, nil
	}
	if count == 0 {
		return nil, nil
	}
	if count > 1 {
		return branch, nil
	}
	child, err := t.resolve(branch.children[index], append(cloneBytes(path), byte(index)))
	if err != nil {
		return nil, err
	}
	if child != nil && t.observer != nil {
		t.observer.ObserveTrieAccess(append(cloneBytes(path), byte(index)))
	}
	switch child := child.(type) {
	case *leafNode:
		return &leafNode{persistRevision: t.revision, path: concatPath(nil, byte(index), child.path), value: cloneBytes(child.value)}, nil
	case *extensionNode:
		return &extensionNode{persistRevision: t.revision, path: concatPath(nil, byte(index), child.path), child: child.child}, nil
	default:
		return &extensionNode{persistRevision: t.revision, path: []byte{byte(index)}, child: child}, nil
	}
}

// Hash computes the current root without writing nodes to the store.
func (t *Trie) Hash() (Hash, error) {
	if t.root == nil {
		return EmptyRootHash, nil
	}
	if root, ok, err := resolvedReferenceHash(t.root); ok {
		return Hash(root), err
	}
	hash, _, err := hashCached(t.root, nil)
	return hash, err
}

// Commit synchronously writes all new hashed nodes in one atomic batch.
func (t *Trie) Commit() (Hash, error) {
	if t.materializedBase {
		return t.Hash()
	}
	if t.root == nil {
		return EmptyRootHash, nil
	}
	if root, ok, err := resolvedReferenceHash(t.root); ok {
		return Hash(root), err
	}
	prepared, err := t.prepareCommit()
	if err != nil {
		return Hash{}, err
	}
	batch := t.store.NewBatch()
	if err := prepared.putNodes(batch); err != nil {
		return Hash{}, err
	}
	if err := batch.Write(); err != nil {
		return Hash{}, err
	}
	t.acceptCommit(prepared.root)
	return prepared.root, nil
}

// preparedCommit is an encoded trie root and the hash-addressed nodes needed
// to make it durable. It lets package-level state managers add their metadata
// to the same atomic storage batch as the trie nodes.
type preparedCommit struct {
	root  Hash
	nodes map[Hash][]byte
}

func (t *Trie) prepareCommit() (*preparedCommit, error) {
	prepared := &preparedCommit{nodes: make(map[Hash][]byte)}
	if t.materializedBase {
		h, err := t.Hash()
		prepared.root = h
		return prepared, err
	}
	if t.root == nil {
		prepared.root = EmptyRootHash
		return prepared, nil
	}
	if root, ok, err := resolvedReferenceHash(t.root); ok {
		prepared.root = Hash(root)
		return prepared, err
	}

	encoded, root, err := encodeNode(t.root, prepared.nodes)
	if err != nil {
		return nil, err
	}
	prepared.root = root
	prepared.nodes[root] = encoded
	return prepared, nil
}

func (p *preparedCommit) putNodes(batch Batch) error {
	keys := make([]Hash, 0, len(p.nodes))
	for hash := range p.nodes {
		keys = append(keys, hash)
	}
	sort.Slice(keys, func(i, j int) bool { return string(keys[i][:]) < string(keys[j][:]) })
	for _, hash := range keys {
		if err := batch.Put(hash[:], p.nodes[hash]); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trie) acceptCommit(root Hash) {
	t.AcceptLogicalPersistence(root)
	t.materializedBase = false
	if root == EmptyRootHash {
		t.root = nil
		return
	}
	t.root = hashNode(root)
}

func (t *Trie) resolve(n node, path []byte) (node, error) {
	if d, ok := n.(*deferredNode); ok {
		v, err := d.ref.Materialize()
		if err != nil {
			return nil, err
		}
		if v.root == nil {
			return nil, ErrMissingNode
		}
		return v.root, nil
	}
	hash, ok := referenceHash(n)
	if !ok {
		return n, nil
	}
	resolvedHash := Hash(hash)
	if located, ok := n.(*locatedHashNode); ok {
		if source, ok := t.store.(LocatedMaterializedStore); ok {
			loaded, err := source.GetLocatedMaterializedNode(path, resolvedHash, located.location)
			if err != nil {
				return nil, fmt.Errorf("%w: %x: %w", ErrMissingNode, resolvedHash, err)
			}
			if loaded.root == nil || loaded.hash != resolvedHash {
				return nil, fmt.Errorf("mpt: located node mismatch")
			}
			return loaded.root, nil
		}
	}
	if source, ok := t.store.(MaterializedStore); ok {
		n, err := source.GetMaterializedNode(path, resolvedHash)
		if err != nil {
			return nil, fmt.Errorf("%w: %x: %w", ErrMissingNode, resolvedHash, err)
		}
		if n.root == nil || n.hash != resolvedHash {
			return nil, fmt.Errorf("mpt: materialized node mismatch")
		}
		return n.root, nil
	}
	blob, err := ReadNode(t.store, path, resolvedHash)
	if err != nil {
		return nil, fmt.Errorf("%w: %x", ErrMissingNode, Hash(hash))
	}
	if hashBytes(blob) != Hash(hash) {
		return nil, fmt.Errorf("mpt: node hash mismatch for %x", Hash(hash))
	}
	decoded, err := decodeNode(blob)
	if err != nil {
		return nil, fmt.Errorf("mpt: decode node %x: %w", Hash(hash), err)
	}
	// This node was authenticated above. Initialize its immutable digest cache
	// without treating it as a newly durable version in any other store.
	publishDigest(decoded, resolvedHash, uint32(len(blob)))
	return decoded, nil
}
