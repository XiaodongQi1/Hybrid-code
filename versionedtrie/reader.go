package versionedtrie

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/crypto/sha3"
	"github.com/XiaodongQi1/HybridTrie/internal/kvcache"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
)

type versionReader interface {
	Get([]byte, *opt.ReadOptions) ([]byte, error)
	NewIterator(*util.Range, *opt.ReadOptions) iterator.Iterator
}

func predecessor(db versionReader, head []byte, h uint32) (uint32, []byte, error) {
	target := append(append([]byte(nil), head...), be32(h)...)
	it := db.NewIterator(util.BytesPrefix(head), nil)
	defer it.Release()
	if it.Seek(target) {
		if !bytes.Equal(it.Key(), target) {
			it.Prev()
		}
	} else {
		if e := it.Error(); e != nil {
			return 0, nil, e
		}
		it.Last()
	}
	if e := it.Error(); e != nil {
		return 0, nil, e
	}
	if !it.Valid() {
		return 0, nil, ErrNotFound
	}
	key := it.Key()
	if !sameHeader(key, head) {
		return 0, nil, ErrCorrupt
	}
	found := binary.BigEndian.Uint32(key[len(head):])
	if found > h {
		return 0, nil, ErrCorrupt
	}
	if len(it.Value()) > MaxRecordBytes {
		return 0, nil, ErrLimit
	}
	return found, append([]byte(nil), it.Value()...), nil
}

type View struct {
	mu       sync.RWMutex
	store    *Store
	snap     *kvcache.Snapshot
	height   uint32
	root     mpt.Hash
	commitID [32]byte
	closed   bool
}

func (s *Store) ViewAt(height uint64) (*View, error) {
	h, e := u32(height)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e = s.check(); e != nil {
		return nil, e
	}
	if !s.head.Initialized || h < s.cfg.InitialHeight || h > s.head.Height {
		return nil, ErrNotFound
	}
	snap, e := s.db.GetSnapshot()
	if e != nil {
		return nil, e
	}
	raw, e := snap.Get(rootKey(h), nil)
	if e != nil {
		snap.Release()
		if errors.Is(e, leveldb.ErrNotFound) {
			return nil, ErrCorrupt
		}
		return nil, e
	}
	if len(raw) != 64 {
		snap.Release()
		return nil, ErrCorrupt
	}
	v := &View{store: s, snap: snap, height: h}
	copy(v.root[:], raw[:32])
	copy(v.commitID[:], raw[32:])
	if v.root == (mpt.Hash{}) || v.commitID == ([32]byte{}) {
		snap.Release()
		return nil, ErrCorrupt
	}
	s.views++
	return v, nil
}
func (v *View) Root() mpt.Hash     { return v.root }
func (v *View) Height() uint32     { return v.height }
func (v *View) CommitID() [32]byte { return v.commitID }
func (v *View) Close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.store.mu.Lock()
	defer v.store.mu.Unlock()
	v.snap.Release()
	v.store.views--
	v.closed = true
	return nil
}

// Node returns a physical record, not a proof of logical reachability.
func (v *View) Node(id uint64, path []byte) (uint32, Record, error) {
	head, e := nodeHeader(id, path)
	if e != nil {
		return 0, Record{}, e
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return 0, Record{}, ErrClosed
	}
	h, raw, e := predecessor(v.snap, head, v.height)
	if e != nil {
		return 0, Record{}, e
	}
	r, e := DecodeRecord(raw)
	if e != nil {
		return 0, Record{}, e
	}
	if e = r.validate(len(path)); e != nil {
		return 0, Record{}, e
	}
	return h, r, nil
}
func (v *View) Lifecycle(owner [32]byte) (uint32, Lifecycle, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return 0, Lifecycle{}, ErrClosed
	}
	h, raw, e := predecessor(v.snap, lifecycleHeader(owner), v.height)
	if e != nil {
		return 0, Lifecycle{}, e
	}
	life, e := decodeLife(raw)
	return h, life, e
}
func codeHash(b []byte) mpt.Hash {
	h := sha3.NewLegacyKeccak256()
	h.Write(b)
	var out mpt.Hash
	h.Sum(out[:0])
	return out
}
func (v *View) Code(hash mpt.Hash) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrClosed
	}
	raw, e := v.snap.Get(append([]byte{'C'}, hash[:]...), nil)
	if errors.Is(e, leveldb.ErrNotFound) {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	if len(raw) > MaxRecordBytes || codeHash(raw) != hash {
		return nil, ErrCorrupt
	}
	return append([]byte(nil), raw...), nil
}
