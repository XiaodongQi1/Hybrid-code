package versionedtrie

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"math"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// Update is single-owner. It contains pending physical versions, not hashDirty
// flags. Begin/Abort never change the durable allocator or publish a head.
type Update struct {
	store                         *Store
	baseEpoch, baseNextID, nextID uint64
	height                        uint32
	root                          mpt.Hash
	commitID                      [32]byte
	puts                          map[string][]byte
	lives                         map[[32]byte]Lifecycle
	used                          uint64
	done                          bool
	baseHead                      Head
	projectionError               error
	projected                     map[uint32]bool
}

func (s *Store) Begin(height uint64, root mpt.Hash, id [32]byte) (*Update, error) {
	h, e := u32(height)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e = s.check(); e != nil {
		return nil, e
	}
	if s.readOnly {
		return nil, ErrReadOnly
	}
	if root == (mpt.Hash{}) || id == ([32]byte{}) {
		return nil, ErrCorrupt
	}
	want := uint64(s.cfg.InitialHeight)
	if s.head.Initialized {
		want = uint64(s.head.Height) + 1
	}
	if height != want {
		return nil, ErrConflict
	}
	return &Update{store: s, baseEpoch: s.epoch, baseNextID: s.head.NextTrieID, nextID: s.head.NextTrieID, height: h, root: root, commitID: id, puts: map[string][]byte{}, lives: map[[32]byte]Lifecycle{}, baseHead: s.head, projected: map[uint32]bool{}}, nil
}
func (u *Update) check() error {
	if u.done {
		return ErrClosed
	}
	if u.projectionError != nil {
		return u.projectionError
	}
	return nil
}
func (u *Update) AllocateTrieID() (uint32, error) {
	if e := u.check(); e != nil {
		return 0, e
	}
	if u.nextID > math.MaxUint32 {
		return 0, ErrLimit
	}
	id := uint32(u.nextID)
	u.nextID++
	return id, nil
}
func (u *Update) put(key, value []byte) error {
	if e := u.check(); e != nil {
		return e
	}
	k := string(key)
	size := uint64(len(key) + len(value))
	next := u.used + size
	if old, ok := u.puts[k]; ok {
		next -= uint64(len(key) + len(old))
	}
	if next > u.store.cfg.MaxBatchBytes {
		return ErrLimit
	}
	u.puts[k] = append([]byte(nil), value...)
	u.used = next
	return nil
}
func (u *Update) PutNode(id uint64, path []byte, r Record) error {
	if e := u.check(); e != nil {
		return e
	}
	if _, e := u32(id); e != nil {
		return e
	}
	if id >= u.nextID {
		return ErrCorrupt
	}
	if e := r.validate(len(path)); e != nil {
		return e
	}
	key, e := EncodeNodeKey(id, path, uint64(u.height))
	if e != nil {
		return e
	}
	value, e := EncodeRecord(r)
	if e != nil {
		return e
	}
	return u.put(key, value)
}
func (u *Update) PutLifecycle(owner [32]byte, life Lifecycle) error {
	if e := u.check(); e != nil {
		return e
	}
	if life.Active && uint64(life.TrieID) >= u.nextID {
		return ErrCorrupt
	}
	raw, e := encodeLife(life)
	if e != nil {
		return e
	}
	if e = u.put(lifecycleKey(owner, u.height), raw); e != nil {
		return e
	}
	u.lives[owner] = life
	return nil
}
func (u *Update) PutCode(hash mpt.Hash, code []byte) error {
	if e := u.check(); e != nil {
		return e
	}
	if len(code) > MaxRecordBytes {
		return ErrLimit
	}
	if codeHash(code) != hash {
		return ErrCorrupt
	}
	return u.put(append([]byte{'C'}, hash[:]...), code)
}
func (u *Update) Abort() { u.done = true; u.puts = nil; u.lives = nil }
func (u *Update) Commit() error {
	if e := u.check(); e != nil {
		return e
	}
	s := u.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	if s.readOnly {
		return ErrReadOnly
	}
	if u.baseEpoch != s.epoch || u.baseNextID != s.head.NextTrieID {
		return ErrConflict
	}
	// Existing IDs may only continue the same active owner lifecycle. New IDs
	// may be assigned once, even if several lifecycles change in the same block.
	assigned := map[uint32][32]byte{}
	for owner, life := range u.lives {
		if !life.Active {
			continue
		}
		if uint64(life.TrieID) >= u.baseNextID {
			if prior, ok := assigned[life.TrieID]; ok && prior != owner {
				return ErrConflict
			}
			assigned[life.TrieID] = owner
		} else {
			if !s.head.Initialized {
				return ErrCorrupt
			}
			_, raw, e := predecessor(s.db, lifecycleHeader(owner), s.head.Height)
			if e != nil {
				if errors.Is(e, ErrNotFound) {
					return ErrConflict
				}
				return e
			}
			old, e := decodeLife(raw)
			if e != nil {
				return e
			}
			if !old.Active || old.TrieID != life.TrieID {
				return ErrConflict
			}
		}
	}
	head := Head{Initialized: true, Height: u.height, Root: u.root, CommitID: u.commitID, NextTrieID: u.nextID}
	b := new(leveldb.Batch)
	keys := make([]string, 0, len(u.puts))
	for key := range u.puts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		b.Put([]byte(key), u.puts[key])
	}
	b.Put(rootKey(u.height), encodeRoot(u.root, u.commitID))
	wire, _ := json.Marshal(head)
	b.Put(headKey, wire)
	// Includes batch framing and metadata, not only caller's payload estimates.
	if uint64(len(b.Dump())) > s.cfg.MaxBatchBytes {
		return ErrLimit
	}
	if e := s.write(b); e != nil {
		s.uncertain = true
		u.Abort()
		return fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	s.head = head
	s.epoch++
	s.logical.AddBatch(b)
	u.Abort()
	return nil
}
