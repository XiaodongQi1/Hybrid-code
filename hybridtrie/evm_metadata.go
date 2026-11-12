package hybridtrie

import (
	"bytes"
	"errors"
	"github.com/XiaodongQi1/HybridTrie/mpt"

	"golang.org/x/crypto/sha3"
)

func codeDigest(code []byte) (h mpt.Hash) {
	d := sha3.NewLegacyKeccak256()
	d.Write(code)
	d.Sum(h[:0])
	return
}
func evmBlockKey(height uint64) []byte {
	k := make([]byte, 1+HeightBytes)
	k[0] = 'E'
	put48(k[1:], height)
	return k
}
func codeKey(hash mpt.Hash) []byte { return append([]byte{'C'}, hash[:]...) }

// BlockID is the EVM caller's authenticated block ID, distinct from the local
// storage CAS CommitID. Absence is an error, including before an imported base.
func (v *View) BlockID() ([32]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	var id [32]byte
	if v.closed {
		return id, ErrClosed
	}
	raw, err := v.snapshot.get(metaColumn, evmBlockKey(v.height))
	if err != nil {
		return id, err
	}
	if len(raw) != len(id) || bytes.Equal(raw, make([]byte, 32)) {
		return id, ErrCorrupt
	}
	copy(id[:], raw)
	return id, nil
}

// Code returns immutable hash-addressed bytecode from the pinned snapshot.
func (v *View) Code(hash mpt.Hash) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrClosed
	}
	return readCode(v.snapshot, hash)
}

func readCode(snapshot diskSnapshot, hash mpt.Hash) ([]byte, error) {
	if hash == codeDigest(nil) {
		return nil, nil
	}
	raw, err := snapshot.get(metaColumn, codeKey(hash))
	if err != nil {
		return nil, err
	}
	if codeDigest(raw) != hash {
		return nil, ErrHashMismatch
	}
	return raw, nil
}

func (u *ForestUpdate) SetBlockID(id [32]byte) error {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if err := u.checkLocked(); err != nil {
		return err
	}
	if id == ([32]byte{}) || u.blockID != ([32]byte{}) && u.blockID != id {
		return ErrConflict
	}
	u.blockID = id
	return nil
}

// PutCode charges staging before copying. Code and block IDs share the same
// atomic write and fail-stop rules as nodes, lifecycle mappings and Head.
func (u *ForestUpdate) PutCode(hash mpt.Hash, code []byte) error {
	u.store.mu.Lock()
	defer u.store.mu.Unlock()
	if err := u.checkLocked(); err != nil {
		return err
	}
	if len(code) > MaxRecordBytes || codeDigest(code) != hash {
		return ErrCorrupt
	}
	if hash == codeDigest(nil) {
		return nil
	}
	if old, exists := u.codes[hash]; exists {
		if !bytes.Equal(old, code) {
			return ErrConflict
		}
		return nil
	}
	n := uint64(33 + len(code))
	if n > u.store.cfg.MaxBatchBytes || u.codeBytes > u.store.cfg.MaxBatchBytes-n {
		return ErrLimit
	}
	u.codeBytes += n
	u.codes[hash] = bytes.Clone(code)
	return nil
}

func (u *ForestUpdate) stageEVM(snap diskSnapshot, p *writePlan) ([]entry, error) {
	var metadata []entry
	if u.blockID != ([32]byte{}) {
		if _, ok := u.updates[AccountTrieID]; !ok {
			return nil, ErrConflict
		}
		key := evmBlockKey(u.height)
		if err := p.add(metaColumn, key, u.blockID[:]); err != nil {
			return nil, err
		}
		metadata = append(metadata, entry{metaColumn, key, append([]byte(nil), u.blockID[:]...)})
	}
	for hash, code := range u.codes {
		key := codeKey(hash)
		old, err := snap.get(metaColumn, key)
		if err == nil {
			if !bytes.Equal(old, code) {
				return nil, ErrHashMismatch
			}
			continue
		}
		if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		if err = p.add(metaColumn, key, code); err != nil {
			return nil, err
		}
		metadata = append(metadata, entry{metaColumn, key, code})
	}
	return metadata, nil
}
