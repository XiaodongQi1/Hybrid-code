package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type SnapshotSourceInfo struct {
	BatchBytes           uint64
	SourceID             string
	Height               uint64
	BlockID              [32]byte
	Root                 mpt.Hash
	Accounts, NextTrieID uint64
	InputDigest          [32]byte
	Provenance           []byte
	RecordAccounts       bool
}

// SnapshotSource pins a ready, caller-owned construction database. No target
// receives its intermediate hash nodes, account-source rows or build metadata.
// This is a single-owner reader; close it before closing its builder.
type SnapshotSource struct {
	snapshot diskSnapshot
	info     SnapshotSourceInfo
	path     string
	closed   bool
}

func (b *SnapshotBuilder) OpenSource() (*SnapshotSource, error) {
	if e := b.check(); e != nil {
		return nil, e
	}
	if !b.progress.Ready {
		return nil, ErrSnapshotPending
	}
	s := b.db.snapshot()
	root, e := readRoot(s, 0, b.cfg.Height)
	if e != nil {
		s.close()
		return nil, e
	}
	return &SnapshotSource{snapshot: s, path: b.path, info: SnapshotSourceInfo{b.cfg.BatchBytes, b.cfg.SourceID, b.cfg.Height, b.cfg.BlockID, root, b.progress.Cursor, b.progress.NextTrieID, b.progress.Digest, bytes.Clone(b.cfg.Provenance), b.cfg.RecordAccounts}}, nil
}
func (s *SnapshotSource) Info() SnapshotSourceInfo {
	i := s.info
	i.Provenance = bytes.Clone(i.Provenance)
	return i
}
func (s *SnapshotSource) IndependentOf(path string) bool {
	b := SnapshotBuilder{path: s.path}
	return b.ownedTarget(path)
}
func (s *SnapshotSource) Close() error {
	if !s.closed {
		s.snapshot.close()
		s.closed = true
	}
	return nil
}
func (s *SnapshotSource) Get(key []byte) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	return s.snapshot.get(hashColumn, key)
}
func (s *SnapshotSource) Code(hash mpt.Hash) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	if hash == codeDigest(nil) {
		return nil, nil
	}
	raw, e := s.snapshot.get(metaColumn, codeKey(hash))
	if e != nil {
		return nil, e
	}
	if codeDigest(raw) != hash {
		return nil, ErrHashMismatch
	}
	return raw, nil
}
func (s *SnapshotSource) NewBatch() mpt.Batch { return deniedBatch{} }

// WalkTries traverses account ID zero followed by the assigned storage IDs.
// Address is zero for account trie; it is the original contract address otherwise.
func (s *SnapshotSource) WalkTries(ctx context.Context, visit func(id uint64, address [20]byte, root mpt.Hash) error) error {
	if s.closed {
		return ErrClosed
	}
	if visit == nil {
		return ErrConflict
	}
	for id := uint64(0); id < s.info.NextTrieID; id++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		root, e := readRoot(s.snapshot, id, s.info.Height)
		if errors.Is(e, ErrNotFound) && id > 0 {
			root = mpt.EmptyRootHash
		} else if e != nil {
			return e
		}
		var address [20]byte
		if id > 0 {
			var found bool
			address, found, e = readStorageOwner(s.snapshot, id)
			if e != nil {
				return e
			}
			if !found {
				return ErrCorrupt
			}
			_, life, e := readStorageLifecycle(s.snapshot, address, s.info.Height)
			if e != nil {
				return e
			}
			if !life.Active || life.TrieID != id {
				return ErrCorrupt
			}
		}
		if e = visit(id, address, root); e != nil {
			return e
		}
	}
	return nil
}
func (s *SnapshotSource) WalkAccounts(ctx context.Context, visit func(address [20]byte, raw []byte) error) error {
	if s.closed {
		return ErrClosed
	}
	if !s.info.RecordAccounts || visit == nil {
		return ErrConflict
	}
	scanner, ok := s.snapshot.(rangeSource)
	if !ok {
		return ErrEngineUnavailable
	}
	return scanner.scan(metaColumn, []byte{'A'}, []byte{'B'}, func(k, v []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if len(k) != 21 {
			return ErrCorrupt
		}
		var a [20]byte
		copy(a[:], k[1:])
		return visit(a, v)
	})
}
func (s *SnapshotSource) WalkCode(ctx context.Context, visit func(hash mpt.Hash, raw []byte) error) error {
	if s.closed {
		return ErrClosed
	}
	if visit == nil {
		return ErrConflict
	}
	scanner, ok := s.snapshot.(rangeSource)
	if !ok {
		return ErrEngineUnavailable
	}
	return scanner.scan(metaColumn, []byte{'C'}, []byte{'D'}, func(k, v []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if len(k) != 33 {
			return ErrCorrupt
		}
		var h mpt.Hash
		copy(h[:], k[1:])
		if codeDigest(v) != h {
			return ErrHashMismatch
		}
		return visit(h, v)
	})
}
