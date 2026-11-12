package backend

import (
	"errors"
	"math"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

// Head is backend-neutral. CommitID is the EVM block ID; a storage backend may
// maintain a separate private CAS ID. Heights/IDs are never truncated to uint32.
type Head struct {
	Initialized bool
	Height      uint64
	Root        mpt.Hash
	CommitID    [32]byte
	NextTrieID  uint64
}
type stateSnapshot interface {
	Root() mpt.Hash
	CommitID() [32]byte
	Close() error
	Reader(uint64, mpt.Hash) (mpt.Store, error)
	StorageID(common.Address) (uint64, bool, error)
	Code(mpt.Hash) ([]byte, error)
}
type historicalProver interface {
	HistoricalValueProof(id uint64, root mpt.Hash, key []byte, serving bool) (mpt.ProofResult, error)
}
type stateUpdate interface {
	PutCode(mpt.Hash, []byte) error
	Storage(storagePlan) error
	Accounts(mpt.Hash, *mpt.Trie) error
	Commit() error
	Abort()
}
type stateStore interface {
	Head() (Head, error)
	Close() error
	ViewAt(uint64) (stateSnapshot, error)
	ExecutionView(uint64) (stateSnapshot, func(), error)
	Begin(uint64, mpt.Hash, [32]byte) (stateUpdate, error)
}

type versionStore struct{ *vt.Store }

func (s versionStore) Head() (Head, error) {
	h, e := s.Store.Head()
	return Head{h.Initialized, uint64(h.Height), h.Root, h.CommitID, h.NextTrieID}, e
}
func (s versionStore) ViewAt(h uint64) (stateSnapshot, error) {
	v, e := s.Store.ViewAt(h)
	if e != nil {
		return nil, e
	}
	return versionSnapshot{v}, nil
}
func (s versionStore) ExecutionView(h uint64) (stateSnapshot, func(), error) {
	head, e := s.Head()
	if e != nil {
		return nil, nil, e
	}
	if !head.Initialized {
		return nil, func() {}, nil
	}
	v, e := s.ViewAt(head.Height)
	return v, func() {}, e
}
func (s versionStore) Begin(h uint64, root mpt.Hash, id [32]byte) (stateUpdate, error) {
	u, e := s.Store.Begin(h, root, id)
	if e != nil {
		return nil, e
	}
	return versionUpdate{u}, nil
}

type versionSnapshot struct{ *vt.View }

func (v versionSnapshot) Reader(id uint64, _ mpt.Hash) (mpt.Store, error) { return v.TrieReader(id) }
func (v versionSnapshot) StorageID(a common.Address) (uint64, bool, error) {
	_, life, e := v.Lifecycle(owner(a))
	if errors.Is(e, vt.ErrNotFound) {
		return 0, false, nil
	}
	return uint64(life.TrieID), life.Active, e
}

type versionUpdate struct{ *vt.Update }

func (u versionUpdate) Storage(p storagePlan) error {
	q := p.oldID
	if !p.active || p.reset {
		if p.needed {
			id, e := u.AllocateTrieID()
			if e != nil {
				return e
			}
			q = uint64(id)
			if e = u.PutLifecycle(owner(p.address), vt.Lifecycle{Active: true, TrieID: id}); e != nil {
				return e
			}
		} else if p.active {
			if e := u.PutLifecycle(owner(p.address), vt.Lifecycle{}); e != nil {
				return e
			}
		}
	}
	if p.tree != nil && (p.needed || p.active && !p.reset) {
		_, e := u.ProjectTrie(q, p.before, p.tree)
		return e
	}
	return nil
}
func (u versionUpdate) Accounts(before mpt.Hash, t *mpt.Trie) error {
	_, e := u.ProjectTrie(0, before, t)
	return e
}

func maxHeightForLegacy() uint64 { return math.MaxUint32 }
