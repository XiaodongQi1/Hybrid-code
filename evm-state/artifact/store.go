// Package artifact stores immutable experiment inputs, outcomes and traces
// outside measured state databases. Multiple methods share one content store.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
)

const MaxBlobBytes = 64 << 20

var (
	ErrCorrupt  = errors.New("artifact: corrupt object or identity mismatch")
	ErrNotFound = errors.New("artifact: object not found")
	ErrReadOnly = errors.New("artifact: read only")
	ErrClosed   = errors.New("artifact: closed")
	ErrLimit    = errors.New("artifact: object exceeds size limit")
)

type Config struct {
	WorkloadID string
	Codec      codec.Mode
}
type Store struct {
	mu               sync.RWMutex
	path             string
	cfg              Config
	readOnly, closed bool
}

func Open(path string, c Config, create, ro bool) (*Store, error) {
	if path == "" || c.WorkloadID == "" || c.Codec.Validate() != nil || create && ro {
		return nil, ErrCorrupt
	}
	wire, _ := json.Marshal(struct {
		Schema string
		Config Config
	}{"evm-artifact-store/v1", c})
	if create {
		if e := os.MkdirAll(filepath.Dir(path), 0755); e != nil {
			return nil, e
		}
		if e := os.Mkdir(path, 0700); e != nil {
			return nil, e
		}
		if e := publish(path, "manifest.json", wire); e != nil {
			return nil, e
		}
		if e := syncDir(filepath.Dir(path)); e != nil {
			return nil, e
		}
	} else {
		old, e := os.ReadFile(filepath.Join(path, "manifest.json"))
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(old, wire) {
			return nil, ErrCorrupt
		}
	}
	return &Store{path: path, cfg: c, readOnly: ro}, nil
}
func (s *Store) Matches(workload string, mode codec.Mode) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.closed && s.cfg.WorkloadID == workload && s.cfg.Codec == mode
}
func validKind(kind string) bool {
	if len(kind) == 0 || len(kind) > 63 {
		return false
	}
	for _, c := range kind {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func (s *Store) identity(kind string, raw []byte) common.Hash {
	h := sha256.New()
	h.Write([]byte("evm-artifact/v1\x00" + s.cfg.WorkloadID + "\x00" + string(s.cfg.Codec) + "\x00" + kind + "\x00"))
	h.Write(raw)
	return common.BytesToHash(h.Sum(nil))
}
func blobName(id common.Hash) string { return hex.EncodeToString(id[:]) + ".blob" }
func (s *Store) get(kind string, id common.Hash) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	if !validKind(kind) || id == (common.Hash{}) {
		return nil, ErrCorrupt
	}
	path := filepath.Join(s.path, kind, blobName(id))
	st, e := os.Stat(path)
	if os.IsNotExist(e) {
		return nil, ErrNotFound
	}
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() {
		return nil, ErrCorrupt
	}
	if st.Size() > MaxBlobBytes {
		return nil, ErrLimit
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	if len(raw) > MaxBlobBytes {
		return nil, ErrLimit
	}
	if s.identity(kind, raw) != id {
		return nil, ErrCorrupt
	}
	return raw, nil
}
func (s *Store) Get(kind string, id common.Hash) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.get(kind, id)
}

// Ensure publishes and syncs a complete immutable object before a state commit
// may reference it. Existing objects are validated and reused, including from a
// read-only artifact store. Orphan objects from rejected commits are harmless.
func (s *Store) Ensure(kind string, raw []byte) (common.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return common.Hash{}, ErrClosed
	}
	if !validKind(kind) {
		return common.Hash{}, ErrCorrupt
	}
	if len(raw) > MaxBlobBytes {
		return common.Hash{}, ErrLimit
	}
	id := s.identity(kind, raw)
	if _, e := s.get(kind, id); e == nil {
		// Another writer may have linked the complete file but not synced the
		// directory yet. Do not publish a state reference ahead of that sync.
		if !s.readOnly {
			if e := syncDir(filepath.Join(s.path, kind)); e != nil {
				return common.Hash{}, e
			}
		}
		return id, nil
	} else if !errors.Is(e, ErrNotFound) {
		return common.Hash{}, e
	}
	if s.readOnly {
		return common.Hash{}, ErrReadOnly
	}
	dir := filepath.Join(s.path, kind)
	if e := os.Mkdir(dir, 0700); e == nil {
		if e = syncDir(s.path); e != nil {
			return common.Hash{}, e
		}
	} else if !os.IsExist(e) {
		return common.Hash{}, e
	}
	if e := publish(dir, blobName(id), raw); e != nil {
		if !os.IsExist(e) {
			return common.Hash{}, e
		}
		if _, e = s.get(kind, id); e != nil {
			return common.Hash{}, e
		}
		if e = syncDir(dir); e != nil {
			return common.Hash{}, e
		}
	}
	return id, nil
}
func (s *Store) Close() error { s.mu.Lock(); defer s.mu.Unlock(); s.closed = true; return nil }
func syncDir(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func publish(dir, name string, raw []byte) error {
	if strings.ContainsAny(name, "/\\") {
		return ErrCorrupt
	}
	f, e := os.CreateTemp(dir, ".artifact-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	_, e = f.Write(raw)
	if e == nil {
		e = f.Sync()
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	// Link publishes an already-synced immutable file without replacing a
	// concurrently published or corrupted object at the same identity.
	if e = os.Link(f.Name(), filepath.Join(dir, name)); e != nil {
		return e
	}
	if e = syncDir(dir); e != nil {
		return fmt.Errorf("artifact publication sync: %w", e)
	}
	return nil
}
