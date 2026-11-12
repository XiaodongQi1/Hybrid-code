package runner

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// Options controls reuse of a run created by this runner. Cache state is never
// checkpointed; resumed execution opens a cold, bounded execution cache.
type Options struct{ Resume bool }

type pendingBlock struct {
	Schema, WorkloadID string
	Index              int
	Result             backend.Result
}

// writeAtomic publishes a complete synced file, then syncs its directory.
func writeAtomic(path string, raw []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(raw); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func readJSON(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err = d.Decode(out); err != nil {
		return fmt.Errorf("%s: %w", filepath.Base(path), err)
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("%s: trailing JSON content", filepath.Base(path))
	}
	return nil
}

func unlockRun(f *os.File) { _ = f.Close() } // close releases the OS lock, including after a crash

// prepareRun verifies all immutable bindings before opening/modifying a database.
// A run lock also protects result files while the database is closed for proofs.
func prepareRun(output string, cfg Config, dbCfg any, wire []byte, resume bool) (lock *os.File, create bool, err error) {
	files := map[string][]byte{"workload.json": wire}
	for name, value := range map[string]any{"configuration.json": cfg, "database-config.json": dbCfg, "chain-config.json": rt.DefaultChainConfig()} {
		raw, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			return nil, false, e
		}
		files[name] = append(raw, '\n')
	}
	digests := map[string]string{}
	for name, raw := range files {
		sum := sha256.Sum256(raw)
		digests[name] = hex.EncodeToString(sum[:])
	}
	manifest, _ := json.MarshalIndent(struct {
		Schema string
		Files  map[string]string
	}{"versioned-evm-resume/v1", digests}, "", "  ")
	manifest = append(manifest, '\n')
	manifestPath := filepath.Join(output, "run-manifest.json")
	validate := func() (bool, error) {
		raw, e := os.ReadFile(manifestPath)
		if e != nil {
			return false, fmt.Errorf("resume requires intact run-manifest.json from a resumable run: %w", e)
		}
		if !bytes.Equal(raw, manifest) {
			return false, fmt.Errorf("resume configuration, workload or chain rules changed")
		}
		_, e = os.Stat(filepath.Join(output, "db"))
		newDB := errors.Is(e, os.ErrNotExist)
		if e != nil && !newDB {
			return false, e
		}
		for name, want := range files {
			raw, e = os.ReadFile(filepath.Join(output, name))
			if newDB && errors.Is(e, os.ErrNotExist) {
				continue // interruption during initial metadata publication
			}
			if e != nil {
				return false, e
			}
			if !bytes.Equal(raw, want) {
				return false, fmt.Errorf("resume artifact changed: %s", name)
			}
		}
		return newDB, nil
	}
	if resume {
		if _, err = validate(); err != nil {
			return nil, false, err
		}
	} else {
		if err = os.MkdirAll(filepath.Dir(output), 0700); err != nil {
			return nil, false, err
		}
		if err = os.Mkdir(output, 0700); err != nil {
			return nil, false, err
		}
	}
	lock, err = os.OpenFile(filepath.Join(output, "run.lock"), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if err != nil {
			unlockRun(lock)
		}
	}()
	if e := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return lock, false, fmt.Errorf("run is already active: %w", e)
	}
	if resume {
		create, err = validate()
		if err != nil {
			return lock, false, err
		}
	} else {
		create = true
		if err = writeAtomic(manifestPath, manifest); err != nil {
			return lock, false, err
		}
	}
	if create {
		var names []string
		for name := range files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if err = writeAtomic(filepath.Join(output, name), files[name]); err != nil {
				return lock, false, err
			}
		}
	}
	return lock, create, nil
}

type journalState struct {
	Count   int
	Offset  int64
	Torn    bool
	Recover *backend.Result
	Pending bool
}

func validateResult(r backend.Result, index int, w block.Workload) error {
	if index < 0 || index >= len(w.Blocks) {
		return fmt.Errorf("result index outside workload: %d", index)
	}
	in := w.Blocks[index]
	if r.Height != in.Height || r.ID != in.ID || len(r.Outcomes) != len(in.Transactions) || r.Root == (mpt.Hash{}) {
		return fmt.Errorf("block result does not match workload at index %d", index)
	}
	return nil
}

func committedResult(b *backend.Backend, r backend.Result) error {
	v, err := b.ViewAt(r.Height)
	if err != nil {
		return err
	}
	defer v.Close()
	if v.Root() != r.Root || v.BlockID() != r.ID {
		return fmt.Errorf("database and recorded result differ at height %d", r.Height)
	}
	return nil
}

// inspectJournal never mutates files. At most one committed block can be ahead
// of the synced log because its durable pending result precedes the DB commit.
func inspectJournal(output, workloadID string, w block.Workload, b *backend.Backend, accept func(int, backend.Result)) (s journalState, err error) {
	head, err := b.Head()
	if err != nil {
		return s, err
	}
	committed := 0
	if head.Initialized {
		if head.Height < w.InitialHeight() || head.Height-w.InitialHeight() >= uint64(len(w.Blocks)) {
			return s, fmt.Errorf("database head outside workload")
		}
		committed = int(head.Height-w.InitialHeight()) + 1
		if head.CommitID != [32]byte(w.Blocks[committed-1].ID) {
			return s, fmt.Errorf("database head block ID differs from workload")
		}
	}
	var last backend.Result
	f, err := os.Open(filepath.Join(output, "blocks.jsonl"))
	if err == nil {
		defer f.Close()
		rd := bufio.NewReader(f)
		for {
			line, e := rd.ReadBytes('\n')
			if e == io.EOF {
				s.Torn = len(line) != 0
				break
			}
			if e != nil {
				return s, e
			}
			var r backend.Result
			if e = json.Unmarshal(line, &r); e != nil {
				return s, fmt.Errorf("invalid complete result line %d: %w", s.Count, e)
			}
			if e = validateResult(r, s.Count, w); e != nil {
				return s, e
			}
			if s.Count >= committed {
				return s, fmt.Errorf("result log is ahead of database")
			}
			if e = committedResult(b, r); e != nil {
				return s, e
			}
			accept(s.Count, r)
			last = r
			s.Count++
			s.Offset += int64(len(line))
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return s, err
	}
	var pending pendingBlock
	err = readJSON(filepath.Join(output, "pending-block.json"), &pending)
	s.Pending = err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return s, err
	}
	if s.Pending {
		if pending.Schema != "versioned-evm-pending/v1" || pending.WorkloadID != workloadID {
			return s, fmt.Errorf("pending result identity mismatch")
		}
		if err = validateResult(pending.Result, pending.Index, w); err != nil {
			return s, err
		}
	}
	switch {
	case committed == s.Count+1:
		if !s.Pending || pending.Index != s.Count {
			return s, fmt.Errorf("committed block has no matching durable pending result")
		}
		if err = committedResult(b, pending.Result); err != nil {
			return s, err
		}
		s.Recover = &pending.Result
	case committed == s.Count:
		if s.Torn {
			return s, fmt.Errorf("partial result line without an unlogged committed block")
		}
		if s.Pending {
			switch pending.Index {
			case s.Count: // uncommitted attempt; the entire block will run again
			case s.Count - 1:
				old, _ := json.Marshal(last)
				got, _ := json.Marshal(pending.Result)
				if !bytes.Equal(old, got) {
					return s, fmt.Errorf("pending result conflicts with completed log")
				}
			default:
				return s, fmt.Errorf("pending result is not adjacent to database head")
			}
		}
	default:
		return s, fmt.Errorf("database and result log have an unrecoverable gap")
	}
	return s, nil
}

func appendResult(f *os.File, result backend.Result) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func clearPending(output string) error {
	err := os.Remove(filepath.Join(output, "pending-block.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncDir(output)
}
