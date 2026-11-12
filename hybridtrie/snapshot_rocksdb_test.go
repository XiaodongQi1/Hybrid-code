//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func buildSnapshotTest(t *testing.T, path, id string, n int) *SnapshotBuilder {
	t.Helper()
	b, e := OpenSnapshotBuilder(path, SnapshotBuildConfig{SourceID: id, Height: 0, BlockID: [32]byte{9}, BatchBytes: 4096, ChunkEntries: 16})
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < n; i++ {
		if e = b.Put(0, []byte{byte(i >> 8), byte(i)}, bytes.Repeat([]byte{byte(i%251 + 1)}, 64)); e != nil {
			t.Fatal(e)
		}
	}
	if e = b.Finish(SnapshotProgress{Cursor: uint64(n), NextTrieID: 1}); e != nil {
		t.Fatal(e)
	}
	return b
}
func TestSnapshotMultipleBatchesAtomicPublicationAndReopen(t *testing.T) {
	dir := t.TempDir()
	c := DefaultConfig("raw-bytes/v1", "snapshot-basic")
	c.MaxBatchBytes = 4096
	b := buildSnapshotTest(t, filepath.Join(dir, "build"), c.WorkloadID, 500)
	defer b.Close()
	root, e := b.Root(0)
	if e != nil {
		t.Fatal(e)
	}
	s := openTestStore(t, filepath.Join(dir, "db"), c, true, false)
	real := s.write
	writes := 0
	s.write = func(es []entry) error {
		var n uint64
		for _, e := range es {
			n += uint64(len(e.key) + len(e.value))
		}
		if n > c.MaxBatchBytes {
			t.Fatal("oversized write", n)
		}
		if writes > 0 && s.head.Initialized {
			t.Fatal("early publication")
		}
		writes++
		return real(es)
	}
	head, stats, e := s.ImportSnapshot(b)
	if e != nil {
		t.Fatal(e)
	}
	if !head.Initialized || stats.Batches < 2 || stats.PeakBatchBytes > 4096 || stats.VerifiedNodes < 500 {
		t.Fatal(head, stats)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTestStore(t, filepath.Join(dir, "db"), c, false, true)
	defer s.Close()
	assertProofs(t, s, 0, root, map[string][]byte{string([]byte{0, 1}): bytes.Repeat([]byte{2}, 64)}, [][]byte{{0, 1}, {9, 9}})
}
func TestSnapshotImportResumeBeforeAfterAndFinalWrite(t *testing.T) {
	for _, failAfter := range []bool{false, true} {
		for _, failAt := range []int{1, 3, 0} {
			t.Run(fmt.Sprintf("%v-%d", failAfter, failAt), func(t *testing.T) {
				dir := t.TempDir()
				c := DefaultConfig("raw-bytes/v1", "snapshot-fault")
				c.MaxBatchBytes = 4096
				b := buildSnapshotTest(t, filepath.Join(dir, "build"), c.WorkloadID, 120)
				defer b.Close()
				path := filepath.Join(dir, "db")
				s := openTestStore(t, path, c, true, false)
				real := s.write
				count := 0
				s.write = func(es []entry) error {
					count++
					final := false
					for _, e := range es {
						if bytes.Equal(e.key, headKey) {
							final = true
						}
					}
					if failAt > 0 && count == failAt || failAt == 0 && final {
						if failAfter {
							if e := real(es); e != nil {
								return e
							}
						}
						return errors.New("injected")
					}
					return real(es)
				}
				_, _, e := s.ImportSnapshot(b)
				if !errors.Is(e, ErrUncertain) {
					t.Fatal(e)
				}
				s.Close()
				s = openTestStore(t, path, c, false, false)
				h, _ := s.Head()
				if !h.Initialized {
					if _, e = s.ViewAt(0); !errors.Is(e, ErrNotFound) {
						t.Fatal("partial state visible", e)
					}
					if _, e = s.BeginForest(0, [32]byte{}); failAt != 1 && !errors.Is(e, ErrSnapshotPending) {
						t.Fatal("writer accepted partial import", e)
					}
				}
				head, stats, e := s.ImportSnapshot(b)
				if e != nil || !head.Initialized {
					t.Fatal(head, stats, e)
				}
				root, _ := b.Root(0)
				v, e := s.ViewAt(0)
				if e != nil {
					t.Fatal(e)
				}
				p, e := v.Prove(0, root, []byte{0, 1})
				v.Close()
				if e != nil {
					t.Fatal(e)
				}
				if _, _, e = mpt.VerifyProof(root, []byte{0, 1}, p); e != nil {
					t.Fatal(e)
				}
				s.Close()
			})
		}
	}
}
func TestSnapshotBuilderEmptyRootAndResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "build")
	cfg := SnapshotBuildConfig{SourceID: "builder-resume", BlockID: [32]byte{1}, BatchBytes: 4096, ChunkEntries: 2}
	b, e := OpenSnapshotBuilder(path, cfg)
	if e != nil {
		t.Fatal(e)
	}
	b.Put(0, []byte{1}, []byte("value"))
	b.Save(SnapshotProgress{Cursor: 1, NextTrieID: 1})
	b.Close()
	b, e = OpenSnapshotBuilder(path, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if b.Progress().Cursor != 1 {
		t.Fatal("lost source cursor")
	}
	b.Put(0, []byte{1}, nil)
	root, e := b.Root(0)
	if e != nil || root != mpt.EmptyRootHash {
		t.Fatal("empty root lost", root, e)
	}
}

func TestSnapshotCancellationResumeAndWriteAccounting(t *testing.T) {
	dir := t.TempDir()
	c := DefaultConfig("raw-bytes/v1", "snapshot-cancel")
	c.MaxBatchBytes = 4096
	b := buildSnapshotTest(t, filepath.Join(dir, "build"), c.WorkloadID, 180)
	defer b.Close()
	path := filepath.Join(dir, "db")
	s := openTestStore(t, path, c, true, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	real := s.write
	calls := 0
	var bytesWritten uint64
	s.write = func(es []entry) error {
		if e := real(es); e != nil {
			return e
		}
		calls++
		for _, e := range es {
			bytesWritten += uint64(len(e.key) + len(e.value))
		}
		if calls == 3 {
			cancel()
		}
		return nil
	}
	if _, _, e := s.ImportSnapshotContext(ctx, b); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	status, complete, e := s.SnapshotImportStatus()
	if e != nil || complete || status.Batches != uint64(calls) || status.Bytes != bytesWritten || status.Records == 0 {
		t.Fatal(status, e, calls, bytesWritten)
	}
	if _, e = s.ViewAt(0); !errors.Is(e, ErrNotFound) {
		t.Fatal("cancelled snapshot visible", e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	// An incomplete target can be inspected read-only without exposing a view.
	ro := openTestStore(t, path, c, false, true)
	if _, e = ro.ViewAt(0); !errors.Is(e, ErrNotFound) {
		t.Fatal(e)
	}
	ro.Close()
	s = openTestStore(t, path, c, false, false)
	defer s.Close()
	real = s.write
	s.write = func(es []entry) error {
		if e := real(es); e != nil {
			return e
		}
		calls++
		for _, e := range es {
			bytesWritten += uint64(len(e.key) + len(e.value))
		}
		return nil
	}
	_, stats, e := s.ImportSnapshot(b)
	if e != nil || !stats.Resumed || stats.SkippedRecords == 0 || stats.Batches != uint64(calls) || stats.Bytes != bytesWritten {
		t.Fatal(stats, e, calls, bytesWritten)
	}
	measurement, e := s.db.(*rocksEngine).measure()
	if e != nil || measurement.Families["hash"].Records != 0 {
		t.Fatal("construction hashes copied to final target", measurement, e)
	}
}

func TestSnapshotBuilderRejectsOtherManifestWithoutLeakingNativeHandles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "build")
	cfg := SnapshotBuildConfig{SourceID: "owned", BlockID: [32]byte{1}, BatchBytes: 4096, ChunkEntries: 8}
	b, e := OpenSnapshotBuilder(path, cfg)
	if e != nil {
		t.Fatal(e)
	}
	b.Close()
	bad := cfg
	bad.SourceID = "other"
	for i := 0; i < 4; i++ {
		if _, e = OpenSnapshotBuilder(path, bad); !errors.Is(e, ErrConflict) {
			t.Fatal(e)
		}
	}
	b, e = OpenSnapshotBuilder(path, cfg)
	if e != nil {
		t.Fatal(e)
	}
	b.Close()
}

func TestSnapshotReadyRequiresFinishedSource(t *testing.T) {
	dir := t.TempDir()
	cfg := SnapshotBuildConfig{SourceID: "snapshot-ready", BlockID: [32]byte{3}, BatchBytes: 4096, ChunkEntries: 8}
	b, e := OpenSnapshotBuilder(filepath.Join(dir, "build"), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if e = b.Save(SnapshotProgress{NextTrieID: 1, Ready: true}); !errors.Is(e, ErrConflict) {
		t.Fatal("Save bypassed Finish", e)
	}
	if e = b.Finish(SnapshotProgress{NextTrieID: 1}); e != nil {
		t.Fatal(e)
	}
	// A malformed ready source cannot publish an unusable EVM block directory.
	if e = b.write([]entry{{metaColumn, evmBlockKey(0), []byte{0}}}); e != nil {
		t.Fatal(e)
	}
	s := openTestStore(t, filepath.Join(dir, "db"), DefaultConfig("raw-bytes/v1", cfg.SourceID), true, false)
	defer s.Close()
	if _, _, e = s.ImportSnapshot(b); !errors.Is(e, ErrCorrupt) {
		t.Fatal("invalid source block accepted", e)
	}
	h, e := s.Head()
	if e != nil || h.Initialized {
		t.Fatal(h, e)
	}
}
