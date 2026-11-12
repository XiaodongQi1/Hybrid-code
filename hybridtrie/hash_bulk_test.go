package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestHashBulkLevelDBPublicationRecovery(t *testing.T) { runHashBulkRecovery(t, kvstore.LevelDB) }
func runHashBulkRecovery(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	for _, after := range []bool{false, true} {
		for _, failAt := range []int{3, 0} {
			t.Run(fmt.Sprintf("%s/%t/%d", engine, after, failAt), func(t *testing.T) {
				dir := t.TempDir()
				o := kvstore.DefaultOptions(engine)
				o.BlockCacheBytes, o.WriteBufferBytes = 1<<20, 1<<20
				b, e := OpenSnapshotBuilder(filepath.Join(dir, "source"), SnapshotBuildConfig{SourceID: "hash-recovery", BlockID: [32]byte{9}, BatchBytes: 4096, ChunkEntries: 8, Storage: &o})
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				for i := 0; i < 100; i++ {
					if e = b.Put(0, []byte{byte(i)}, bytes.Repeat([]byte{byte(i + 1)}, 48)); e != nil {
						t.Fatal(e)
					}
				}
				if e = b.Finish(SnapshotProgress{Cursor: 100, NextTrieID: 1}); e != nil {
					t.Fatal(e)
				}
				root, e := b.Root(0)
				if e != nil {
					t.Fatal(e)
				}
				c := DefaultConfig("raw-bytes/v1", "hash-recovery")
				c.Storage = &o
				c.MaxBatchBytes = 4096
				path := filepath.Join(dir, "target")
				s, e := Open(path, c, true, false)
				if e != nil {
					t.Fatal(e)
				}
				real := s.write
				calls := 0
				s.write = func(es []entry) error {
					calls++
					final := false
					for _, e := range es {
						if bytes.Equal(e.key, headKey) {
							final = true
						}
					}
					if calls == failAt || failAt == 0 && final {
						if after {
							if e := real(es); e != nil {
								return e
							}
						}
						return errors.New("injected")
					}
					return real(es)
				}
				if _, _, e = s.ImportHashSnapshotContext(context.Background(), b); !errors.Is(e, ErrUncertain) {
					t.Fatal(e)
				}
				s.Close()
				s, e = Open(path, c, false, false)
				if e != nil {
					t.Fatal(e)
				}
				defer s.Close()
				h, _ := s.Head()
				if !h.Initialized {
					if _, e = s.ViewAt(0); !errors.Is(e, ErrNotFound) {
						t.Fatal("partial root visible", e)
					}
				}
				_, stats, e := s.ImportHashSnapshotContext(context.Background(), b)
				if e != nil || stats.PeakBatchBytes > 4096 {
					t.Fatal(stats, e)
				}
				v, e := s.ViewAt(0)
				if e != nil {
					t.Fatal(e)
				}
				proof, e := v.Prove(0, root, []byte{1})
				v.Close()
				if e != nil {
					t.Fatal(e)
				}
				value, found, e := mpt.VerifyProof(root, []byte{1}, proof)
				if e != nil || !found || !bytes.Equal(value, bytes.Repeat([]byte{2}, 48)) {
					t.Fatal(e)
				}
			})
		}
	}
}
