package kvstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

func TestReadCacheConcurrentSnapshotGenerations(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			o := testOptions(engine)
			o.ApplicationCacheBytes = 64 << 10
			d, e := Open(filepath.Join(t.TempDir(), "db"), o, true, false)
			must(t, e)
			defer d.Close()
			put := func(n int) error {
				b := NewBatch()
				for _, column := range []string{"prefix", "hash"} {
					b.Put(column, []byte("k"), []byte(fmt.Sprint(n)))
				}
				return d.Write(b)
			}
			must(t, put(0))
			var wg sync.WaitGroup
			failures := make(chan error, 5)
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 1; i <= 50; i++ {
					if e := put(i); e != nil {
						failures <- e
						return
					}
				}
			}()
			for g := 0; g < 4; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < 60; i++ {
						s, e := d.NewSnapshot()
						if e != nil {
							failures <- e
							return
						}
						read := func() error {
							defer s.Close()
							for j := 0; j < 3; j++ {
								a, e := s.Get("prefix", []byte("k"))
								if e != nil {
									return e
								}
								b, e := s.Get("hash", []byte("k"))
								if e != nil {
									return e
								}
								if !bytes.Equal(a, b) {
									return fmt.Errorf("cached snapshot mixed generations: %s %s", a, b)
								}
							}
							return nil
						}
						if e = read(); e != nil {
							failures <- e
							return
						}
					}
				}()
			}
			wg.Wait()
			close(failures)
			for e := range failures {
				t.Fatal(e)
			}
			stats, e := d.Statistics()
			must(t, e)
			if stats.ReadCache.Hits == 0 {
				t.Fatal("cache not exercised")
			}
		})
	}
}

func TestFixedReadCacheRandomReplacementAndAccounting(t *testing.T) {
	p, e := PlanReadCache(64 << 10)
	must(t, e)
	budget := p.FixedBytes + 7*p.SlotBytes
	c := newPointCache(budget)
	model := map[string][]byte{}
	rng := rand.New(rand.NewSource(71))
	for step := 0; step < 3000; step++ {
		key := fmt.Sprint(rng.Intn(20))
		column := "default"
		if step%3 == 0 {
			column = "hash"
		}
		id := column + ":" + key
		if rng.Intn(3) == 0 {
			c.invalidate(column, []byte(key))
			delete(model, id)
		} else {
			value := bytes.Repeat([]byte{byte(step)}, rng.Intn(100))
			c.put(column, []byte(key), value)
			model[id] = value
		}
		for full, want := range model {
			split := bytes.IndexByte([]byte(full), ':')
			v, ok := c.get(full[:split], []byte(full[split+1:]))
			if ok && !bytes.Equal(v, want) {
				t.Fatal("stale cached value")
			}
			if len(v) > 0 {
				v[0] ^= 1
			}
		}
		var entries, live uint64
		seen := map[int32]bool{}
		for n := c.head; n != 0; n = c.entries[n-1].next {
			if seen[n] {
				t.Fatal("LRU cycle")
			}
			seen[n] = true
			e := c.entries[n-1]
			if e.columnLength > 0 {
				entries++
				live += uint64(e.valueLength)
				if c.find(string(e.column[:e.columnLength]), e.key[:e.keyLength], e.hash) != n {
					t.Fatal("hash-chain lost entry")
				}
			}
		}
		if len(seen) != int(c.used) || entries != c.stats.Entries || live != c.stats.LiveValueBytes || entries > 7 || c.stats.AllocatedBytes != budget {
			t.Fatal("cache accounting", entries, live, c.stats)
		}
	}
	if c.stats.Evictions == 0 || c.stats.Invalidations == 0 {
		t.Fatal(c.stats)
	}
}

func TestSharedReadCacheSnapshotsAndCounterScopes(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			o := testOptions(engine)
			o.ApplicationCacheBytes = 32 << 10
			o.ExecutionCacheOnly = true
			o.EnableStatistics = true
			o.GlobalWriteBufferBytes = 2 << 20
			d, e := Open(path, o, true, false)
			must(t, e)
			defer func() { d.Close() }()
			before, e := d.Statistics()
			must(t, e)
			b := NewBatch()
			b.Put("hash", []byte("x"), []byte("one"))
			b.Put("hash", []byte("x"), []byte("two"))
			b.Delete("hash", []byte("x"))
			b.Put("default", []byte("c"), []byte("v"))
			must(t, d.Write(b))
			v, e := d.Get("default", []byte("c"))
			must(t, e)
			v[0] = 'q'
			v, e = d.Get("default", []byte("c"))
			must(t, e)
			if string(v) != "v" {
				t.Fatal("unowned cached value")
			}
			s, e := d.NewSnapshot()
			must(t, e)
			b.Reset()
			b.Put("default", []byte("c"), []byte("new"))
			must(t, d.Write(b))
			v, e = d.Get("default", []byte("c"))
			must(t, e)
			if string(v) != "new" {
				t.Fatal("write did not invalidate cache")
			}
			v, e = s.Get("default", []byte("c"))
			must(t, e)
			if string(v) != "v" {
				t.Fatal("snapshot used latest cache")
			}
			must(t, s.Close())
			after, e := d.Statistics()
			must(t, e)
			delta, e := WriteDelta(before, after)
			must(t, e)
			if delta.Commits != 2 || delta.Total.Puts != 4 || delta.Total.Deletes != 1 || delta.BatchBytes <= delta.Total.ApplicationBytes() || !delta.BatchBytesComplete || after.ReadCache.Hits == 0 || after.ReadCache.Invalidations == 0 {
				t.Fatal(after, delta)
			}
			if after.ReadCache.AllocatedBytes > o.ApplicationCacheBytes || after.ReadCache.SlackBytes >= after.ReadCache.SlotBytes {
				t.Fatal(after.ReadCache)
			}
			invalid := NewBatch()
			invalid.Put("missing", []byte("bad"), nil)
			if e = d.Write(invalid); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
			end, e := d.Statistics()
			must(t, e)
			if end.Writes.Commits != after.Writes.Commits {
				t.Fatal("invalid batch was counted")
			}
			must(t, d.Close())
			d, e = Open(path, o, false, true)
			must(t, e)
			reopen, e := d.Statistics()
			must(t, e)
			if reopen.Scope.ID == after.Scope.ID || reopen.Scope.CreatedWithDatabase || reopen.ReadCache.AllocatedBytes != 0 || reopen.Writes.Commits != 0 {
				t.Fatal("reopen counter/cache scope", reopen)
			}
			if _, e = WriteDelta(after, reopen); !errors.Is(e, ErrInvalid) {
				t.Fatal("reopen counters passed as complete run", e)
			}
			v, e = d.Get("default", []byte("c"))
			must(t, e)
			if string(v) != "new" {
				t.Fatal("reopen lost commit")
			}
		})
	}
}

func TestMeasurementClosesNamespacesFilesAndSignedDelta(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			ctx := context.Background()
			o := testOptions(engine)
			o.EnableStatistics = true
			o.GlobalWriteBufferBytes = 2 << 20
			d, e := Open(filepath.Join(t.TempDir(), "db"), o, true, false)
			must(t, e)
			defer d.Close()
			classify := func(column string, k, v []byte) (Classification, error) {
				if string(k) == "config" {
					return Classification{Name: "configuration"}, nil
				}
				if string(k) == "state" {
					return Classification{Name: column, State: true}, nil
				}
				return Classification{}, ErrCorrupt
			}
			b := NewBatch()
			b.Put("default", []byte("config"), []byte("configuration"))
			b.Put("prefix", []byte("state"), bytes.Repeat([]byte{1}, 1000))
			b.Put("hash", []byte("state"), []byte{2})
			must(t, d.Write(b))
			s, e := d.NewSnapshot()
			must(t, e)
			if _, e = d.Measure(ctx, classify, true); !errors.Is(e, ErrBusy) {
				t.Fatal(e)
			}
			must(t, s.Close())
			initial, e := d.Measure(ctx, classify, true)
			must(t, e)
			if !initial.Stabilized || initial.Files.SSTBytes == 0 || initial.Inventory.State.Bytes()+initial.Inventory.Excluded.Bytes() != initial.Inventory.All.Bytes() || initial.Files.DirectoryBytes != initial.Files.SSTBytes+initial.Files.WALBytes+initial.Files.LogBytes+initial.Files.OtherBytes {
				t.Fatal(initial)
			}
			var composed uint64
			for _, c := range initial.Inventory.Components {
				composed += c.Bytes()
			}
			if composed != initial.Inventory.State.Bytes() {
				t.Fatal("state composition does not close")
			}
			if initial.Inventory.EncodedKVBytes != initial.Inventory.All.Bytes()+initial.Inventory.NamespaceKeyBytes+initial.Inventory.RegistryBytes {
				t.Fatal("namespace composition does not close")
			}
			if engine == LevelDB {
				if initial.Inventory.RegistryBytes == 0 || initial.Inventory.NamespaceKeyBytes == 0 || initial.AfterMaintenance.Engine.WALWriteBytes != nil || initial.AfterMaintenance.Engine.MemtableBytes != nil {
					t.Fatal("unsupported measurements fabricated", initial)
				}
			} else {
				if initial.Inventory.RegistryBytes != 0 || initial.Inventory.NamespaceKeyBytes != 0 || initial.AfterMaintenance.Engine.WALWriteBytes == nil || *initial.AfterMaintenance.Engine.WALWriteBytes == 0 || initial.AfterMaintenance.Engine.FlushWriteBytes == nil || *initial.AfterMaintenance.Engine.FlushWriteBytes == 0 {
					t.Fatal("native engine counters missing", initial)
				}
			}
			b.Reset()
			b.Delete("hash", []byte("state"))
			b.Put("prefix", []byte("state"), []byte{3})
			must(t, d.Write(b))
			final, e := d.Measure(ctx, classify, true)
			must(t, e)
			delta, e := Delta(initial, final)
			must(t, e)
			wrong := final
			wrong.DatabasePath += "-different"
			if _, e = Delta(initial, wrong); !errors.Is(e, ErrInvalid) {
				t.Fatal("different database baseline accepted", e)
			}
			incomplete := initial
			incomplete.Complete = false
			if _, e = Delta(incomplete, final); !errors.Is(e, ErrInvalid) {
				t.Fatal("incomplete initial inventory accepted", e)
			}
			if delta.State.ValueBytes != -1000 || delta.State.Records != -1 || delta.Components["hash"].Records != -1 {
				t.Fatal("negative storage delta was lost", delta)
			}
			writes, e := WriteDelta(initial.AfterMaintenance, final.BeforeMaintenance)
			must(t, e)
			if writes.Commits != 1 || writes.Total.Puts != 1 || writes.Total.Deletes != 1 {
				t.Fatal("maintenance counted as application write", writes)
			}
			b.Reset()
			b.Put("default", []byte("unknown"), nil)
			must(t, d.Write(b))
			if _, e = d.Measure(ctx, classify, false); !errors.Is(e, ErrCorrupt) {
				t.Fatal("unknown record silently excluded", e)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, e = d.Measure(canceled, classify, true); !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
		})
	}
}
