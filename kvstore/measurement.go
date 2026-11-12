package kvstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

type RecordCount struct{ Records, KeyBytes, ValueBytes uint64 }

func (c RecordCount) Bytes() uint64 { return c.KeyBytes + c.ValueBytes }
func (c RecordCount) add(k, v []byte) RecordCount {
	c.Records++
	c.KeyBytes += uint64(len(k))
	c.ValueBytes += uint64(len(v))
	return c
}

type Classification struct {
	Name  string
	State bool
}

// A classifier must reject unknown records. It is called under the DB's
// maintenance lock and must not reenter the database or retain borrowed bytes.
type Classifier func(column string, key, value []byte) (Classification, error)
type Inventory struct {
	State, Excluded, All                    RecordCount
	Components, ExcludedComponents, Columns map[string]RecordCount
	NamespaceKeyBytes, RegistryBytes        uint64
	EncodedKVBytes                          uint64
	SHA256                                  string
}
type FileInventory struct {
	DirectoryBytes, SSTBytes, WALBytes, LogBytes, OtherBytes uint64
	Files                                                    uint64
}
type Measurement struct {
	DatabasePath                        string
	Complete                            bool
	Schema                              string
	Inventory                           Inventory
	Files                               FileInventory
	Stabilized                          bool
	BeforeMaintenance, AfterMaintenance Statistics
}

func (d *DB) statsLocked() (Statistics, error) {
	out := Statistics{Info: d.info(), Scope: d.scope, Writes: d.writes.clone()}
	out.ReadCachePolicy = "disabled"
	if d.options.ApplicationCacheBytes > 0 {
		out.ReadCachePolicy = "point-read-lru"
		if d.readOnly && d.options.ExecutionCacheOnly {
			out.ReadCachePolicy = "disabled-for-readonly"
		}
	}
	if d.cache != nil {
		d.cache.mu.Lock()
		out.ReadCache = d.cache.stats
		d.cache.mu.Unlock()
	}
	if s, ok := d.raw.(statisticsSource); ok {
		var e error
		out.Engine, e = s.statistics()
		if e != nil {
			return out, e
		}
	}
	return out, nil
}

// Measure performs an offline logical inventory and a separate file inventory.
// stabilize flushes and fully compacts before scanning; it requires a writer
// with no live user snapshots. It never belongs inside block/query timing.
// Inventory scans can warm the engine's block cache; reopen for a cold run.
func (d *DB) Measure(ctx context.Context, classify Classifier, stabilize bool) (Measurement, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := Measurement{Schema: "kvstore-measurement/v1", DatabasePath: d.path}
	if e := d.check(); e != nil {
		return out, e
	}
	if e := ctx.Err(); e != nil {
		return out, e
	}
	if classify == nil {
		return out, ErrInvalid
	}
	var e error
	out.BeforeMaintenance, e = d.statsLocked()
	if e != nil {
		return out, e
	}
	if stabilize {
		if d.readOnly {
			return out, ErrReadOnly
		}
		if d.views != 0 {
			return out, ErrBusy
		}
		if e = d.raw.compact(); e != nil {
			return out, e
		}
		out.Stabilized = true
	}
	s, e := d.raw.snapshot()
	if e != nil {
		return out, e
	}
	defer s.close()
	i := Inventory{Components: map[string]RecordCount{}, ExcludedComponents: map[string]RecordCount{}, Columns: map[string]RecordCount{}}
	hasher := sha256.New()
	names := append([]string(nil), d.options.Columns...)
	sort.Strings(names)
	for _, column := range names {
		it, e := s.iterator(column, Bounds{})
		if e != nil {
			return out, e
		}
		scan := func() error {
			defer it.close()
			for ok := it.first(); ok; ok = it.next() {
				if e := ctx.Err(); e != nil {
					return e
				}
				k, v := it.key(), it.value()
				if len(k) > MaxKeyBytes || len(v) > MaxValueBytes {
					return ErrLimit
				}
				class, e := classify(column, k, v)
				if e != nil {
					return e
				}
				if class.Name == "" {
					return fmt.Errorf("%w: unclassified record in %s", ErrCorrupt, column)
				}
				i.All = i.All.add(k, v)
				i.Columns[column] = i.Columns[column].add(k, v)
				if class.State {
					i.State = i.State.add(k, v)
					i.Components[class.Name] = i.Components[class.Name].add(k, v)
				} else {
					i.Excluded = i.Excluded.add(k, v)
					i.ExcludedComponents[class.Name] = i.ExcludedComponents[class.Name].add(k, v)
				}
				if d.options.Engine == LevelDB {
					i.NamespaceKeyBytes += uint64(1 + len(column))
				}
				for _, data := range [][]byte{[]byte(column), k, v} {
					var size [8]byte
					binary.BigEndian.PutUint64(size[:], uint64(len(data)))
					hasher.Write(size[:])
					hasher.Write(data)
				}
			}
			return it.err()
		}
		if e = scan(); e != nil {
			return out, e
		}
	}
	if d.options.Engine == LevelDB {
		r, ok := d.raw.(interface{ registryBytes() (uint64, error) })
		if !ok {
			return out, ErrUnsupported
		}
		i.RegistryBytes, e = r.registryBytes()
		if e != nil {
			return out, e
		}
	}
	i.EncodedKVBytes = i.All.Bytes() + i.NamespaceKeyBytes + i.RegistryBytes
	i.SHA256 = hex.EncodeToString(hasher.Sum(nil))
	out.Inventory = i
	out.Files, e = measureFiles(ctx, d.path)
	if e != nil {
		return out, e
	}
	out.AfterMaintenance, e = d.statsLocked()
	if e == nil {
		out.Complete = true
	}
	return out, e
}
func measureFiles(ctx context.Context, root string) (FileInventory, error) {
	var out FileInventory
	e := filepath.WalkDir(root, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			if path != root && errors.Is(e, fs.ErrNotExist) {
				return nil
			}
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlink in measured DB", ErrCorrupt)
		}
		if d.IsDir() {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			// Obsolete SST deletion is asynchronous even after a completed
			// compaction. A vanished directory entry is no longer disk usage;
			// retain errors for a missing DB root and all other IO failures.
			if path != root && errors.Is(e, fs.ErrNotExist) {
				return nil
			}
			return e
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return ErrCorrupt
		}
		n := uint64(info.Size())
		out.DirectoryBytes += n
		out.Files++
		name := d.Name()
		switch {
		case strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".ldb"):
			out.SSTBytes += n
		case name == "LOG" || strings.HasPrefix(name, "LOG.old"):
			out.LogBytes += n
		case strings.HasSuffix(name, ".log"):
			out.WALBytes += n
		default:
			out.OtherBytes += n
		}
		return nil
	})
	return out, e
}

type SignedRecordCount struct{ Records, KeyBytes, ValueBytes int64 }
type InventoryDelta struct {
	State, Excluded, All                             SignedRecordCount
	Components                                       map[string]SignedRecordCount
	NamespaceKeyBytes, RegistryBytes, EncodedKVBytes int64
}

func signedDelta(a, b uint64) (int64, error) {
	if a > math.MaxInt64 || b > math.MaxInt64 {
		return 0, ErrLimit
	}
	return int64(b) - int64(a), nil
}
func recordDelta(a, b RecordCount) (SignedRecordCount, error) {
	var d SignedRecordCount
	var e error
	d.Records, e = signedDelta(a.Records, b.Records)
	if e != nil {
		return d, e
	}
	d.KeyBytes, e = signedDelta(a.KeyBytes, b.KeyBytes)
	if e != nil {
		return d, e
	}
	d.ValueBytes, e = signedDelta(a.ValueBytes, b.ValueBytes)
	return d, e
}

// Delta preserves negative increments; initialization must be measured for
// each method, rather than replaced with another method's initial inventory.
func Delta(before, after Measurement) (InventoryDelta, error) {
	var out InventoryDelta
	a, b := before.BeforeMaintenance.Info, after.BeforeMaintenance.Info
	ac, bc := append([]string(nil), a.Columns...), append([]string(nil), b.Columns...)
	sort.Strings(ac)
	sort.Strings(bc)
	if !before.Complete || !after.Complete || before.DatabasePath == "" || before.DatabasePath != after.DatabasePath || before.Schema != "kvstore-measurement/v1" || before.Schema != after.Schema || a.Engine != b.Engine || a.Version != b.Version || a.Compression != b.Compression || !slices.Equal(ac, bc) {
		return out, ErrInvalid
	}
	var e error
	out.State, e = recordDelta(before.Inventory.State, after.Inventory.State)
	if e != nil {
		return out, e
	}
	out.Excluded, e = recordDelta(before.Inventory.Excluded, after.Inventory.Excluded)
	if e != nil {
		return out, e
	}
	out.All, e = recordDelta(before.Inventory.All, after.Inventory.All)
	if e != nil {
		return out, e
	}
	out.Components = map[string]SignedRecordCount{}
	keys := map[string]bool{}
	for k := range before.Inventory.Components {
		keys[k] = true
	}
	for k := range after.Inventory.Components {
		keys[k] = true
	}
	for k := range keys {
		out.Components[k], e = recordDelta(before.Inventory.Components[k], after.Inventory.Components[k])
		if e != nil {
			return out, e
		}
	}
	out.NamespaceKeyBytes, e = signedDelta(before.Inventory.NamespaceKeyBytes, after.Inventory.NamespaceKeyBytes)
	if e != nil {
		return out, e
	}
	out.RegistryBytes, e = signedDelta(before.Inventory.RegistryBytes, after.Inventory.RegistryBytes)
	if e != nil {
		return out, e
	}
	out.EncodedKVBytes, e = signedDelta(before.Inventory.EncodedKVBytes, after.Inventory.EncodedKVBytes)
	return out, e
}
