//go:build rocksdb && cgo

package hybridtrie

import "github.com/XiaodongQi1/Hybrid-code/mpt"

// Diagnostic counters count live user Key/Value bytes, including all local
// metadata. They do not equate logical bytes or compacted SSTs with total disk IO.
type familyMeasurement struct {
	Records, KeyBytes, ValueBytes, LiveSSTBytes                                    uint64
	BranchRecords, ExtensionRecords, LeafRecords, ExplicitHashBytes, EmbeddedBytes uint64
}

func (f familyMeasurement) total() uint64 { return f.KeyBytes + f.ValueBytes }

type storageMeasurement struct {
	Families                                                                                map[string]familyMeasurement
	LogicalBytes, LiveSSTBytes, DirectoryBytes, SSTDirectoryBytes, WALBytes, OtherFileBytes uint64
	HashCopiesInPrefix, HashCopyBytes                                                       uint64
}

func (r *kvEngine) property(cf column, name string) (uint64, error) {
	col, e := family(cf)
	if e != nil {
		return 0, e
	}
	return r.store.IntProperty(col, name)
}
func (r *kvEngine) stabilize() error { return r.store.Compact() }
func (r *kvEngine) measure() (storageMeasurement, error) {
	result := storageMeasurement{Families: map[string]familyMeasurement{}}
	snap := r.snapshot().(*kvSnapshot)
	defer snap.close()
	prefixDigests := map[mpt.Hash]bool{}
	for cf := metaColumn; cf <= hashColumn; cf++ {
		f := familyMeasurement{}
		scanErr := snap.scan(cf, nil, nil, func(key, value []byte) error {
			if len(key) > 128 {
				return ErrLimit
			}
			f.Records++
			f.KeyBytes += uint64(len(key))
			f.ValueBytes += uint64(len(value))
			var kind mpt.CanonicalKind
			if cf == prefixColumn {
				record, e := DecodeRecord(value)
				if e != nil {
					return e
				}
				kind = record.Kind
				prefixDigests[record.Hash] = true
				_, bh, _ := record.Bitmaps()
				for i := 0; i < 16; i++ {
					if bh&(1<<i) != 0 {
						f.ExplicitHashBytes += 32
					}
					f.EmbeddedBytes += uint64(len(record.Children[i].Inline))
				}
			} else if cf == hashColumn {
				if len(key) != 32 {
					return ErrCorrupt
				}
				var digest mpt.Hash
				copy(digest[:], key)
				desc, e := canonicalHash(value, digest)
				if e != nil {
					return e
				}
				kind = desc.Kind
				for _, ref := range desc.Children {
					if ref.Present && len(ref.Inline) == 0 {
						f.ExplicitHashBytes += 32
					}
					f.EmbeddedBytes += uint64(len(ref.Inline))
				}
				if prefixDigests[digest] {
					result.HashCopiesInPrefix++
					result.HashCopyBytes += uint64(len(key) + len(value))
				}
			}
			if cf != metaColumn {
				switch kind {
				case mpt.CanonicalBranch:
					f.BranchRecords++
				case mpt.CanonicalExtension:
					f.ExtensionRecords++
				case mpt.CanonicalLeaf:
					f.LeafRecords++
				}
			}
			return nil
		})
		if scanErr != nil {
			return result, scanErr
		}
		var e error
		f.LiveSSTBytes, e = r.property(cf, "rocksdb.live-sst-files-size")
		if e != nil {
			return result, e
		}
		names := []string{"metadata", "prefix", "hash"}
		result.Families[names[cf]] = f
		result.LogicalBytes += f.total()
		result.LiveSSTBytes += f.LiveSSTBytes
	}
	return result, nil
}

// exportFamily copies exact live user entries for a separately labelled,
// engine-independent compressed-SST projection. No addressing fields are dropped.
func (r *kvEngine) exportFamily(cf column, visit func([]byte, []byte) error) error {
	snap := r.snapshot().(*kvSnapshot)
	defer snap.close()
	return snap.scan(cf, nil, nil, func(key, value []byte) error {
		if len(key) > 128 {
			return ErrLimit
		}
		return visit(key, value)
	})
}
