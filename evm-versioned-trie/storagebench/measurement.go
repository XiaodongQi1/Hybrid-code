package storagebench

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"os"
	"path/filepath"
	vb "github.com/XiaodongQi1/Hybrid-code/internal/versionedbench"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

func stateClass(method string, k, v []byte) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("empty key")
	}
	switch method {
	case "HashArchive":
		if len(k) == 32 {
			n, e := mpt.InspectCanonicalNode(v)
			if e != nil {
				return "", e
			}
			switch n.Kind {
			case mpt.CanonicalBranch:
				return "branch-nodes", nil
			case mpt.CanonicalExtension:
				return "extension-nodes", nil
			case mpt.CanonicalLeaf:
				return "leaf-nodes", nil
			}
		}
		if len(k) == 33 && k[0] == 'C' {
			return "code", nil
		}
	case "HashVersioned":
		switch k[0] {
		case 'N':
			key, e := vt.DecodeNodeKey(k)
			if e != nil {
				return "", e
			}
			if _, e = vt.DecodeRecord(v); e != nil {
				return "", e
			}
			if key.TrieID == 0 {
				return "account-node-versions", nil
			}
			return "storage-node-versions", nil
		case 'L':
			return "lifecycle", nil
		case 'C':
			return "code", nil
		}
	case "PathV7":
		switch k[0] {
		case 'P':
			return "current-trie", nil
		case 'F':
			return "current-flat", nil
		case 'H', 'D', 'G':
			return "history-payload-and-framing", nil
		case 'R':
			return "history-directory", nil
		case 'X', 'Y', 'I', 'J':
			return "history-index", nil
		case 'Q':
			return "history-checkpoints", nil
		case 'C':
			return "code", nil
		}
	default:
		return "", fmt.Errorf("unknown method")
	}
	return "", nil
}

// measure extracts unchanged actual state KV encodings to an isolated LevelDB
// and adds the SAME canonical root directory to every method. This excludes the
// Path engine's ledger/receipts without pretending mixed-engine SST files can
// be attributed to one logical table. Both endpoints use identical compaction.
func measure(source, projection, method string, c Config, id string, roots []rootRow) (out Measurement, err error) {
	out.Components = map[string]vb.Counts{}
	src, e := leveldb.OpenFile(source, dbOptions(c, false, true))
	if e != nil {
		return out, e
	}
	defer src.Close()
	dst, e := leveldb.OpenFile(projection, dbOptions(c, true, false))
	if e != nil {
		return out, e
	}
	defer func() {
		if dst != nil {
			dst.Close()
		}
	}()
	batch := new(leveldb.Batch)
	hasher := sha256.New()
	flush := func() error {
		if batch.Len() == 0 {
			return nil
		}
		e := dst.Write(batch, &opt.WriteOptions{Sync: true})
		batch.Reset()
		return e
	}
	it := src.NewIterator(nil, nil)
	for it.Next() {
		k, v := it.Key(), it.Value()
		out.SourceAllKVBytes += int64(len(k) + len(v))
		class, e := stateClass(method, k, v)
		if e != nil {
			it.Release()
			return out, e
		}
		if class == "" {
			continue
		}
		n := out.Components[class]
		n.Entries++
		n.KeyBytes += int64(len(k))
		n.ValueBytes += int64(len(v))
		out.Components[class] = n
		out.StateBytes += int64(len(k) + len(v))
		fmt.Fprintf(hasher, "%d:%d:", len(k), len(v))
		hasher.Write(k)
		hasher.Write(v)
		batch.Put(k, v)
		if len(batch.Dump()) >= 4<<20 {
			if e = flush(); e != nil {
				it.Release()
				return out, e
			}
		}
	}
	e = it.Error()
	it.Release()
	if e != nil {
		return out, e
	}
	for _, r := range roots {
		k, v := rootKey(r.Height), rootValue(r)
		batch.Put(k, v)
		out.CanonicalRootBytes += int64(len(k) + len(v))
	}
	if e = flush(); e != nil {
		return out, e
	}
	if e = dst.Close(); e != nil {
		return out, e
	}
	dst = nil
	out.StateKVHash = hex.EncodeToString(hasher.Sum(nil))
	out.Projected, e = vb.SealInventory(projection, method, c.versionConfig(id))
	if e != nil {
		return out, e
	}
	if out.Projected.Total.Bytes() != out.StateBytes+out.CanonicalRootBytes {
		return out, fmt.Errorf("projected KV inventory does not close")
	}
	e = filepath.WalkDir(source, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			s, e := d.Info()
			if e != nil {
				return e
			}
			out.SourceDirectoryBytes += s.Size()
		}
		return nil
	})
	return out, e
}
