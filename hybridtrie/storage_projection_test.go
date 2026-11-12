//go:build rocksdb && cgo

package hybridtrie

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/table"
	"hash"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"strings"
	"testing"
)

type projectedFamily struct {
	Records, KeyBytes, ValueBytes, SSTBytes uint64
	KV_SHA256, FileSHA256                   string
}
type projectedStorage struct {
	Families map[string]projectedFamily
	SSTBytes uint64
	Verified bool
}
type projectionResult struct {
	Workload, Method, TraceID, ExperimentBinaryID string
	Initial, Final                                projectedStorage
}

func hashProjectedKV(h hash.Hash, key, value []byte) {
	var sizes [16]byte
	binary.BigEndian.PutUint64(sizes[:], uint64(len(key)))
	binary.BigEndian.PutUint64(sizes[8:], uint64(len(value)))
	h.Write(sizes[:])
	h.Write(key)
	h.Write(value)
}
func projectCompressedStorage(native *rocksEngine, out string) (projectedStorage, error) {
	p := projectedStorage{Families: map[string]projectedFamily{}}
	if e := os.MkdirAll(out, 0700); e != nil {
		return p, e
	}
	options := &opt.Options{Compression: opt.SnappyCompression, BlockSize: 4096, BlockRestartInterval: 16}
	for cf, name := range []string{"metadata", "prefix", "hash"} {
		path := filepath.Join(out, name+".sst")
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return p, e
		}
		writer := table.NewWriter(f, options, nil, 0)
		family := projectedFamily{}
		original := sha256.New()
		e = native.exportFamily(column(cf), func(key, value []byte) error {
			family.Records++
			family.KeyBytes += uint64(len(key))
			family.ValueBytes += uint64(len(value))
			hashProjectedKV(original, key, value)
			return writer.Append(key, value)
		})
		if e == nil {
			e = writer.Close()
		}
		if e == nil {
			e = f.Sync()
		}
		closeErr := f.Close()
		if e != nil {
			return p, e
		}
		if closeErr != nil {
			return p, closeErr
		}
		info, e := os.Stat(path)
		if e != nil {
			return p, e
		}
		family.SSTBytes = uint64(info.Size())
		family.KV_SHA256 = hex.EncodeToString(original.Sum(nil))
		read, e := os.Open(path)
		if e != nil {
			return p, e
		}
		reader, e := table.NewReader(read, info.Size(), storage.FileDesc{Type: storage.TypeTable, Num: int64(cf)}, nil, nil, options)
		if e != nil {
			read.Close()
			return p, e
		}
		iterator := reader.NewIterator(nil, &opt.ReadOptions{Strict: opt.StrictAll})
		check := sha256.New()
		var records uint64
		for iterator.Next() {
			records++
			hashProjectedKV(check, iterator.Key(), iterator.Value())
		}
		e = iterator.Error()
		iterator.Release()
		reader.Release()
		if e != nil {
			return p, e
		}
		if records != family.Records || hex.EncodeToString(check.Sum(nil)) != family.KV_SHA256 {
			return p, fmt.Errorf("projected SST content mismatch")
		}
		raw, e := os.ReadFile(path)
		if e != nil {
			return p, e
		}
		sum := sha256.Sum256(raw)
		family.FileSHA256 = hex.EncodeToString(sum[:])
		p.Families[name] = family
		p.SSTBytes += family.SSTBytes
	}
	p.Verified = true
	return p, nil
}
func compareProjectionInventory(p projectedStorage, m storageMeasurement) error {
	for name, f := range p.Families {
		want := m.Families[name]
		if f.Records != want.Records || f.KeyBytes != want.KeyBytes || f.ValueBytes != want.ValueBytes {
			return fmt.Errorf("projection inventory mismatch for %s", name)
		}
	}
	return nil
}

// This supplements sealed experiments, never substitutes projected Snappy bytes
// for their actual uncompressed RocksDB files. Each projected SST is reopened
// and its complete ordered Key/Value digest checked against the source scan.
func TestHybridStorageSnappyProjection(t *testing.T) {
	output := os.Getenv("HYBRID_STORAGE_RESULTS")
	if output == "" {
		t.Skip("set HYBRID_STORAGE_RESULTS to a completed storage experiment")
	}
	raw, e := os.ReadFile(filepath.Join(output, "completion.json"))
	if e != nil {
		t.Fatal(e)
	}
	var completion struct {
		Verified bool
		Groups   int
	}
	if e = json.Unmarshal(raw, &completion); e != nil || !completion.Verified {
		t.Fatal("experiment incomplete", e)
	}
	raw, e = os.ReadFile(filepath.Join(output, "results.json"))
	if e != nil {
		t.Fatal(e)
	}
	var groups []experimentResult
	if e = json.Unmarshal(raw, &groups); e != nil || len(groups) != completion.Groups {
		t.Fatal("invalid sealed result set", e)
	}
	dir := filepath.Join(output, "snappy-projection")
	if _, e = os.Stat(dir); e == nil {
		t.Fatal("projection already exists; preserving previous output")
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	traces := map[string]experimentTrace{}
	var results []projectionResult
	for _, group := range groups {
		if !group.Verified || !group.ReadOnlyUnchanged {
			t.Fatal("unverified source group")
		}
		tr, ok := traces[group.Workload]
		if !ok {
			tr, e = buildExperimentTrace(group.Config, group.Workload)
			if e != nil {
				t.Fatal(e)
			}
			traces[group.Workload] = tr
		}
		if tr.ID != group.TraceID {
			t.Fatal("trace identity differs")
		}
		var variant experimentVariant
		found := false
		for _, candidate := range storageVariants {
			if candidate.Name == group.Method {
				variant = candidate
				found = true
			}
		}
		if !found {
			t.Fatal("unknown method")
		}
		cfg := DefaultConfig("raw-bytes/v1", tr.ID)
		row := projectionResult{Workload: group.Workload, Method: group.Method, TraceID: tr.ID, ExperimentBinaryID: group.BinaryID}
		// Regenerate only the initial snapshot. Check its inventory against the sealed
		// initial measurement; no execution blocks are replayed for this supplement.
		temp := filepath.Join(t.TempDir(), "initial")
		runner, e := openExperimentRunner(temp, cfg, variant.Archive, true, false)
		if e != nil {
			t.Fatal(e)
		}
		tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
		before := tree.Copy()
		for i, key := range tr.Keys {
			tree.Update(key, tr.Initial[i])
		}
		if root, _ := tree.Hash(); root != tr.Roots[0] {
			t.Fatal("initial root changed")
		}
		if _, e = runner.commit(0, before, tree, variant.layout(0, group.Config.Blocks)); e != nil {
			t.Fatal(e)
		}
		row.Initial, e = projectCompressedStorage(runner.db.(*rocksEngine), filepath.Join(dir, group.Workload, group.Method, "initial"))
		runner.close()
		if e != nil {
			t.Fatal(e)
		}
		if e = compareProjectionInventory(row.Initial, group.Initial); e != nil {
			t.Fatal(e)
		}
		database := filepath.Join(output, group.Workload, group.Method, "db")
		fingerprint, e := experimentFingerprint(database)
		if e != nil || fingerprint != group.DatabaseSHA256 {
			t.Fatal("sealed database changed", e)
		}
		runner, e = openExperimentRunner(database, cfg, variant.Archive, false, true)
		if e != nil {
			t.Fatal(e)
		}
		row.Final, e = projectCompressedStorage(runner.db.(*rocksEngine), filepath.Join(dir, group.Workload, group.Method, "final"))
		runner.close()
		if e != nil {
			t.Fatal(e)
		}
		if e = compareProjectionInventory(row.Final, group.Final); e != nil {
			t.Fatal(e)
		}
		after, e := experimentFingerprint(database)
		if e != nil || after != fingerprint {
			t.Fatal("projection changed source database", e)
		}
		results = append(results, row)
		if e = writeExperimentJSON(filepath.Join(dir, "results.json"), results); e != nil {
			t.Fatal(e)
		}
		t.Logf("projected workload=%s method=%s initial=%d final=%d delta=%d", row.Workload, row.Method, row.Initial.SSTBytes, row.Final.SSTBytes, int64(row.Final.SSTBytes)-int64(row.Initial.SSTBytes))
	}
	var report strings.Builder
	report.WriteString("# 统一Snappy SST投影\n\n这是逐CF导出完整live Key/Value后，以GoLevelDB table格式、Snappy、4KiB块及restart=16生成的统一投影，不是RocksDB自身压缩后的物理空间。所有寻址字段、selfHash、元数据均保留。初始化仅重建snapshot并核对原封存inventory，执行区间直接读取原始只读数据库。每个SST重开后按完整有序KV摘要逐条验证。\n\n|负载|方法|初始化Snappy MiB|执行增量Snappy MiB|相对Archive|\n|---|---|---:|---:|---:|\n")
	baselines := map[string]int64{}
	for _, r := range results {
		if r.Method == "native-hash-archive" {
			baselines[r.Workload] = int64(r.Final.SSTBytes) - int64(r.Initial.SSTBytes)
		}
	}
	for _, r := range results {
		delta := int64(r.Final.SSTBytes) - int64(r.Initial.SSTBytes)
		fmt.Fprintf(&report, "|%s|%s|%.3f|%.3f|%.3fx|\n", r.Workload, r.Method, float64(r.Initial.SSTBytes)/(1<<20), float64(delta)/(1<<20), float64(delta)/float64(baselines[r.Workload]))
	}
	report.WriteString("\n两个终点之差不等同于单独隔离出的history文件；投影删除了引擎内部sequence metadata/多版本垃圾，但没有删除任何应用KV。它回答同一序列化、压缩器下的表示体积，不预测RocksDB换压缩策略后的读写性能。\n")
	if e = os.WriteFile(filepath.Join(dir, "report.zh.md"), []byte(report.String()), 0600); e != nil {
		t.Fatal(e)
	}
	if e = writeExperimentJSON(filepath.Join(dir, "completion.json"), map[string]interface{}{"Verified": true, "Groups": len(results), "Projection": "exact-live-kv-goleveldb-snappy-4k-restart16/v1", "SourceUnchanged": true}); e != nil {
		t.Fatal(e)
	}
}
