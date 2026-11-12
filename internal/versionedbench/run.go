package versionedbench

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
	"strconv"
	"strings"
	"time"
)

var methods = []string{"HashArchive", "FullNodeVersioned", "HashVersioned"}

type MethodResult struct {
	Name                                                 string
	Initial, Final                                       Inventory
	NodeGrowth, LiveKVGrowth, SSTGrowth, DirectoryGrowth int64
}
type Result struct {
	MeasurementVersion                                                 string
	Spec                                                               Spec
	WorkloadID                                                         string
	UniqueChangedKeys, RequestedWrites, VerifiedRoots, VerifiedQueries int
	Roots                                                              []string
	Methods                                                            []MethodResult
	NodeRatioDefined, SSTRatioDefined                                  bool
	NodeRatioToArchive, SSTRatioToArchive                              float64
	ElapsedSeconds                                                     float64
	Verified                                                           bool
}
type handles struct {
	src     *archiveStore
	full    *leveldb.DB
	compact *vt.Store
}

func (h *handles) close() error {
	var first error
	if h.src != nil {
		if e := h.src.db.Close(); e != nil {
			first = e
		}
		h.src = nil
	}
	if h.full != nil {
		if e := h.full.Close(); e != nil && first == nil {
			first = e
		}
		h.full = nil
	}
	if h.compact != nil {
		if e := h.compact.Close(); e != nil && first == nil {
			first = e
		}
		h.compact = nil
	}
	return first
}
func openHandles(dir string, cfg vt.Config, create bool) (h handles, err error) {
	defer func() {
		if err != nil {
			h.close()
		}
	}()
	db, e := openPlain(filepath.Join(dir, methods[0]), "hash-archive-bench/v1", cfg, create, false)
	if e != nil {
		return h, e
	}
	h.src = &archiveStore{db, false}
	h.full, e = openPlain(filepath.Join(dir, methods[1]), "full-versioned-bench/v1", cfg, create, false)
	if e != nil {
		return h, e
	}
	h.compact, e = vt.Open(filepath.Join(dir, methods[2]), cfg, create, false)
	return h, e
}

type sample struct {
	Index int
	Value []byte
}

func samples(tr Trace, values [][]byte, h, q int) []sample {
	out := make([]sample, 0, q)
	for j := 0; j < q; j++ {
		index := (h*1543 + j*7919) % len(values)
		if j == 0 && h > 0 {
			b := tr.Blocks[h-1]
			index = b[len(b)-1].Index
		}
		out = append(out, sample{index, append([]byte(nil), values[index]...)})
	}
	return out
}
func rootCheck(h *handles, height uint32, root mpt.Hash) error {
	if _, e := mpt.Open(root, h.src); e != nil {
		return e
	}
	if _, e := mpt.Open(root, fullReader{h.full, height}); e != nil {
		return e
	}
	v, e := h.compact.ViewAt(uint64(height))
	if e != nil {
		return e
	}
	defer v.Close()
	if v.Root() != root {
		return fmt.Errorf("stored root mismatch")
	}
	r, e := v.TrieReader(0)
	if e != nil {
		return e
	}
	_, e = mpt.Open(root, r)
	return e
}
func sealInventory(dir string, cfg vt.Config) (map[string]Inventory, error) {
	out := map[string]Inventory{}
	for _, method := range methods {
		v, e := inventory(filepath.Join(dir, method), method, cfg)
		if e != nil {
			return nil, e
		}
		out[method] = v
	}
	return out, nil
}
func verifyHistory(dir string, cfg vt.Config, tr Trace, roots []mpt.Hash, queries [][]sample) (int, error) {
	source, e := leveldb.OpenFile(filepath.Join(dir, methods[0]), options(cfg, false, true))
	if e != nil {
		return 0, e
	}
	defer source.Close()
	full, e := leveldb.OpenFile(filepath.Join(dir, methods[1]), options(cfg, false, true))
	if e != nil {
		return 0, e
	}
	defer full.Close()
	compact, e := vt.Open(filepath.Join(dir, methods[2]), cfg, false, true)
	if e != nil {
		return 0, e
	}
	defer compact.Close()
	verified := 0
	for height, root := range roots {
		e := func() error {
			v, e := compact.ViewAt(uint64(height))
			if e != nil {
				return e
			}
			defer v.Close()
			r, e := v.TrieReader(0)
			if e != nil {
				return e
			}
			stores := []mpt.Store{&archiveStore{source, true}, fullReader{full, uint32(height)}, r}
			for _, store := range stores {
				tree, e := mpt.Open(root, store)
				if e != nil {
					return e
				}
				for _, q := range queries[height] {
					key := tr.Keys[q.Index][:]
					got, e := tree.Get(key)
					if e != nil || !bytes.Equal(got, q.Value) {
						return fmt.Errorf("historical value mismatch at height %d: %v", height, e)
					}
					p, e := tree.Prove(key)
					if e != nil {
						return e
					}
					got, exists, e := mpt.VerifyProof(root, key, p)
					if e != nil || !exists || !bytes.Equal(got, q.Value) {
						return fmt.Errorf("historical proof mismatch at height %d: %v", height, e)
					}
					verified++
				}
			}
			return nil
		}()
		if e != nil {
			return verified, e
		}
	}
	return verified, nil
}
func RunCase(spec Spec, dir string, log io.Writer) (Result, error) {
	start := time.Now()
	res := Result{Spec: spec, MeasurementVersion: "sealed-live-sst/v3"}
	tr, e := Generate(spec)
	if e != nil {
		return res, e
	}
	res.WorkloadID = tr.ID
	res.UniqueChangedKeys = tr.UniqueChangedKeys
	res.RequestedWrites = spec.Config.Blocks * spec.Config.UpdatesPerBlock
	if e = os.Mkdir(dir, 0755); e != nil {
		return res, e
	}
	cfg := vt.DefaultConfig("raw-bytes/v1", tr.ID)
	cfg.MaxBatchBytes = 64 << 20
	h, e := openHandles(dir, cfg, true)
	if e != nil {
		return res, e
	}
	defer h.close()
	before := mpt.EmptyRootHash
	values := make([][]byte, len(tr.Initial))
	copy(values, tr.Initial)
	var roots []mpt.Hash
	var queries [][]sample
	var initial map[string]Inventory
	var emitted uint64
	for height := 0; height <= spec.Config.Blocks; height++ {
		var tree, previous *mpt.Trie
		if height == 0 {
			tree, e = mpt.NewEmpty(h.src)
			previous, _ = mpt.NewEmpty(h.src)
			for i, k := range tr.Keys {
				if e = tree.Update(k[:], values[i]); e != nil {
					return res, e
				}
			}
		} else {
			tree, e = mpt.Open(before, h.src)
			if e != nil {
				return res, e
			}
			previous, e = mpt.Open(before, h.src)
			if e != nil {
				return res, e
			}
			for _, m := range tr.Blocks[height-1] {
				if e = tree.Update(tr.Keys[m.Index][:], m.Value); e != nil {
					return res, e
				}
				values[m.Index] = m.Value
			}
		}
		root, e := tree.Hash()
		if e != nil {
			return res, e
		}
		id := sha256.Sum256([]byte(tr.ID + "/" + strconv.Itoa(height)))
		u, e := h.compact.Begin(uint64(height), root, id)
		if e != nil {
			return res, e
		}
		stats, e := u.ProjectTrie(0, before, tree)
		if e != nil {
			u.Abort()
			return res, e
		}
		fb := new(leveldb.Batch)
		fullStats, e := tree.DiffLogicalNodes(previous, vt.MaxProjectionNodes, func(ch mpt.LogicalChange) error {
			k, e := vt.EncodeNodeKey(0, ch.Path, uint64(height))
			if e != nil {
				return e
			}
			fb.Put(k, ch.RLP)
			return nil
		})
		if e != nil {
			u.Abort()
			return res, e
		}
		if fullStats.Emitted != stats.Emitted {
			u.Abort()
			return res, fmt.Errorf("different emitted node sets")
		}
		emitted += stats.Emitted
		batch := h.src.NewBatch().(*archiveBatch)
		staged, e := tree.Stage(batch)
		if e != nil || staged != root {
			u.Abort()
			return res, fmt.Errorf("archive staging failed: %v", e)
		}
		meta(batch.b, uint32(height), root, id)
		meta(fb, uint32(height), root, id)
		if e = batch.Write(); e != nil {
			u.Abort()
			return res, e
		}
		if e = h.full.Write(fb, &opt.WriteOptions{Sync: true}); e != nil {
			u.Abort()
			return res, e
		}
		if e = u.Commit(); e != nil {
			return res, e
		}
		if e = rootCheck(&h, uint32(height), root); e != nil {
			return res, e
		}
		res.VerifiedRoots += 3
		roots = append(roots, root)
		res.Roots = append(res.Roots, hex.EncodeToString(root[:]))
		queries = append(queries, samples(tr, values, height, spec.Config.QueriesPerHeight))
		before = root
		if height == 0 {
			if e = h.close(); e != nil {
				return res, e
			}
			initial, e = sealInventory(dir, cfg)
			if e != nil {
				return res, e
			}
			h, e = openHandles(dir, cfg, false)
			if e != nil {
				return res, e
			}
		}
		if height%10 == 0 || height == spec.Config.Blocks {
			fmt.Fprintf(log, "value=%d pattern=%s block=%d/%d\n", spec.ValueBytes, spec.Pattern, height, spec.Config.Blocks)
		}
	}
	if e = h.close(); e != nil {
		return res, e
	}
	final, e := sealInventory(dir, cfg)
	if e != nil {
		return res, e
	}
	if final[methods[1]].NodeKeysSHA256 != final[methods[2]].NodeKeysSHA256 || initial[methods[1]].NodeKeysSHA256 != initial[methods[2]].NodeKeysSHA256 {
		return res, fmt.Errorf("different version key sets")
	}
	if final[methods[1]].Nodes.Entries != int64(emitted) || final[methods[2]].Nodes.Entries != int64(emitted) {
		return res, fmt.Errorf("node inventory mismatch")
	}
	for _, method := range methods {
		a, b := initial[method], final[method]
		res.Methods = append(res.Methods, MethodResult{method, a, b, b.Nodes.Bytes() - a.Nodes.Bytes(), b.Total.Bytes() - a.Total.Bytes(), b.SSTBytes - a.SSTBytes, b.DirectoryBytes - a.DirectoryBytes})
	}
	res.VerifiedQueries, e = verifyHistory(dir, cfg, tr, roots, queries)
	if e != nil {
		return res, e
	}
	// Read-only validation must not change any file bytes or size. SST/directory
	// inventory is measured before and after without triggering another compact.
	for _, m := range res.Methods {
		size, e := directorySize(filepath.Join(dir, m.Name))
		if e != nil {
			return res, e
		}
		if size != m.Final.DirectoryBytes {
			return res, fmt.Errorf("read-only validation changed database size")
		}
		fingerprint, e := directoryFingerprint(filepath.Join(dir, m.Name))
		if e != nil {
			return res, e
		}
		if fingerprint != m.Final.Fingerprint {
			return res, fmt.Errorf("read-only validation changed database content")
		}
	}
	if res.Methods[0].NodeGrowth > 0 && res.Methods[2].NodeGrowth >= 0 {
		res.NodeRatioDefined = true
		res.NodeRatioToArchive = float64(res.Methods[2].NodeGrowth) / float64(res.Methods[0].NodeGrowth)
	}
	if res.Methods[0].SSTGrowth > 0 && res.Methods[2].SSTGrowth >= 0 {
		res.SSTRatioDefined = true
		res.SSTRatioToArchive = float64(res.Methods[2].SSTGrowth) / float64(res.Methods[0].SSTGrowth)
	}
	res.ElapsedSeconds = time.Since(start).Seconds()
	res.Verified = true
	return res, nil
}
func directorySize(path string) (int64, error) {
	var n int64
	e := filepath.WalkDir(path, func(_ string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			i, e := d.Info()
			if e != nil {
				return e
			}
			n += i.Size()
		}
		return nil
	})
	return n, e
}
func writeJSON(path string, v interface{}) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.Write(append(raw, '\n')); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(name, path)
}
func Run(cfg Config, out string, log io.Writer) error {
	if e := cfg.Validate(); e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(out), 0755); e != nil {
		return e
	}
	if e := os.Mkdir(out, 0755); e != nil {
		return e
	}
	revision, _ := exec.Command("git", "rev-parse", "HEAD").Output()
	dirty, _ := exec.Command("git", "status", "--porcelain").Output()
	wire, _ := json.Marshal(cfg)
	manifest := struct {
		Config              Config
		ConfigID, Commit    string
		Dirty               bool
		GoVersion, Platform string
	}{cfg, digest(wire), strings.TrimSpace(string(revision)), len(dirty) != 0, runtime.Version(), runtime.GOOS + "/" + runtime.GOARCH}
	if e := writeJSON(filepath.Join(out, "configuration.json"), manifest); e != nil {
		return e
	}
	results := []Result{}
	for _, width := range cfg.ValueBytes {
		for _, pattern := range cfg.Patterns {
			name := fmt.Sprintf("value%d-%s", width, pattern)
			if e := writeJSON(filepath.Join(out, "status.json"), map[string]interface{}{"Running": name, "Completed": len(results), "Total": len(cfg.ValueBytes) * len(cfg.Patterns)}); e != nil {
				return e
			}
			r, e := RunCase(Spec{cfg, width, pattern}, filepath.Join(out, name), log)
			if e != nil {
				writeJSON(filepath.Join(out, "failure.json"), map[string]interface{}{"Case": name, "Error": e.Error(), "Completed": len(results)})
				return e
			}
			if e = writeJSON(filepath.Join(out, name, "result.json"), r); e != nil {
				return e
			}
			results = append(results, r)
			if e = writeJSON(filepath.Join(out, "results.json"), results); e != nil {
				return e
			}
			fmt.Fprintf(log, "completed %s: node %s; SST net growth %s\n", name, ratio(r.NodeRatioToArchive, r.NodeRatioDefined), ratio(r.SSTRatioToArchive, r.SSTRatioDefined))
		}
	}
	if e := writeCSV(filepath.Join(out, "results.csv"), results); e != nil {
		return e
	}
	if e := report(filepath.Join(out, "report.txt"), results); e != nil {
		return e
	}
	return writeJSON(filepath.Join(out, "status.json"), map[string]interface{}{"Status": "complete", "Completed": len(results), "Verified": true})
}
func writeCSV(path string, results []Result) error {
	f, e := os.Create(path)
	if e != nil {
		return e
	}
	w := csv.NewWriter(f)
	w.Write([]string{"value_bytes", "pattern", "method", "initial_node_kv", "final_node_kv", "node_kv_growth", "initial_live_kv", "final_live_kv", "live_kv_growth", "initial_live_sst", "final_live_sst", "live_sst_net_growth", "initial_directory", "final_directory", "directory_net_growth"})
	for _, r := range results {
		for _, m := range r.Methods {
			row := []string{strconv.Itoa(r.Spec.ValueBytes), r.Spec.Pattern, m.Name}
			for _, n := range []int64{m.Initial.Nodes.Bytes(), m.Final.Nodes.Bytes(), m.NodeGrowth, m.Initial.Total.Bytes(), m.Final.Total.Bytes(), m.LiveKVGrowth, m.Initial.SSTBytes, m.Final.SSTBytes, m.SSTGrowth, m.Initial.DirectoryBytes, m.Final.DirectoryBytes, m.DirectoryGrowth} {
				row = append(row, strconv.FormatInt(n, 10))
			}
			w.Write(row)
		}
	}
	w.Flush()
	if e = w.Error(); e != nil {
		f.Close()
		return e
	}
	return f.Close()
}
func report(path string, results []Result) error {
	var b bytes.Buffer
	b.WriteString("Synthetic block-write storage comparison (no EVM)\n\nCommon LevelDB/Snappy/cache budgets and Sync writes. Initialization and final measurements both follow offline compaction and an engine-managed obsolete-file cleanup reopen.\nNode KV growth excludes initialization exactly. SST growth is final-minus-initial LIVE table bytes from LevelDB metadata, excluding obsolete files. Compaction mixes initialization and history, so this is not an independently isolated history file size. Directory bytes separately include unreclaimed obsolete files and engine metadata.\nFullNodeVersioned stores full RLP at identical version keys; HashArchive is the original hash-key archive. Raw random leaf bytes are not Ethereum account/storage encodings.\n\n")
	for _, r := range results {
		fmt.Fprintf(&b, "value=%d pattern=%s keys=%d blocks=%d requested-writes=%d net-key-updates=%d\n", r.Spec.ValueBytes, r.Spec.Pattern, r.Spec.Config.Keys, r.Spec.Config.Blocks, r.RequestedWrites, r.UniqueChangedKeys)
		for _, m := range r.Methods {
			fmt.Fprintf(&b, "  %-18s node KV +%.3f MiB; live KV +%.3f MiB; live SST +%.3f MiB; final directory %.3f MiB\n", m.Name, float64(m.NodeGrowth)/(1<<20), float64(m.LiveKVGrowth)/(1<<20), float64(m.SSTGrowth)/(1<<20), float64(m.Final.DirectoryBytes)/(1<<20))
		}
		fmt.Fprintf(&b, "  compact/archive: node growth %s; SST net growth %s. Roots=%d queries=%d verified.\n\n", ratio(r.NodeRatioToArchive, r.NodeRatioDefined), ratio(r.SSTRatioToArchive, r.SSTRatioDefined), r.VerifiedRoots, r.VerifiedQueries)
	}
	return os.WriteFile(path, b.Bytes(), 0644)
}

func ratio(v float64, defined bool) string {
	if !defined {
		return "N/A (nonpositive net growth)"
	}
	return fmt.Sprintf("%.2f%% (saving %.2f%%)", 100*v, 100*(1-v))
}
