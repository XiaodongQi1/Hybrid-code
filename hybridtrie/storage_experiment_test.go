//go:build rocksdb && cgo

package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/sha3"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"strconv"
	"strings"
	"testing"
	"time"
)

type storageExperimentConfig struct {
	Keys, Blocks, WritesPerBlock, ValueBytes, Queries int
	Seed                                              int64
	Workloads                                         []string
	OutputDir                                         string
}
type experimentWrite struct {
	ID    int
	Value []byte
}
type experimentBlock struct{ Writes []experimentWrite }
type experimentTrace struct {
	Keys, Initial [][]byte
	Blocks        []experimentBlock
	Roots         []mpt.Hash
	ID            string
}
type experimentVariant struct {
	Name     string
	Archive  bool
	Frontier int
	RootMask uint16
	Switch   bool
}

var storageVariants = []experimentVariant{
	{Name: "native-hash-archive", Archive: true},
	{Name: "full-prefix", Frontier: 65, RootMask: 0xffff},
	{Name: "full-hash-dual", Frontier: 0},
	{Name: "root-half-H", Frontier: 1, RootMask: 0x00ff},
	{Name: "root-all-H", Frontier: 1, RootMask: 0xffff},
	{Name: "frontier-2", Frontier: 2, RootMask: 0xffff},
	{Name: "frontier-2-switch", Frontier: 2, RootMask: 0xffff, Switch: true},
}

func (v experimentVariant) layout(block, blocks int) LayoutSelector {
	frontier := v.Frontier
	if v.Switch && block > blocks/2 && block <= blocks*3/4 {
		frontier = 0
	}
	return func(ctx NodeKey, eligible uint16) (uint16, error) {
		if len(ctx.Path) >= frontier {
			return 0, nil
		}
		if len(ctx.Path) == 0 {
			return eligible & v.RootMask, nil
		}
		return eligible, nil
	}
}
func experimentValue(seed int64, height, write, id, size int) []byte {
	var input [32]byte
	binary.BigEndian.PutUint64(input[:], uint64(seed))
	binary.BigEndian.PutUint64(input[8:], uint64(height))
	binary.BigEndian.PutUint64(input[16:], uint64(write))
	binary.BigEndian.PutUint64(input[24:], uint64(id))
	sum := sha256.Sum256(input[:])
	out := make([]byte, size)
	for i := range out {
		out[i] = sum[i%32]
	}
	return out
}
func buildExperimentTrace(c storageExperimentConfig, workload string) (experimentTrace, error) {
	tr := experimentTrace{}
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	for i := 0; i < c.Keys; i++ {
		h := sha3.NewLegacyKeccak256()
		var id [16]byte
		binary.BigEndian.PutUint64(id[:], uint64(c.Seed))
		binary.BigEndian.PutUint64(id[8:], uint64(i))
		h.Write(id[:])
		key := h.Sum(nil)
		value := experimentValue(c.Seed, 0, 0, i, c.ValueBytes)
		tr.Keys = append(tr.Keys, key)
		tr.Initial = append(tr.Initial, value)
		if e := tree.Update(key, value); e != nil {
			return tr, e
		}
	}
	root, e := tree.Hash()
	if e != nil {
		return tr, e
	}
	tr.Roots = append(tr.Roots, root)
	rng := rand.New(rand.NewSource(c.Seed))
	for height := 1; height <= c.Blocks; height++ {
		b := experimentBlock{}
		for w := 0; w < c.WritesPerBlock; w++ {
			id := rng.Intn(c.Keys)
			if workload == "hot" && rng.Intn(10) < 9 {
				id = rng.Intn(c.Keys / 5)
			}
			value := experimentValue(c.Seed, height, w, id, c.ValueBytes)
			b.Writes = append(b.Writes, experimentWrite{id, value})
			if e := tree.Update(tr.Keys[id], value); e != nil {
				return tr, e
			}
		}
		tr.Blocks = append(tr.Blocks, b)
		root, e = tree.Hash()
		if e != nil {
			return tr, e
		}
		tr.Roots = append(tr.Roots, root)
	}
	raw, _ := json.Marshal(struct {
		Keys, Initial [][]byte
		Blocks        []experimentBlock
	}{tr.Keys, tr.Initial, tr.Blocks})
	sum := sha256.Sum256(raw)
	tr.ID = hex.EncodeToString(sum[:])
	return tr, nil
}
func expectedExperimentValue(tr experimentTrace, height, id int) []byte {
	value := tr.Initial[id]
	for i := 0; i < height; i++ {
		for _, w := range tr.Blocks[i].Writes {
			if w.ID == id {
				value = w.Value
			}
		}
	}
	return value
}

type experimentRunner struct {
	s       *Store
	db      diskEngine
	head    Head
	cfg     Config
	archive bool
}

func openExperimentRunner(path string, cfg Config, archive, create, ro bool) (*experimentRunner, error) {
	r := &experimentRunner{cfg: cfg, archive: archive, head: Head{NextTrieID: 1}}
	if !archive {
		s, e := Open(path, cfg, create, ro)
		if e != nil {
			return nil, e
		}
		r.s = s
		r.db = s.db
		r.head = s.head
		return r, nil
	}
	db, e := openEngine(path, create, ro)
	if e != nil {
		return nil, e
	}
	r.db = db
	manifest, _ := json.Marshal(struct {
		Layout              string
		Config              Config
		Engine, Compression string
	}{"native-hash-archive", cfg, "rocksdb/9.10.0", "none"})
	if create {
		head, _ := json.Marshal(r.head)
		e = db.write([]entry{{metaColumn, manifestKey, manifest}, {metaColumn, headKey, head}})
	} else {
		snap := db.snapshot()
		var raw []byte
		raw, e = snap.get(metaColumn, manifestKey)
		if e == nil && !bytes.Equal(raw, manifest) {
			e = ErrCorrupt
		}
		if e == nil {
			raw, e = snap.get(metaColumn, headKey)
			if e == nil {
				e = json.Unmarshal(raw, &r.head)
			}
		}
		snap.close()
	}
	if e != nil {
		db.close()
		return nil, e
	}
	return r, nil
}
func (r *experimentRunner) close() error {
	if r.s != nil {
		return r.s.Close()
	}
	r.db.close()
	return nil
}
func (r *experimentRunner) commit(height uint64, before, tree *mpt.Trie, selector LayoutSelector) (CommitStats, error) {
	if !r.archive {
		head, stats, e := r.s.CommitWithLayout(height, r.head.CommitID, []TrieUpdate{{TrieID: 0, Tree: tree}}, selector)
		if e == nil {
			r.head = head
		}
		return stats, e
	}
	if height > MaxHeight || !r.head.Initialized && height != 0 || r.head.Initialized && height != r.head.Height+1 {
		return CommitStats{}, ErrConflict
	}
	snap := r.db.snapshot()
	defer snap.close()
	p := &writePlan{entries: map[string]entry{}, limit: r.cfg.MaxBatchBytes}
	_, e := tree.DiffLogicalNodes(before, r.cfg.MaxProjectionNodes, func(c mpt.LogicalChange) error {
		if len(c.Path) > 0 && len(c.RLP) < 32 {
			return nil
		}
		p.stats.ClosureReads++
		raw, e := snap.get(hashColumn, c.Hash[:])
		p.stats.ClosureReadBytes += uint64(len(raw))
		if e == nil {
			_, e = canonicalHash(raw, c.Hash)
			return e
		}
		if !errors.Is(e, ErrNotFound) {
			return e
		}
		return p.add(hashColumn, c.Hash[:], c.RLP)
	})
	if e != nil {
		return p.stats, e
	}
	root, e := tree.Hash()
	if e != nil {
		return p.stats, e
	}
	rk, _ := rootKey(0, height)
	if e = p.add(metaColumn, rk, root[:]); e != nil {
		return p.stats, e
	}
	identity := sha256.New()
	identity.Write([]byte(StoreFormat))
	identity.Write(r.head.CommitID[:])
	hs, _ := heightSuffix(height)
	identity.Write(hs)
	identity.Write(rk)
	identity.Write(root[:])
	next := Head{Initialized: true, Height: height, NextTrieID: 1}
	copy(next.CommitID[:], identity.Sum(nil))
	raw, _ := json.Marshal(next)
	bk, _ := blockKey(height)
	if e = p.add(metaColumn, bk, next.CommitID[:]); e != nil {
		return p.stats, e
	}
	if e = p.add(metaColumn, headKey, raw); e != nil {
		return p.stats, e
	}
	var entries []entry
	for _, entry := range p.entries {
		entries = append(entries, entry)
	}
	// Sort before writing just as the production commit does.
	sortEntries(entries)
	if e = r.db.write(entries); e != nil {
		return p.stats, e
	}
	r.head = next
	return p.stats, nil
}
func sortEntries(entries []entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].cf != entries[j].cf {
			return entries[i].cf < entries[j].cf
		}
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
}
func fileMeasurement(path string, m *storageMeasurement) error {
	return filepath.Walk(path, func(name string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.IsDir() {
			return nil
		}
		n := uint64(info.Size())
		m.DirectoryBytes += n
		switch filepath.Ext(name) {
		case ".sst":
			m.SSTDirectoryBytes += n
		case ".log":
			m.WALBytes += n
		default:
			m.OtherFileBytes += n
		}
		return nil
	})
}
func (r *experimentRunner) checkpoint(path string) (storageMeasurement, error) {
	native := r.db.(*rocksEngine)
	if e := native.stabilize(); e != nil {
		return storageMeasurement{}, e
	}
	m, e := native.measure()
	if e != nil {
		return m, e
	}
	if e = r.close(); e != nil {
		return m, e
	}
	e = fileMeasurement(path, &m)
	return m, e
}
func addCommitStats(total *CommitStats, s CommitStats) {
	total.PrefixRecords += s.PrefixRecords
	total.HashRecords += s.HashRecords
	total.PrefixBytes += s.PrefixBytes
	total.HashBytes += s.HashBytes
	total.ClosureReads += s.ClosureReads
	total.ClosureReadBytes += s.ClosureReadBytes
	total.ClosureBackfillRecords += s.ClosureBackfillRecords
	total.ClosureBackfillBytes += s.ClosureBackfillBytes
}

type experimentResult struct {
	Schema, Workload, Method, TraceID, BinaryID string
	DatabaseSHA256                              string
	Config                                      storageExperimentConfig
	Initial, Final                              storageMeasurement
	InitCommit, ExecutionCommits                CommitStats
	CommitNS                                    int64
	MaxHashWriteBytes, MaxHashWriteHeight       uint64
	MaxBackfillBytes, MaxBackfillHeight         uint64
	RootChecks, VerifiedProofs                  int
	ReadOnlyUnchanged, Verified                 bool
}

func verifyExperiment(r *experimentRunner, tr experimentTrace, c storageExperimentConfig) (int, error) {
	rng := rand.New(rand.NewSource(c.Seed + 991))
	verified := 0
	for i := 0; i < c.Queries; i++ {
		height := rng.Intn(c.Blocks + 1)
		id := rng.Intn(c.Keys)
		key := tr.Keys[id]
		expected := expectedExperimentValue(tr, height, id)
		if i%5 == 0 {
			key = append([]byte(nil), key...)
			key[0] ^= 0xff
			expected = nil
			for _, existing := range tr.Keys {
				if bytes.Equal(existing, key) {
					return verified, errors.New("generated missing key collides with workload")
				}
			}
		}
		root := tr.Roots[height]
		var proof mpt.Proof
		var e error
		if !r.archive {
			v, e := r.s.ViewAt(uint64(height))
			if e != nil {
				return verified, e
			}
			proof, e = v.Prove(0, root, key)
			v.Close()
			if e != nil {
				return verified, e
			}
		} else {
			snap := r.db.snapshot()
			directory, e := readRoot(snap, 0, uint64(height))
			if e != nil || directory != root {
				snap.close()
				return verified, ErrHashMismatch
			}
			tree, e := mpt.Open(root, archiveExperimentReader{snap})
			if e == nil {
				proof, e = tree.Prove(key)
			}
			snap.close()
			if e != nil {
				return verified, e
			}
		}
		value, exists, e := mpt.VerifyProof(root, key, proof)
		if e != nil || exists != (len(expected) != 0) || !bytes.Equal(value, expected) {
			return verified, fmt.Errorf("query %d height %d: %w", i, height, ErrHashMismatch)
		}
		verified++
	}
	return verified, nil
}

type archiveExperimentReader struct{ snap diskSnapshot }

func (r archiveExperimentReader) Get(key []byte) ([]byte, error) { return r.snap.get(hashColumn, key) }
func (archiveExperimentReader) NewBatch() mpt.Batch              { return deniedBatch{} }
func experimentFingerprint(path string) (string, error) {
	h := sha256.New()
	e := filepath.Walk(path, func(name string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(path, name)
		if e != nil {
			return e
		}
		fmt.Fprintf(h, "%s:%d:", rel, info.Size())
		f, e := os.Open(name)
		if e != nil {
			return e
		}
		_, e = io.Copy(h, f)
		f.Close()
		return e
	})
	return hex.EncodeToString(h.Sum(nil)), e
}
func writeExperimentJSON(path string, v interface{}) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, append(raw, '\n'), 0600); e != nil {
		return e
	}
	return os.Rename(tmp, path)
}
func runExperimentGroup(c storageExperimentConfig, workload string, variant experimentVariant, tr experimentTrace, binaryID string) (experimentResult, error) {
	result := experimentResult{Schema: "hybrid-storage-group/v1", Workload: workload, Method: variant.Name, TraceID: tr.ID, BinaryID: binaryID, Config: c}
	dir := filepath.Join(c.OutputDir, workload, variant.Name)
	sealed := filepath.Join(dir, "result.json")
	if raw, e := os.ReadFile(sealed); e == nil {
		var old experimentResult
		if e = json.Unmarshal(raw, &old); e != nil {
			return result, e
		}
		if old.TraceID != tr.ID || old.BinaryID != binaryID || !reflect.DeepEqual(old.Config, c) || !old.Verified || !old.ReadOnlyUnchanged || old.RootChecks != c.Blocks+1 || old.VerifiedProofs != c.Queries {
			return result, errors.New("checkpoint identity or verification mismatch")
		}
		actual, e := experimentFingerprint(filepath.Join(dir, "db"))
		if e != nil {
			return result, e
		}
		if old.DatabaseSHA256 == "" || old.DatabaseSHA256 != actual {
			return result, errors.New("sealed database fingerprint changed")
		}
		return old, nil
	}
	if _, e := os.Stat(dir); e == nil {
		return result, fmt.Errorf("incomplete group preserved at %s; choose a fresh output directory", dir)
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return result, e
	}
	path := filepath.Join(dir, "db")
	cfg := DefaultConfig("raw-bytes/v1", tr.ID)
	runner, e := openExperimentRunner(path, cfg, variant.Archive, true, false)
	if e != nil {
		return result, e
	}
	closed := false
	defer func() {
		if !closed {
			runner.close()
		}
	}()
	tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	before := tree.Copy()
	for i, key := range tr.Keys {
		if e = tree.Update(key, tr.Initial[i]); e != nil {
			return result, e
		}
	}
	root, _ := tree.Hash()
	if root != tr.Roots[0] {
		return result, ErrHashMismatch
	}
	result.RootChecks++
	result.InitCommit, e = runner.commit(0, before, tree, variant.layout(0, c.Blocks))
	if e != nil {
		return result, e
	}
	result.Initial, e = runner.checkpoint(path)
	closed = true
	if e != nil {
		return result, e
	}
	runner, e = openExperimentRunner(path, cfg, variant.Archive, false, false)
	if e != nil {
		return result, e
	}
	closed = false
	rows, e := os.Create(filepath.Join(dir, "blocks.csv"))
	if e != nil {
		return result, e
	}
	defer rows.Close()
	csvOut := csv.NewWriter(rows)
	csvOut.Write([]string{"height", "prefix_records", "hash_records", "prefix_bytes", "hash_bytes", "hash_probes", "hash_value_read_bytes", "unchanged_backfill_records", "unchanged_backfill_bytes", "commit_ns"})
	for block, b := range tr.Blocks {
		before = tree.Copy()
		for _, w := range b.Writes {
			if e = tree.Update(tr.Keys[w.ID], w.Value); e != nil {
				return result, e
			}
		}
		root, _ = tree.Hash()
		if root != tr.Roots[block+1] {
			return result, ErrHashMismatch
		}
		result.RootChecks++
		started := time.Now()
		stats, e := runner.commit(uint64(block+1), before, tree, variant.layout(block+1, c.Blocks))
		elapsed := time.Since(started).Nanoseconds()
		if e != nil {
			return result, e
		}
		result.CommitNS += elapsed
		addCommitStats(&result.ExecutionCommits, stats)
		if stats.HashBytes > result.MaxHashWriteBytes {
			result.MaxHashWriteBytes = stats.HashBytes
			result.MaxHashWriteHeight = uint64(block + 1)
		}
		if stats.ClosureBackfillBytes > result.MaxBackfillBytes {
			result.MaxBackfillBytes = stats.ClosureBackfillBytes
			result.MaxBackfillHeight = uint64(block + 1)
		}
		numbers := []uint64{uint64(block + 1), stats.PrefixRecords, stats.HashRecords, stats.PrefixBytes, stats.HashBytes, stats.ClosureReads, stats.ClosureReadBytes, stats.ClosureBackfillRecords, stats.ClosureBackfillBytes, uint64(elapsed)}
		row := make([]string, len(numbers))
		for i, n := range numbers {
			row[i] = strconv.FormatUint(n, 10)
		}
		if e = csvOut.Write(row); e != nil {
			return result, e
		}
	}
	csvOut.Flush()
	if e = csvOut.Error(); e != nil {
		return result, e
	}
	if e = rows.Close(); e != nil {
		return result, e
	}
	result.Final, e = runner.checkpoint(path)
	closed = true
	if e != nil {
		return result, e
	}
	// Accounting must equal the actual live CF delta, including content dedup.
	for name, want := range map[string]uint64{"prefix": result.ExecutionCommits.PrefixBytes, "hash": result.ExecutionCommits.HashBytes} {
		delta := result.Final.Families[name].total() - result.Initial.Families[name].total()
		if delta != want {
			return result, fmt.Errorf("%s accounting mismatch %d != %d", name, delta, want)
		}
	}
	fingerprint, e := experimentFingerprint(path)
	if e != nil {
		return result, e
	}
	runner, e = openExperimentRunner(path, cfg, variant.Archive, false, true)
	if e != nil {
		return result, e
	}
	closed = false
	result.VerifiedProofs, e = verifyExperiment(runner, tr, c)
	if e != nil {
		return result, e
	}
	if e = runner.close(); e != nil {
		return result, e
	}
	closed = true
	after, e := experimentFingerprint(path)
	if e != nil {
		return result, e
	}
	result.ReadOnlyUnchanged = after == fingerprint
	if !result.ReadOnlyUnchanged {
		return result, errors.New("read-only proof queries changed files")
	}
	result.DatabaseSHA256 = after
	result.Verified = true
	if e = writeExperimentJSON(sealed, result); e != nil {
		return result, e
	}
	return result, nil
}

func TestHybridStorageExperiment(t *testing.T) {
	configPath := os.Getenv("HYBRID_STORAGE_CONFIG")
	if configPath == "" {
		t.Skip("set HYBRID_STORAGE_CONFIG to run the storage experiment")
	}
	raw, e := os.ReadFile(configPath)
	if e != nil {
		t.Fatal(e)
	}
	var cfg storageExperimentConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if e = decoder.Decode(&cfg); e != nil {
		t.Fatal(e)
	}
	if cfg.Keys < 16 || cfg.Keys > 10000 || cfg.Blocks < 1 || cfg.Blocks > 10000 || cfg.WritesPerBlock < 1 || cfg.WritesPerBlock > cfg.Keys || cfg.Blocks*cfg.WritesPerBlock > 1000000 || cfg.ValueBytes < 1 || cfg.ValueBytes > 1024 || cfg.Queries < 5 || cfg.Queries > 100 || len(cfg.Workloads) == 0 || cfg.OutputDir == "" {
		t.Fatal("invalid experiment config")
	}
	if !filepath.IsAbs(cfg.OutputDir) {
		base := os.Getenv("HYBRID_STORAGE_OUTPUT_BASE")
		if base == "" {
			base, e = filepath.Abs("..")
			if e != nil {
				t.Fatal(e)
			}
		}
		cfg.OutputDir = filepath.Join(base, cfg.OutputDir)
	}
	cfg.OutputDir, e = filepath.Abs(cfg.OutputDir)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(cfg.OutputDir, 0700); e != nil {
		t.Fatal(e)
	}
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	binary, e := os.ReadFile(exe)
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(binary)
	binaryID := hex.EncodeToString(sum[:])
	source, e := os.ReadFile("store.go")
	if e != nil {
		t.Fatal(e)
	}
	sourceSum := sha256.Sum256(source)
	environment := map[string]interface{}{"Schema": "hybrid-storage-environment/v1", "BinarySHA256": binaryID, "StoreSourceSHA256": hex.EncodeToString(sourceSum[:]), "GoVersion": runtime.Version(), "GOOS": runtime.GOOS, "GOARCH": runtime.GOARCH, "Engine": "RocksDB 9.10.0", "Compression": "none", "Config": cfg}
	environmentPath := filepath.Join(cfg.OutputDir, "environment.json")
	if previous, readErr := os.ReadFile(environmentPath); readErr == nil {
		expected, _ := json.MarshalIndent(environment, "", "  ")
		if !bytes.Equal(bytes.TrimSpace(previous), expected) {
			t.Fatal("existing experiment identity differs; choose a fresh output directory")
		}
	} else if !os.IsNotExist(readErr) {
		t.Fatal(readErr)
	} else if e = writeExperimentJSON(environmentPath, environment); e != nil {
		t.Fatal(e)
	}
	var results []experimentResult
	seen := map[string]bool{}
	for _, workload := range cfg.Workloads {
		if workload != "uniform" && workload != "hot" || seen[workload] {
			t.Fatal("unsupported or duplicate workload")
		}
		seen[workload] = true
		trace, e := buildExperimentTrace(cfg, workload)
		if e != nil {
			t.Fatal(e)
		}
		for _, variant := range storageVariants {
			t.Logf("starting workload=%s method=%s", workload, variant.Name)
			result, e := runExperimentGroup(cfg, workload, variant, trace, binaryID)
			if e != nil {
				writeExperimentJSON(filepath.Join(cfg.OutputDir, "failure.json"), map[string]string{"Workload": workload, "Method": variant.Name, "Error": e.Error()})
				t.Fatal(e)
			}
			results = append(results, result)
			if e = writeExperimentJSON(filepath.Join(cfg.OutputDir, "results.json"), results); e != nil {
				t.Fatal(e)
			}
			t.Logf("sealed workload=%s method=%s logical_delta=%d sst_delta=%d proofs=%d", workload, variant.Name, int64(result.Final.LogicalBytes)-int64(result.Initial.LogicalBytes), int64(result.Final.LiveSSTBytes)-int64(result.Initial.LiveSSTBytes), result.VerifiedProofs)
		}
	}
	if e = writeExperimentSummary(cfg, results); e != nil {
		t.Fatal(e)
	}
	if e = writeExperimentJSON(filepath.Join(cfg.OutputDir, "completion.json"), map[string]interface{}{"Verified": true, "Groups": len(results), "Queries": cfg.Queries * len(results), "RootChecks": (cfg.Blocks + 1) * len(results)}); e != nil {
		t.Fatal(e)
	}
}

func writeExperimentSummary(c storageExperimentConfig, results []experimentResult) error {
	f, e := os.Create(filepath.Join(c.OutputDir, "results.csv"))
	if e != nil {
		return e
	}
	out := csv.NewWriter(f)
	out.Write([]string{"workload", "method", "initial_logical_bytes", "execution_logical_bytes", "execution_prefix_bytes", "execution_hash_bytes", "execution_metadata_bytes", "initial_live_sst_bytes", "execution_live_sst_bytes", "execution_directory_bytes", "hash_copy_bytes_final", "hash_probes", "hash_value_read_bytes", "unchanged_backfill_bytes", "commit_seconds", "proofs"})
	var report strings.Builder
	fmt.Fprintf(&report, "# HybridTrie 小规模存储实验\n\n%d键，%d个执行块，每块%d次写入，值%d字节，seed=%d。初始化高度0单列，不计入执行增量。每组关闭、只读重开后验证%d条固定历史查询，并检查全部数据库文件内容不变。\n\n", c.Keys, c.Blocks, c.WritesPerBlock, c.ValueBytes, c.Seed, c.Queries)
	report.WriteString("使用相同原生MPT、RocksDB 9.10.0、三个CF和无压缩设置。每组初始化和终点都同步flush、全范围compact后测live SST，再关闭数据库统计目录。主口径包含全部Key、Value及元数据；目录还含WAL和日志，单独保留。没有压缩率或EVM在线性能结论。\n\n")
	report.WriteString("native-hash-archive只存原生哈希节点及历史根，不保存Prefix副本；full-hash-dual是当前双Store框架的全Hash消融。root-half-H为根下8个H槽，其余D；root-all-H为16个H孩子；frontier-2为更深的固定连通边界。frontier-2-switch在后半轮的前半段切到全Hash，再恢复，以暴露首次闭包物化。全Prefix是全内存引用极限，不是已实现的有界缓存。\n\n")
	report.WriteString("uniform均匀选择更新键；hot将90%的选择限制在固定20%键中，其余10%仍均匀选择。此为合成单Trie写入，未执行EVM。所有组使用相同输入与逐高度原生根。\n\n")
	report.WriteString("|负载|方法|初始化KV MiB|执行KV MiB|其中P MiB|其中H MiB|执行SST MiB|相对Archive KV|未变子树回填 KiB|\n|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
	baselines := map[string]int64{}
	for _, r := range results {
		if r.Method == "native-hash-archive" {
			baselines[r.Workload] = int64(r.Final.LogicalBytes) - int64(r.Initial.LogicalBytes)
		}
	}
	for _, r := range results {
		delta := int64(r.Final.LogicalBytes) - int64(r.Initial.LogicalBytes)
		p := int64(r.Final.Families["prefix"].total()) - int64(r.Initial.Families["prefix"].total())
		h := int64(r.Final.Families["hash"].total()) - int64(r.Initial.Families["hash"].total())
		sst := int64(r.Final.LiveSSTBytes) - int64(r.Initial.LiveSSTBytes)
		dir := int64(r.Final.DirectoryBytes) - int64(r.Initial.DirectoryBytes)
		row := []string{r.Workload, r.Method, strconv.FormatUint(r.Initial.LogicalBytes, 10), fmt.Sprint(delta), fmt.Sprint(p), fmt.Sprint(h), fmt.Sprint(delta - p - h), strconv.FormatUint(r.Initial.LiveSSTBytes, 10), fmt.Sprint(sst), fmt.Sprint(dir), fmt.Sprint(r.Final.HashCopyBytes), fmt.Sprint(r.ExecutionCommits.ClosureReads), fmt.Sprint(r.ExecutionCommits.ClosureReadBytes), fmt.Sprint(r.ExecutionCommits.ClosureBackfillBytes), fmt.Sprintf("%.6f", float64(r.CommitNS)/1e9), fmt.Sprint(r.VerifiedProofs)}
		out.Write(row)
		ratio := float64(delta) / float64(baselines[r.Workload])
		fmt.Fprintf(&report, "|%s|%s|%.3f|%.3f|%.3f|%.3f|%.3f|%.3fx|%.1f|\n", r.Workload, r.Method, float64(r.Initial.LogicalBytes)/(1<<20), float64(delta)/(1<<20), float64(p)/(1<<20), float64(h)/(1<<20), float64(sst)/(1<<20), ratio, float64(r.ExecutionCommits.ClosureBackfillBytes)/1024)
	}
	report.WriteString("\nHash回填计为本块新Hash记录中不属于本块新Prefix digest集合的部分；这是未变内容物化诊断，不等于磁盘物理I/O。hash probes为Hash Store查找次数，read bytes为实际返回的Value字节。闭包在已有不可变Hash记录处停止，不重复扫描旧闭包。每块计数保存在各组blocks.csv中。commit_seconds只计物化提交，不包含交易执行，也未做重复性能轮。\n\n所有组result.json为完成检查点，results.json保留初始化、终点、CF/节点类型组成及回填峰值。恢复只跳过参数、输入和二进制身份一致且Verified、ReadOnlyUnchanged均为true的完整组；未完成组保留并报错，不覆盖。\n")
	out.Flush()
	e = out.Error()
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	return os.WriteFile(filepath.Join(c.OutputDir, "report.zh.md"), []byte(report.String()), 0600)
}
