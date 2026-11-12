package storagebench

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	vb "github.com/XiaodongQi1/Hybrid-code/internal/versionedbench"
	"time"
)

func loadRecords(path string, fn func(int, executionRecord) error) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	d := json.NewDecoder(f)
	for i := 0; ; i++ {
		var r executionRecord
		e = d.Decode(&r)
		if e == io.EOF {
			return nil
		}
		if e != nil {
			return e
		}
		if e = fn(i, r); e != nil {
			return e
		}
	}
}
func fileHash(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func updateStatus(out, stage, name string, done, total, methods int) error {
	return writeJSON(filepath.Join(out, "status.json"), status{Stage: stage, Method: name, CompletedBlocks: done, TotalBlocks: total, CompletedMethods: methods})
}

func verifyHistory(out, name, id string, c Config) (roots, queries int, unchanged bool, err error) {
	path := rootPath(out, name)
	before, e := vb.DirectoryFingerprint(path)
	if e != nil {
		return 0, 0, false, e
	}
	m, e := openMethod(path, name, c, id, false, true)
	if e != nil {
		return 0, 0, false, e
	}
	err = loadRecords(filepath.Join(out, "execution.jsonl"), func(i int, r executionRecord) error {
		stored, e := m.root(r.Height)
		if e != nil {
			return e
		}
		if stored.Root != r.Root || stored.Height != r.Height {
			return fmt.Errorf("stored historical root mismatch at %d", r.Height)
		}
		roots++
		for _, s := range r.Samples {
			response, e := m.query(s.Request)
			if e != nil {
				return fmt.Errorf("query %d %s: %w", r.Height, s.Request.Kind, e)
			}
			v, e := q.Verify(context.Background(), r.Root, s.Request, response, q.DefaultLimits())
			if e != nil {
				return e
			}
			if !reflect.DeepEqual(v, s.Expected) {
				return fmt.Errorf("historical answer mismatch at %d", r.Height)
			}
			queries++
		}
		return nil
	})
	closeErr := m.close()
	if err != nil {
		return roots, queries, false, err
	}
	if closeErr != nil {
		return roots, queries, false, closeErr
	}
	after, e := vb.DirectoryFingerprint(path)
	if e != nil {
		return roots, queries, false, e
	}
	if after != before {
		return roots, queries, false, fmt.Errorf("read-only verification changed source database")
	}
	return roots, queries, true, nil
}

func finish(out, name, id string, c Config, initial, final Measurement, executed int, start time.Time) (Result, error) {
	r := Result{Method: name, WorkloadID: id, Codec: "legacy-raw105", MeasurementVersion: "state-kv-projected-live-sst/v1", Initial: initial, Final: final, LogicalGrowth: final.StateBytes - initial.StateBytes, SSTGrowth: final.Projected.SSTBytes - initial.Projected.SSTBytes, ProjectedDirectoryGrowth: final.Projected.DirectoryBytes - initial.Projected.DirectoryBytes, ExecutedEVMTransactions: executed}
	var e error
	r.VerifiedRoots, r.VerifiedQueries, r.ReadOnlyUnchanged, e = verifyHistory(out, name, id, c)
	if e != nil {
		return r, e
	}
	if r.VerifiedRoots != c.Workload.Blocks+1 || r.VerifiedQueries != 8*(c.Workload.Blocks+1) {
		return r, fmt.Errorf("incomplete verification")
	}
	r.ElapsedSeconds = time.Since(start).Seconds()
	r.Verified = true
	e = writeJSON(filepath.Join(out, name, "result.json"), r)
	return r, e
}

func source(out, id string, c Config, w block.Workload) (Result, error) {
	name := Methods[0]
	start := time.Now()
	dir := filepath.Join(out, name)
	if e := os.Mkdir(dir, 0700); e != nil {
		return Result{}, e
	}
	s, e := openHash(rootPath(out, name), c, true, false)
	if e != nil {
		return Result{}, e
	}
	defer func() {
		if s != nil {
			s.db.Close()
		}
	}()
	f, e := os.OpenFile(filepath.Join(out, "execution.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return Result{}, e
	}
	defer f.Close()
	var roots []rootRow
	var initial Measurement
	for i, b := range w.Blocks {
		r, e := s.execute(b)
		if e != nil {
			return Result{}, fmt.Errorf("source height %d: %w", b.Height, e)
		}
		if e = json.NewEncoder(f).Encode(r); e != nil {
			return Result{}, e
		}
		if e = f.Sync(); e != nil {
			return Result{}, e
		}
		roots = append(roots, r.rootRow)
		if i == 0 {
			if e = s.db.Close(); e != nil {
				return Result{}, e
			}
			s = nil
			initial, e = measure(rootPath(out, name), filepath.Join(dir, "state-initial"), name, c, id, roots)
			if e != nil {
				return Result{}, e
			}
			if e = writeJSON(filepath.Join(dir, "initial.json"), initial); e != nil {
				return Result{}, e
			}
			s, e = openHash(rootPath(out, name), c, false, false)
			if e != nil {
				return Result{}, e
			}
			s.root = r.Root
		}
		if e = updateStatus(out, "executing-and-recording", name, i, c.Workload.Blocks, 0); e != nil {
			return Result{}, e
		}
		if i%100 == 0 {
			fmt.Printf("%s %d/%d blocks\n", name, i, c.Workload.Blocks)
		}
	}
	if e = f.Close(); e != nil {
		return Result{}, e
	}
	if e = s.db.Close(); e != nil {
		return Result{}, e
	}
	s = nil
	final, e := measure(rootPath(out, name), filepath.Join(dir, "state-final"), name, c, id, roots)
	if e != nil {
		return Result{}, e
	}
	return finish(out, name, id, c, initial, final, c.Workload.Blocks*c.Workload.TransactionsPerBlock, start)
}

func buildMethod(out, name, id string, c Config, w block.Workload, completed int) (Result, error) {
	start := time.Now()
	dir := filepath.Join(out, name)
	if e := os.Mkdir(dir, 0700); e != nil {
		return Result{}, e
	}
	m, e := openMethod(rootPath(out, name), name, c, id, true, false)
	if e != nil {
		return Result{}, e
	}
	defer func() {
		if m != nil {
			m.close()
		}
	}()
	var roots []rootRow
	var initial Measurement
	count := 0
	e = loadRecords(filepath.Join(out, "execution.jsonl"), func(i int, r executionRecord) error {
		if i >= len(w.Blocks) || r.Height != w.Blocks[i].Height || r.ID != w.Blocks[i].ID {
			return fmt.Errorf("execution/input mismatch")
		}
		root, e := m.apply(w.Blocks[i], r)
		if e != nil {
			return fmt.Errorf("%s height %d: %w", name, r.Height, e)
		}
		if root != r.Root {
			return fmt.Errorf("cross-method root mismatch")
		}
		roots = append(roots, r.rootRow)
		count++
		if i == 0 {
			if p, ok := m.(*pathMethod); ok {
				inv, e := p.archive.StorageInventory()
				if e != nil {
					return e
				}
				if e = writeJSON(filepath.Join(dir, "path-components-initial.json"), inv); e != nil {
					return e
				}
			}
			if e = m.close(); e != nil {
				return e
			}
			m = nil
			initial, e = measure(rootPath(out, name), filepath.Join(dir, "state-initial"), name, c, id, roots)
			if e != nil {
				return e
			}
			if e = writeJSON(filepath.Join(dir, "initial.json"), initial); e != nil {
				return e
			}
			m, e = openMethod(rootPath(out, name), name, c, id, false, false)
			if e != nil {
				return e
			}
		}
		if e = updateStatus(out, "building-from-records", name, i, c.Workload.Blocks, completed); e != nil {
			return e
		}
		if i%100 == 0 {
			fmt.Printf("%s %d/%d blocks\n", name, i, c.Workload.Blocks)
		}
		return nil
	})
	if e != nil {
		return Result{}, e
	}
	if count != len(w.Blocks) {
		return Result{}, fmt.Errorf("truncated execution stream")
	}
	if p, ok := m.(*pathMethod); ok {
		inv, e := p.archive.StorageInventory()
		if e != nil {
			return Result{}, e
		}
		if e = writeJSON(filepath.Join(dir, "path-components-final.json"), inv); e != nil {
			return Result{}, e
		}
	}
	if e = m.close(); e != nil {
		return Result{}, e
	}
	m = nil
	final, e := measure(rootPath(out, name), filepath.Join(dir, "state-final"), name, c, id, roots)
	if e != nil {
		return Result{}, e
	}
	if name == "PathV7" {
		for _, endpoint := range []struct {
			name  string
			value int64
		}{{"initial", initial.StateBytes}, {"final", final.StateBytes}} {
			raw, e := os.ReadFile(filepath.Join(dir, "path-components-"+endpoint.name+".json"))
			if e != nil {
				return Result{}, e
			}
			var check struct{ StateBytes uint64 }
			if e = json.Unmarshal(raw, &check); e != nil {
				return Result{}, e
			}
			if int64(check.StateBytes) != endpoint.value {
				return Result{}, fmt.Errorf("Path state inventory mismatch")
			}
		}
	}
	return finish(out, name, id, c, initial, final, 0, start)
}

func Run(configPath, out string) (summary Summary, err error) {
	raw, e := os.ReadFile(configPath)
	if e != nil {
		return summary, e
	}
	var c Config
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e = d.Decode(&c); e != nil {
		return summary, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return summary, fmt.Errorf("trailing configuration content")
	}
	if e = c.validate(); e != nil {
		return summary, e
	}
	w, e := workload.Synthetic(c.Workload)
	if e != nil {
		return summary, e
	}
	id, wire, e := w.Freeze()
	if e != nil {
		return summary, e
	}
	if e = os.MkdirAll(filepath.Dir(out), 0700); e != nil {
		return summary, e
	}
	if e = os.Mkdir(out, 0700); e != nil {
		return summary, e
	}
	defer func() {
		if err != nil {
			_ = writeJSON(filepath.Join(out, "failure.json"), struct{ Error string }{err.Error()})
		}
	}()
	if e = writeJSON(filepath.Join(out, "configuration.json"), c); e != nil {
		return summary, e
	}
	if e = os.WriteFile(filepath.Join(out, "workload.json"), wire, 0600); e != nil {
		return summary, e
	}
	if e = writeJSON(filepath.Join(out, "path-config.json"), pathConfig(c, id)); e != nil {
		return summary, e
	}
	if e = writeJSON(filepath.Join(out, "versioned-config.json"), c.versionConfig(id)); e != nil {
		return summary, e
	}
	summary = Summary{Schema: "versioned-evm-storage-results/v1", WorkloadID: id, Configuration: c, Blocks: c.Workload.Blocks, Transactions: c.Workload.Blocks * c.Workload.TransactionsPerBlock, BackgroundAccounts: c.Workload.BackgroundAccounts, TotalInitialAccounts: len(w.Blocks[0].Allocations), Notes: []string{
		"Synthetic EVM workload: four-slot contract calls and transfers; not mainnet transactions.",
		"Only HashArchive executes transactions. PathV7 and HashVersioned reuse the same final changes/outcomes and check every block root.",
		"Initialization excluded: report final minus each method actual initial state. Canonical root directories are identical in physical projections.",
		"State logical KV includes trie, code, flat/history/indexes or lifecycle as applicable; excludes input ledgers, receipts and engine configuration.",
		"State SST comes from isolated exact KV projections with matching Snappy/options/compaction. It is not an attribution of mixed-engine SST files.",
		"SST growth is final minus initial compacted live tables; it is not a separately isolated history-only database size.",
		"Application cache disabled, matching LevelDB caches, one Sync commit per block; elapsed time is not a latency benchmark.",
	}}
	r, e := source(out, id, c, w)
	if e != nil {
		return summary, e
	}
	summary.Methods = append(summary.Methods, r)
	digest, e := fileHash(filepath.Join(out, "execution.jsonl"))
	if e != nil {
		return summary, e
	}
	if e = writeJSON(filepath.Join(out, "execution-seal.json"), struct {
		SHA256, WorkloadID   string
		Blocks, Transactions int
	}{digest, id, len(w.Blocks), summary.Transactions}); e != nil {
		return summary, e
	}
	for _, name := range Methods[1:] {
		current, e := fileHash(filepath.Join(out, "execution.jsonl"))
		if e != nil || current != digest {
			return summary, fmt.Errorf("execution record seal mismatch: %v", e)
		}
		r, e := buildMethod(out, name, id, c, w, len(summary.Methods))
		if e != nil {
			return summary, e
		}
		summary.Methods = append(summary.Methods, r)
		if e = writeJSON(filepath.Join(out, "partial-results.json"), summary); e != nil {
			return summary, e
		}
	}
	if e = writeJSON(filepath.Join(out, "results.json"), summary); e != nil {
		return summary, e
	}
	if e = writeReport(out, summary); e != nil {
		return summary, e
	}
	if e = updateStatus(out, "complete", "", c.Workload.Blocks, c.Workload.Blocks, len(Methods)); e != nil {
		return summary, e
	}
	return summary, nil
}

func writeReport(out string, s Summary) error {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "EVM 存储对照实验\n背景账户: %d; 初始总账户: %d; 执行块: %d; 交易: %d\n编码: legacy-raw105; 工作负载: %s\n\n", s.BackgroundAccounts, s.TotalInitialAccounts, s.Blocks, s.Transactions, s.WorkloadID)
	fmt.Fprintln(&buf, "方法\t初始化状态KV MiB\t新增状态KV MiB\t初始化状态SST MiB\t新增状态SST MiB\tSST较Archive减少")
	base := float64(s.Methods[0].SSTGrowth)
	for _, r := range s.Methods {
		saving := 100 * (1 - float64(r.SSTGrowth)/base)
		fmt.Fprintf(&buf, "%s\t%.4f\t%.4f\t%.4f\t%.4f\t%.2f%%\n", r.Method, float64(r.Initial.StateBytes)/(1<<20), float64(r.LogicalGrowth)/(1<<20), float64(r.Initial.Projected.SSTBytes)/(1<<20), float64(r.SSTGrowth)/(1<<20), saving)
	}
	fmt.Fprintln(&buf, "\n测量说明:")
	for _, note := range s.Notes {
		fmt.Fprintln(&buf, "- "+note)
	}
	if e := os.WriteFile(filepath.Join(out, "report.txt"), buf.Bytes(), 0600); e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(out, "results.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if e = w.Write([]string{"method", "initial_state_kv_bytes", "state_kv_growth_bytes", "initial_state_sst_bytes", "state_sst_growth_bytes", "verified_roots", "verified_queries", "read_only_unchanged"}); e != nil {
		return e
	}
	for _, r := range s.Methods {
		if e = w.Write([]string{r.Method, fmt.Sprint(r.Initial.StateBytes), fmt.Sprint(r.LogicalGrowth), fmt.Sprint(r.Initial.Projected.SSTBytes), fmt.Sprint(r.SSTGrowth), fmt.Sprint(r.VerifiedRoots), fmt.Sprint(r.VerifiedQueries), fmt.Sprint(r.ReadOnlyUnchanged)}); e != nil {
			return e
		}
	}
	w.Flush()
	return w.Error()
}
