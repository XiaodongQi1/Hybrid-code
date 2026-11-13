package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/artifact"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

type boundaryTemplate ht.BoundaryConfig

func (b *boundaryTemplate) UnmarshalJSON(raw []byte) error {
	c := ht.DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e := d.Decode(&c); e != nil {
		return e
	}
	*b = boundaryTemplate(c)
	return nil
}

type config struct {
	ParallelProof                 ht.ParallelProofConfig `json:",omitempty,omitzero"`
	HybridBoundary                *boundaryTemplate      `json:",omitempty"`
	PrefixSnapshot                bool                   `json:",omitempty"`
	HybridDeltaInterval           uint8                  `json:",omitempty"`
	Methods, Suites               []string
	Engine                        kvstore.Engine
	Resources                     comparison.ResourceBudget
	Synthetic                     *workload.Options
	WorkloadFile, QueryFile       string
	Queries, Trials, WarmupBlocks int
	QuerySeed                     int64
}

var allSuites = []string{"storage", "execution", "historical-query"}
var allMethods = []string{"archive", "path", "hybrid"}

type experiment struct {
	dir, id, runID string
	requestsFile   []q.Request
	cfg            config
	plan           comparison.ResourcePlan
	w              block.Workload
	snap           snapshot
	truth          []truthBlock
	corpus         []queryCase
	corpusID       string
}

func strictFile(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if e = d.Decode(v); e != nil {
		return e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return fmt.Errorf("trailing data in %s", path)
	}
	return nil
}
func save(path string, value any) error {
	data, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	return atomicBytes(path, append(data, '\n'))
}
func digest(v any) (string, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func resolved(base, path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
func validateSelection(values, allowed []string) error {
	if len(values) == 0 {
		return fmt.Errorf("empty selection, expected one or more of %v", allowed)
	}
	for i, v := range values {
		if !slices.Contains(allowed, v) || slices.Contains(values[:i], v) {
			return fmt.Errorf("unknown or duplicate selection %q, allowed %v", v, allowed)
		}
	}
	return nil
}
func (c config) has(suite string) bool { return slices.Contains(c.Suites, suite) }

func run(ctx context.Context, configPath, out string) (err error) {
	return runMode(ctx, configPath, out, runOptions{})
}

type runOptions struct{ resume, snapshotOnly, throughStorage bool }

func runMode(ctx context.Context, configPath, out string, opts runOptions) (err error) {
	c := config{
		Methods: slices.Clone(allMethods), Suites: slices.Clone(allSuites),
		Engine: kvstore.LevelDB, Resources: comparison.DefaultResourceBudget(),
		Queries: 30, Trials: 1, QuerySeed: 1, WarmupBlocks: workload.DefaultWarmupBlocks,
	}
	if opts.resume {
		// Runs created before the default changed may have omitted WarmupBlocks.
		var saved config
		if err := strictFile(filepath.Join(out, "config.json"), &saved); err == nil {
			c.WarmupBlocks = saved.WarmupBlocks
		}
	}
	if e := strictFile(configPath, &c); e != nil {
		return e
	}
	if e := c.ParallelProof.Validate(); e != nil {
		return e
	}
	if (c.Synthetic == nil) == (c.WorkloadFile == "") || c.Trials < 1 || c.Trials > 100 || c.WarmupBlocks < 0 || c.Queries < 1 || c.Queries > 1_000_000 || out == "" {
		return fmt.Errorf("specify one workload source, a new output directory and valid trial/query counts")
	}
	if e := validateSelection(c.Methods, allMethods); e != nil {
		return e
	}
	if e := validateSelection(c.Suites, allSuites); e != nil {
		return e
	}
	if opts.throughStorage && (opts.snapshotOnly || !c.has("storage")) {
		return fmt.Errorf("-through-storage requires the storage suite and cannot be combined with -snapshot-only")
	}
	layout := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	if c.HybridBoundary != nil {
		layout.Boundary = ht.BoundaryConfig(*c.HybridBoundary)
	}
	plan, e := c.Resources.Resolve(c.Engine, layout)
	if e != nil {
		return e
	}
	if c.ParallelProof.Enabled {
		plan.Hybrid.ProofContextBudgetBytes = c.ParallelProof.MaxContextBytes
	}
	x := experiment{cfg: c, plan: plan, dir: out}
	if c.Synthetic != nil {
		x.w, e = workload.Synthetic(*c.Synthetic)
	} else {
		e = strictFile(resolved(filepath.Dir(configPath), c.WorkloadFile), &x.w)
	}
	if e != nil {
		return e
	}
	id, wire, e := x.w.Freeze()
	if e != nil {
		return e
	}
	x.id = id
	if len(x.w.Blocks) < 2 || (c.has("execution") && c.WarmupBlocks >= len(x.w.Blocks)-1) {
		return fmt.Errorf("at least one measured block is required after %d warmup blocks (workload has %d execution blocks)", c.WarmupBlocks, len(x.w.Blocks)-1)
	}
	lock, e := x.prepare(configPath, wire, opts.resume)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = x.buildSnapshot(ctx); e != nil {
		return fmt.Errorf("shared snapshot: %w", e)
	}
	defer func() { err = errors.Join(err, x.snap.source.Close(), x.snap.builder.Close()) }()
	if opts.snapshotOnly {
		return nil
	}
	ac := artifact.Config{WorkloadID: id, Codec: codec.Ethereum}
	ap := filepath.Join(out, "artifacts")
	inventory := map[string]string{}
	var report reportData
	report.Schema = "hybrid-compare/v1"
	report.WorkloadID = id
	report.Engine = c.Engine
	e = func() (err error) {
		ready, e := exists(filepath.Join(out, "reference-ready.json.seal"))
		if e != nil {
			return e
		}
		if !ready {
			if e := prepareDatabase(ap, func(temp string) (io.Closer, error) { return artifact.Open(temp, ac, true, false) }); e != nil {
				return e
			}
		}
		a, e := artifact.Open(ap, ac, false, false)
		if e != nil {
			return e
		}
		defer func() { err = errors.Join(err, a.Close()) }()
		if e = x.prepareReference(ctx, a); e != nil {
			return fmt.Errorf("reference: %w", e)
		}
		for _, method := range c.Methods {
			// Path also needs the immutable change objects for a live execution trial.
			if !c.has("storage") && !c.has("historical-query") && method != "path" {
				continue
			}
			s, e := x.storedResult(ctx, method, a)
			if e != nil {
				return fmt.Errorf("%s storage: %w", method, e)
			}
			inventory[method] = s.Final.Inventory.SHA256
			if c.has("storage") {
				report.Storage = append(report.Storage, s)
			}
		}
		return nil
	}()
	if e != nil {
		return e
	}
	if opts.throughStorage {
		return nil
	}
	a, e := artifact.Open(ap, ac, false, true)
	if e != nil {
		return e
	}
	defer a.Close()
	for trial := 0; trial < c.Trials && (c.has("execution") || c.has("historical-query")); trial++ {
		for j := range c.Methods {
			idx := (trial + j) % len(c.Methods)
			m := c.Methods[idx]
			g, e := x.measureTrial(ctx, m, trial+1, a, inventory[m])
			if e != nil {
				return fmt.Errorf("%s trial %d: %w", m, trial+1, e)
			}
			report.Trials = append(report.Trials, g)
		}
	}
	report.CorpusID = x.corpusID
	return save(filepath.Join(out, "results.json"), report)
}
func main() {
	cp := flag.String("config", "", "experiment JSON")
	out := flag.String("out", "", "fresh output directory")
	resume := flag.Bool("resume", false, "continue a matching run in the output directory")
	snapshotOnly := flag.Bool("snapshot-only", false, "prepare and verify the shared initial snapshot, then stop")
	throughStorage := flag.Bool("through-storage", false, "stop after building and sealing the configured storage groups")
	flag.Parse()
	if *cp == "" || *out == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := runMode(ctx, *cp, *out, runOptions{resume: *resume, snapshotOnly: *snapshotOnly, throughStorage: *throughStorage}); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if *throughStorage {
		fmt.Println(*out)
		return
	}
	name := "results.json"
	if *snapshotOnly {
		name = "snapshot.json"
	}
	fmt.Println(filepath.Join(*out, name))
}
