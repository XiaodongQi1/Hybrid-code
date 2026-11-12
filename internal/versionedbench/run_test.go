package versionedbench

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestTraceDeterminismAndIdentity(t *testing.T) {
	c := DefaultConfig()
	c.Keys = 100
	c.Blocks = 3
	c.UpdatesPerBlock = 20
	spec := Spec{c, 32, "uniform"}
	a, e := Generate(spec)
	if e != nil {
		t.Fatal(e)
	}
	b, e := Generate(spec)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("non-deterministic trace", e)
	}
	spec.Pattern = "hot"
	hot, e := Generate(spec)
	if e != nil || hot.ID == a.ID {
		t.Fatal("unbound distribution", e)
	}
	spec.ValueBytes = 105
	different, e := Generate(spec)
	if e != nil || different.ID == hot.ID {
		t.Fatal("unbound value width", e)
	}
}
func TestStorageMatrixSmokeAndNoOverwrite(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Keys = 100
	cfg.Blocks = 3
	cfg.UpdatesPerBlock = 10
	cfg.ValueBytes = []int{32}
	out := filepath.Join(t.TempDir(), "run")
	if e := Run(cfg, out, io.Discard); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(filepath.Join(out, "results.json"))
	if e != nil {
		t.Fatal(e)
	}
	var rows []Result
	if e = json.Unmarshal(raw, &rows); e != nil || len(rows) != 2 {
		t.Fatal(e)
	}
	for _, r := range rows {
		if !r.Verified || r.VerifiedRoots != 12 || r.VerifiedQueries != 48 || len(r.Methods) != 3 {
			t.Fatal("incomplete result", r)
		}
		if r.Methods[1].Final.Nodes.Entries != r.Methods[2].Final.Nodes.Entries {
			t.Fatal("different node sets")
		}
		for _, m := range r.Methods {
			if m.NodeGrowth != m.Final.Nodes.Bytes()-m.Initial.Nodes.Bytes() || m.SSTGrowth != m.Final.SSTBytes-m.Initial.SSTBytes {
				t.Fatal("growth accounting")
			}
		}
	}
	if e = Run(cfg, out, io.Discard); e == nil {
		t.Fatal("existing results overwritten")
	}
}

func TestNetKeyChangesCountFinalBlockState(t *testing.T) {
	c := DefaultConfig()
	c.Keys = 10
	c.Blocks = 3
	c.UpdatesPerBlock = 100
	c.ValueBytes = []int{1}
	tr, e := Generate(Spec{c, 1, "uniform"})
	if e != nil {
		t.Fatal(e)
	}
	values := make([]string, len(tr.Initial))
	for i, v := range tr.Initial {
		values[i] = string(v)
	}
	want := 0
	for _, block := range tr.Blocks {
		before := append([]string(nil), values...)
		for _, m := range block {
			values[m.Index] = string(m.Value)
		}
		for i := range values {
			if values[i] != before[i] {
				want++
			}
		}
	}
	if tr.UniqueChangedKeys != want || want > c.Keys*c.Blocks {
		t.Fatal("counted intermediate writes", tr.UniqueChangedKeys, want)
	}
}
