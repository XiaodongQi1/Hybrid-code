package kvstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

type bulkFault struct {
	driver
	failAt, calls int
	after, final  bool
}

func (f *bulkFault) write(es []operation) error {
	f.calls++
	isFinal := false
	for _, e := range es {
		if string(e.key) == "head" {
			isFinal = true
		}
	}
	if f.calls == f.failAt || f.final && isFinal {
		if f.after {
			if e := f.driver.write(es); e != nil {
				return e
			}
		}
		return errors.New("injected bulk failure")
	}
	return f.driver.write(es)
}
func bulkFixture() (BulkPlan, BulkWalk) {
	final := NewBatch()
	final.Put("default", []byte("head"), []byte("verified-root"))
	p := BulkPlan{Identity: "frozen-source-1", MarkerKey: []byte("M/import"), BatchBytes: 4096, Final: final}
	walk := func(emit BulkEmit) error {
		for i := 0; i < 100; i++ {
			if e := emit("hash", []byte(fmt.Sprintf("node-%04d", i)), []byte(fmt.Sprintf("%0100d", i))); e != nil {
				return e
			}
		}
		return nil
	}
	return p, walk
}
func TestBulkResumeAndAtomicHead(t *testing.T) {
	for _, engine := range testEngines() {
		for _, after := range []bool{false, true} {
			for _, fail := range []int{1, 3, 0} {
				t.Run(fmt.Sprintf("%s/%t/%d", engine, after, fail), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "db")
					o := testOptions(engine)
					d, e := Open(path, o, true, false)
					must(t, e)
					d.raw = &bulkFault{driver: d.raw, failAt: fail, after: after, final: fail == 0}
					p, walk := bulkFixture()
					if _, e = d.ImportInitial(context.Background(), p, walk); !errors.Is(e, ErrUncertain) {
						t.Fatal(e)
					}
					must(t, d.Close())
					d, e = Open(path, o, false, false)
					must(t, e)
					defer d.Close()
					marker, e := d.BulkStatus(p.MarkerKey)
					if e == nil && !marker.Complete {
						if _, e = d.Get("default", []byte("head")); !errors.Is(e, ErrNotFound) {
							t.Fatal("partial Head became visible", e)
						}
					}
					stats, e := d.ImportInitial(context.Background(), p, walk)
					must(t, e)
					if stats.Records != 100 || stats.VerifiedRecords != 100 || stats.PeakBatchBytes > 4096 || stats.Batches < 2 {
						t.Fatal(stats)
					}
					marker, e = d.BulkStatus(p.MarkerKey)
					must(t, e)
					if !marker.Complete {
						t.Fatal(marker)
					}
					v, e := d.Get("default", []byte("head"))
					must(t, e)
					if string(v) != "verified-root" {
						t.Fatal(v)
					}
					wrong := p
					wrong.Identity = "changed"
					if _, e = d.ImportInitial(context.Background(), wrong, walk); !errors.Is(e, ErrCorrupt) {
						t.Fatal("changed source accepted", e)
					}
				})
			}
		}
	}
}
func TestBulkCancellationKeepsCheckpointAndVerifiesPrefix(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			d, e := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, e)
			defer d.Close()
			p, walk := bulkFixture()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			commits := 0
			p.Committed = func(*Batch) {
				commits++
				if commits == 3 {
					cancel()
				}
			}
			if _, e = d.ImportInitial(ctx, p, walk); !errors.Is(e, context.Canceled) {
				t.Fatal(e)
			}
			m, e := d.BulkStatus(p.MarkerKey)
			must(t, e)
			if m.Complete || m.Stats.Records == 0 {
				t.Fatal(m)
			}
			b := NewBatch()
			b.Put("hash", []byte("node-0000"), []byte("corrupt"))
			must(t, d.Write(b))
			p.Committed = nil
			if _, e = d.ImportInitial(context.Background(), p, walk); !errors.Is(e, ErrCorrupt) {
				t.Fatal("saved prefix was trusted without verification", e)
			}
			if _, e = d.Get("default", []byte("head")); !errors.Is(e, ErrNotFound) {
				t.Fatal("corrupt import published", e)
			}
		})
	}
}
