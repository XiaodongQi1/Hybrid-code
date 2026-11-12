package backend

import (
	"errors"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
)

func TestRecorderFailureAbortsAndAllowsRetry(t *testing.T) {
	b, err := Open(filepath.Join(t.TempDir(), "db"), smallConfig(codec.Ethereum), true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	in := inputAt(0)
	in.Allocations = []block.Allocation{{Address: common.HexToAddress("0x1234"), Balance: big.NewInt(123)}}
	before, err := b.Head()
	if err != nil {
		t.Fatal(err)
	}
	stop := errors.New("recorder could not sync")
	var recorded Result
	_, err = b.ApplyWithRecorder(in, nil, func(r Result) error { recorded = r; return stop })
	if !errors.Is(err, stop) {
		t.Fatal(err)
	}
	after, err := b.Head()
	if err != nil || after != before {
		t.Fatal("recorder failure published state", after, err)
	}
	r, err := b.ApplyWithRecorder(in, nil, nil)
	if err != nil || r.Root != recorded.Root || r.ID != recorded.ID {
		t.Fatal("retry changed recorded state", r, err)
	}
	v, err := b.ViewAt(in.Height)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if v.BlockID() != in.ID || v.Root() != r.Root {
		t.Fatal("committed view is not bound to the recorded block")
	}
}
