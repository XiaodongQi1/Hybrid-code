package evmruntime

import (
	"bytes"
	"context"
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/vm"
)

func TestMachinePoolMatchesFreshExecution(t *testing.T) {
	fresh, pooled := setup(t), setup(t)
	pool := NewMachinePool(1) // force every operation to reuse the same machine
	pooled.e.SetMachinePool(pool)
	addr := common.HexToAddress("0x1234")
	// Store calldata in slot 0 and return it; also exercise revert and OOG.
	for _, code := range [][]byte{
		{0x60, 0, 0x35, 0x60, 0, 0x55, 0x60, 0, 0x54, 0x60, 0, 0x52, 0x60, 32, 0x60, 0, 0xf3},
		{0x60, 1, 0x60, 0, 0x55, 0x60, 0, 0x60, 0, 0xfd},
		{0x5b, 0x60, 0, 0x56},
	} {
		install(fresh, addr, code)
		install(pooled, addr, code)
		for i := 0; i < 3; i++ {
			data := common.LeftPadBytes([]byte{byte(i + 1)}, 32)
			a := fresh.apply(t, addr, data, 100000, 0)
			b := pooled.apply(t, addr, data, 100000, 0)
			if !reflect.DeepEqual(a, b) || fresh.s.GetState(addr, common.Hash{}) != pooled.s.GetState(addr, common.Hash{}) || fresh.s.GetNonce(fresh.sender) != pooled.s.GetNonce(pooled.sender) {
				t.Fatal("reused execution differs", a, b)
			}
		}
	}
	// Cancellation, static mode and transaction data must not survive a lease.
	m, done := pooled.e.machine(vm.BlockContext{BlockNumber: big.NewInt(1), Random: new(common.Hash)}, vm.Config{NoBaseFee: true})
	m.Cancel()
	m.SetTxContext(vm.TxContext{Origin: addr, GasPrice: big.NewInt(99)})
	done()
	if m.StateDB != nil || m.Cancelled() || m.TxContext.Origin != (common.Address{}) {
		t.Fatal("lease retained state")
	}
	install(pooled, addr, []byte{0x43, 0x60, 0, 0x52, 0x60, 32, 0x60, 0, 0xf3})
	for _, height := range []uint64{5, 20, 3} {
		e, err := New(pooled.s, nil, Block{Number: height, GasLimit: 100000})
		if err != nil {
			t.Fatal(err)
		}
		e.SetMachinePool(pool)
		r, err := e.Call(context.Background(), CallRequest{From: pooled.sender, To: addr, Gas: 50000, Static: true})
		if err != nil || !bytes.Equal(r.ReturnData, common.LeftPadBytes([]byte{byte(height)}, 32)) {
			t.Fatal("stale context", r, err)
		}
	}
}

func TestMachinePoolConcurrentLeases(t *testing.T) {
	pool := NewMachinePool(4)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := NewMemoryState()
			if err != nil {
				t.Error(err)
				return
			}
			e, err := New(s, nil, Block{Number: 1, GasLimit: 100000})
			if err != nil {
				t.Error(err)
				return
			}
			e.SetMachinePool(pool)
			for j := 0; j < 20; j++ {
				_, err := e.Call(context.Background(), CallRequest{To: common.HexToAddress("0xabcd"), Gas: 21000})
				if err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if len(pool.idle) != 4 {
		t.Fatal("lost pool slots")
	}
}

// Measures construction/reset overhead only, not end-to-end query speedup.
func BenchmarkMachinePreparation(b *testing.B) {
	for _, pooled := range []bool{false, true} {
		name := "fresh"
		if pooled {
			name = "pooled"
		}
		b.Run(name, func(b *testing.B) {
			s, _ := NewMemoryState()
			e, _ := New(s, nil, Block{Number: 1, GasLimit: 100000})
			if pooled {
				e.SetMachinePool(NewMachinePool(4))
			}
			bc := vm.BlockContext{BlockNumber: big.NewInt(1), Random: new(common.Hash)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				m, done := e.machine(bc, vm.Config{})
				if m.StateDB != s {
					b.Fatal("state")
				}
				done()
			}
		})
	}
}
