package evmruntime

import "github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/vm"

// MachinePool retains preallocated, exclusively leased EVM scratch resources.
// It never caches state or query answers, and never limits replay concurrency.
// Excess concurrent leases allocate temporary machines, discarded on return
// when all retained slots are occupied.
type MachinePool struct{ idle chan *vm.EVM }

func NewMachinePool(capacity int) *MachinePool {
	if capacity < 1 {
		capacity = 4
	}
	p := &MachinePool{idle: make(chan *vm.EVM, capacity)}
	for i := 0; i < capacity; i++ {
		p.idle <- vm.NewReusableEVM()
	}
	return p
}

// SetMachinePool binds the node's shared pool before execution. Engine itself
// remains single-owner: gas accounting and state must never be shared by jobs.
func (e *Engine) SetMachinePool(p *MachinePool) { e.pool = p }

func (e *Engine) machine(b vm.BlockContext, c vm.Config) (*vm.EVM, func()) {
	if e.pool == nil {
		return vm.NewEVM(b, e.state, e.chain, c), func() {}
	}
	var m *vm.EVM
	select {
	case m = <-e.pool.idle:
	default:
		m = vm.NewReusableEVM()
	}
	m.Reset(b, e.state, e.chain, c)
	return m, func() {
		m.Release()
		select {
		case e.pool.idle <- m:
		default:
		}
	}
}
