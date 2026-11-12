package hybridtrie

import "errors"

var errAdmissionReadLimit = errors.New("hybridtrie: optional admission read budget exhausted")

type admissionReadBudget struct {
	nodes, bytes, maxNodes, maxBytes, bound uint64
	pending                                 bool
}

func newAdmissionReadBudget(c BoundaryConfig) *admissionReadBudget {
	n, b := c.AdmissionReadNodes, c.AdmissionReadBytes
	if n == 0 {
		n = 64
	}
	if b == 0 {
		b = 256 << 10
	}
	bound := uint64(MaxRecordBytes) + 128
	if c.MaxNodeBytes == 532 {
		bound = 4096
	}
	return &admissionReadBudget{maxNodes: n, maxBytes: b, bound: bound}
}

// Reserve a whole maximum record before issuing I/O, then charge its actual
// logical bytes. This bounds operations and bytes even near the budget end.
func (b *readBudget) beforeRead() error {
	a := b.admission
	if a == nil {
		return nil
	}
	if a.nodes >= a.maxNodes || a.bytes > a.maxBytes || a.bound > a.maxBytes-a.bytes {
		return errAdmissionReadLimit
	}
	a.nodes++
	a.pending = true
	return nil
}

// afterRead bills optional I/O independently from a query's traversal limits.
func (a *admissionReadBudget) afterRead(n uint64) error {
	if a == nil || !a.pending {
		return nil
	}
	a.pending = false
	a.bytes += n
	if n > a.bound {
		return ErrCorrupt
	}
	return nil
}
