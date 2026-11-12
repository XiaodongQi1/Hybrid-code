package hybridtrie

// These indexes belong to the single resident cache, not to a pending cache.
func (b *boundaryBank) node(slot int) *boundaryNode { return &b.Nodes[slot] }
func (b *boundaryBank) edit(slot int) *boundaryNode {
	word, mask := slot/64, uint64(1)<<uint(slot%64)
	if b.tracking && b.marked[word]&mask == 0 {
		b.marked[word] |= mask
		b.changed = append(b.changed, int32(slot))
	}
	return &b.Nodes[slot]
}
func (b *boundaryBank) beginChanges() { b.finishChanges(); b.tracking = true; b.Pruned = 0 }
func (b *boundaryBank) finishChanges() {
	for _, slot := range b.changed {
		if b.typed() {
			b.Headers[slot].Flags &^= typedUpdated
		} else {
			b.Nodes[slot].Updated = false
		}
		b.marked[slot/64] &^= uint64(1) << uint(slot%64)
	}
	b.changed = b.changed[:0]
	b.tracking = false
}
func (b *boundaryBank) setIndex(at int, value int32) { b.Index[at] = value }
func (b *boundaryBank) unindex(slot int) {
	pos := b.position(slot)
	start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(len(b.Index)))
	for j := 0; j < len(b.Index); j++ {
		p := (start + j) % len(b.Index)
		if b.Index[p] == 0 {
			return
		}
		if b.Index[p] == int32(slot+1) {
			// Close this probe-chain hole locally. Tombstones accumulate across
			// blocks and eventually turn every miss into a full-table scan.
			hole, size := p, len(b.Index)
			for next := (p + 1) % size; b.Index[next] != 0; next = (next + 1) % size {
				other := b.position(int(b.Index[next] - 1))
				home := int(positionHash(other.ID, other.Path[:other.Length]) % uint64(size))
				if (hole-home+size)%size < (next-home+size)%size {
					b.Index[hole] = b.Index[next]
					hole = next
				}
			}
			b.Index[hole] = 0
			return
		}
	}
}
func (b *boundaryBank) occupy(slot int) {
	at := int(b.freeAt[slot])
	if at < 0 {
		return
	}
	last := b.free[len(b.free)-1]
	b.free[at] = last
	b.freeAt[last] = int32(at)
	b.free = b.free[:len(b.free)-1]
	b.freeAt[slot] = -1
}
func (b *boundaryBank) vacate(slot int) {
	if b.freeAt[slot] >= 0 {
		return
	}
	b.freeAt[slot] = int32(len(b.free))
	b.free = append(b.free, int32(slot))
}
func (b *boundaryBank) freeSlot() int {
	if len(b.free) == 0 {
		return -1
	}
	return int(b.free[len(b.free)-1])
}

// previousSibling finds the one link to detach without modifying other siblings.
func (b *boundaryBank) previousSibling(slot int) int {
	n := b.linkAt(slot)
	if n.Parent == 0 {
		return -1
	}
	previous := -1
	for child := b.linkAt(int(n.Parent) - 1).FirstChild; child != int32(slot+1); child = b.linkAt(int(child) - 1).NextSibling {
		previous = int(child) - 1
	}
	return previous
}
