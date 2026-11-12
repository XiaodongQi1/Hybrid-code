package hybridtrie

import (
	"encoding/binary"
	"math/bits"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"unsafe"
)

const (
	BoundaryFixedLayout       = "fixed"
	BoundaryTypedLayout       = "typed"
	BoundaryUpdatedVictims    = "updated"
	BoundaryFrontierVictims   = "frontier"
	BoundaryStrictReservation = "strict"
	BoundaryBorrowReservation = "borrow"
	typedFrameBytes           = 1024
	typedUsed                 = 1
	typedUpdated              = 2
	typedKnown                = 4
)

// Exactly one digest and no retained slice/NodeImage/RLP. Links use stable IDs,
// not body addresses. Flags bits 3..4 encode the allocated body class.
type boundaryHeader struct {
	Hash              mpt.Hash
	Heat              residentHeat
	Link              boundaryLink
	Height            [6]byte
	Source            uint8
	Flags             uint8
	Offset            uint32
	Fields, Canonical uint16
}

type boundaryFrame struct {
	Owners     [4]int32 // one-based NodeIDs
	Next, Prev int32
	Class      uint16 // 0 empty, 1/2/3 => 256/512/1024
	Mask       uint8
	Listed     bool
}

type boundaryFrames struct {
	entries []boundaryFrame
	heads   [4]int32 // -1 means no non-full frame
}

func (a *boundaryFrames) init(n int) {
	a.entries = make([]boundaryFrame, n)
	a.reset()
}
func (a *boundaryFrames) reset() {
	a.heads = [4]int32{-1, -1, -1, -1}
	for i := range a.entries {
		a.entries[i] = boundaryFrame{Next: -1, Prev: -1}
		a.add(i)
	}
}
func (a *boundaryFrames) unlink(i int) {
	f := &a.entries[i]
	if !f.Listed {
		return
	}
	if f.Prev < 0 {
		a.heads[f.Class] = f.Next
	} else {
		a.entries[f.Prev].Next = f.Next
	}
	if f.Next >= 0 {
		a.entries[f.Next].Prev = f.Prev
	}
	f.Next, f.Prev, f.Listed = -1, -1, false
}
func (a *boundaryFrames) add(i int) {
	f := &a.entries[i]
	if f.Listed {
		return
	}
	f.Next, f.Prev, f.Listed = a.heads[f.Class], -1, true
	if f.Next >= 0 {
		a.entries[f.Next].Prev = int32(i)
	}
	a.heads[f.Class] = int32(i)
}
func typedCapacity(class uint8) int                  { return 128 << class }
func (a *boundaryFrames) available(class uint8) bool { return a.heads[class] >= 0 || a.heads[0] >= 0 }
func (a *boundaryFrames) allocate(class uint8, slot int) (uint32, bool) {
	i := a.heads[class]
	if i < 0 {
		i = a.heads[0]
		if i < 0 {
			return 0, false
		}
		a.unlink(int(i))
		a.entries[i].Class = uint16(class)
		a.add(int(i))
	}
	f := &a.entries[i]
	n := typedFrameBytes / typedCapacity(class)
	unit := bits.TrailingZeros8(^f.Mask)
	if unit >= n {
		panic("hybridtrie: full frame on free list")
	}
	f.Mask |= 1 << unit
	f.Owners[unit] = int32(slot + 1)
	if int(f.Mask) == (1<<n)-1 {
		a.unlink(int(i))
	}
	return uint32(int(i)*typedFrameBytes + unit*typedCapacity(class)), true
}
func (a *boundaryFrames) release(offset uint32) {
	i := int(offset) / typedFrameBytes
	f := &a.entries[i]
	unit := (int(offset) % typedFrameBytes) / typedCapacity(uint8(f.Class))
	f.Mask &^= 1 << unit
	f.Owners[unit] = 0
	if f.Mask == 0 {
		a.unlink(i)
		f.Class = 0
		a.add(i)
	} else {
		a.add(i)
	}
}
func (b *boundaryBank) typed() bool { return b.Headers != nil }
func (b *boundaryBank) capacity() int {
	if b.typed() {
		return len(b.Headers)
	}
	return len(b.Nodes)
}
func (b *boundaryBank) usedAt(i int) bool {
	if b.typed() {
		return b.Headers[i].Flags&typedUsed != 0
	}
	return b.Nodes[i].Used
}
func (b *boundaryBank) updatedAt(i int) bool {
	if b.typed() {
		return b.Headers[i].Flags&typedUpdated != 0
	}
	return b.Nodes[i].Updated
}
func (b *boundaryBank) linkAt(i int) boundaryLink {
	if b.typed() {
		return b.Headers[i].Link
	}
	return b.Nodes[i].Link
}
func (b *boundaryBank) hashAt(i int) mpt.Hash {
	if b.typed() {
		return b.Headers[i].Hash
	}
	return b.Nodes[i].Hash
}
func (b *boundaryBank) heatAt(i int) residentHeat {
	if b.typed() {
		return b.Headers[i].Heat
	}
	return b.Nodes[i].Heat
}
func (b *boundaryBank) versionAt(i int) forestVersion {
	if b.typed() {
		n := &b.Headers[i]
		return forestVersion{read48(n.Height[:]), ReferenceKind(n.Source), n.Flags&typedKnown != 0}
	}
	return b.Nodes[i].Version
}
func (b *boundaryBank) imageAt(i int) mpt.NodeImage {
	if b.typed() {
		n := &b.Headers[i]
		if !b.usedAt(i) {
			return mpt.NodeImage{}
		}
		return mpt.ViewNodeImage(b.Payload[int(n.Offset):int(n.Offset)+int(n.Fields)], n.Hash, uint32(n.Canonical))
	}
	return b.Nodes[i].Image
}
func (b *boundaryBank) edgeAtSlot(i, slot int) boundaryEdge {
	if !b.typed() {
		return b.Nodes[i].Edges[slot]
	}
	n := &b.Headers[i]
	image := b.imageAt(i)
	d, e := image.Identity()
	if e != nil || d.Kind == mpt.CanonicalLeaf {
		return boundaryEdge{}
	}
	raw := b.Payload[int(n.Offset)+int(n.Fields):]
	pm := binary.LittleEndian.Uint16(raw)
	hm := binary.LittleEndian.Uint16(raw[2:])
	bit := uint16(1) << slot
	if pm&bit != 0 {
		at := 4 + 6*bits.OnesCount16(pm&(bit-1))
		return edgeAt(read48(raw[at:at+6]), boundaryPrefix)
	}
	if hm&bit != 0 {
		return boundaryEdge{Kind: boundaryHash}
	}
	return boundaryEdge{}
}
func (b *boundaryBank) edgesAt(i int) (edges [16]boundaryEdge) {
	if !b.typed() {
		return b.Nodes[i].Edges
	}
	n := &b.Headers[i]
	d, e := b.imageAt(i).Identity()
	if e != nil || d.Kind == mpt.CanonicalLeaf {
		return
	}
	raw := b.Payload[int(n.Offset)+int(n.Fields):]
	pm := binary.LittleEndian.Uint16(raw)
	hm := binary.LittleEndian.Uint16(raw[2:])
	at := 4
	for j := range edges {
		if pm&(1<<j) != 0 {
			edges[j] = edgeAt(read48(raw[at:at+6]), boundaryPrefix)
			at += 6
		} else if hm&(1<<j) != 0 {
			edges[j].Kind = boundaryHash
		}
	}
	return
}
func (b *boundaryBank) routingSize(i int) uint64 {
	if !b.typed() || !b.usedAt(i) {
		return 0
	}
	d, e := b.imageAt(i).Identity()
	if e != nil || d.Kind == mpt.CanonicalLeaf {
		return 0
	}
	n := &b.Headers[i]
	raw := b.Payload[int(n.Offset)+int(n.Fields):]
	return uint64(4 + 6*bits.OnesCount16(binary.LittleEndian.Uint16(raw)))
}

func (b *boundaryBank) snapshot(i int) boundaryNode {
	if !b.typed() {
		return b.Nodes[i]
	}
	return boundaryNode{Link: b.linkAt(i), Hash: b.hashAt(i), Image: b.imageAt(i), Version: b.versionAt(i), Heat: b.heatAt(i), Edges: b.edgesAt(i), Size: uint32(b.Headers[i].Canonical), Used: b.usedAt(i), Updated: b.updatedAt(i)}
}
func (b *boundaryBank) markChanged(i int) {
	word, mask := i/64, uint64(1)<<uint(i%64)
	if b.tracking && b.marked[word]&mask == 0 {
		b.marked[word] |= mask
		b.changed = append(b.changed, int32(i))
	}
}
func (b *boundaryBank) setLink(i int, link boundaryLink) {
	b.markChanged(i)
	if b.typed() {
		b.Headers[i].Link = link
	} else {
		b.Nodes[i].Link = link
	}
	b.refreshFrontier(i)
}
func (b *boundaryBank) eventHeat(i int, h, half uint64) {
	if b.typed() {
		b.Headers[i].Heat.event(h, half)
	} else {
		b.Nodes[i].Heat.event(h, half)
	}
}
func (b *boundaryBank) setEdges(i int, edges [16]boundaryEdge) {
	b.markChanged(i)
	if !b.typed() {
		b.Nodes[i].Edges = edges
		return
	}
	n := &b.Headers[i]
	image := b.imageAt(i)
	d, e := image.Identity()
	if e != nil {
		panic(e)
	}
	if d.Kind == mpt.CanonicalLeaf {
		return
	}
	raw := b.Payload[int(n.Offset)+int(n.Fields) : int(n.Offset)+b.bodyCapacity(i)]
	oldSize := b.routingSize(i)
	mask := image.IndependentChildren()
	var pm, hm uint16
	for j, edge := range edges {
		if mask&(1<<j) == 0 {
			continue
		}
		if edge.Kind == boundaryPrefix {
			pm |= 1 << j
		}
		if edge.Kind == boundaryHash {
			hm |= 1 << j
		}
	}
	binary.LittleEndian.PutUint16(raw, pm)
	binary.LittleEndian.PutUint16(raw[2:], hm)
	b.hintBytes -= oldSize
	b.hintBytes += uint64(4 + 6*bits.OnesCount16(pm))
	at := 4
	for j, edge := range edges {
		if pm&(1<<j) != 0 {
			copy(raw[at:at+6], edge.Height[:])
			at += 6
		}
	}
}
func (b *boundaryBank) bodyCapacity(i int) int {
	if b.typed() {
		return typedCapacity((b.Headers[i].Flags >> 3) & 3)
	}
	return b.Stride
}
func (b *boundaryBank) entryCharge(i int) uint64 {
	if b.typed() {
		return uint64(unsafe.Sizeof(boundaryHeader{})) + uint64(b.bodyCapacity(i))
	}
	return uint64(unsafe.Sizeof(boundaryNode{})) + uint64(b.Stride)
}
func (b *boundaryBank) saveNode(i int, n boundaryNode, offset uint32, class uint8) {
	b.markChanged(i)
	if !b.typed() {
		b.Nodes[i] = n
		return
	}
	flags := uint8(typedUsed) | (class << 3)
	if n.Updated {
		flags |= typedUpdated
	}
	if n.Version.Known {
		flags |= typedKnown
	}
	v := boundaryHeader{Hash: n.Hash, Heat: n.Heat, Link: n.Link, Source: uint8(n.Version.Source), Flags: flags, Offset: offset, Fields: uint16(n.Image.DataBytes()), Canonical: uint16(n.Size)}
	put48(v.Height[:], n.Version.Height)
	b.Headers[i] = v
	d, _ := n.Image.Identity()
	if d.Kind != mpt.CanonicalLeaf {
		raw := b.Payload[int(offset)+int(v.Fields):]
		for j := 0; j < 4; j++ {
			raw[j] = 0
		}
		b.hintBytes += 4
	}
	b.setEdges(i, n.Edges)
}
func (b *boundaryBank) clearSlot(i int) {
	b.markChanged(i)
	if b.typed() {
		b.frames.release(b.Headers[i].Offset)
		b.Headers[i] = boundaryHeader{}
	} else {
		b.Nodes[i] = boundaryNode{}
	}
	b.refreshFrontier(i)
}
func typedImageClass(image mpt.NodeImage, pos forestPosition) (uint8, error) {
	d, e := image.Identity()
	if e != nil {
		return 0, e
	}
	var class uint8
	var extra int
	switch d.Kind {
	case mpt.CanonicalLeaf:
		class = 1
	case mpt.CanonicalExtension:
		class = 2
		extra = 10
	case mpt.CanonicalBranch:
		class = 3
		extra = 100
	default:
		return 0, ErrCorrupt
	}
	if image.DataBytes()+extra > typedCapacity(class) {
		return 0, ErrLimit
	}
	if pos.ID == AccountTrieID && pos.Length == 0 {
		class = 3
	}
	return class, nil
}
