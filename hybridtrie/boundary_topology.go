package hybridtrie

import (
	"math/bits"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func (b *boundaryBank) trieID(n boundaryNode) uint64 {
	return b.Roots[n.Link.Root].ID
}

// position reconstructs a temporary key upward from the resident links. An
// extension's fragment belongs to its node image, so no full prefix is retained.
func (b *boundaryBank) position(i int) forestPosition {
	var chain [65]int
	count := 0
	at := i
	for b.linkAt(at).Parent != 0 {
		if count == len(chain) {
			panic("hybridtrie: cyclic cache topology")
		}
		chain[count] = at
		count++
		at = int(b.linkAt(at).Parent) - 1
	}
	pos := position(b.Roots[b.linkAt(i).Root].ID, nil)
	for j := count - 1; j >= 0; j-- {
		child := b.linkAt(chain[j])
		parent := b.imageAt(int(child.Parent - 1))
		d, err := parent.Identity()
		if err != nil {
			panic("hybridtrie: invalid resident image")
		}
		if d.Kind == mpt.CanonicalExtension {
			copy(pos.Path[int(pos.Length):], d.Fragment[:d.Length])
			pos.Length += d.Length
		} else {
			pos.Path[pos.Length] = child.Edge
			pos.Length++
		}
	}
	return pos
}

func (b *boundaryBank) observation(i int) boundaryObservation {
	return boundaryObservation{Pos: b.position(i), boundaryNode: (b.snapshot(i))}
}

// Compare against the supplied path in place; do not rebuild a temporary
// full position or copy every ancestor node on each hash-table probe.
func (b *boundaryBank) matchesPosition(i int, pos forestPosition) bool {
	n := b.linkAt(i)
	if b.Roots[n.Root].ID != pos.ID {
		return false
	}
	end := int(pos.Length)
	for steps := 0; n.Parent != 0; steps++ {
		if steps >= 64 {
			return false
		}
		parent := b.linkAt(int(n.Parent) - 1)
		d, err := b.imageAt(int(n.Parent) - 1).Identity()
		if err != nil {
			return false
		}
		if d.Kind == mpt.CanonicalExtension {
			length := int(d.Length)
			if end < length {
				return false
			}
			for j := 0; j < length; j++ {
				if pos.Path[end-length+j] != d.Fragment[j] {
					return false
				}
			}
			end -= length
		} else {
			if end == 0 || pos.Path[end-1] != n.Edge {
				return false
			}
			end--
		}
		n = parent
	}
	return end == 0
}

func (b *boundaryBank) find(pos forestPosition) int {
	for _, index := range [][]int32{b.Index} {
		if len(index) == 0 {
			continue
		}
		start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(len(index)))
		for j := 0; j < len(index); j++ {
			v := index[(start+j)%len(index)]
			if v == 0 {
				break
			}
			if v > 0 && b.usedAt(int(v-1)) && b.matchesPosition(int(v-1), pos) {
				return int(v - 1)
			}
		}
	}
	return -1
}

func (b *boundaryBank) index(i int) {
	pos := b.position(i)
	start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(len(b.Index)))
	for j := 0; j < len(b.Index); j++ {
		p := (start + j) % len(b.Index)
		v := b.Index[p]
		if v == int32(i+1) {
			return
		}
		if v == 0 {
			b.setIndex(p, int32(i+1))
			return
		}
	}
}

// remove detaches a subtree while the old images still describe its paths.
// Policy victims are frontiers; structural changes may remove descendants too.
func (b *boundaryBank) remove(i int) int {
	if !b.usedAt(i) {
		return 0
	}
	removed := 1
	for b.linkAt(i).FirstChild != 0 {
		removed += b.remove(int(b.linkAt(i).FirstChild) - 1)
	}
	n := (b.snapshot(i))
	class := cacheClass(b.trieID(n))
	if b.removed != nil {
		b.removed(b.position(i))
	}
	b.unindex(i)
	if n.Link.Parent != 0 {
		previous := b.previousSibling(i)
		if previous < 0 {
			link := b.linkAt(int(n.Link.Parent) - 1)
			link.FirstChild = n.Link.NextSibling
			b.setLink(int(n.Link.Parent)-1, link)
		} else {
			link := b.linkAt(previous)
			link.NextSibling = n.Link.NextSibling
			b.setLink(previous, link)
		}
	}
	if n.Link.Parent == 0 {
		b.Roots[n.Link.Root] = boundaryRoot{}
	}
	b.counts[class]--
	b.usage[class] -= b.entryCharge(i)
	b.fieldBytes -= uint64(n.Image.DataBytes())
	b.hintBytes -= b.routingSize(i)
	b.clearSlot(i)
	b.vacate(i)
	return removed
}

func (b *boundaryBank) putImage(i int, n boundaryObservation, image mpt.NodeImage) error {
	return b.putLocatedImage(i, n, image, -2, -1)
}

func (b *boundaryBank) putLocatedImage(i int, n boundaryObservation, image mpt.NodeImage, knownParent, knownEdge int) error {
	// Changing an extension fragment changes descendants' positions. Remove
	// those residents before replacing the fragment used to derive their keys.
	if b.usedAt(i) {
		if b.position(i) != n.Pos {
			b.Pruned += uint64(b.remove(i))
		} else {
			old, err := b.imageAt(i).Identity()
			if err != nil {
				return err
			}
			next, err := image.Identity()
			if err != nil {
				return err
			}
			if old.Kind != next.Kind || old.Length != next.Length || old.Fragment != next.Fragment {
				for b.linkAt(i).FirstChild != 0 {
					b.Pruned += uint64(b.remove(int(b.linkAt(i).FirstChild) - 1))
				}
			}
		}
	}
	link := b.linkAt(i)
	if !b.usedAt(i) {
		ri := b.root(n.Pos.ID)
		if ri < 0 {
			if n.Pos.Length != 0 {
				return ErrCorrupt
			}
			for k := range b.Roots {
				if !b.Roots[k].Used {
					ri = k
					break
				}
			}
			if ri < 0 {
				return ErrLimit
			}
			b.Roots[ri] = boundaryRoot{ID: n.Pos.ID, Hash: n.Hash, Version: n.Version, Used: true}
		}
		link = boundaryLink{Root: uint16(ri)}
		if n.Pos.Length != 0 {
			parent := knownParent
			if parent == -2 {
				parent = -1
				for depth := int(n.Pos.Length) - 1; depth >= 0; depth-- {
					if at := b.find(position(n.Pos.ID, n.Pos.Path[:depth])); at >= 0 {
						parent = at
						break
					}
				}
			}
			if parent < 0 {
				return ErrCorrupt
			}
			if knownEdge < 0 {
				p := b.observation(parent)
				d, err := p.Image.Identity()
				if err != nil {
					return err
				}
				slot := 0
				if d.Kind == mpt.CanonicalBranch {
					slot = int(n.Pos.Path[p.Pos.Length])
				}
				child, h, ok := boundaryImageChild(p, p.Image, slot)
				if !ok || child != n.Pos || h != n.Hash {
					return ErrCorrupt
				}
				knownEdge = slot
			}
			link.Parent, link.Edge = int32(parent+1), uint8(knownEdge)
			link.NextSibling = b.linkAt(parent).FirstChild
		}
	}
	oldNode := b.snapshot(i)
	var offset uint32
	var class uint8
	dst := b.Payload[:0]
	oldCharge := uint64(0)
	oldHints := b.routingSize(i)
	if oldNode.Used {
		oldCharge = b.entryCharge(i)
	}
	if b.typed() {
		var err error
		class, err = typedImageClass(image, n.Pos)
		if err != nil {
			return err
		}
		if oldNode.Used && typedCapacity(class) <= b.bodyCapacity(i) {
			class = (b.Headers[i].Flags >> 3) & 3
			offset = b.Headers[i].Offset
		} else {
			var ok bool
			offset, ok = b.frames.allocate(class, i)
			if !ok && oldNode.Used {
				fi := int(b.Headers[i].Offset) / typedFrameBytes
				if bits.OnesCount8(b.frames.entries[fi].Mask) == 1 {
					b.frames.release(b.Headers[i].Offset)
					offset, ok = b.frames.allocate(class, i)
				} else {
					b.ResizeDrops++
					b.Pruned += uint64(b.remove(i))
					return nil
				}
			} else if ok && oldNode.Used {
				b.frames.release(b.Headers[i].Offset)
			}
			if !ok {
				return ErrLimit
			}
		}
		dst = b.Payload[int(offset) : int(offset)+typedCapacity(class)]
	} else {
		dst = b.Payload[i*b.Stride : (i+1)*b.Stride]
	}

	copyImage, err := image.CloneTo(dst)
	if err != nil || copyImage.Hash() != n.Hash {
		return ErrHashMismatch
	}
	if !b.usedAt(i) && link.Parent != 0 {
		pl := b.linkAt(int(link.Parent) - 1)
		pl.FirstChild = int32(i + 1)
		b.setLink(int(link.Parent)-1, pl)
	}
	n.Image, n.Used, n.Size, n.Link = copyImage, true, copyImage.EncodedSize(), link
	if oldNode.Used {
		b.fieldBytes -= uint64(oldNode.Image.DataBytes())
	} else {
		b.counts[cacheClass(n.Pos.ID)]++
		b.occupy(i)
	}
	b.fieldBytes += uint64(copyImage.DataBytes())
	b.hintBytes -= oldHints
	b.saveNode(i, n.boundaryNode, offset, class)
	b.usage[cacheClass(n.Pos.ID)] -= oldCharge
	b.usage[cacheClass(n.Pos.ID)] += b.entryCharge(i)
	b.refreshFrontier(i)
	b.index(i)
	return nil
}

func (b *boundaryBank) put(i int, n boundaryObservation, raw []byte) error {
	// Decode into temporary storage: putImage must inspect the previous image
	// before the resident arena is overwritten.
	var small [804]byte
	buf := small[:]
	if b.Stride > len(buf) {
		buf = make([]byte, b.Stride)
	}
	image, err := mpt.CaptureTrustedLocalNodeImage(buf, raw, n.Hash)
	if err != nil {
		return err
	}
	return b.putImage(i, n, image)
}

// putFields consumes a block-local parse shared with validation and projection.
func (b *boundaryBank) putFields(i int, n boundaryObservation, fields mpt.CanonicalNode, size uint32) error {
	var small [804]byte
	buf := small[:]
	if b.Stride > len(buf) {
		buf = make([]byte, b.Stride)
	}
	image, err := mpt.CaptureLocalFieldsImage(buf, fields, n.Hash, size)
	if err != nil {
		return err
	}
	return b.putImage(i, n, image)
}
