package hybridtrie

import (
	"math/bits"
	"time"
	"unsafe"
)

const proofQueueGroups = 16

func (s *ProofSession) schedule(f *plannedProofFrame) error {
	if f.remaining == 0 || s.cfg.Workers == 0 {
		return nil
	}
	s.pending += uint64(bits.OnesCount16(f.remaining))
	if s.queue == nil {
		if s.pending < uint64(s.cfg.MinBackgroundReads) {
			return nil
		}
		if e := s.retain(proofQueueGroups * uint64(unsafe.Sizeof(proofSiblingTask{}))); e != nil {
			return e
		}
		s.queue = make(chan proofSiblingTask, proofQueueGroups)
		// Only once, at activation. Earlier frames were never queued.
		for i := 0; i < s.count; i++ {
			for j := 0; j < s.plans[i].count; j++ {
				if old := s.plans[i].items[j].frame; old != nil {
					s.submit(old)
				}
			}
		}
		return s.check()
	}
	s.submit(f)
	return s.check()
}
func (s *ProofSession) submit(f *plannedProofFrame) {
	for f.remaining != 0 {
		if s.check() != nil {
			return
		}
		mask, left := uint16(0), f.remaining
		for n := 0; n < 4 && left != 0; n++ {
			bit := uint16(1) << bits.TrailingZeros16(left)
			mask |= bit
			left &^= bit
		}
		if s.started < int(s.cfg.Workers) {
			w := &s.workers[s.started]
			w.session = s
			s.started++
			s.stats.WorkerStarts++
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				for t := range s.queue {
					if s.ctx.Err() != nil || s.failure.Load() != nil {
						return
					}
					if e := w.siblings(t); e != nil {
						s.fail(e)
						return
					}
				}
			}()
		}
		select {
		case s.queue <- proofSiblingTask{f, mask}:
			f.remaining &^= mask
			s.stats.SubmittedGroups++
		default:
			s.stats.DeferredGroups++
			return
		}
	}
}
func (s *ProofSession) join() {
	if s.joined {
		return
	}
	if s.queue != nil {
		close(s.queue)
	}
	start := time.Now()
	s.wg.Wait()
	s.stats.WaitNanos += uint64(time.Since(start))
	s.joined = true
}
