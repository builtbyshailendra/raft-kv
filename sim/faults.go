package sim

import (
	"fmt"
	"strings"
	"time"
)

// Note is a fault or recovery action, kept for the run's timeline.
type Note struct {
	At   time.Duration
	Text string
}

// Notes lists the fault actions so far.
func (w *World) Notes() []Note { return w.notes }

func (w *World) note(format string, args ...any) {
	w.notes = append(w.notes, Note{At: w.now, Text: fmt.Sprintf(format, args...)})
}

// Partition splits the nodes into groups that can't reach each other. Nodes
// not listed are each cut off on their own.
func (w *World) Partition(groups ...[]uint64) {
	for _, s := range w.servers {
		s.group = -int(s.id)
	}
	var parts []string
	for i, g := range groups {
		for _, id := range g {
			w.server(id).group = i + 1
		}
		parts = append(parts, fmt.Sprint(g))
	}
	w.Stats.Partitions++
	w.note("partition %s", strings.Join(parts, " | "))
}

// Heal removes every partition.
func (w *World) Heal() {
	for _, s := range w.servers {
		s.group = 0
	}
	w.note("heal")
}

// SetLoss sets the probability that any message (Raft or client) is lost.
func (w *World) SetLoss(p float64) {
	w.loss = p
	w.note("message loss %.0f%%", p*100)
}

// SetDup sets the probability that a Raft message is delivered twice.
func (w *World) SetDup(p float64) { w.dup = p }

// Crash stops a node. Its disk (log, HardState, snapshot) survives; its
// memory (state machine, pending requests, Raft's volatile state) doesn't.
func (w *World) Crash(id uint64) {
	s := w.server(id)
	if s.n == nil {
		return
	}
	s.n, s.paused, s.inbox = nil, false, nil
	w.Stats.Crashes++
	w.note("crash %d", id)
}

// Restart starts a crashed node from its disk.
func (w *World) Restart(id uint64) {
	s := w.server(id)
	if s.n != nil {
		return
	}
	w.start(s)
	w.Stats.Restarts++
	w.note("restart %d", id)
}

// Down lists the crashed nodes.
func (w *World) Down() []uint64 {
	var ids []uint64
	for _, s := range w.servers {
		if s.n == nil {
			ids = append(ids, s.id)
		}
	}
	return ids
}

// Pause freezes a node, like a long garbage-collection pause or a stalled
// VM. It keeps its state and wakes up believing whatever it believed.
func (w *World) Pause(id uint64) {
	s := w.server(id)
	if s.n == nil || s.paused {
		return
	}
	s.paused = true
	w.Stats.Pauses++
	w.note("pause %d", id)
}

// Resume unfreezes a node. It first works through everything that arrived
// while it was paused, in arrival order.
func (w *World) Resume(id uint64) {
	s := w.server(id)
	if !s.paused {
		return
	}
	s.paused = false
	w.note("resume %d", id)
	inbox := s.inbox
	s.inbox = nil
	for _, fn := range inbox {
		if s.n == nil || s.paused {
			return
		}
		fn(s.n)
	}
}

// Paused reports whether a node is paused.
func (w *World) Paused(id uint64) bool { return w.server(id).paused }
