package sim

import (
	"fmt"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/check"
	"github.com/builtbyshailendra/raft-kv/kv"
	"github.com/builtbyshailendra/raft-kv/node"
)

// Workload is what the clients do. Zero values get defaults.
type Workload struct {
	Clients       int           // default 5
	Keys          int           // default 3: few keys, many conflicts
	Reads         float64       // share of operations that are Gets, default 0.5
	FollowerReads float64       // share of Gets sent to a random node instead of the presumed leader
	Duration      time.Duration // clients start new operations until then, default 5s
	ClientTimeout time.Duration // a client gives up on a node after this, default 300ms
}

func (wl Workload) withDefaults() Workload {
	if wl.Clients == 0 {
		wl.Clients = 5
	}
	if wl.Keys == 0 {
		wl.Keys = 3
	}
	if wl.Reads == 0 {
		wl.Reads = 0.5
	}
	if wl.Duration == 0 {
		wl.Duration = 5 * time.Second
	}
	if wl.ClientTimeout == 0 {
		wl.ClientTimeout = 300 * time.Millisecond
	}
	return wl
}

// Faults is what the fault scheduler may do while the workload runs. Every
// 200 ms to 1 s it picks one allowed action at random. It never takes down
// or pauses more than a minority at once, so the cluster can keep working.
type Faults struct {
	Partitions bool
	Crashes    bool
	Pauses     bool
	Loss       float64 // highest message-loss rate it sets
	Dup        float64 // Raft message duplication rate during the faults
}

// AllFaults turns everything on.
func AllFaults() Faults {
	return Faults{Partitions: true, Crashes: true, Pauses: true, Loss: 0.2, Dup: 0.05}
}

func (f Faults) any() bool { return f.Partitions || f.Crashes || f.Pauses || f.Loss > 0 || f.Dup > 0 }

// RunConfig is a whole test run: a cluster, a workload and faults.
type RunConfig struct {
	Config
	Workload Workload
	Faults   Faults
	// After the workload's Duration every fault is healed and every node
	// restarted; operations still in flight must finish within Recovery.
	// Default 30s.
	Recovery time.Duration
}

// Result is what a run produced.
type Result struct {
	Seed    uint64
	History []porcupine.Operation
	Notes   []Note
	// Violations are safety failures the simulator saw directly (two
	// leaders in a term, different entries at one index, replicas that
	// disagree at the end). Linearizability is checked separately.
	Violations []string
	// Unfinished counts operations never answered even after every fault
	// was healed: a liveness failure.
	Unfinished int
	Stats      Stats
	TraceHash  uint64
	End        time.Duration
}

// Annotations turns the run's timeline into labels for Porcupine's
// visualization.
func (r *Result) Annotations() []porcupine.Annotation {
	out := make([]porcupine.Annotation, 0, len(r.Notes))
	for _, n := range r.Notes {
		a := porcupine.Annotation{Tag: n.Tag, Start: int64(n.At), Description: n.Text}
		if n.Tag == "fault" {
			a.BackgroundColor = "#fde2e1"
		} else {
			a.BackgroundColor = "#e3ebfb"
		}
		out = append(out, a)
	}
	return out
}

// Run executes one seeded run and returns its history for checking.
func Run(cfg RunConfig) *Result {
	cfg.Workload = cfg.Workload.withDefaults()
	if cfg.Recovery == 0 {
		cfg.Recovery = 30 * time.Second
	}
	w := New(cfg.Config)
	defer w.Close()
	wl := cfg.Workload

	w.clients = map[uint64]*client{}
	var clients []*client
	for i := range wl.Clients {
		c := &client{w: w, wl: wl, idx: i, id: uint64(i + 1), leader: uint64(1 + w.rng.IntN(len(w.servers)))}
		w.clients[c.id] = c
		clients = append(clients, c)
		w.after(w.think(), c.next)
	}
	if cfg.Faults.any() {
		w.faults, w.faultsOn = cfg.Faults, true
		w.SetDup(cfg.Faults.Dup)
		w.after(w.gap(), w.nemesis)
		w.after(wl.Duration, w.endFaults)
	}

	idle := func() bool {
		for _, c := range clients {
			if !c.finished {
				return false
			}
		}
		return true
	}
	w.RunUntil(func() bool { return w.now >= wl.Duration && idle() }, wl.Duration+cfg.Recovery)

	r := &Result{Seed: cfg.Seed}
	for _, c := range clients {
		if c.cur == nil {
			continue
		}
		r.Unfinished++
		// A write that never got an answer may still have taken effect.
		// Keep it, open-ended, so a read that saw it isn't flagged falsely.
		if c.cur.in.Kind != kv.Get {
			w.ops = append(w.ops, porcupine.Operation{ClientId: c.idx, Input: c.cur.in, Call: int64(c.cur.call), Output: check.Output{}, Return: int64(w.now) + 1})
		}
	}
	if r.Unfinished == 0 {
		w.checkConvergence()
	}
	r.History, r.Notes, r.Violations = w.ops, w.notes, w.violations
	r.Stats, r.TraceHash, r.End = w.Stats, w.TraceHash(), w.now
	return r
}

// checkConvergence waits for every node to apply the same index, then
// compares their state machines.
func (w *World) checkConvergence() {
	same := func() bool {
		idx := w.servers[0].n.Applied()
		for _, s := range w.servers[1:] {
			if s.n.Applied() != idx {
				return false
			}
		}
		return true
	}
	if !w.RunUntil(same, 5*time.Second) {
		w.violate("replicas did not reach one applied index after recovery")
		return
	}
	first := w.servers[0]
	for _, s := range w.servers[1:] {
		if s.n.Hash() != first.n.Hash() {
			w.violate("node %d and node %d differ at applied index %d", first.id, s.id, s.n.Applied())
		}
	}
}

func (w *World) think() time.Duration {
	return time.Duration(w.rng.Int64N(int64(20 * time.Millisecond)))
}

func (w *World) gap() time.Duration {
	return 200*time.Millisecond + time.Duration(w.rng.Int64N(int64(800*time.Millisecond)))
}

// nemesis is the fault scheduler.
func (w *World) nemesis() {
	if !w.faultsOn {
		return
	}
	f := w.faults
	var acts []func()
	if f.Partitions {
		acts = append(acts, w.randomPartition, w.Heal)
	}
	if f.Crashes {
		acts = append(acts, w.crashOne, w.restartOne)
	}
	if f.Pauses {
		acts = append(acts, w.pauseOne)
	}
	if f.Loss > 0 {
		acts = append(acts, func() { w.SetLoss([]float64{0, f.Loss / 2, f.Loss}[w.rng.IntN(3)]) })
	}
	if len(acts) > 0 {
		acts[w.rng.IntN(len(acts))]()
	}
	w.after(w.gap(), w.nemesis)
}

// endFaults heals everything so the run can finish.
func (w *World) endFaults() {
	w.faultsOn = false
	w.Heal()
	w.SetLoss(0)
	w.SetDup(0)
	for _, s := range w.servers {
		if s.paused {
			w.Resume(s.id)
		}
		if s.n == nil {
			w.Restart(s.id)
		}
	}
}

// impaired counts nodes that are down or paused.
func (w *World) impaired() int {
	k := 0
	for _, s := range w.servers {
		if s.n == nil || s.paused {
			k++
		}
	}
	return k
}

// victim picks a healthy node, the leader half the time: faults at the
// leader are the interesting ones.
func (w *World) victim() uint64 {
	if l := w.Leader(); l != 0 && !w.server(l).paused && w.rng.IntN(2) == 0 {
		return l
	}
	var ok []uint64
	for _, s := range w.servers {
		if s.n != nil && !s.paused {
			ok = append(ok, s.id)
		}
	}
	if len(ok) == 0 {
		return 0
	}
	return ok[w.rng.IntN(len(ok))]
}

func (w *World) randomPartition() {
	ids := w.IDs()
	w.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if l := w.Leader(); l != 0 && w.rng.IntN(2) == 0 { // often cut the leader off
		for i, id := range ids {
			if id == l {
				ids[0], ids[i] = ids[i], ids[0]
			}
		}
	}
	k := 1 + w.rng.IntN((len(ids)-1)/2) // minority size
	minority := append([]uint64(nil), ids[:k]...)
	majority := append([]uint64(nil), ids[k:]...)
	sort.Slice(minority, func(i, j int) bool { return minority[i] < minority[j] })
	sort.Slice(majority, func(i, j int) bool { return majority[i] < majority[j] })
	w.Partition(minority, majority)
}

func (w *World) crashOne() {
	if w.impaired() >= (len(w.servers)-1)/2 {
		return
	}
	if id := w.victim(); id != 0 {
		w.Crash(id)
	}
}

func (w *World) restartOne() {
	if down := w.Down(); len(down) > 0 {
		w.Restart(down[w.rng.IntN(len(down))])
	}
}

func (w *World) pauseOne() {
	if w.impaired() >= (len(w.servers)-1)/2 {
		return
	}
	id := w.victim()
	if id == 0 {
		return
	}
	w.Pause(id)
	w.after(300*time.Millisecond+time.Duration(w.rng.Int64N(int64(1500*time.Millisecond))), func() { w.Resume(id) })
}

// toNode carries a client request to a node.
func (w *World) toNode(to uint64, c kv.Command) {
	if w.lost() {
		return
	}
	w.after(w.delay(), func() {
		w.deliver(w.server(to), func(n *node.Node) { n.Handle(c) })
	})
}

// client runs operations one at a time. It knows only node IDs: it starts
// from a guess, follows "not leader" hints, moves to the next node on a
// timeout, and retries with the same Seq so a write applies at most once.
type client struct {
	w        *World
	wl       Workload
	idx      int    // Porcupine client ID
	id       uint64 // kv client ID
	seq      uint64
	leader   uint64 // where writes go
	cur      *pendingOp
	retrying bool
	finished bool
}

type pendingOp struct {
	in      check.Input
	call    time.Duration
	attempt int
}

func (c *client) next() {
	if c.w.now >= c.wl.Duration {
		c.finished = true
		return
	}
	c.seq++
	in := check.Input{Key: fmt.Sprintf("k%d", c.w.rng.IntN(c.wl.Keys))}
	switch r := c.w.rng.Float64(); {
	case r < c.wl.Reads:
		in.Kind = kv.Get
	case r < c.wl.Reads+(1-c.wl.Reads)/2:
		in.Kind, in.Value = kv.Put, fmt.Sprintf("%d.%d;", c.id, c.seq)
	default:
		in.Kind, in.Value = kv.Append, fmt.Sprintf("%d.%d;", c.id, c.seq)
	}
	c.cur = &pendingOp{in: in, call: c.w.now}
	c.send()
}

func (c *client) send() {
	op := c.cur
	op.attempt++
	attempt, seq := op.attempt, c.seq
	target := c.leader
	if op.in.Kind == kv.Get && c.wl.FollowerReads > 0 && c.w.rng.Float64() < c.wl.FollowerReads {
		target = uint64(1 + c.w.rng.IntN(len(c.w.servers)))
	}
	c.w.toNode(target, kv.Command{Client: c.id, Seq: seq, Kind: op.in.Kind, Key: op.in.Key, Value: op.in.Value})
	c.w.after(c.wl.ClientTimeout, func() {
		if c.cur != nil && c.seq == seq && c.cur.attempt == attempt {
			c.leader = c.nextNode(c.leader)
			c.send()
		}
	})
}

func (c *client) nextNode(id uint64) uint64 { return id%uint64(len(c.w.servers)) + 1 }

func (c *client) onReply(from uint64, r node.Response) {
	if c.cur == nil || r.Seq != c.seq {
		return // a late answer to an operation already finished
	}
	if r.OK {
		c.w.ops = append(c.w.ops, porcupine.Operation{
			ClientId: c.idx, Input: c.cur.in, Call: int64(c.cur.call),
			Output: check.Output{Value: r.Value}, Return: int64(c.w.now),
			Metadata: from, // which node answered
		})
		if c.cur.in.Kind != kv.Get {
			c.leader = from
		}
		c.cur = nil
		c.w.after(c.w.think(), c.next)
		return
	}
	switch {
	case r.Leader != 0:
		c.leader = r.Leader
	case r.Err == node.ErrNotLeader:
		c.leader = c.nextNode(from)
	}
	if c.retrying {
		return
	}
	c.retrying = true
	seq := c.seq
	c.w.after(10*time.Millisecond, func() {
		c.retrying = false
		if c.cur != nil && c.seq == seq {
			c.send()
		}
	})
}
