// Package sim runs a whole cluster inside one goroutine on a virtual clock.
//
// Everything that happens (a tick, a message arriving, a client retrying, a
// fault) is an event in one priority queue, and every random choice comes
// from one seeded generator. The same seed therefore produces the same run,
// event for event, which turns a failure found by chance into a failure that
// can be replayed and debugged.
package sim

import (
	"container/heap"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/builtbyshailendra/raft-kv/kv"
	"github.com/builtbyshailendra/raft-kv/node"
)

// Config describes a simulated cluster. Zero values get defaults.
type Config struct {
	Seed  uint64
	Nodes int // default 3

	Tick      time.Duration // virtual time per Raft tick, default 10ms
	NetDelay  time.Duration // minimum one-way message delay, default 1ms
	NetJitter time.Duration // extra random delay; it reorders messages. Default 10ms

	Node node.Options
}

func (c Config) withDefaults() Config {
	if c.Nodes == 0 {
		c.Nodes = 3
	}
	if c.Tick == 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.NetDelay == 0 {
		c.NetDelay = time.Millisecond
	}
	if c.NetJitter == 0 {
		c.NetJitter = 10 * time.Millisecond
	}
	return c
}

// Stats count what happened in a run.
type Stats struct {
	Messages  int // Raft messages sent
	Delivered int
	Dropped   int
	Bytes     int // encoded size of Raft messages sent
	Elections int // times a node became leader

	Partitions int
	Crashes    int
	Restarts   int
	Pauses     int
}

// Call is a request sent with World.Request; it fills in when answered.
type Call struct {
	Done bool
	Resp node.Response
	At   time.Duration
}

type callKey struct{ client, seq uint64 }

type server struct {
	id   uint64
	disk *raft.MemoryStorage // survives a crash
	n    *node.Node          // nil while crashed
	// Nodes talk only to nodes in the same group. All 0 = no partition.
	group int
	// A paused node (a long GC pause, a stalled VM) neither ticks nor
	// handles anything; what arrives meanwhile waits in its inbox.
	paused bool
	inbox  []func(*node.Node)
}

// Only one World may exist at a time: Raft draws its randomized election
// timeouts from crypto/rand, and a World swaps crypto/rand.Reader for a
// seeded stream while it is alive. That is a simulation-only trick; it
// makes election timing part of the seed.
var worldMu sync.Mutex

// World is a simulated cluster.
type World struct {
	cfg       Config
	now       time.Duration
	q         queue
	seq       uint64
	rng       *rand.Rand
	oldReader io.Reader
	closed    bool

	servers []*server
	calls   map[callKey]*Call
	trace   hash.Hash64
	loss    float64 // probability that any message is lost
	dup     float64 // probability that a Raft message is delivered twice
	notes   []Note  // faults, for the timeline

	Stats Stats

	leaders    map[uint64]uint64 // term -> the node that led it
	applied    map[uint64]uint64 // index -> hash of the entry applied there
	violations []string
}

// New builds a cluster with every node up. Call Close when done.
func New(cfg Config) *World {
	cfg = cfg.withDefaults()
	worldMu.Lock()
	w := &World{
		cfg:     cfg,
		rng:     rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15)),
		calls:   map[callKey]*Call{},
		trace:   fnv.New64a(),
		leaders: map[uint64]uint64{},
		applied: map[uint64]uint64{},
	}
	var key [32]byte
	binary.LittleEndian.PutUint64(key[:], cfg.Seed)
	w.oldReader, crand.Reader = crand.Reader, rand.NewChaCha8(key)

	peers := make([]uint64, cfg.Nodes)
	for i := range peers {
		peers[i] = uint64(i + 1)
	}
	for _, id := range peers {
		s := &server{id: id, disk: node.NewDisk(peers)}
		w.servers = append(w.servers, s)
		w.start(s)
	}
	return w
}

// Close restores crypto/rand and releases the World.
func (w *World) Close() {
	if w.closed {
		return
	}
	w.closed = true
	crand.Reader = w.oldReader
	worldMu.Unlock()
}

// Now is the virtual time since the start.
func (w *World) Now() time.Duration { return w.now }

// Node returns a running node, or nil if it is down.
func (w *World) Node(id uint64) *node.Node { return w.server(id).n }

// IDs lists the node IDs.
func (w *World) IDs() []uint64 {
	ids := make([]uint64, len(w.servers))
	for i, s := range w.servers {
		ids[i] = s.id
	}
	return ids
}

// Leader returns the running node that believes it leads the highest term,
// or 0. During a partition an old leader may still believe it leads a lower
// term; this returns the newest.
func (w *World) Leader() uint64 {
	var best, term uint64
	for _, s := range w.servers {
		if s.n != nil && s.n.IsLeader() && s.n.Term() > term {
			best, term = s.id, s.n.Term()
		}
	}
	return best
}

// Violations lists safety invariants broken so far.
func (w *World) Violations() []string { return w.violations }

// TraceHash fingerprints every delivered message and answered request so
// far. Two runs with the same seed must produce the same hash.
func (w *World) TraceHash() uint64 { return w.trace.Sum64() }

// RunFor advances virtual time by d.
func (w *World) RunFor(d time.Duration) { w.runUntil(w.now+d, nil) }

// RunUntil advances until cond holds (checked after every event) or limit
// virtual time passes. It reports whether cond held.
func (w *World) RunUntil(cond func() bool, limit time.Duration) bool {
	return w.runUntil(w.now+limit, cond)
}

func (w *World) runUntil(end time.Duration, cond func() bool) bool {
	if cond != nil && cond() {
		return true
	}
	for len(w.q) > 0 && w.q[0].at <= end {
		e := heap.Pop(&w.q).(*event)
		w.now = e.at
		e.fn()
		if cond != nil && cond() {
			return true
		}
	}
	w.now = end
	return false
}

// after schedules fn d from now. Events at the same time run in the order
// they were scheduled.
func (w *World) after(d time.Duration, fn func()) {
	w.seq++
	heap.Push(&w.q, &event{at: w.now + d, seq: w.seq, fn: fn})
}

func (w *World) server(id uint64) *server { return w.servers[id-1] }

func (w *World) start(s *server) {
	n, err := node.Start(s.id, s.disk, &env{w: w, from: s.id}, w.cfg.Node)
	if err != nil {
		panic(err)
	}
	s.n = n
	w.startTicks(s, n)
}

// Each node ticks on its own schedule, starting at a random phase so the
// nodes aren't in lockstep.
func (w *World) startTicks(s *server, n *node.Node) {
	period := w.cfg.Tick
	var tick func()
	tick = func() {
		if s.n != n { // crashed or restarted since
			return
		}
		if !s.paused { // a paused process misses its ticks; it doesn't catch up
			n.Tick()
		}
		w.after(period, tick)
	}
	w.after(1+time.Duration(w.rng.Int64N(int64(period))), tick)
}

func (w *World) delay() time.Duration {
	return w.cfg.NetDelay + time.Duration(w.rng.Int64N(int64(w.cfg.NetJitter)+1))
}

// sendRaft carries a Raft message. It is encoded and decoded like on a real
// wire, so nodes never share memory.
func (w *World) sendRaft(from uint64, m *pb.Message) {
	to := m.GetTo()
	data, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	w.Stats.Messages++
	w.Stats.Bytes += len(data)
	if !w.connected(from, to) || w.lost() {
		w.Stats.Dropped++
		return
	}
	copies := 1
	if w.dup > 0 && w.rng.Float64() < w.dup {
		copies = 2
	}
	for range copies {
		w.after(w.delay(), func() {
			// A partition that starts while a message is in flight drops it too.
			if !w.connected(from, to) {
				w.Stats.Dropped++
				return
			}
			w.deliver(w.server(to), func(n *node.Node) {
				msg := &pb.Message{}
				if err := proto.Unmarshal(data, msg); err != nil {
					panic(err)
				}
				w.Stats.Delivered++
				w.mix(uint64(w.now), from, to, uint64(msg.GetType()), msg.GetTerm(), msg.GetIndex())
				n.Step(msg)
			})
		})
	}
}

// deliver hands something to a node: dropped if it's down, queued if it's
// paused.
func (w *World) deliver(s *server, fn func(*node.Node)) {
	switch {
	case s.n == nil:
		w.Stats.Dropped++
	case s.paused:
		s.inbox = append(s.inbox, fn)
	default:
		fn(s.n)
	}
}

func (w *World) connected(a, b uint64) bool { return w.server(a).group == w.server(b).group }

func (w *World) lost() bool { return w.loss > 0 && w.rng.Float64() < w.loss }

// Request sends a client command straight to one node.
func (w *World) Request(to uint64, c kv.Command) *Call {
	call := &Call{}
	w.calls[callKey{c.Client, c.Seq}] = call
	if w.lost() {
		return call
	}
	w.after(w.delay(), func() {
		w.deliver(w.server(to), func(n *node.Node) { n.Handle(c) })
	})
	return call
}

// Clients sit outside the cluster: partitions between nodes don't cut them
// off, but message loss does.
func (w *World) reply(from uint64, r node.Response) {
	if w.lost() {
		return
	}
	w.after(w.delay(), func() {
		w.mix(uint64(w.now), from, r.Client, r.Seq, boolInt(r.OK))
		if call, ok := w.calls[callKey{r.Client, r.Seq}]; ok && !call.Done {
			call.Done, call.Resp, call.At = true, r, w.now
		}
	})
}

// observe checks Raft's safety properties as they happen.
func (w *World) observe(e node.Event) {
	switch e.Kind {
	case node.BecameLeader:
		w.Stats.Elections++
		// Election Safety: at most one leader per term.
		if prev, ok := w.leaders[e.Term]; ok && prev != e.Node {
			w.violate("two leaders in term %d: nodes %d and %d", e.Term, prev, e.Node)
		}
		w.leaders[e.Term] = e.Node
	case node.Applied:
		// State Machine Safety: every node applies the same entry at an index.
		h := fnv.New64a()
		_ = binary.Write(h, binary.LittleEndian, e.Term)
		h.Write(e.Data)
		sum := h.Sum64()
		if prev, ok := w.applied[e.Index]; ok && prev != sum {
			w.violate("node %d applied a different entry at index %d", e.Node, e.Index)
		}
		w.applied[e.Index] = sum
	}
}

func (w *World) violate(format string, args ...any) {
	w.violations = append(w.violations, fmt.Sprintf("t=%v: ", w.now)+fmt.Sprintf(format, args...))
}

func (w *World) mix(vals ...uint64) {
	var b [8]byte
	for _, v := range vals {
		binary.LittleEndian.PutUint64(b[:], v)
		w.trace.Write(b[:])
	}
}

func boolInt(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// env is what a node sees of the world.
type env struct {
	w    *World
	from uint64
}

func (e *env) Send(m *pb.Message)                 { e.w.sendRaft(e.from, m) }
func (e *env) Reply(from uint64, r node.Response) { e.w.reply(from, r) }
func (e *env) Observe(ev node.Event)              { e.w.observe(ev) }

type event struct {
	at  time.Duration
	seq uint64
	fn  func()
}

type queue []*event

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *queue) Pop() any {
	old := *q
	e := old[len(old)-1]
	*q = old[:len(old)-1]
	return e
}
