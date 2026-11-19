// Package node runs one replica: an etcd raft RawNode, its durable log, and
// the key-value state machine.
//
// RawNode is a pure state machine with no goroutines, clocks or I/O. The
// caller feeds it ticks, messages and proposals, and after each input this
// package drains Ready: persist, then send, then apply, then Advance. That
// makes a node easy to drive from a deterministic simulator.
package node

import (
	"encoding/binary"
	"fmt"
	"sort"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/builtbyshailendra/raft-kv/kv"
)

// Options configure a node. Ticks are the only clock Raft has.
type Options struct {
	ElectionTick  int // ticks without a leader before campaigning (randomized to [n, 2n))
	HeartbeatTick int // ticks between leader heartbeats

	// SnapshotEvery takes a snapshot after this many entries are applied
	// since the last one, then drops the log before it except KeepEntries,
	// which lets a follower that is slightly behind catch up from the log
	// instead of needing a whole snapshot. 0 disables snapshots.
	SnapshotEvery uint64
	KeepEntries   uint64
}

func (o Options) withDefaults() Options {
	if o.ElectionTick == 0 {
		o.ElectionTick = 10
	}
	if o.HeartbeatTick == 0 {
		o.HeartbeatTick = 1
	}
	return o
}

// Response answers one client request.
type Response struct {
	Client, Seq uint64
	OK          bool
	Value       string
	Err         string // ErrNotLeader or ErrRetry when OK is false
	Leader      uint64 // hint with ErrNotLeader; 0 if unknown
}

const (
	ErrNotLeader = "not leader"
	ErrRetry     = "retry" // leadership changed while the request was in flight
)

// EventKind tells an Env what happened inside a node.
type EventKind int

const (
	BecameLeader     EventKind = iota + 1 // Term
	Applied                               // Index, Term, Data (nil for an empty entry)
	SnapshotTaken                         // Index
	SnapshotRestored                      // Index: state replaced by a snapshot from the leader
)

// Event is reported to Env.Observe so a harness can check invariants.
type Event struct {
	Kind  EventKind
	Node  uint64
	Term  uint64
	Index uint64
	Data  []byte
}

// Env connects a node to the world: a simulator or a real transport.
type Env interface {
	// Send delivers a Raft message to another node. It is called only after
	// the entries and HardState the message depends on are persisted.
	Send(m *pb.Message)
	// Reply answers a client.
	Reply(from uint64, r Response)
	// Observe reports an event.
	Observe(e Event)
}

// NewDisk returns durable storage for a brand-new cluster member. The
// membership lives in an empty initial snapshot, so the log starts at 1.
// In the simulator this object is "the disk": it survives a crash, while the
// Node (RawNode, state machine, pending requests) does not.
func NewDisk(peers []uint64) *raft.MemoryStorage {
	ms := raft.NewMemoryStorage()
	err := ms.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{ConfState: &pb.ConfState{Voters: peers}}})
	if err != nil {
		panic(err)
	}
	return ms
}

type writeKey struct{ client, seq uint64 }

type pendingRead struct {
	cmd   kv.Command
	tick  uint64 // when the read was asked for
	index uint64 // the read index, once Raft has confirmed it
}

// Node is one running replica. All methods must be called from one goroutine.
type Node struct {
	id    uint64
	opts  Options
	env   Env
	disk  *raft.MemoryStorage
	rn    *raft.RawNode
	store *kv.Store
	conf  *pb.ConfState

	lead  uint64
	state raft.StateType
	ticks uint64
	inc   uint64 // which start of this node this is; part of read IDs

	snapIndex uint64 // index of the latest snapshot on disk

	writes   map[writeKey]kv.Command // proposed here, waiting to be applied
	reads    map[uint64]*pendingRead // asked Raft for a read index, no answer yet
	waiting  []*pendingRead          // have a read index, waiting for apply to reach it
	nextRead uint64
}

// Start creates a node over disk, recovering from whatever is on it: the
// state machine is restored from the latest snapshot and Raft replays the
// committed entries after it. incarnation must differ on every start of the
// same node (it keeps read request IDs unique across restarts).
func Start(id, incarnation uint64, disk *raft.MemoryStorage, env Env, opts Options) (*Node, error) {
	opts = opts.withDefaults()
	snap, err := disk.Snapshot()
	if err != nil {
		return nil, err
	}
	store := kv.New(true)
	if err := store.Restore(snap.GetData(), snap.GetMetadata().GetIndex()); err != nil {
		return nil, err
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID:              id,
		ElectionTick:    opts.ElectionTick,
		HeartbeatTick:   opts.HeartbeatTick,
		Storage:         disk,
		Applied:         snap.GetMetadata().GetIndex(),
		MaxSizePerMsg:   64 << 10,
		MaxInflightMsgs: 64,
		// CheckQuorum: a leader that can't hear from a quorum steps down.
		// PreVote: a node rejoining after a partition doesn't disrupt the
		// leader by forcing a new term it can't win.
		CheckQuorum: true,
		PreVote:     true,
		// Clients must send writes to the leader; followers say who it is.
		DisableProposalForwarding: true,
		Logger:                    quiet{},
	})
	if err != nil {
		return nil, err
	}
	return &Node{
		id: id, inc: incarnation, opts: opts, env: env, disk: disk, rn: rn, store: store,
		conf:      snap.GetMetadata().GetConfState(),
		snapIndex: snap.GetMetadata().GetIndex(),
		writes:    map[writeKey]kv.Command{},
		reads:     map[uint64]*pendingRead{},
	}, nil
}

// ID is the node's Raft ID.
func (n *Node) ID() uint64 { return n.id }

// IsLeader says whether this node believes it is the leader. An old leader
// cut off from the others can believe this for a while.
func (n *Node) IsLeader() bool { return n.state == raft.StateLeader }

// Lead is the leader this node knows of (0 if none).
func (n *Node) Lead() uint64 { return n.lead }

// Term is the node's current term.
func (n *Node) Term() uint64 { return n.rn.BasicStatus().GetTerm() }

// Applied is the index of the last entry applied to the state machine.
func (n *Node) Applied() uint64 { return n.store.Applied() }

// LogLen is the number of entries on disk (after compaction).
func (n *Node) LogLen() uint64 {
	first, _ := n.disk.FirstIndex()
	last, _ := n.disk.LastIndex()
	if last < first {
		return 0
	}
	return last - first + 1
}

// SnapshotIndex is the index of the latest snapshot on disk.
func (n *Node) SnapshotIndex() uint64 { return n.snapIndex }

// ReportSnapshot tells Raft whether a snapshot this node sent arrived. Raft
// stops sending to a follower while its snapshot is in flight, so a lost
// snapshot must be reported or that follower is stuck.
func (n *Node) ReportSnapshot(to uint64, ok bool) {
	status := raft.SnapshotFinish
	if !ok {
		status = raft.SnapshotFailure
	}
	n.rn.ReportSnapshot(to, status)
	n.process()
}

// Hash summarizes the state machine for comparing replicas.
func (n *Node) Hash() uint64 { return n.store.Hash() }

// Read returns a key from local state, with no consistency guarantee.
func (n *Node) Read(key string) string { return n.store.Get(key) }

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() {
	n.ticks++
	n.rn.Tick()
	n.expireReads()
	n.process()
}

// Step hands the node a message from another node.
func (n *Node) Step(m *pb.Message) {
	// Errors here are messages raft chooses to ignore (for example from a
	// node it doesn't know); a real transport would drop them too.
	_ = n.rn.Step(m)
	n.process()
}

// Campaign makes the node start an election now (used by tests).
func (n *Node) Campaign() {
	_ = n.rn.Campaign()
	n.process()
}

// Handle serves a client request. The answer arrives later through Env.Reply.
func (n *Node) Handle(c kv.Command) {
	if c.Kind == kv.Get {
		n.read(c)
		return
	}
	if n.state != raft.StateLeader {
		n.reply(c, Response{Err: ErrNotLeader, Leader: n.lead})
		return
	}
	if err := n.rn.Propose(kv.Encode(c)); err != nil {
		n.reply(c, Response{Err: ErrRetry})
		return
	}
	// Answer only once the entry is committed and applied. Answering now
	// would acknowledge a write that a new leader may later overwrite.
	n.writes[writeKey{c.Client, c.Seq}] = c
	n.process()
}

// read serves a Get with ReadIndex. Any node can do it: a follower asks the
// leader for the current commit index; the leader confirms it is still
// leader with a round of heartbeats to a quorum (raft's ReadOnlySafe) before
// answering. The read is then served once this node has applied up to that
// index, so it reflects every write acknowledged before the read began.
func (n *Node) read(c kv.Command) {
	if n.lead == raft.None {
		n.reply(c, Response{Err: ErrNotLeader})
		return
	}
	n.nextRead++
	ctx := make([]byte, 24)
	binary.BigEndian.PutUint64(ctx, n.id)
	binary.BigEndian.PutUint64(ctx[8:], n.inc)
	binary.BigEndian.PutUint64(ctx[16:], n.nextRead)
	n.reads[n.nextRead] = &pendingRead{cmd: c, tick: n.ticks}
	n.rn.ReadIndex(ctx)
	n.process()
}

func (n *Node) readID(ctx []byte) (uint64, bool) {
	if len(ctx) != 24 || binary.BigEndian.Uint64(ctx) != n.id || binary.BigEndian.Uint64(ctx[8:]) != n.inc {
		return 0, false
	}
	return binary.BigEndian.Uint64(ctx[16:]), true
}

// A read index request can vanish (sent to a leader that just lost its
// role). Give up on it after a few election timeouts; the client retries.
func (n *Node) expireReads() {
	limit := uint64(3 * n.opts.ElectionTick)
	var ids []uint64
	for id, r := range n.reads {
		if n.ticks-r.tick > limit {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		n.reply(n.reads[id].cmd, Response{Err: ErrRetry})
		delete(n.reads, id)
	}
}

func (n *Node) serveReads() {
	kept := n.waiting[:0]
	for _, r := range n.waiting {
		if r.index <= n.store.Applied() {
			n.reply(r.cmd, Response{OK: true, Value: n.store.Get(r.cmd.Key)})
		} else {
			kept = append(kept, r)
		}
	}
	n.waiting = kept
}

func (n *Node) reply(c kv.Command, r Response) {
	r.Client, r.Seq = c.Client, c.Seq
	n.env.Reply(n.id, r)
}

// process drains Ready in the order Raft requires.
func (n *Node) process() {
	for n.rn.HasReady() {
		rd := n.rn.Ready()
		if rd.SoftState != nil {
			n.softState(*rd.SoftState)
		}
		// 1. Persist: snapshot, entries, HardState (term, vote, commit).
		if !raft.IsEmptySnap(rd.Snapshot) {
			must(n.disk.ApplySnapshot(rd.Snapshot))
		}
		must(n.disk.Append(rd.Entries))
		if rd.HardState != nil {
			must(n.disk.SetHardState(proto.Clone(rd.HardState).(*pb.HardState)))
		}
		// 2. Send, now that what the messages promise is on disk.
		for _, m := range rd.Messages {
			n.env.Send(m)
		}
		// 3. Apply: first a snapshot from the leader (it replaces the state
		// machine), then committed entries.
		if !raft.IsEmptySnap(rd.Snapshot) {
			i := rd.Snapshot.GetMetadata().GetIndex()
			if err := n.store.Restore(rd.Snapshot.GetData(), i); err != nil {
				panic(fmt.Sprintf("node %d: %v", n.id, err))
			}
			n.conf, n.snapIndex = rd.Snapshot.GetMetadata().GetConfState(), i
			n.env.Observe(Event{Kind: SnapshotRestored, Node: n.id, Index: i})
		}
		for _, e := range rd.CommittedEntries {
			n.apply(e)
		}
		// 4. Reads whose index Raft confirmed can be served once applied.
		for _, rs := range rd.ReadStates {
			if id, ok := n.readID(rs.RequestCtx); ok {
				if r, ok := n.reads[id]; ok {
					delete(n.reads, id)
					r.index = rs.Index
					n.waiting = append(n.waiting, r)
				}
			}
		}
		n.serveReads()
		n.rn.Advance(rd)
		n.maybeSnapshot()
	}
}

// maybeSnapshot saves the state machine at the applied index and compacts
// the log behind it.
func (n *Node) maybeSnapshot() {
	every := n.opts.SnapshotEvery
	applied := n.store.Applied()
	if every == 0 || applied-n.snapIndex < every {
		return
	}
	data := n.store.Snapshot(true)
	if _, err := n.disk.CreateSnapshot(applied, n.conf, data); err != nil {
		panic(fmt.Sprintf("node %d: snapshot at %d: %v", n.id, applied, err))
	}
	n.snapIndex = applied
	if applied > n.opts.KeepEntries {
		compact := applied - n.opts.KeepEntries
		if first, _ := n.disk.FirstIndex(); compact >= first {
			must(n.disk.Compact(compact))
		}
	}
	n.env.Observe(Event{Kind: SnapshotTaken, Node: n.id, Index: applied})
}

func (n *Node) softState(ss raft.SoftState) {
	wasLeader := n.state == raft.StateLeader
	leadChanged := ss.Lead != n.lead
	n.lead, n.state = ss.Lead, ss.RaftState
	if n.state == raft.StateLeader && !wasLeader {
		n.env.Observe(Event{Kind: BecameLeader, Node: n.id, Term: n.Term()})
	}
	if wasLeader && n.state != raft.StateLeader {
		// Proposals still in flight may or may not commit. Tell the clients
		// to retry; the session table makes a retry of a write that did
		// commit harmless.
		n.failWrites()
	}
	if leadChanged {
		// Unconfirmed read index requests went to a leader that is gone.
		// Reads that already have an index stay valid and keep waiting.
		n.failReads()
	}
}

func (n *Node) failReads() {
	ids := make([]uint64, 0, len(n.reads))
	for id := range n.reads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		n.reply(n.reads[id].cmd, Response{Err: ErrRetry})
		delete(n.reads, id)
	}
}

func (n *Node) failWrites() {
	keys := make([]writeKey, 0, len(n.writes))
	for k := range n.writes {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { // deterministic order for the simulator
		if keys[i].client != keys[j].client {
			return keys[i].client < keys[j].client
		}
		return keys[i].seq < keys[j].seq
	})
	for _, k := range keys {
		n.reply(n.writes[k], Response{Err: ErrRetry})
		delete(n.writes, k)
	}
}

func (n *Node) apply(e *pb.Entry) {
	i := e.GetIndex()
	switch {
	case e.GetType() == pb.EntryNormal && len(e.GetData()) > 0:
		c, err := kv.Decode(e.GetData())
		if err != nil {
			panic(fmt.Sprintf("node %d: bad entry %d: %v", n.id, i, err))
		}
		n.store.Apply(i, c)
		k := writeKey{c.Client, c.Seq}
		if w, ok := n.writes[k]; ok {
			delete(n.writes, k)
			n.reply(w, Response{OK: true})
		}
	case e.GetType() == pb.EntryConfChange:
		var cc pb.ConfChange
		must(proto.Unmarshal(e.GetData(), &cc))
		n.conf = n.rn.ApplyConfChange(&cc)
		n.store.Skip(i)
	case e.GetType() == pb.EntryConfChangeV2:
		var cc pb.ConfChangeV2
		must(proto.Unmarshal(e.GetData(), &cc))
		n.conf = n.rn.ApplyConfChange(&cc)
		n.store.Skip(i)
	default: // the empty entry a new leader commits to learn the commit index
		n.store.Skip(i)
	}
	n.env.Observe(Event{Kind: Applied, Node: n.id, Term: e.GetTerm(), Index: i, Data: e.GetData()})
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// quiet discards raft's logging but keeps its invariant failures fatal.
type quiet struct{}

func (quiet) Debug(...any)              {}
func (quiet) Debugf(string, ...any)     {}
func (quiet) Error(...any)              {}
func (quiet) Errorf(string, ...any)     {}
func (quiet) Info(...any)               {}
func (quiet) Infof(string, ...any)      {}
func (quiet) Warning(...any)            {}
func (quiet) Warningf(string, ...any)   {}
func (quiet) Fatal(v ...any)            { panic(fmt.Sprint(v...)) }
func (quiet) Fatalf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
func (quiet) Panic(v ...any)            { panic(fmt.Sprint(v...)) }
func (quiet) Panicf(f string, v ...any) { panic(fmt.Sprintf(f, v...)) }
