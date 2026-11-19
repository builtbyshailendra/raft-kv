// Package node runs one replica: an etcd raft RawNode, its durable log, and
// the key-value state machine.
//
// RawNode is a pure state machine with no goroutines, clocks or I/O. The
// caller feeds it ticks, messages and proposals, and after each input this
// package drains Ready: persist, then send, then apply, then Advance. That
// makes a node easy to drive from a deterministic simulator.
package node

import (
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
	BecameLeader EventKind = iota + 1 // Term
	Applied                           // Index, Term, Data (nil for an empty entry)
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

	writes map[writeKey]kv.Command // proposed here, waiting to be applied
}

// Start creates a node over disk, recovering from whatever is on it: the
// state machine is restored from the latest snapshot and Raft replays the
// committed entries after it.
func Start(id uint64, disk *raft.MemoryStorage, env Env, opts Options) (*Node, error) {
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
		id: id, opts: opts, env: env, disk: disk, rn: rn, store: store,
		conf:   snap.GetMetadata().GetConfState(),
		writes: map[writeKey]kv.Command{},
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

// Hash summarizes the state machine for comparing replicas.
func (n *Node) Hash() uint64 { return n.store.Hash() }

// Read returns a key from local state, with no consistency guarantee.
func (n *Node) Read(key string) string { return n.store.Get(key) }

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() {
	n.rn.Tick()
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
		n.reply(c, Response{Err: "reads not supported yet"})
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
		// 3. Apply committed entries to the state machine.
		for _, e := range rd.CommittedEntries {
			n.apply(e)
		}
		n.rn.Advance(rd)
	}
}

func (n *Node) softState(ss raft.SoftState) {
	wasLeader := n.state == raft.StateLeader
	n.lead, n.state = ss.Lead, ss.RaftState
	if n.state == raft.StateLeader && !wasLeader {
		n.env.Observe(Event{Kind: BecameLeader, Node: n.id, Term: n.Term()})
	}
	if wasLeader && n.state != raft.StateLeader {
		// Proposals still in flight may or may not commit. Tell the clients
		// to retry; the session table makes a retry of a write that did
		// commit harmless.
		n.failPending()
	}
}

func (n *Node) failPending() {
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
