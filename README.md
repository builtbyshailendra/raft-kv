# raft-kv

A replicated key-value store on etcd's Raft library, tested inside a deterministic simulator: one goroutine, a virtual clock, one seeded random generator. The same seed replays the same run event for event.

Built pairing with Claude Code, as a learning project. Nothing here has run outside this laptop.

## Build log

### Step 1: one node, the Ready loop, a simulator

- `kv/`: the state machine. A map plus a session table (client → last applied sequence number), so a retried write applies once. Snapshots include the session table.
- `node/`: wraps etcd raft's `RawNode` (v3.7). After every input (tick, message, proposal) it drains `Ready` in the order Raft requires: persist snapshot, entries and HardState; then send messages; then apply committed entries; then `Advance`. Writes are answered only after they are applied, never on propose.
- `sim/`: the world. A priority queue of events on a virtual clock; each node ticks every 10 ms from a random phase; messages are protobuf-encoded and decoded on the way, with 1–11 ms delay, which reorders them. Two Raft safety properties are checked on every event: at most one leader per term, and every node applies the same entry at a given index.
- **Determinism:** etcd raft draws its randomized election timeout from `crypto/rand`. The simulator swaps `crypto/rand.Reader` for a seeded ChaCha8 stream while a world exists (simulation only), so election timing is part of the seed too. A test runs seed 42 twice and compares a hash of every delivered message.

Tests: a leader is elected (3 and 5 nodes; about 150–175 ms of virtual time), 60 writes reach every replica with identical state, a follower answers a write with "not leader" and names the leader, and the same seed gives the same trace.

### Step 2: faults, and what an old leader can and can't do

The simulator can now break things:

| Fault | How | Models |
|---|---|---|
| Partition | nodes only reach nodes in their own group; messages in flight across the cut are dropped | a network split |
| Crash and restart | the `Node` (RawNode, state machine, pending requests) is thrown away; the disk (`MemoryStorage`: log, HardState, snapshot) is kept; restart rebuilds from disk | a process crash with fsync'd storage |
| Pause and resume | the node misses its ticks; what arrives waits in an inbox and is processed in order on resume | a long GC pause, a stalled VM |
| Loss, duplication | any message may be lost; Raft messages may arrive twice | a bad network |

Clients sit outside the cluster: partitions between nodes don't cut them off, but loss does.

Tests:
- **An isolated leader can't commit.** The leader is cut off and a write is sent to it. It still believes it leads, so it appends the write, but no quorum can hear it. The majority elects a new leader in a higher term, the new leader commits a different value, and after healing every node holds the new value: the old leader's uncommitted entry was overwritten. The old leader never acknowledged its write.
- **Acknowledged writes survive leader crashes:** 6 rounds of 10 writes, crashing the leader after each round and restarting the previous one.
- **The whole cluster restarts from disk:** all three nodes crash; on restart each rebuilds its state by replaying committed entries.
- **20% loss and 10% duplication:** 50 appends retried until acknowledged; the result has each value exactly once, thanks to the session table.
- **A paused leader wakes up believing it leads.** During a 2-second pause the others elect a new leader and commit `x=new`; the paused node still thinks it leads and still has `x=old`. Only processing the new term's heartbeats from its inbox corrects it.

What I learned here: with CheckQuorum on and equal clocks, the isolated leader stepped down (93 ms after the cut) before the majority elected a new one (111 ms). So a leader answering reads from its own memory would rarely be caught by partitions alone. A pause is what opens the window: the paused node doesn't tick, so it can't notice anything. That's why the pause fault exists.
