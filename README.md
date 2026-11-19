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

### Step 3: clients, linearizable reads, Porcupine

- **Clients** know only node IDs. Each runs one operation at a time: it sends to its guess of the leader, follows "not leader" hints, moves to the next node after a 300 ms timeout, and retries with the same sequence number. 5 clients on 3 keys, half reads, half writes (put or append, every value unique).
- **Reads use ReadIndex** (raft's `ReadOnlySafe`). Any node can serve one: a follower asks the leader for the commit index; the leader confirms it's still leader with a heartbeat round to a quorum; the read is answered once the node has applied up to that index. 30% of reads go to a random node to exercise follower reads.
- **The fault scheduler** picks a random action every 200 ms–1 s: partition (cutting off the leader half the time), heal, crash, restart, pause for 0.3–1.8 s, or set message loss to 0/10/20%. It never impairs more than a minority at once. After 5 s it heals everything and the run must finish.
- **Every run is checked three ways:** the simulator's safety checks (one leader per term; the same entry at every index; replicas identical at the end); liveness (every operation answered after healing); and **Porcupine** on the client history: is there one order of all operations, consistent with real time, that a single-copy key-value store could have produced? The model is partitioned by key, so each key is checked on its own.
- An operation that never got an answer is kept in the history as a write that may have happened at any time after it was sent, so a read that saw it isn't flagged by mistake.

Porcupine was first tested on hand-written histories: it rejects a stale read, a value that flips back, an append applied twice and a lost acknowledged write, and accepts a read overlapping a write that sees either value.

Results (`go test ./sim -run TestLinearizableUnderFaults -args -seeds 1000`):

| | 3 nodes | 5 nodes |
|---|---|---|
| Seeds | 1,000 | 250 |
| Operations checked | 376,701 (188,386 reads) | 93,625 (46,699 reads) |
| Partitions / crashes / pauses | 1,235 / 791 / 762 | 320 / 313 / 281 |
| Elections | 2,766 | 785 |
| Raft messages (dropped) | 4.88 M (451 k) | 2.55 M (198 k) |
| Time: simulation, Porcupine | 11.9 s (12 ms a run), 0.5 s | 5.5 s (22 ms a run), 0.1 s |
| Failing seeds | 0 | 0 |

That's 470,326 operations, all linearizable, in about 19 seconds. A passing checker means nothing until it is shown to fail on bugs: that's step 5.

### Step 4: snapshots and log compaction

- After `SnapshotEvery` applied entries a node saves its state machine (data **and** session table) with `CreateSnapshot` and compacts the log except the last `KeepEntries`, which lets a follower slightly behind catch up from the log.
- A follower too far behind gets the leader's snapshot (`MsgSnap`); it replaces its state machine and continues from the snapshot index.
- The simulated transport reports each snapshot's fate back to the leader (`ReportSnapshot`): raft stops sending to a follower while a snapshot is in flight, so a lost snapshot that's never reported leaves that follower stuck.
- A restart now recovers from the last snapshot on disk plus the log after it.

Tests:
- A node down while 300 writes happen comes back to a leader whose log starts at 290. It catches up through a snapshot and ends identical to the others.
- 1,000 writes: the longest log is 1,001 entries without snapshots and 120 with them (every 100, keep 20).
- The session table survives a snapshot and a whole-cluster restart: a very late retry of append #100 (applied long before the snapshot) is recognized and not applied again.
- 500 seeds with every fault on and aggressive snapshots (every 20 entries, keep 5): 188,419 operations, all linearizable. Leaders sent 749 snapshots and followers installed 715; the other 34 were lost in transit, reported as failed, and resent.

**What broke:** the first snapshot test failed with "node 2 state differs". The bug was in my test helper, not the store: it used 0 to mean "no applied index seen yet", so a node that had just restarted (applied index 0) was skipped and the helper declared convergence too early. Fixed to compare all running nodes; every earlier test that uses it was rerun and still passes.
