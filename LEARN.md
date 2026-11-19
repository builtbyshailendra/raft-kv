# Learning notes: raft-kv

## 1. Raft in five minutes

A few machines keep one copy of a log in agreement, so that a state machine built from the log survives the loss of a minority of them.

- **Terms.** Time is divided into numbered terms. Each term has at most one leader. A node that hears a higher term than its own adopts it immediately and stops leading.
- **Election.** A follower that hears nothing from a leader for an *election timeout* (randomized, here 10–20 ticks of 10 ms) becomes a candidate, increments the term and asks for votes. A majority of votes makes it leader. Each node votes once per term, and the vote is written to disk before it's sent.
- **Replication.** Clients send commands to the leader. It appends them to its log and sends them to followers (`MsgApp`). Once a majority has an entry on disk, the leader marks it **committed** and tells followers the new commit index.
- **Applying.** Every node applies committed entries to its state machine in log order. Same commands, same order, same state.
- **Why a majority:** any two majorities overlap in at least one node, so a new leader always includes every committed entry (a candidate whose log is behind can't win votes).

The safety properties this project checks directly, on every event:

| Property | Meaning | Where |
|---|---|---|
| Election Safety | at most one leader per term | `World.observe`, `BecameLeader` |
| State Machine Safety | no two nodes apply different entries at the same index | `World.observe`, `Applied` |
| Convergence | after healing, every replica has the same state | `checkConvergence` |

Two settings in `node.Start` matter a lot:
- **CheckQuorum:** a leader that hasn't heard from a quorum for an election timeout steps down. Without it, a leader cut off by a partition believes it leads forever.
- **PreVote:** a node rejoining after a partition first asks "would you vote for me?" without raising its term, so it can't force a healthy leader to step down.

## 2. etcd raft is a state machine, not a server

`RawNode` has no goroutines, sockets, disk or clock. You feed it inputs and it tells you what to do:

| Input | Call |
|---|---|
| time passing | `Tick()` |
| a message from another node | `Step(msg)` |
| a client command | `Propose(data)` |
| a linearizable read | `ReadIndex(ctx)` |

After any input, `Ready()` returns what must happen next, **in this order** (`node.process`):
1. **Persist** the snapshot, new entries and HardState (term, vote, commit). On a real system that's an fsync'd write-ahead log.
2. **Send** messages. Only now: a vote or an "I have entry 7" acknowledgment must never be sent before it's on disk, or a crash could make a node vote twice in one term or forget an entry it promised to keep.
3. **Apply** a snapshot from the leader, then committed entries, to the state machine.
4. **Answer reads** whose read index has been applied.
5. `Advance()`.

Because of this design, the same code runs in the simulator and could run in production; only the `Env` (send, reply) changes.

## 3. Linearizability, and how Porcupine checks it

A key-value store is **linearizable** if every operation appears to take effect at one instant between its call and its return, and the result is what a single copy would give. In practice:
- A read sees every write that was **acknowledged before the read began**.
- Once any read has seen a new value, no later read sees an older one.
- Operations that overlap in time may be ordered either way.

Porcupine takes the history (each operation's call time, return time, input and output) and searches for such an order. If none exists, it says `Illegal`, and the history is a proof of a bug. Two things make it fast here:
- **Partitioning by key:** operations on different keys can't conflict, so each key is checked on its own.
- **Unique values:** every put and append writes a value no one else writes, so an append applied twice is visible (`"1;1;"`).

An operation that never got an answer is a problem: it may or may not have happened. Leaving it out could make a read that saw it look impossible. This project keeps unanswered writes in the history with a return time at the end of the run ("happened at some point after it was sent").

## 4. Exactly-once writes: sessions

Clients retry. A request can be lost, or its answer can be lost, or the leader can change before answering. If the client sends the same append again and both copies commit, the value is appended twice.

The fix (`kv.Store`): each client has an ID and numbers its operations 1, 2, 3. The state machine remembers the highest number applied per client. A command with a number at or below that is a duplicate: it's acknowledged but not applied. This works because a client has one operation in flight at a time.

Two mistakes this project plants and catches:
- **No session table:** retries apply twice (caught in 101 of 200 seeds).
- **Sessions left out of snapshots:** a node restored from a snapshot forgets which retries it has seen and applies an old one again. Its state diverges from the others; the end-of-run comparison caught this in 23 seeds and Porcupine in 9.

## 5. Reads: the hard part

Writes go through the log, so they're ordered. Reads are tempting to answer locally, and that's where bugs hide.

| Read mode | How | Safe? |
|---|---|---|
| **ReadIndex** (raft `ReadOnlySafe`) | leader records its commit index, confirms it's still leader with a heartbeat round to a quorum, then serves once applied up to that index | yes, under every fault here |
| **Lease** (raft `ReadOnlyLeaseBased`) | leader skips the heartbeat round, trusting that no one else can have been elected yet | no: stale in 41 of 200 targeted runs with equal clocks, more with a slow clock |
| **Leader-local** | leader answers from its own state | no: an old leader that doesn't know it's old, or a new leader that doesn't yet know what's committed |
| **Any-local** | any node answers from its own state | no: followers lag |

**Why two leaders overlap.** The leader checks for a quorum only once per election timeout and counts any reply received since the previous check. A reply just after one check keeps it leader through the next, so it can believe it leads for almost two election timeouts after last hearing from a quorum. Followers may elect someone else after one. Measured: in 260 of 500 cut-off-the-leader runs the new leader was elected first, overlapping by up to 89 ms.

**Why a new leader can be stale.** The old leader commits `x=2`, acknowledges it, and is cut off before telling anyone the new commit index. The new leader has the entry but doesn't know it's committed. raft makes `ReadIndex` wait until the new leader commits an entry of its own term, which commits everything before it.

**Follower reads** are safe with ReadIndex: the follower asks the leader for the commit index (the leader still does its quorum check), then waits until it has applied that far.

## 6. Deterministic simulation testing

Distributed bugs need rare interleavings. Real clusters and real time make them rare *and* unrepeatable. This simulator makes them repeatable:
- **One goroutine, one event queue, a virtual clock.** Ticks, message deliveries, client timeouts and faults are all events, run in time order (ties broken by insertion order).
- **One seeded random generator** decides delays, losses, fault choices and client operations.
- **Raft's own randomness** (election timeouts) comes from `crypto/rand`. The simulator swaps `crypto/rand.Reader` for a seeded ChaCha8 stream while a run is alive. That's acceptable only in a simulator; inside tests Go also offers `testing/cryptotest.SetGlobalRandom`.
- **Proof:** a test runs the same seed twice and compares a hash of every delivered message.

The payoff: `go run ./cmd/raftkv-sim -read leader-local -seed 380004` replays the exact failure, every time, in 15 ms. The same idea is behind FoundationDB's simulator, TigerBeetle's VOPR and Antithesis.

The limit: random search only finds what the workload makes likely. Leader-local and lease reads passed 200 seeds until clients retried after 50 ms instead of 300 ms. Targeted tests for known mechanisms complement random search.

## 7. Snapshots and compaction

Without them the log grows forever (1,001 entries after 1,000 writes; 120 with snapshots every 100, keeping 20).
- Every `SnapshotEvery` applied entries: serialize the state machine **with the session table**, `CreateSnapshot`, then `Compact` the log except the last `KeepEntries`.
- A follower too far behind gets `MsgSnap` with the leader's snapshot and replaces its state.
- The transport must call `ReportSnapshot` with success or failure. Raft stops sending to a follower while its snapshot is in flight; a lost snapshot that's never reported leaves that follower stuck (planted: caught 10 times by the liveness check).

## 8. What broke while building

1. **A one-seed conclusion.** I concluded that the old leader always steps down before a new one is elected, from one seed (93 ms vs 111 ms). Measured over 500 seeds, it's the other way round 52% of the time.
2. **Random testing missed two real bugs** (leader-local and lease reads without pauses): 0 of 200 seeds, because clients took longer to find the new leader than the overlap lasted.
3. **Lease reads failed with equal clocks**, which raft's documentation doesn't warn about (it mentions clock drift).
4. **My own test helper was wrong:** it used 0 to mean "not set", so a just-restarted node at applied index 0 was skipped and convergence was declared too early.
5. **A failure I couldn't explain** until each history entry recorded which node answered it; then seed 380004 read like a story (paused leader, resumes, answers three reads from memory).

## 9. Explain it without notes

1. What does `Ready` contain, and why must entries be persisted before messages are sent?
2. Why does a leader need a majority to commit, and why does that make a new leader safe?
3. What do CheckQuorum and PreVote each prevent?
4. Define linearizability with an example of a history that breaks it.
5. How does Porcupine check a history, and why partition by key?
6. How do client sessions make retries safe? Why must they be in the snapshot?
7. Walk through a ReadIndex read on a follower.
8. Why can two nodes both believe they're the leader at the same time? How long can it last here?
9. Why is a lease read unsafe even with equal clocks in etcd raft's implementation?
10. How is the simulator deterministic, including Raft's randomized election timeouts?
11. What does a planted bug prove that 1,250 passing seeds don't?
12. Why did random testing miss the leader-local bug, and what fixed that?

## 10. Your turn

1. **Membership change.** Add a fourth node as a learner (`ConfChangeV2`, `ConfChangeAddLearnerNode`), let it catch up, then promote it. Add "add node" and "remove node" to the fault scheduler and keep every check passing.
2. **A disk that lies.** Make a crash lose the last few entries that were "written but not fsync'd". What breaks if a node sends its vote before fsync? Write the test that catches it.
3. **Compare-and-swap.** Add `CAS(key, old, new)` to the state machine and the Porcupine model, and use it in the workload.
4. **Leave the simulator.** Run three processes with etcd's `wal` package and an HTTP transport, and drive them with the same clients. Which guarantees can you still check, and how?
5. **Fix the lease.** Make lease reads safe: for example, a leader serves a lease read only if a quorum acknowledged it within the last (election timeout − maximum drift) of its own clock. Show the 41 stale runs drop to 0.
