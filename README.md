# raft-kv

[![ci](https://github.com/builtbyshailendra/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/builtbyshailendra/raft-kv/actions/workflows/ci.yml)

A replicated key-value store on etcd's Raft library, tested inside a deterministic simulator: one goroutine, a virtual clock, one seeded random generator. The same seed replays the same run event for event.

Built pairing with Claude Code, as a learning project. Nothing here has run outside this laptop.

**What it shows:**
- 1,250 seeded runs with partitions, crashes, process pauses, message loss and duplication: 470,326 client operations, all **linearizable** by Porcupine, with Raft's safety properties checked on every event.
- The checks have teeth: every planted bug (acknowledging writes early, no session table, sessions missing from snapshots, local reads, lease reads, unreported snapshots) is caught, and the correct system never fails. Two of the bugs were caught only after clients were made to retry faster, or by a targeted test (step 5).
- A measured finding: when the leader is cut off, a new leader is elected before the old one steps down in 52% of runs, and etcd raft's lease reads go stale in 41 of 200 targeted runs **even with equal clock speeds**.

Learning notes and explain-it questions: [LEARN.md](LEARN.md). Interview brief: [`../briefs/raft-kv.md`](../briefs/raft-kv.md).

```text
clients (5, outside the cluster)        nemesis: partition / crash / pause / loss
   │ put, append, get (retry, same seq)       │
   ▼                                          ▼
 ┌──────────── sim: one goroutine, virtual clock, one seeded RNG ────────────┐
 │  node 1            node 2            node 3     protobuf-encoded messages,│
 │  RawNode           RawNode           RawNode    1–11 ms delay, reordered  │
 │  MemoryStorage ◄── the "disk": survives crash                             │
 │  kv.Store (data + sessions) ◄── memory: lost on crash                     │
 └───────────────────────────────────────────────────────────────────────────┘
   history of (call, return, input, output) ──► Porcupine: linearizable?
   every event ──► one leader per term? same entry at every index?
```

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

What I thought I learned here: with CheckQuorum on and equal clocks, the isolated leader stepped down (93 ms after the cut) before the majority elected a new one (111 ms), so only a pause could open a window where two nodes believe they lead. That's why the pause fault exists.

**Correction (step 5):** that was one seed. Measured over 500 seeds, a new leader is elected before the old one steps down in 260 of them (52%), by up to 89 ms. See step 5.

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

### Step 5: plant bugs and make sure they're caught

A checker that never fails proves nothing. `TestPlantedBugsAreCaught` turns on one known mistake at a time and runs 200 seeds against it (runs are deterministic, so these counts are stable):

| Planted bug | Expected | Seeds failing | Not linearizable | Safety check | Liveness |
|---|---|---|---|---|---|
| control: ReadIndex, no bugs, all faults | must pass | 0 / 200 | 0 | 0 | 0 |
| control: same, snapshots every 20 entries | must pass | 0 / 200 | 0 | 0 | 0 |
| control: same, clients time out after 50 ms | must pass | 0 / 200 | 0 | 0 | 0 |
| answer a write before it commits | must be caught | 138 / 200 | 138 | 0 | 0 |
| no session table | must be caught | 101 / 200 | 101 | 0 | 0 |
| snapshots leave out the session table | must be caught | 27 / 200 | 9 | 23 | 0 |
| leader never told whether a snapshot arrived | must be caught | 10 / 200 | 0 | 0 | 10 |
| any node answers reads from its own state | must be caught | 161 / 200 | 161 | 0 | 0 |
| leader answers reads from its own state | must be caught | 4 / 200 | 4 | 0 | 0 |
| leader answers reads from its own state, no pauses | measured | 0 / 200 | 0 | 0 | 0 |
| same, clients time out after 50 ms | must be caught | 2 / 200 | 2 | 0 | 0 |
| lease reads (raft `ReadOnlyLeaseBased`), all faults | must be caught | 1 / 200 | 1 | 0 | 0 |
| lease reads, no pauses | measured | 0 / 200 | 0 | 0 | 0 |
| lease reads, no pauses, clocks drift up to 50% | measured | 0 / 200 | 0 | 0 | 0 |
| lease reads, no pauses, clients time out after 50 ms | must be caught | 1 / 200 | 1 | 0 | 0 |

"Safety check" is the simulator's own checks: the session-table-in-snapshot bug makes replicas diverge, which the end-of-run comparison catches more often (23) than Porcupine does (9). "Liveness" is a follower stuck forever because its leader was never told a snapshot was lost.

**What the numbers taught me:**

1. **Two leaders at once is common, not rare.** Cutting off the leader in 500 seeds: a new leader was elected before the old one stepped down in 260 (52%), overlapping by up to 89 ms (`TestTwoLeadersOverlap`). The reason is in raft's code: the leader checks for a quorum only once per election timeout and counts any reply since the previous check, so it can keep believing it leads for nearly two election timeouts after last hearing from a quorum. Followers can elect a new leader after one.
2. **Random testing missed real bugs, and the reason was the workload.** Leader-local and lease reads were caught 0 times in 200 seeds without pauses, because clients waited 300 ms before trying another node, longer than the overlap. Nobody had found the new leader and written anything before the old one stepped down. With a 50 ms client timeout, both are caught.
3. **So I wrote targeted tests for the mechanisms:**
   - `TestNewLeaderReadingItsOwnStateIsStale`: the old leader commits and acknowledges `x=2`, then is cut off before telling anyone the new commit index. The new leader, reading its own state at once, returns `x=1`. ReadIndex returns `x=2`, because raft holds read-index requests until the new leader has committed an entry of its own term.
   - `TestLeaseReadsGoStale`: with the old leader's clock at half speed, a lease read returns a stale value after the new leader acknowledged a newer one. **With every clock at the same speed, lease reads were still stale in 41 of 200 runs**, because of the check-once-per-timeout behaviour above. raft's documentation warns only about clock drift. ReadIndex was stale in 0 of 200. (I haven't checked whether this is discussed upstream.)

**One failure, traced.** Leader-local reads, seed 380004 (`go run ./cmd/raftkv-sim -read leader-local -seed 380004`):

```
     1.877s  fault  partition [1] | [2 3]
     2.001s  leader 2 leads term 2
     3.487s  fault  pause 2
     3.736s  fault  partition [2] | [1 3]
     3.853s  leader 3 leads term 3
     4.906s  fault  resume 2
```

Clients 0, 1 and 4 had sent reads to node 2 while it was paused. When it resumed at 4.906 s it still believed it led term 2 and answered all three from memory before reading anything from term 3. Client 0 got `k0 = "4.78;4.79;4.80;"`, a value replaced by an acknowledged write around 4.3 s; node 3 had been serving the newer value since then. Porcupine reports the history as not linearizable; the visualization shows which node answered each operation.

Visualizations of failing runs, in `results/` (open in a browser; one row per client, rows for faults and leader changes, hover for the answering node):
- `leader-local-read-seed-380004.html`: the run above
- `lease-read-seed-410038.html`: lease reads, all faults

## Run it

```bash
go test ./...                                        # everything, about a minute
go test ./sim -run TestLinearizableUnderFaults -args -seeds 1000
go run ./cmd/raftkv-sim -seeds 200                   # the correct system
go run ./cmd/raftkv-sim -read leader-local -seeds 200 -from 380001
go run ./cmd/raftkv-sim -read leader-local -seed 380004 -viz out.html
go run ./cmd/raftkv-sim -bug no-sessions -seeds 50   # also: ack-before-commit, sessions-not-in-snapshot, no-snapshot-reports
```

## What it doesn't do

- **It never leaves the simulator.** No real network, disk or processes; `MemoryStorage` stands in for an fsync'd write-ahead log. A real deployment needs a WAL (etcd's `wal` package), a transport (gRPC or HTTP) and real timers.
- **Membership never changes.** No adding or removing nodes; the conf-change code path is there but unused.
- **The disk never lies.** A crash loses memory, never acknowledged writes; there's no torn write, lost fsync or corrupted entry.
- **Clients sit outside the partitions.** Every client can reach every node unless messages are lost.
- **Passing seeds are evidence, not proof.** The random runs missed two real bugs until the client timeout was shortened, so the targeted tests matter as much as the seed count.
- CI (badge above) was added when the project was published, 20 Nov 2026, after the build.
