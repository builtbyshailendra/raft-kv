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
