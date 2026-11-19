package sim

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/check"
	"github.com/builtbyshailendra/raft-kv/kv"
	"github.com/builtbyshailendra/raft-kv/node"
)

var snapOpts = node.Options{SnapshotEvery: 50, KeepEntries: 10}

func TestLaggingNodeCatchesUpFromSnapshot(t *testing.T) {
	w := newWorld(t, Config{Seed: 21, Node: snapOpts})
	lead := waitLeader(t, w)
	behind := lead%3 + 1
	w.Crash(behind)
	for i := 1; i <= 300; i++ {
		write(t, w, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Put, Key: fmt.Sprintf("k%d", i%7), Value: fmt.Sprint(i)})
	}
	// The leader no longer has the entries the crashed node is missing.
	l := w.Node(w.Leader())
	t.Logf("leader log: %d entries after snapshot at %d", l.LogLen(), l.SnapshotIndex())
	if l.SnapshotIndex() < 250 {
		t.Fatalf("leader snapshot at %d, expected compaction past the crashed node", l.SnapshotIndex())
	}

	w.Restart(behind)
	converged(t, w)
	if w.Stats.SnapshotsSent == 0 || w.Stats.SnapshotsRestored == 0 {
		t.Fatalf("expected the restarted node to get a snapshot: sent %d restored %d", w.Stats.SnapshotsSent, w.Stats.SnapshotsRestored)
	}
	for k := range 7 {
		key := fmt.Sprintf("k%d", k)
		if got, want := w.Node(behind).Read(key), w.Node(w.Leader()).Read(key); got != want {
			t.Errorf("%s = %q on the restarted node, %q on the leader", key, got, want)
		}
	}
	noViolations(t, w)
}

func TestSnapshotsKeepTheLogShort(t *testing.T) {
	logLen := func(opts node.Options) int {
		w := New(Config{Seed: 22, Node: opts})
		defer w.Close()
		for i := 1; i <= 1000; i++ {
			write(t, w, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Put, Key: "k", Value: fmt.Sprint(i)})
		}
		return w.Stats.MaxLog
	}
	without, with := logLen(node.Options{}), logLen(node.Options{SnapshotEvery: 100, KeepEntries: 20})
	t.Logf("1000 writes: longest log %d entries without snapshots, %d with (every 100, keep 20)", without, with)
	if without < 1000 || with > 100+20+10 {
		t.Fatalf("without %d, with %d", without, with)
	}
}

func TestSessionsSurviveSnapshotAndRestart(t *testing.T) {
	w := newWorld(t, Config{Seed: 23, Node: snapOpts})
	var want strings.Builder
	for i := 1; i <= 120; i++ {
		write(t, w, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Append, Key: "log", Value: fmt.Sprintf("%d,", i)})
		want.WriteString(fmt.Sprintf("%d,", i))
	}
	for _, id := range w.IDs() {
		w.Crash(id)
	}
	for _, id := range w.IDs() {
		w.Restart(id) // state comes back from the snapshot plus the log after it
	}
	// A very late retry of an append that was applied long ago, before the
	// snapshot. The session table is in the snapshot, so it's recognized.
	write(t, w, kv.Command{Client: 1, Seq: 100, Kind: kv.Append, Key: "log", Value: "100,"})
	converged(t, w)
	for _, id := range w.IDs() {
		if got := w.Node(id).Read("log"); got != want.String() {
			t.Fatalf("node %d log = %q\nwant %q", id, got, want.String())
		}
	}
	noViolations(t, w)
}

func TestLinearizableWithSnapshotsUnderFaults(t *testing.T) {
	cfg := RunConfig{
		// Small numbers so snapshots, compaction and snapshot transfers
		// happen constantly, including while messages are being lost.
		Config:   Config{Node: node.Options{SnapshotEvery: 20, KeepEntries: 5}},
		Workload: Workload{FollowerReads: 0.3},
		Faults:   AllFaults(),
	}
	s := explore(t, cfg, 200_001, max(*flagSeeds/2, 1))
	s.log(t)
	if len(s.failed) > 0 {
		t.Fatalf("%d of %d seeds failed; first: %s", len(s.failed), s.runs, s.firstFailure)
	}
	r := Run(RunConfig{Config: Config{Seed: 200_001, Node: cfg.Node}, Workload: cfg.Workload, Faults: cfg.Faults})
	t.Logf("seed 200001: %d snapshots taken, %d sent, %d restored, longest log %d", r.Stats.SnapshotsTaken, r.Stats.SnapshotsSent, r.Stats.SnapshotsRestored, r.Stats.MaxLog)
	if res, _ := check.Linearizable(r.History, 10*time.Second); res != porcupine.Ok {
		t.Fatal(res)
	}
}
