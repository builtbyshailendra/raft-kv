package sim

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/builtbyshailendra/raft-kv/kv"
)

// write keeps retrying one command (same client and Seq) until a node
// acknowledges it. It cheats by asking the World who leads; the real
// clients in step 3 don't.
func write(t *testing.T, w *World, c kv.Command) {
	t.Helper()
	deadline := w.Now() + 30*time.Second
	for w.Now() < deadline {
		w.RunUntil(func() bool { return w.Leader() != 0 }, 2*time.Second)
		target := w.Leader()
		if target == 0 {
			continue
		}
		call := w.Request(target, c)
		w.RunUntil(func() bool { return call.Done }, 300*time.Millisecond)
		if call.Done && call.Resp.OK {
			return
		}
	}
	t.Fatalf("write %+v not acknowledged in 30s", c)
}

func others(w *World, id uint64) []uint64 {
	var ids []uint64
	for _, x := range w.IDs() {
		if x != id {
			ids = append(ids, x)
		}
	}
	return ids
}

func TestIsolatedLeaderCannotCommit(t *testing.T) {
	w := newWorld(t, Config{Seed: 4})
	old := waitLeader(t, w)
	w.RunFor(time.Second)
	oldTerm := w.Node(old).Term()

	cut := w.Now()
	w.Partition([]uint64{old}, others(w, old))
	// The old leader doesn't know yet, so it accepts this write and appends
	// it to its log. It can never commit it: no quorum can hear it.
	lost := w.Request(old, kv.Command{Client: 9, Seq: 1, Kind: kv.Put, Key: "x", Value: "lost"})

	// Watch for two moments: the majority electing a new leader, and the
	// old leader noticing (CheckQuorum: a leader that hasn't heard from a
	// quorum for an election timeout steps down). Until it notices, it still
	// believes it leads.
	var newLead uint64
	var elected, stepped time.Duration
	w.RunUntil(func() bool {
		if l := w.Leader(); newLead == 0 && l != 0 && l != old {
			newLead, elected = l, w.Now()-cut
		}
		if stepped == 0 && !w.Node(old).IsLeader() {
			stepped = w.Now() - cut
		}
		return newLead != 0 && stepped != 0
	}, 5*time.Second)
	if newLead == 0 || stepped == 0 {
		t.Fatalf("new leader %d, old leader stepped down: %v", newLead, stepped != 0)
	}
	if w.Node(newLead).Term() <= oldTerm {
		t.Fatalf("new leader's term %d is not above %d", w.Node(newLead).Term(), oldTerm)
	}
	t.Logf("partitioned at %v: old leader %d stepped down after %v, new leader %d elected after %v", cut, old, stepped, newLead, elected)

	kept := w.Request(newLead, kv.Command{Client: 10, Seq: 1, Kind: kv.Put, Key: "x", Value: "kept"})
	waitDone(t, w, kept)
	if !kept.Resp.OK {
		t.Fatalf("new leader refused the write: %+v", kept.Resp)
	}
	if lost.Done && lost.Resp.OK {
		t.Fatal("the isolated leader acknowledged a write it could not commit")
	}

	w.Heal()
	converged(t, w)
	for _, id := range w.IDs() {
		if got := w.Node(id).Read("x"); got != "kept" {
			t.Errorf("node %d x = %q, want kept (the old leader's entry must be replaced)", id, got)
		}
	}
	noViolations(t, w)
}

func TestAcknowledgedWritesSurviveLeaderCrashes(t *testing.T) {
	w := newWorld(t, Config{Seed: 5})
	want := map[string]string{}
	seq := uint64(0)
	var crashed uint64
	for round := range 6 {
		for i := range 10 {
			key, val := fmt.Sprintf("k%d", i), fmt.Sprintf("r%d", round)
			seq++
			write(t, w, kv.Command{Client: 1, Seq: seq, Kind: kv.Put, Key: key, Value: val})
			want[key] = val
		}
		if crashed != 0 {
			w.Restart(crashed)
		}
		crashed = w.Leader()
		w.Crash(crashed)
	}
	w.Restart(crashed)
	converged(t, w)
	for _, id := range w.IDs() {
		for k, v := range want {
			if got := w.Node(id).Read(k); got != v {
				t.Errorf("node %d %s = %q, want %q", id, k, got, v)
			}
		}
	}
	t.Logf("%d crashes, %d elections", w.Stats.Crashes, w.Stats.Elections)
	noViolations(t, w)
}

func TestWholeClusterRestartsFromDisk(t *testing.T) {
	w := newWorld(t, Config{Seed: 6})
	for i := 1; i <= 20; i++ {
		write(t, w, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Append, Key: "log", Value: fmt.Sprintf("%d,", i)})
	}
	for _, id := range w.IDs() {
		w.Crash(id)
	}
	w.RunFor(time.Second)
	for _, id := range w.IDs() {
		w.Restart(id)
	}
	// Nothing is in memory now. Each node rebuilds its state machine by
	// replaying committed entries from its disk once a leader tells it the
	// commit index.
	waitLeader(t, w)
	converged(t, w)
	want := ""
	for i := 1; i <= 20; i++ {
		want += fmt.Sprintf("%d,", i)
	}
	for _, id := range w.IDs() {
		if got := w.Node(id).Read("log"); got != want {
			t.Errorf("node %d log = %q, want %q", id, got, want)
		}
	}
	noViolations(t, w)
}

func TestLossAndDuplicatesApplyEachWriteOnce(t *testing.T) {
	w := newWorld(t, Config{Seed: 7})
	w.SetLoss(0.2)
	w.SetDup(0.1)
	var want strings.Builder
	for i := 1; i <= 50; i++ {
		v := fmt.Sprintf("%d,", i)
		// Lost requests and lost acknowledgements make write() resend the
		// same Seq. Without the session table some appends would apply twice.
		write(t, w, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Append, Key: "log", Value: v})
		want.WriteString(v)
	}
	w.SetLoss(0)
	converged(t, w)
	for _, id := range w.IDs() {
		if got := w.Node(id).Read("log"); got != want.String() {
			t.Errorf("node %d log = %q\nwant %q", id, got, want.String())
		}
	}
	t.Logf("%d Raft messages, %d dropped", w.Stats.Messages, w.Stats.Dropped)
	noViolations(t, w)
}

func TestPausedLeaderWakesUpBelievingItLeads(t *testing.T) {
	w := newWorld(t, Config{Seed: 8})
	old := waitLeader(t, w)
	w.RunFor(time.Second)
	write(t, w, kv.Command{Client: 1, Seq: 1, Kind: kv.Put, Key: "x", Value: "old"})

	pausedAt := w.Now()
	w.Pause(old) // a 2-second stop-the-world pause
	if !w.RunUntil(func() bool { l := w.Leader(); return l != 0 && l != old }, 5*time.Second) {
		t.Fatal("no new leader while the old one was paused")
	}
	write(t, w, kv.Command{Client: 1, Seq: 2, Kind: kv.Put, Key: "x", Value: "new"})
	w.RunFor(pausedAt + 2*time.Second - w.Now())

	// The paused node's memory is unchanged: it still thinks it leads, and
	// its state still says x=old. Anything it answered from memory right now
	// would be stale. Only messages from the new term can correct it.
	if !w.Node(old).IsLeader() || w.Node(old).Read("x") != "old" {
		t.Fatal("expected the paused node to still believe it leads with x=old")
	}
	w.Resume(old) // works through its inbox: the new leader's heartbeats
	if w.Node(old).IsLeader() {
		t.Fatal("still leader after reading messages from a newer term")
	}
	converged(t, w)
	if got := w.Node(old).Read("x"); got != "new" {
		t.Fatalf("x = %q after catching up", got)
	}
	noViolations(t, w)
}
