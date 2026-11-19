package sim

import (
	"sort"
	"testing"
	"time"

	"github.com/builtbyshailendra/raft-kv/kv"
	"github.com/builtbyshailendra/raft-kv/node"
)

// measureOverlap cuts the leader off and measures two moments: when the
// majority elects a new leader, and when the old leader (via CheckQuorum)
// stops believing it leads. If the first comes before the second, two nodes
// believe they lead at once, and anything that trusts "I am the leader"
// without asking a quorum can serve stale reads.
func measureOverlap(t *testing.T, cfg Config, seeds int) (overlaps int, worst time.Duration, gaps []time.Duration) {
	t.Helper()
	for i := range seeds {
		cfg.Seed = uint64(500_001 + i)
		w := New(cfg)
		w.RunUntil(func() bool { return w.Leader() != 0 }, 10*time.Second)
		w.RunFor(time.Duration(200+w.rng.IntN(800)) * time.Millisecond)
		old := w.Leader()
		cut := w.Now()
		w.Partition([]uint64{old}, others(w, old))
		var elected, stepped time.Duration
		w.RunUntil(func() bool {
			if l := w.Leader(); elected == 0 && l != 0 && l != old {
				elected = w.Now() - cut
			}
			if stepped == 0 && !w.Node(old).IsLeader() {
				stepped = w.Now() - cut
			}
			return elected != 0 && stepped != 0
		}, 10*time.Second)
		w.Close()
		gaps = append(gaps, stepped-elected)
		if elected < stepped {
			overlaps++
			worst = max(worst, stepped-elected)
		}
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i] < gaps[j] })
	return overlaps, worst, gaps
}

func TestTwoLeadersOverlap(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drift float64
	}{{"clocks in step", 0}, {"clocks drift up to 50%", 0.5}} {
		t.Run(tc.name, func(t *testing.T) {
			n, worst, _ := measureOverlap(t, Config{ClockDrift: tc.drift}, 500)
			t.Logf("leader cut off in 500 seeds: a new leader was elected before the old one stepped down in %d (worst overlap %v)", n, worst)
		})
	}
}

// staleLeaseRead cuts the leader off, writes x=2 through the new leader,
// then reads x from the old leader. It reports whether that read returned
// the old value 1 (it skips runs where the setup didn't happen).
func staleLeaseRead(t *testing.T, cfg Config, campaign uint64) (stale, ran bool) {
	t.Helper()
	w := New(cfg)
	defer w.Close()
	if campaign != 0 {
		w.RunFor(50 * time.Millisecond)
		w.Node(campaign).Campaign()
		w.RunUntil(func() bool { return w.Leader() == campaign }, 5*time.Second)
	}
	waitLeader(t, w)
	w.RunFor(time.Duration(100+w.rng.IntN(500)) * time.Millisecond)
	write(t, w, kv.Command{Client: 1, Seq: 1, Kind: kv.Put, Key: "x", Value: "1"})
	old := w.Leader()
	w.Partition([]uint64{old}, others(w, old))
	w.RunUntil(func() bool { l := w.Leader(); return l != 0 && l != old }, 5*time.Second)
	c := w.Request(w.Leader(), kv.Command{Client: 2, Seq: 1, Kind: kv.Put, Key: "x", Value: "2"})
	w.RunUntil(func() bool { return c.Done }, 2*time.Second)
	if !c.Done || !c.Resp.OK {
		return false, false
	}
	// x=2 is acknowledged. A client still talking to the old leader reads.
	r := w.Request(old, kv.Command{Client: 3, Seq: 1, Kind: kv.Get, Key: "x"})
	w.RunUntil(func() bool { return r.Done }, 2*time.Second)
	return r.Done && r.Resp.OK && r.Resp.Value == "1", true
}

// raft documents that lease reads can go wrong when clocks drift. They also
// go wrong with every clock at the same speed: the leader checks for a
// quorum only once per election timeout, counting any reply since the last
// check, so it can keep its belief for nearly two election timeouts after
// last hearing from a quorum. Followers may elect a new leader after one.
func TestLeaseReadsGoStale(t *testing.T) {
	t.Run("old leader's clock at half speed", func(t *testing.T) {
		for _, mode := range []node.ReadMode{node.ReadLease, node.ReadIndex} {
			stale, ran := staleLeaseRead(t, Config{Seed: 31, Node: node.Options{Read: mode}, Speed: map[uint64]float64{1: 0.5, 2: 1.5, 3: 1.5}}, 1)
			t.Logf("%v: stale=%v", mode, stale)
			if !ran || stale != (mode == node.ReadLease) {
				t.Errorf("%v: ran=%v stale=%v", mode, ran, stale)
			}
		}
	})
	t.Run("clocks at the same speed, 200 seeds", func(t *testing.T) {
		for _, mode := range []node.ReadMode{node.ReadLease, node.ReadIndex} {
			stale, ran := 0, 0
			for seed := uint64(600_001); seed <= 600_200; seed++ {
				s, r := staleLeaseRead(t, Config{Seed: seed, Node: node.Options{Read: mode}}, 0)
				if r {
					ran++
				}
				if s {
					stale++
				}
			}
			t.Logf("%v: stale read in %d of %d runs", mode, stale, ran)
			if (mode == node.ReadLease) != (stale > 0) {
				t.Errorf("%v: %d stale reads", mode, stale)
			}
		}
	})
}

// A newly elected leader may not yet know which entries are committed: the
// old leader acknowledged x=2 and was cut off before telling anyone the new
// commit index. ReadIndex waits until the new leader commits an entry of its
// own term; reading its local state right away returns x=1.
func TestNewLeaderReadingItsOwnStateIsStale(t *testing.T) {
	for _, mode := range []node.ReadMode{node.ReadLeaderLocal, node.ReadIndex} {
		t.Run(mode.String(), func(t *testing.T) {
			w := newWorld(t, Config{Seed: 32, Node: node.Options{Read: mode}})
			old := waitLeader(t, w)
			write(t, w, kv.Command{Client: 1, Seq: 1, Kind: kv.Put, Key: "x", Value: "1"})
			w.RunFor(time.Second)

			// Write x=2 and cut the leader off the moment it has applied
			// (committed and acknowledged) it: the messages carrying the new
			// commit index are still in flight, and the partition drops them.
			c := w.Request(old, kv.Command{Client: 1, Seq: 2, Kind: kv.Put, Key: "x", Value: "2"})
			w.RunUntil(func() bool { return w.Node(old).Read("x") == "2" }, 5*time.Second)
			w.Partition([]uint64{old}, others(w, old))
			waitDone(t, w, c) // clients aren't partitioned: the ack still arrives
			if !c.Resp.OK {
				t.Fatalf("x=2 not acknowledged: %+v", c.Resp)
			}

			// Read on the new leader at the instant it is elected.
			var lead uint64
			if !w.RunUntil(func() bool { lead = w.Leader(); return lead != 0 && lead != old }, 5*time.Second) {
				t.Fatal("no new leader")
			}
			r := &Call{}
			w.calls[callKey{3, 1}] = r
			w.Node(lead).Handle(kv.Command{Client: 3, Seq: 1, Kind: kv.Get, Key: "x"})
			w.RunUntil(func() bool { return r.Done }, 2*time.Second)
			t.Logf("new leader %d answered %+v", lead, r.Resp)
			stale := r.Done && r.Resp.OK && r.Resp.Value == "1"
			if mode == node.ReadLeaderLocal && !stale {
				t.Fatal("expected the local read to return the stale value 1")
			}
			if mode == node.ReadIndex && (!r.Done || r.Resp.Value != "2") {
				t.Fatalf("ReadIndex: %+v, want x=2", r.Resp)
			}
		})
	}
}
