package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/builtbyshailendra/raft-kv/kv"
	"github.com/builtbyshailendra/raft-kv/node"
)

func newWorld(t *testing.T, cfg Config) *World {
	t.Helper()
	w := New(cfg)
	t.Cleanup(w.Close)
	return w
}

func noViolations(t *testing.T, w *World) {
	t.Helper()
	for _, v := range w.Violations() {
		t.Error(v)
	}
}

func waitLeader(t *testing.T, w *World) uint64 {
	t.Helper()
	if !w.RunUntil(func() bool { return w.Leader() != 0 }, 10*time.Second) {
		t.Fatalf("no leader after 10s (t=%v)", w.Now())
	}
	return w.Leader()
}

func waitDone(t *testing.T, w *World, calls ...*Call) {
	t.Helper()
	ok := w.RunUntil(func() bool {
		for _, c := range calls {
			if !c.Done {
				return false
			}
		}
		return true
	}, 10*time.Second)
	if !ok {
		t.Fatalf("requests not answered by t=%v", w.Now())
	}
}

// converged runs until every running node has applied the same index, then
// compares state machines.
func converged(t *testing.T, w *World) {
	t.Helper()
	same := func() bool {
		var idx uint64
		for _, id := range w.IDs() {
			n := w.Node(id)
			if n == nil {
				continue
			}
			if idx == 0 {
				idx = n.Applied()
			} else if n.Applied() != idx {
				return false
			}
		}
		return true
	}
	if !w.RunUntil(same, 5*time.Second) {
		t.Fatalf("replicas did not reach the same applied index")
	}
	var h uint64
	for _, id := range w.IDs() {
		if n := w.Node(id); n != nil {
			if h == 0 {
				h = n.Hash()
			} else if n.Hash() != h {
				t.Fatalf("node %d state differs at applied index %d", id, n.Applied())
			}
		}
	}
}

func TestElectsOneLeader(t *testing.T) {
	for _, nodes := range []int{3, 5} {
		t.Run(fmt.Sprint(nodes, " nodes"), func(t *testing.T) {
			w := newWorld(t, Config{Seed: 1, Nodes: nodes})
			lead := waitLeader(t, w)
			w.RunFor(time.Second) // heartbeats tell everyone
			for _, id := range w.IDs() {
				if got := w.Node(id).Lead(); got != lead {
					t.Errorf("node %d thinks the leader is %d, want %d", id, got, lead)
				}
			}
			noViolations(t, w)
			t.Logf("leader %d elected at t=%v", lead, w.Now()-time.Second)
		})
	}
}

func TestReplicatesWrites(t *testing.T) {
	w := newWorld(t, Config{Seed: 2})
	lead := waitLeader(t, w)

	var calls []*Call
	want := map[string]string{}
	for i := 1; i <= 60; i++ {
		key := fmt.Sprintf("k%d", i%4)
		val := fmt.Sprintf("%d,", i)
		kind := kv.Append
		if i%10 == 0 {
			kind, want[key] = kv.Put, ""
		}
		want[key] += val
		// One at a time: the network reorders messages, and appends to the
		// same key must land in the order the test expects.
		c := w.Request(lead, kv.Command{Client: uint64(i), Seq: 1, Kind: kind, Key: key, Value: val})
		waitDone(t, w, c)
		calls = append(calls, c)
	}
	for i, c := range calls {
		if !c.Resp.OK {
			t.Fatalf("write %d: %+v", i+1, c.Resp)
		}
	}
	converged(t, w)
	for _, id := range w.IDs() {
		for k, v := range want {
			if got := w.Node(id).Read(k); got != v {
				t.Errorf("node %d %s = %q, want %q", id, k, got, v)
			}
		}
	}
	noViolations(t, w)
	t.Logf("%d messages, %d bytes", w.Stats.Messages, w.Stats.Bytes)
}

func TestWriteToFollowerNamesTheLeader(t *testing.T) {
	w := newWorld(t, Config{Seed: 3})
	lead := waitLeader(t, w)
	w.RunFor(time.Second)
	follower := lead%3 + 1
	c := w.Request(follower, kv.Command{Client: 1, Seq: 1, Kind: kv.Put, Key: "a", Value: "b"})
	waitDone(t, w, c)
	if c.Resp.OK || c.Resp.Err != node.ErrNotLeader || c.Resp.Leader != lead {
		t.Fatalf("got %+v, want not-leader pointing at %d", c.Resp, lead)
	}
}

func TestSameSeedSameRun(t *testing.T) {
	run := func(seed uint64) (uint64, uint64) {
		w := New(Config{Seed: seed})
		defer w.Close()
		lead := waitLeader(t, w)
		var calls []*Call
		for i := 1; i <= 20; i++ {
			calls = append(calls, w.Request(lead, kv.Command{Client: 1, Seq: uint64(i), Kind: kv.Append, Key: "k", Value: "x"}))
		}
		waitDone(t, w, calls...)
		w.RunFor(time.Second)
		return w.TraceHash(), lead
	}
	h1, l1 := run(42)
	h2, l2 := run(42)
	if h1 != h2 || l1 != l2 {
		t.Fatalf("seed 42 ran differently: %x/%d vs %x/%d", h1, l1, h2, l2)
	}
	h3, _ := run(43)
	if h3 == h1 {
		t.Fatal("seeds 42 and 43 produced the same trace")
	}
}
