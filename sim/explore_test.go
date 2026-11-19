package sim

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/check"
	"github.com/builtbyshailendra/raft-kv/kv"
)

var (
	flagSeeds = flag.Int("seeds", 100, "number of seeds each exploration runs")
	flagSeed  = flag.Uint64("seed", 0, "run only this seed")
	flagViz   = flag.String("viz", "", "write Porcupine visualizations of failing seeds here")
)

type summary struct {
	runs, ops, gets, unfinished            int
	partitions, crashes, pauses, elections int
	messages, dropped                      int
	snapSent, snapRestored                 int
	failed                                 []uint64 // seeds that failed the checks
	firstFailure                           string
	simTime, checkTime                     time.Duration
}

// explore runs seeds and checks every run three ways: the simulator's own
// safety checks, liveness (every operation answered after healing), and
// Porcupine's linearizability check of the client history.
func explore(t *testing.T, cfg RunConfig, from uint64, n int) summary {
	t.Helper()
	var s summary
	seeds := make([]uint64, 0, n)
	if *flagSeed != 0 {
		seeds = append(seeds, *flagSeed)
	} else {
		for i := range n {
			seeds = append(seeds, from+uint64(i))
		}
	}
	for _, seed := range seeds {
		cfg.Seed = seed
		start := time.Now()
		r := Run(cfg)
		s.simTime += time.Since(start)
		start = time.Now()
		res, info := check.Linearizable(r.History, 30*time.Second)
		s.checkTime += time.Since(start)

		s.runs++
		s.ops += len(r.History)
		for _, op := range r.History {
			if op.Input.(check.Input).Kind == kv.Get {
				s.gets++
			}
		}
		s.unfinished += r.Unfinished
		s.partitions += r.Stats.Partitions
		s.crashes += r.Stats.Crashes
		s.pauses += r.Stats.Pauses
		s.elections += r.Stats.Elections
		s.messages += r.Stats.Messages
		s.dropped += r.Stats.Dropped
		s.snapSent += r.Stats.SnapshotsSent
		s.snapRestored += r.Stats.SnapshotsRestored

		var problems []string
		problems = append(problems, r.Violations...)
		if r.Unfinished > 0 {
			problems = append(problems, fmt.Sprintf("%d operations never answered after healing", r.Unfinished))
		}
		if res != porcupine.Ok {
			problems = append(problems, fmt.Sprintf("linearizability: %s", res))
			if *flagViz != "" {
				path := filepath.Join(*flagViz, fmt.Sprintf("seed-%d.html", seed))
				_ = os.MkdirAll(*flagViz, 0o755)
				if err := check.Visualize(info, r.Annotations(), path); err == nil {
					problems = append(problems, "see "+path)
				}
			}
		}
		if len(problems) > 0 {
			s.failed = append(s.failed, seed)
			if s.firstFailure == "" {
				s.firstFailure = fmt.Sprintf("seed %d: %v", seed, problems)
			}
		}
	}
	return s
}

func (s summary) log(t *testing.T) {
	t.Helper()
	t.Logf("%d runs: %d operations checked (%d reads), %d partitions, %d crashes, %d pauses, %d elections, %d Raft messages (%d dropped)",
		s.runs, s.ops, s.gets, s.partitions, s.crashes, s.pauses, s.elections, s.messages, s.dropped)
	if s.snapSent > 0 {
		t.Logf("snapshots: %d sent by leaders, %d installed by followers", s.snapSent, s.snapRestored)
	}
	t.Logf("simulation %v (%v per run), Porcupine %v; failing seeds: %d %v",
		s.simTime.Round(time.Millisecond), (s.simTime / time.Duration(max(s.runs, 1))).Round(time.Millisecond),
		s.checkTime.Round(time.Millisecond), len(s.failed), s.failed)
}

func TestNoFaultsIsLinearizable(t *testing.T) {
	r := Run(RunConfig{Config: Config{Seed: 1}})
	res, _ := check.Linearizable(r.History, 10*time.Second)
	if res != porcupine.Ok || len(r.Violations) > 0 || r.Unfinished > 0 {
		t.Fatalf("result %s, violations %v, unfinished %d", res, r.Violations, r.Unfinished)
	}
	t.Logf("%d operations in %v of virtual time", len(r.History), r.End)
}

func TestLinearizableUnderFaults(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nodes int
		seeds int
		from  uint64
	}{
		{"3 nodes", 3, *flagSeeds, 1},
		{"5 nodes", 5, max(*flagSeeds/4, 1), 100_001},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := RunConfig{
				Config:   Config{Nodes: tc.nodes},
				Workload: Workload{FollowerReads: 0.3},
				Faults:   AllFaults(),
			}
			s := explore(t, cfg, tc.from, tc.seeds)
			s.log(t)
			if len(s.failed) > 0 {
				t.Fatalf("%d of %d seeds failed; first: %s", len(s.failed), s.runs, s.firstFailure)
			}
		})
	}
}

func TestFullRunIsDeterministic(t *testing.T) {
	cfg := RunConfig{Config: Config{Seed: 99}, Workload: Workload{FollowerReads: 0.3}, Faults: AllFaults()}
	a, b := Run(cfg), Run(cfg)
	if a.TraceHash != b.TraceHash || len(a.History) != len(b.History) || len(a.Notes) != len(b.Notes) {
		t.Fatalf("seed 99 ran differently: %x/%d ops vs %x/%d ops", a.TraceHash, len(a.History), b.TraceHash, len(b.History))
	}
}
