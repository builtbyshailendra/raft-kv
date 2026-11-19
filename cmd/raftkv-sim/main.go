// Command raftkv-sim runs seeded simulations of the cluster and checks them.
//
//	go run ./cmd/raftkv-sim -seeds 200                      # the correct system
//	go run ./cmd/raftkv-sim -read leader-local -seeds 200   # a planted bug
//	go run ./cmd/raftkv-sim -read leader-local -seed 380004 -timeline -viz out.html
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/check"
	"github.com/builtbyshailendra/raft-kv/node"
	"github.com/builtbyshailendra/raft-kv/sim"
)

func main() {
	var (
		seed     = flag.Uint64("seed", 0, "run only this seed")
		from     = flag.Uint64("from", 1, "first seed when running several")
		seeds    = flag.Int("seeds", 100, "number of seeds")
		nodes    = flag.Int("nodes", 3, "cluster size")
		read     = flag.String("read", "readindex", "read mode: readindex, lease, leader-local, any-local")
		bug      = flag.String("bug", "", "planted bug: ack-before-commit, no-sessions, sessions-not-in-snapshot, no-snapshot-reports")
		snapshot = flag.Uint64("snapshot-every", 0, "snapshot every N applied entries (0 = never)")
		faults   = flag.String("faults", "all", "faults: all, no-pauses, none")
		drift    = flag.Float64("drift", 0, "clock drift, e.g. 0.5 for clocks between 0.5x and 1.5x speed")
		timeout  = flag.Duration("timeout", 300*time.Millisecond, "client timeout before trying another node")
		duration = flag.Duration("duration", 5*time.Second, "how long clients start operations (virtual time)")
		timeline = flag.Bool("timeline", false, "print each run's faults and leader changes")
		viz      = flag.String("viz", "", "write a Porcupine visualization of the first failing run here")
	)
	flag.Parse()

	cfg := sim.RunConfig{
		Config:   sim.Config{Nodes: *nodes, ClockDrift: *drift},
		Workload: sim.Workload{FollowerReads: 0.3, ClientTimeout: *timeout, Duration: *duration},
	}
	modes := map[string]node.ReadMode{"readindex": node.ReadIndex, "lease": node.ReadLease, "leader-local": node.ReadLeaderLocal, "any-local": node.ReadAnyLocal}
	mode, ok := modes[*read]
	if !ok {
		fail("unknown -read %q", *read)
	}
	cfg.Node.Read = mode
	cfg.Node.SnapshotEvery = *snapshot
	if *snapshot > 0 {
		cfg.Node.KeepEntries = *snapshot / 4
	}
	switch *bug {
	case "":
	case "ack-before-commit":
		cfg.Node.Bugs.AckBeforeCommit = true
	case "no-sessions":
		cfg.Node.Bugs.NoSessions = true
	case "sessions-not-in-snapshot":
		cfg.Node.Bugs.SessionsNotInSnapshot = true
	case "no-snapshot-reports":
		cfg.NoSnapshotReports = true
	default:
		fail("unknown -bug %q", *bug)
	}
	switch *faults {
	case "all":
		cfg.Faults = sim.AllFaults()
	case "no-pauses":
		cfg.Faults = sim.Faults{Partitions: true, Crashes: true, Loss: 0.2, Dup: 0.05}
	case "none":
	default:
		fail("unknown -faults %q", *faults)
	}

	list := []uint64{*seed}
	if *seed == 0 {
		list = list[:0]
		for i := range *seeds {
			list = append(list, *from+uint64(i))
		}
	}

	start := time.Now()
	var ops, failed int
	vizDone := false
	for _, s := range list {
		cfg.Seed = s
		r := sim.Run(cfg)
		res, info := check.Linearizable(r.History, 30*time.Second)
		ops += len(r.History)
		var problems []string
		problems = append(problems, r.Violations...)
		if r.Unfinished > 0 {
			problems = append(problems, fmt.Sprintf("%d operations never answered", r.Unfinished))
		}
		if res != porcupine.Ok {
			problems = append(problems, "history is not linearizable ("+string(res)+")")
		}
		if *timeline || len(list) == 1 {
			fmt.Printf("seed %d: %d operations, %d elections, %v virtual time\n", s, len(r.History), r.Stats.Elections, r.End.Round(time.Millisecond))
			for _, n := range r.Notes {
				fmt.Printf("  %10v  %-6s %s\n", n.At.Round(time.Millisecond), n.Tag, n.Text)
			}
		}
		if len(problems) == 0 {
			continue
		}
		failed++
		fmt.Printf("seed %d FAILED: %s\n", s, strings.Join(problems, "; "))
		if *viz != "" && !vizDone && res != porcupine.Ok {
			if err := check.Visualize(info, r.Annotations(), *viz); err != nil {
				fail("visualize: %v", err)
			}
			fmt.Printf("  visualization: %s\n", *viz)
			vizDone = true
		}
	}
	fmt.Printf("%d seeds, %d operations checked, %d failed, %v\n", len(list), ops, failed, time.Since(start).Round(time.Millisecond))
	if failed > 0 {
		os.Exit(1)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}
