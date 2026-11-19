package sim

import (
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/builtbyshailendra/raft-kv/node"
)

var flagMutantSeeds = flag.Int("mutant-seeds", 200, "seeds per planted bug")

// TestPlantedBugsAreCaught plants one known mistake at a time and runs the
// same kind of seeded runs against it. A checker that never fails proves
// nothing; this shows which bugs the simulator and Porcupine catch, and how
// often. Runs are deterministic, so the counts are stable.
func TestPlantedBugsAreCaught(t *testing.T) {
	const (
		mustPass  = "must pass"
		mustCatch = "must be caught"
		measure   = "measured"
	)
	snap := node.Options{SnapshotEvery: 20, KeepEntries: 5}
	noPauses := Faults{Partitions: true, Crashes: true, Loss: 0.2, Dup: 0.05}
	fast := Workload{FollowerReads: 0.3, ClientTimeout: 50 * time.Millisecond}
	bugs := func(b node.Bugs) Config { return Config{Node: node.Options{Bugs: b}} }
	reads := func(m node.ReadMode) Config { return Config{Node: node.Options{Read: m}} }
	noSessionsInSnap := snap
	noSessionsInSnap.Bugs.SessionsNotInSnapshot = true

	cases := []struct {
		name   string
		cfg    RunConfig
		expect string
	}{
		{"control: ReadIndex, no bugs, all faults", RunConfig{Faults: AllFaults()}, mustPass},
		{"control: same, snapshots every 20 entries", RunConfig{Config: Config{Node: snap}, Faults: AllFaults()}, mustPass},
		{"control: same, clients time out after 50 ms", RunConfig{Workload: fast, Faults: AllFaults()}, mustPass},
		{"answer a write before it commits", RunConfig{Config: bugs(node.Bugs{AckBeforeCommit: true}), Faults: AllFaults()}, mustCatch},
		{"no session table", RunConfig{Config: bugs(node.Bugs{NoSessions: true}), Faults: AllFaults()}, mustCatch},
		{"snapshots leave out the session table", RunConfig{Config: Config{Node: noSessionsInSnap}, Faults: AllFaults()}, mustCatch},
		{"leader never told whether a snapshot arrived", RunConfig{Config: Config{Node: snap, NoSnapshotReports: true}, Faults: AllFaults()}, mustCatch},
		{"any node answers reads from its own state", RunConfig{Config: reads(node.ReadAnyLocal), Faults: AllFaults()}, mustCatch},
		{"leader answers reads from its own state", RunConfig{Config: reads(node.ReadLeaderLocal), Faults: AllFaults()}, mustCatch},
		{"leader answers reads from its own state, no pauses", RunConfig{Config: reads(node.ReadLeaderLocal), Faults: noPauses}, measure},
		{"leader answers reads from its own state, no pauses, 50 ms client timeout", RunConfig{Config: reads(node.ReadLeaderLocal), Workload: fast, Faults: noPauses}, mustCatch},
		{"lease reads, all faults", RunConfig{Config: reads(node.ReadLease), Faults: AllFaults()}, mustCatch},
		{"lease reads, no pauses", RunConfig{Config: reads(node.ReadLease), Faults: noPauses}, measure},
		{"lease reads, no pauses, clocks drift up to 50%", RunConfig{Config: Config{Node: node.Options{Read: node.ReadLease}, ClockDrift: 0.5}, Faults: noPauses}, measure},
		{"lease reads, no pauses, 50 ms client timeout", RunConfig{Config: reads(node.ReadLease), Workload: fast, Faults: noPauses}, mustCatch},
	}

	var table strings.Builder
	table.WriteString("| Planted bug | Expected | Seeds failing | Not linearizable | Safety check | Liveness | First failing seed |\n")
	table.WriteString("|---|---|---|---|---|---|---|\n")
	for i, tc := range cases {
		if tc.cfg.Workload == (Workload{}) {
			tc.cfg.Workload = Workload{FollowerReads: 0.3}
		}
		s := explore(t, tc.cfg, 300_001+uint64(i)*10_000, *flagMutantSeeds)
		first := "-"
		if len(s.failed) > 0 {
			first = fmt.Sprint(s.failed[0])
		}
		fmt.Fprintf(&table, "| %s | %s | %d / %d | %d | %d | %d | %s |\n",
			tc.name, tc.expect, len(s.failed), s.runs, s.illegal, s.safety, s.liveness, first)
		caught := len(s.failed) > 0
		if (tc.expect == mustCatch && !caught) || (tc.expect == mustPass && caught) {
			t.Errorf("%s: %s, but %d/%d seeds failed (%s)", tc.name, tc.expect, len(s.failed), s.runs, s.firstFailure)
		}
	}
	t.Log("\n" + table.String())
}
