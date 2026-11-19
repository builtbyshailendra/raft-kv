// Package check decides whether a history of client operations is
// linearizable, using Porcupine.
//
// Linearizable means: there is one order of all operations, consistent with
// real time (if A returned before B was called, A comes first), in which
// every read returns the value of the latest write before it. Porcupine
// searches for that order; if none exists, the history proves a bug.
package check

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/kv"
)

// Input is what a client asked for.
type Input struct {
	Kind  kv.Kind
	Key   string
	Value string
}

// Output is what it got back (the value, for a Get).
type Output struct {
	Value string
}

// Model is the sequential specification of one key: a string that Put
// replaces, Append extends and Get returns. Partitioning by key lets
// Porcupine check each key on its own, which is much faster.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in := state.(string), input.(Input)
		switch in.Kind {
		case kv.Get:
			return output.(Output).Value == s, s
		case kv.Put:
			return true, in.Value
		case kv.Append:
			return true, s + in.Value
		}
		return false, s
	},
	Equal: func(a, b any) bool { return a.(string) == b.(string) },
	Hash: func(s any) uint64 {
		h := fnv.New64a()
		h.Write([]byte(s.(string)))
		return h.Sum64()
	},
	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		switch in.Kind {
		case kv.Get:
			return fmt.Sprintf("get(%s) -> %q", in.Key, short(output.(Output).Value))
		case kv.Put:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		default:
			return fmt.Sprintf("append(%s, %q)", in.Key, in.Value)
		}
	},
	DescribeState: func(s any) string { return fmt.Sprintf("%q", short(s.(string))) },
	DescribeOperationMetadata: func(m any) string {
		if m == nil {
			return ""
		}
		return fmt.Sprintf("answered by node %v", m)
	},
}

// Show the tail of long values: appends grow at the end.
func short(s string) string {
	if len(s) <= 40 {
		return s
	}
	return "..." + s[len(s)-37:]
}

// Linearizable checks a history. The result is Ok, Illegal (a bug), or
// Unknown (the search ran out of time).
func Linearizable(history []porcupine.Operation, timeout time.Duration) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	return porcupine.CheckOperationsVerbose(Model, history, timeout)
}

// Visualize writes Porcupine's interactive HTML view of a history: one row
// per client, the longest linearizable prefix, and where it breaks.
func Visualize(info porcupine.LinearizationInfo, notes []porcupine.Annotation, path string) error {
	info.AddAnnotations(notes)
	return porcupine.VisualizePath(Model, info, path)
}

// Describe prints one operation for logs.
func Describe(op porcupine.Operation) string {
	d := Model.DescribeOperation(op.Input, op.Output)
	by := ""
	if op.Metadata != nil {
		by = fmt.Sprintf(" (node %v)", op.Metadata)
	}
	return fmt.Sprintf("client %d [%v, %v] %s%s", op.ClientId, time.Duration(op.Call).Round(time.Microsecond), time.Duration(op.Return).Round(time.Microsecond), strings.TrimSpace(d), by)
}
