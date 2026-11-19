package check

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/builtbyshailendra/raft-kv/kv"
)

func op(client int, call, ret int64, kind kv.Kind, key, value, out string) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Call: call, Return: ret,
		Input: Input{Kind: kind, Key: key, Value: value}, Output: Output{Value: out}}
}

func TestHandWrittenHistories(t *testing.T) {
	cases := []struct {
		name string
		h    []porcupine.Operation
		want porcupine.CheckResult
	}{
		{"read after write", []porcupine.Operation{
			op(0, 0, 10, kv.Put, "x", "a", ""),
			op(1, 20, 30, kv.Get, "x", "", "a"),
		}, porcupine.Ok},
		{"stale read: b was acknowledged before the read began", []porcupine.Operation{
			op(0, 0, 10, kv.Put, "x", "a", ""),
			op(0, 20, 30, kv.Put, "x", "b", ""),
			op(1, 40, 50, kv.Get, "x", "", "a"),
		}, porcupine.Illegal},
		{"a read overlapping a write may see either value", []porcupine.Operation{
			op(0, 0, 10, kv.Put, "x", "a", ""),
			op(0, 20, 60, kv.Put, "x", "b", ""),
			op(1, 30, 40, kv.Get, "x", "", "a"),
			op(2, 30, 40, kv.Get, "x", "", "b"),
		}, porcupine.Ok},
		{"but not back and forth", []porcupine.Operation{
			op(0, 0, 10, kv.Put, "x", "a", ""),
			op(0, 20, 60, kv.Put, "x", "b", ""),
			op(1, 30, 40, kv.Get, "x", "", "b"),
			op(2, 45, 50, kv.Get, "x", "", "a"),
		}, porcupine.Illegal},
		{"an append applied twice", []porcupine.Operation{
			op(0, 0, 10, kv.Append, "x", "1;", ""),
			op(1, 20, 30, kv.Get, "x", "", "1;1;"),
		}, porcupine.Illegal},
		{"a lost acknowledged write", []porcupine.Operation{
			op(0, 0, 10, kv.Append, "x", "1;", ""),
			op(0, 20, 30, kv.Append, "x", "2;", ""),
			op(1, 40, 50, kv.Get, "x", "", "1;"),
		}, porcupine.Illegal},
		{"keys are independent", []porcupine.Operation{
			op(0, 0, 10, kv.Put, "x", "a", ""),
			op(1, 20, 30, kv.Get, "y", "", ""),
		}, porcupine.Ok},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := Linearizable(tc.h, time.Second); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}
