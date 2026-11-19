// Package kv is the replicated state machine: a string key-value map plus a
// session table that makes retried writes apply once.
//
// It knows nothing about Raft. Raft decides the order of commands; every
// replica applies the same commands in the same order and so ends up with the
// same state.
package kv

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
)

// Kind is the type of a client operation.
type Kind uint8

const (
	Get Kind = iota + 1
	Put
	Append
)

func (k Kind) String() string {
	switch k {
	case Get:
		return "get"
	case Put:
		return "put"
	case Append:
		return "append"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Command is one client operation. Client and Seq identify it: a client sends
// one operation at a time and numbers them 1, 2, 3...; a retry reuses the Seq.
type Command struct {
	Client uint64 `json:"c"`
	Seq    uint64 `json:"s"`
	Kind   Kind   `json:"k"`
	Key    string `json:"key"`
	Value  string `json:"v,omitempty"`
}

// Encode turns a command into a Raft log entry payload.
func Encode(c Command) []byte {
	b, err := json.Marshal(c)
	if err != nil {
		panic(err) // a struct of strings and ints always marshals
	}
	return b
}

// Decode reads a log entry payload back.
func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Store is one replica's state.
type Store struct {
	data     map[string]string
	sessions map[uint64]uint64 // client -> highest Seq applied
	applied  uint64            // Raft index of the last applied entry
	dedupe   bool
}

// New returns an empty store. With dedupe false the session table is ignored
// and a retried write is applied again (a planted bug for the checker).
func New(dedupe bool) *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]uint64{}, dedupe: dedupe}
}

// Apply applies a write at Raft index i and reports whether it was a
// duplicate (already applied under the same client and Seq).
func (s *Store) Apply(i uint64, c Command) (duplicate bool) {
	if i <= s.applied {
		panic(fmt.Sprintf("kv: apply index %d after %d", i, s.applied))
	}
	s.applied = i
	if s.dedupe && c.Seq <= s.sessions[c.Client] {
		return true
	}
	if c.Seq > s.sessions[c.Client] {
		s.sessions[c.Client] = c.Seq
	}
	switch c.Kind {
	case Put:
		s.data[c.Key] = c.Value
	case Append:
		s.data[c.Key] += c.Value
	}
	return false
}

// Skip records that Raft index i was applied without a command (the empty
// entry a new leader appends, or a configuration change).
func (s *Store) Skip(i uint64) {
	if i <= s.applied {
		panic(fmt.Sprintf("kv: skip index %d after %d", i, s.applied))
	}
	s.applied = i
}

// Get reads a key from this replica's local state.
func (s *Store) Get(key string) string { return s.data[key] }

// Applied is the Raft index of the last applied entry.
func (s *Store) Applied() uint64 { return s.applied }

type snapshot struct {
	Data     map[string]string `json:"data"`
	Sessions map[uint64]uint64 `json:"sessions,omitempty"`
}

// Snapshot serializes the state. The session table must be included: a
// replica restored without it would apply a retried write a second time.
// withSessions=false exists only to plant that bug.
func (s *Store) Snapshot(withSessions bool) []byte {
	snap := snapshot{Data: s.data}
	if withSessions {
		snap.Sessions = s.sessions
	}
	b, err := json.Marshal(snap) // map keys are sorted, so this is deterministic
	if err != nil {
		panic(err)
	}
	return b
}

// Restore replaces the state with a snapshot taken at Raft index i.
func (s *Store) Restore(data []byte, i uint64) error {
	snap := snapshot{Data: map[string]string{}, Sessions: map[uint64]uint64{}}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &snap); err != nil {
			return fmt.Errorf("kv: restore: %w", err)
		}
	}
	if snap.Data == nil {
		snap.Data = map[string]string{}
	}
	if snap.Sessions == nil {
		snap.Sessions = map[uint64]uint64{}
	}
	s.data, s.sessions, s.applied = snap.Data, snap.Sessions, i
	return nil
}

// Hash summarizes the data (not the sessions) so replicas can be compared.
func (s *Store) Hash() uint64 {
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, k := range keys {
		fmt.Fprintf(h, "%q=%q;", k, s.data[k])
	}
	return h.Sum64()
}

// Len is the number of keys.
func (s *Store) Len() int { return len(s.data) }
