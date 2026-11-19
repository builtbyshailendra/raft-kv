package kv

import "testing"

func TestApplyAndDedupe(t *testing.T) {
	s := New(true)
	s.Apply(1, Command{Client: 1, Seq: 1, Kind: Put, Key: "a", Value: "x"})
	s.Apply(2, Command{Client: 1, Seq: 2, Kind: Append, Key: "a", Value: "y"})
	// The same append again (a client retry that also got committed).
	if dup := s.Apply(3, Command{Client: 1, Seq: 2, Kind: Append, Key: "a", Value: "y"}); !dup {
		t.Fatal("retry not recognized as a duplicate")
	}
	s.Skip(4)
	if got := s.Get("a"); got != "xy" {
		t.Fatalf("a = %q, want xy", got)
	}
	if s.Applied() != 4 {
		t.Fatalf("applied = %d", s.Applied())
	}
}

func TestWithoutDedupeARetryAppliesTwice(t *testing.T) {
	s := New(false)
	s.Apply(1, Command{Client: 1, Seq: 1, Kind: Append, Key: "a", Value: "y"})
	s.Apply(2, Command{Client: 1, Seq: 1, Kind: Append, Key: "a", Value: "y"})
	if got := s.Get("a"); got != "yy" {
		t.Fatalf("a = %q, want yy", got)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	s := New(true)
	s.Apply(1, Command{Client: 7, Seq: 3, Kind: Put, Key: "k", Value: "v"})
	data := s.Snapshot(true)

	r := New(true)
	if err := r.Restore(data, 1); err != nil {
		t.Fatal(err)
	}
	if r.Get("k") != "v" || r.Applied() != 1 || r.Hash() != s.Hash() {
		t.Fatal("restored store differs")
	}
	// The session table came along, so the old retry is still a duplicate.
	if dup := r.Apply(2, Command{Client: 7, Seq: 3, Kind: Put, Key: "k", Value: "other"}); !dup {
		t.Fatal("session lost in snapshot")
	}
	// Without sessions it is not: the planted bug.
	b := New(true)
	_ = b.Restore(s.Snapshot(false), 1)
	if dup := b.Apply(2, Command{Client: 7, Seq: 3, Kind: Put, Key: "k", Value: "other"}); dup {
		t.Fatal("expected the retry to apply when sessions are left out")
	}
}

func TestEncodeDecode(t *testing.T) {
	c := Command{Client: 1, Seq: 2, Kind: Append, Key: "k", Value: "v"}
	got, err := Decode(Encode(c))
	if err != nil || got != c {
		t.Fatalf("got %+v, %v", got, err)
	}
}
