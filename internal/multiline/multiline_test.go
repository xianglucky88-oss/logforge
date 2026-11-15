package multiline

import "testing"

func TestMergeByStartPattern(t *testing.T) {
	m, err := New(Options{StartPattern: `^\d{4}-`, MaxLines: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Feed("2026-11-14 first"); got != "" {
		t.Fatalf("first line should not emit, got %q", got)
	}
	if got := m.Feed("    at frame1"); got != "" {
		t.Fatalf("continuation should not emit, got %q", got)
	}
	got := m.Feed("2026-11-14 second")
	if got != "2026-11-14 first\n    at frame1" {
		t.Fatalf("unexpected block: %q", got)
	}
	if rest := m.Flush(); rest != "2026-11-14 second" {
		t.Fatalf("flush mismatch: %q", rest)
	}
}

func TestMaxLinesForcesSplit(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, MaxLines: 2})
	m.Feed("START a")
	if got := m.Feed("b"); got != "START a\nb" {
		t.Fatalf("expected split at MaxLines, got %q", got)
	}
}

func TestNoPatternPassesThrough(t *testing.T) {
	m, _ := New(Options{})
	if got := m.Feed("plain"); got != "plain" {
		t.Fatalf("want pass-through, got %q", got)
	}
}
