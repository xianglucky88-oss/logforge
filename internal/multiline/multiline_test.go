package multiline

import (
	"testing"
	"time"
)

func TestMergeByStartPattern(t *testing.T) {
	m, err := New(Options{StartPattern: `^\d{4}-`, MaxLines: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Feed("2026-11-14 first"); got != "" {
		t.Fatalf("first line should not emit, got %q", got)
	}
	if got, _ := m.Feed("    at frame1"); got != "" {
		t.Fatalf("continuation should not emit, got %q", got)
	}
	got, closedPrev := m.Feed("2026-11-14 second")
	if got != "2026-11-14 first\n    at frame1" {
		t.Fatalf("unexpected block: %q", got)
	}
	if !closedPrev {
		t.Fatal("block closed by a new start line must be marked closedPrev")
	}
	if rest := m.Flush(); rest != "2026-11-14 second" {
		t.Fatalf("flush mismatch: %q", rest)
	}
}

func TestMaxLinesForcesSplit(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, MaxLines: 2})
	m.Feed("START a")
	got, closedPrev := m.Feed("b")
	if got != "START a\nb" {
		t.Fatalf("expected split at MaxLines, got %q", got)
	}
	if !closedPrev {
		t.Fatal("MaxLines split closes the block before the current line")
	}
}

func TestNoPatternPassesThrough(t *testing.T) {
	m, _ := New(Options{})
	got, closedPrev := m.Feed("plain")
	if got != "plain" {
		t.Fatalf("want pass-through, got %q", got)
	}
	if closedPrev {
		t.Fatal("pass-through block is the current line itself")
	}
}

func TestFlushIfStale(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, FlushAfter: 50 * time.Millisecond})
	m.Feed("START a")
	if got := m.FlushIfStale(time.Now()); got != "" {
		t.Fatalf("not stale yet, got %q", got)
	}
	if got := m.FlushIfStale(time.Now().Add(time.Second)); got != "START a" {
		t.Fatalf("stale block should flush, got %q", got)
	}
	if got := m.Flush(); got != "" {
		t.Fatalf("already flushed, got %q", got)
	}
}
