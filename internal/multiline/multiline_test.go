package multiline

import (
	"testing"
	"time"
)

// feedAll 是测试辅助：喂一行，断言恰好收尾一条记录并返回它。
func feedAll(t *testing.T, m *Merger, line string) Result {
	t.Helper()
	res := m.Feed(line)
	if len(res) != 1 {
		t.Fatalf("feed %q: want 1 result, got %d (%v)", line, len(res), res)
	}
	return res[0]
}

// feedNone 断言喂一行后没有任何记录收尾。
func feedNone(t *testing.T, m *Merger, line string) {
	t.Helper()
	if res := m.Feed(line); len(res) != 0 {
		t.Fatalf("feed %q: want no result, got %v", line, res)
	}
}

func TestMergeByStartPattern(t *testing.T) {
	m, err := New(Options{StartPattern: `^\d{4}-`, MaxLines: 10})
	if err != nil {
		t.Fatal(err)
	}
	feedNone(t, m, "2026-11-14 first")
	feedNone(t, m, "    at frame1")
	got := feedAll(t, m, "2026-11-14 second")
	if got.Block != "2026-11-14 first\n    at frame1" || got.Reason != ReasonStart {
		t.Fatalf("unexpected block: %+v", got)
	}
	rest := m.Flush()
	if len(rest) != 1 || rest[0].Block != "2026-11-14 second" || rest[0].Reason != ReasonShutdown {
		t.Fatalf("flush mismatch: %+v", rest)
	}
}

func TestMaxLinesTruncateDropsRest(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, MaxLines: 2, OnOverflow: OverflowTruncate})
	feedNone(t, m, "START a")
	got := feedAll(t, m, "b")
	if got.Block != "START a\nb" || got.Reason != ReasonMaxLines {
		t.Fatalf("expected split at MaxLines, got %+v", got)
	}
	// 同一条记录的剩余续行被丢弃并计数，而不是各自单独成条。
	feedNone(t, m, "c")
	feedNone(t, m, "d")
	if n := m.OverflowDropped(); n != 2 {
		t.Fatalf("overflow dropped = %d, want 2", n)
	}
	// 下一条记录不受影响。
	feedNone(t, m, "START next")
	got = feedAll(t, m, "START after")
	if got.Block != "START next" || got.Reason != ReasonStart {
		t.Fatalf("next record mismatch: %+v", got)
	}
}

func TestMaxLinesSplitKeepsData(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, MaxLines: 2, OnOverflow: OverflowSplit})
	feedNone(t, m, "START a")
	if got := feedAll(t, m, "b"); got.Reason != ReasonMaxLines {
		t.Fatalf("want max_lines, got %+v", got)
	}
	// 溢出续行开启新块，数据保留。
	feedNone(t, m, "c")
	got := feedAll(t, m, "START next")
	if got.Block != "c" || got.Reason != ReasonStart {
		t.Fatalf("overflow chunk mismatch: %+v", got)
	}
	if n := m.OverflowDropped(); n != 0 {
		t.Fatalf("split should not drop lines, got %d", n)
	}
}

func TestNoPatternPassesThrough(t *testing.T) {
	m, _ := New(Options{})
	got := feedAll(t, m, "plain")
	if got.Block != "plain" || got.Reason != ReasonPassthrough {
		t.Fatalf("want pass-through, got %+v", got)
	}
}

// 孤立坏行不能粘进上一条记录：有续行模式时，外来行收尾当前记录并单独成条。
func TestForeignLineClosesBlock(t *testing.T) {
	m, err := New(Options{
		StartPattern:        `^(\d{4}-|\{)`,
		ContinuationPattern: `^\s`,
		MaxLines:            10,
	})
	if err != nil {
		t.Fatal(err)
	}
	feedNone(t, m, `{"ts":"1","level":"INFO"}`)
	res := m.Feed("this line is broken 5 billing")
	if len(res) != 2 {
		t.Fatalf("want closed block + orphan, got %v", res)
	}
	if res[0].Block != `{"ts":"1","level":"INFO"}` || res[0].Reason != ReasonForeign {
		t.Fatalf("previous record should close intact, got %+v", res[0])
	}
	if res[1].Block != "this line is broken 5 billing" || res[1].Reason != ReasonOrphan {
		t.Fatalf("bad line should be its own record, got %+v", res[1])
	}
}

// 正常堆栈：缩进续行全部并入，外来行出现前记录不收尾。
func TestStackStaysOneRecord(t *testing.T) {
	m, _ := New(Options{
		StartPattern:        `^\d{4}-`,
		ContinuationPattern: `^\s`,
		MaxLines:            10,
	})
	feedNone(t, m, "2026-11-14T10:00:00Z ERROR svc panic recovered")
	feedNone(t, m, "    at frame1(svc.go:11)")
	feedNone(t, m, "    at frame2(svc.go:22)")
	res := m.Feed("2026-11-14T10:00:01Z INFO svc next")
	if len(res) != 1 || res[0].Reason != ReasonStart {
		t.Fatalf("stack should close on next start, got %v", res)
	}
	want := "2026-11-14T10:00:00Z ERROR svc panic recovered\n    at frame1(svc.go:11)\n    at frame2(svc.go:22)"
	if res[0].Block != want {
		t.Fatalf("stack block mismatch:\n%s", res[0].Block)
	}
}

// 未设置续行模式时，flush_on_complete 让解析器链参与边界判断：
// 当前块已能完整解析，外来行就不能并进去。
func TestFlushOnComplete(t *testing.T) {
	complete := func(block string) bool {
		return len(block) > 0 && block[0] == '{' && block[len(block)-1] == '}'
	}
	m, err := New(Options{
		StartPattern:    `^\{`,
		MaxLines:        10,
		FlushOnComplete: true,
		Complete:        complete,
	})
	if err != nil {
		t.Fatal(err)
	}
	feedNone(t, m, `{"a":1}`)
	res := m.Feed("this line is broken")
	if len(res) != 2 || res[0].Reason != ReasonComplete || res[1].Reason != ReasonOrphan {
		t.Fatalf("complete block should close before foreign line, got %v", res)
	}

	// 块还不完整时，非起始行仍然并入（旧行为）。
	feedNone(t, m, `{"a":`)
	feedNone(t, m, `1}`)
	got := feedAll(t, m, `{"b":2}`)
	if got.Block != "{\"a\":\n1}" || got.Reason != ReasonStart {
		t.Fatalf("incomplete block should keep merging, got %+v", got)
	}
}

// 文件开头就是续行：emit 策略单独成条，buffer 策略聚成一块。
func TestOrphanPolicies(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, ContinuationPattern: `^\s`, Orphan: OrphanEmit})
	got := feedAll(t, m, "    at frame9")
	if got.Reason != ReasonOrphan {
		t.Fatalf("want orphan, got %+v", got)
	}

	m, _ = New(Options{StartPattern: `^START`, ContinuationPattern: `^\s`, Orphan: OrphanBuffer})
	feedNone(t, m, "    at frame9")
	feedNone(t, m, "    at frame10")
	got = feedAll(t, m, "START next")
	if got.Block != "    at frame9\n    at frame10" || got.Reason != ReasonStart {
		t.Fatalf("buffered orphans should regroup, got %+v", got)
	}
}

// 等待超时：超过 FlushAfter 没有新行，记录按 timeout 收尾。
func TestTimeoutFlush(t *testing.T) {
	m, _ := New(Options{StartPattern: `^START`, FlushAfter: 2 * time.Second})
	start := time.Now()
	feedNone(t, m, "START a")
	if res := m.FlushTimeout(start.Add(time.Second)); len(res) != 0 {
		t.Fatalf("not yet expired, got %v", res)
	}
	res := m.FlushTimeout(start.Add(3 * time.Second))
	if len(res) != 1 || res[0].Block != "START a" || res[0].Reason != ReasonTimeout {
		t.Fatalf("want timeout flush, got %v", res)
	}
	// 收尾后再次调用不应重复输出。
	if res := m.FlushTimeout(start.Add(4 * time.Second)); len(res) != 0 {
		t.Fatalf("already flushed, got %v", res)
	}
}

// 坏起始行（半个 JSON 粘了下一条首行）：作为一条记录保留全部原文，
// 后续的缩进续行仍然并入，下一条起始行不受影响。
func TestBadStartLineKeepsRaw(t *testing.T) {
	m, _ := New(Options{
		StartPattern:        `^(\{|\d{4}-)`,
		ContinuationPattern: `^\s`,
	})
	feedNone(t, m, `{"ts":"2026-11-14T10:00:0`+`2026-11-14T10:00:00Z ERROR svc panic`)
	feedNone(t, m, "    at frame1(svc.go:11)")
	res := m.Feed("2026-11-14T10:00:01Z INFO svc ok")
	if len(res) != 1 || res[0].Reason != ReasonStart {
		t.Fatalf("glued record should close on next start, got %v", res)
	}
	want := `{"ts":"2026-11-14T10:00:0` + "2026-11-14T10:00:00Z ERROR svc panic\n    at frame1(svc.go:11)"
	if res[0].Block != want {
		t.Fatalf("raw block mismatch:\n%s", res[0].Block)
	}
}

func TestInvalidOptions(t *testing.T) {
	if _, err := New(Options{StartPattern: `[`}); err == nil {
		t.Fatal("bad start pattern should error")
	}
	if _, err := New(Options{ContinuationPattern: `^\s`}); err == nil {
		t.Fatal("continuation without start should error")
	}
	if _, err := New(Options{StartPattern: `^X`, Orphan: "nope"}); err == nil {
		t.Fatal("unknown orphan policy should error")
	}
	if _, err := New(Options{StartPattern: `^X`, OnOverflow: "nope"}); err == nil {
		t.Fatal("unknown overflow policy should error")
	}
	if _, err := New(Options{StartPattern: `^X`, FlushOnComplete: true}); err == nil {
		t.Fatal("flush_on_complete without Complete should error")
	}
}
