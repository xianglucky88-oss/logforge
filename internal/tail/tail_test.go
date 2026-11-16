package tail

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/logforge/internal/offset"
)

// harness 驱动一个 tailer 并收集行与事件。
type harness struct {
	mu     sync.Mutex
	lines  []Line
	events []Event
	errs   []error
}

func newHarness() *harness { return &harness{} }

func (h *harness) onLine(l Line) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, l)
}

func (h *harness) onEvent(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
}

func (h *harness) onError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errs = append(h.errs, err)
}

func (h *harness) lineTexts() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.lines))
	for i, l := range h.lines {
		out[i] = l.Text
	}
	return out
}

func (h *harness) countEvent(k EventKind) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, ev := range h.events {
		if ev.Kind == k {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

func (h *harness) run(ctx context.Context, opts Options) <-chan []offset.ReadReport {
	h.mu.Lock()
	defer h.mu.Unlock()
	if opts.PollInterval == 0 {
		opts.PollInterval = 10 * time.Millisecond
	}
	if opts.DrainQuietPolls == 0 {
		opts.DrainQuietPolls = 2
	}
	if opts.DrainMax == 0 {
		opts.DrainMax = 5 * time.Second
	}
	opts.OnEvent = h.onEvent
	done := make(chan []offset.ReadReport, 1)
	tl := New(opts, h.onLine, h.onError)
	go func() {
		reps, err := tl.Run(ctx)
		if err != nil {
			h.onError(err)
		}
		done <- reps
	}()
	return done
}

func writeLines(t *testing.T, path string, lines ...string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, l := range lines {
		if _, err := fmt.Fprintln(f, l); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBasicTailAndPendingReport(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "line1", "line2")
	// 半行：没有换行结尾。
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, "partial")
	f.Close()

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path})

	waitFor(t, "2 lines", func() bool { return len(h.lineTexts()) == 2 })
	cancel()
	reps := <-done

	if got := h.lineTexts(); got[0] != "line1" || got[1] != "line2" {
		t.Fatalf("lines mismatch: %v", got)
	}
	if len(reps) != 1 {
		t.Fatalf("want 1 report, got %d", len(reps))
	}
	rep := reps[0]
	if string(rep.Pending) != "partial" {
		t.Fatalf("pending mismatch: %q", rep.Pending)
	}
	// 半行起点 = 两个完整行之后。
	wantStart := int64(len("line1\nline2\n"))
	if rep.PendingStart != wantStart || rep.ReadPos != wantStart+7 {
		t.Fatalf("offsets mismatch: start=%d pos=%d", rep.PendingStart, rep.ReadPos)
	}
}

func TestRotationWhileRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "a1", "a2", "a3")

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path})
	waitFor(t, "3 lines", func() bool { return len(h.lineTexts()) == 3 })

	// 轮转：改名 + 旧文件仍有写入 + 新文件出现。
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path+".1", "a4-late") // 写到已轮转的旧文件
	writeLines(t, path, "b1", "b2")     // 新文件

	waitFor(t, "6 lines", func() bool { return len(h.lineTexts()) == 6 })
	cancel()
	<-done

	got := h.lineTexts()
	want := map[string]bool{"a1": true, "a2": true, "a3": true, "a4-late": true, "b1": true, "b2": true}
	for _, l := range got {
		delete(want, l)
	}
	if len(want) != 0 {
		t.Fatalf("missing lines: %v (got %v)", want, got)
	}
	if h.countEvent(EventRotation) != 1 {
		t.Fatalf("want 1 rotation, got %d", h.countEvent(EventRotation))
	}
	if h.countEvent(EventDrainSeal) != 1 {
		t.Fatalf("want 1 drain seal, got %d", h.countEvent(EventDrainSeal))
	}
}

func TestTruncateWhileRunning(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "old1", "old2", "old3")

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path})
	waitFor(t, "3 lines", func() bool { return len(h.lineTexts()) == 3 })

	// copytruncate：同一 inode 截断为 0。
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "new1", "new2")

	waitFor(t, "5 lines", func() bool { return len(h.lineTexts()) == 5 })
	cancel()
	<-done

	got := h.lineTexts()
	if got[3] != "new1" || got[4] != "new2" {
		t.Fatalf("post-truncate lines mismatch: %v", got)
	}
	if h.countEvent(EventTruncate) != 1 {
		t.Fatalf("want 1 truncate event, got %d", h.countEvent(EventTruncate))
	}
}

func TestStartupBacklogScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	// 模拟停机期间轮转：旧文件被改名且有未读内容，新文件已创建。
	writeLines(t, path, "r1", "r2", "r3", "r4")
	id, ok := identify(path)
	if !ok {
		t.Fatal("identify failed")
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "n1")

	// 位点：旧身份读完了 r1、r2 两行。
	resume := map[offset.FileID]offset.State{
		id: {Offset: int64(len("r1\nr2\n")), Size: int64(len("r1\nr2\n"))},
	}

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path, Resume: resume})

	waitFor(t, "3 lines", func() bool { return len(h.lineTexts()) == 3 })
	cancel()
	<-done

	got := h.lineTexts()
	want := map[string]bool{"r3": true, "r4": true, "n1": true}
	for _, l := range got {
		delete(want, l)
	}
	if len(want) != 0 {
		t.Fatalf("missing: %v (got %v)", want, got)
	}
	if h.countEvent(EventBacklogOpen) != 1 {
		t.Fatalf("want backlog open, got %d", h.countEvent(EventBacklogOpen))
	}
}

func TestPendingResumeAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "line1")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, "partial")
	f.Close()

	// 第一次运行：读出 line1，半行 partial 留在报告里。
	h1 := newHarness()
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := h1.run(ctx1, Options{Path: path})
	waitFor(t, "1 line", func() bool { return len(h1.lineTexts()) == 1 })
	cancel1()
	reps := <-done1
	if len(reps) != 1 || string(reps[0].Pending) != "partial" {
		t.Fatalf("reports mismatch: %+v", reps)
	}

	// 模拟管线：line1 已确认，半行对齐后持久化。
	resume := map[offset.FileID]offset.State{
		reps[0].ID: {Offset: reps[0].PendingStart, Pending: reps[0].Pending, Size: reps[0].Size},
	}

	// 停机期间写者补完了这一行。
	f, _ = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, "-continued\n")
	f.Close()

	h2 := newHarness()
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := h2.run(ctx2, Options{Path: path, Resume: resume})
	waitFor(t, "resumed line", func() bool { return len(h2.lineTexts()) == 1 })
	cancel2()
	<-done2

	if got := h2.lineTexts()[0]; got != "partial-continued" {
		t.Fatalf("want joined line, got %q", got)
	}
	if h2.countEvent(EventPendingResumed) != 1 {
		t.Fatalf("want pending resumed, got %d", h2.countEvent(EventPendingResumed))
	}
}

func TestPendingVerifyMismatchDropsPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "line1")
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, "partial")
	f.Close()

	id, _ := identify(path)
	resume := map[offset.FileID]offset.State{
		id: {Offset: 6, Pending: []byte("partial"), Size: 13},
	}

	// 文件内容被改写（同 inode，pending 区域不再是原内容）。
	if err := os.WriteFile(path, []byte("line1\nREWRITTEN\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path, Resume: resume})
	waitFor(t, "2 lines", func() bool { return len(h.lineTexts()) == 2 })
	cancel()
	<-done

	if h.countEvent(EventPendingDropped) != 1 {
		t.Fatalf("want pending dropped, got %d", h.countEvent(EventPendingDropped))
	}
	// 内容被改写后从头重读。
	got := h.lineTexts()
	if got[0] != "line1" || got[1] != "REWRITTEN" {
		t.Fatalf("lines mismatch: %v", got)
	}
}

func TestBacklogLostWhenFileDeleted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "x1", "x2")
	id, _ := identify(path)
	size := fileSize(path)

	// 停机期间文件被删除：有未读内容但找不回。
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	resume := map[offset.FileID]offset.State{
		id: {Offset: 0, Size: size},
	}

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path, Resume: resume})
	waitFor(t, "backlog lost event", func() bool { return h.countEvent(EventBacklogLost) == 1 })
	cancel()
	<-done
}

func TestRapidPathReuse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	writeLines(t, path, "gen1-a", "gen1-b")

	h := newHarness()
	ctx, cancel := context.WithCancel(context.Background())
	done := h.run(ctx, Options{Path: path})
	waitFor(t, "2 lines", func() bool { return len(h.lineTexts()) == 2 })

	// 同一路径快速复用：连续两轮轮转。
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "gen2-a")
	waitFor(t, "3 lines", func() bool { return len(h.lineTexts()) == 3 })
	if err := os.Rename(path, path+".2"); err != nil {
		t.Fatal(err)
	}
	writeLines(t, path, "gen3-a")

	waitFor(t, "4 lines", func() bool { return len(h.lineTexts()) == 4 })
	cancel()
	<-done

	got := h.lineTexts()
	want := map[string]bool{"gen1-a": true, "gen1-b": true, "gen2-a": true, "gen3-a": true}
	for _, l := range got {
		delete(want, l)
	}
	if len(want) != 0 {
		t.Fatalf("missing: %v (got %v)", want, got)
	}
	if h.countEvent(EventRotation) != 2 {
		t.Fatalf("want 2 rotations, got %d", h.countEvent(EventRotation))
	}
}
