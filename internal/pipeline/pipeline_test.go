package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/internal-platform/logforge/internal/config"
	_ "github.com/internal-platform/logforge/internal/parser"
	_ "github.com/internal-platform/logforge/internal/sink"
)

// writeConfig 生成一份指向临时目录的测试配置。
func writeConfig(t *testing.T, dir, src, out, off string) string {
	t.Helper()
	cfg := fmt.Sprintf(`
source:
  path: %s
  poll_interval_ms: 10
  rotate_drain_quiet_polls: 2
  rotate_drain_max_ms: 3000

multiline:
  start_pattern: '^{'
  max_lines: 50
  flush_after_ms: 500

parsers:
  - name: json

buffer:
  capacity: 4096
  policy: drop_oldest

route:
  rules: []
  default: [all]

sinks:
  - name: all
    type: file
    config:
      path: %s

offset:
  path: %s
  commit_interval_ms: 50
`, src, out, off)
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func appendLines(t *testing.T, path string, lines ...string) {
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

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// outputNS 读取输出文件里所有记录的 n 字段。
func outputNS(t *testing.T, out string) []int {
	t.Helper()
	raw, err := os.ReadFile(out)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	var ns []int
	for _, line := range splitLines(string(raw)) {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("bad output line %q: %v", line, err)
		}
		if n, ok := rec["n"].(float64); ok {
			ns = append(ns, int(n))
		}
	}
	return ns
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}

// runPipeline 启动管线并返回停止函数（停止会优雅收尾并等待退出）
// 与指标读取函数（停止后调用才有意义）。
func runPipeline(t *testing.T, cfgPath string) (stop func(), metrics func() map[string]int64) {
	t.Helper()
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("pipeline run: %v", err)
			}
		})
	}, p.Metrics
}

// TestRotationAndRestart 覆盖：运行中轮转 → 优雅重启 → 继续写入。
// 断言：每条记录恰好输出一次（不重复、不丢失）。
func TestRotationAndRestart(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "app.log")
	out := filepath.Join(dir, "out.log")
	off := filepath.Join(dir, "offset.json")
	cfgPath := writeConfig(t, dir, src, out, off)

	// 第一批 50 条。
	var batch []string
	for i := 1; i <= 50; i++ {
		batch = append(batch, fmt.Sprintf(`{"n":%d}`, i))
	}
	appendLines(t, src, batch...)

	stop1, m1 := runPipeline(t, cfgPath)
	waitFor(t, "first 50 records", func() bool { return len(outputNS(t, out)) == 50 })

	// 运行中轮转，新文件继续写。
	if err := os.Rename(src, src+".1"); err != nil {
		t.Fatal(err)
	}
	batch = nil
	for i := 51; i <= 80; i++ {
		batch = append(batch, fmt.Sprintf(`{"n":%d}`, i))
	}
	appendLines(t, src, batch...)
	waitFor(t, "80 records after rotation", func() bool { return len(outputNS(t, out)) == 80 })

	stop1() // 优雅退出：最终提交
	if got := m1()["rotations_detected"]; got < 1 {
		t.Fatalf("rotation not detected: %v", m1())
	}
	if got := m1()["offset_commits_total"]; got < 1 {
		t.Fatalf("no offset commits: %v", m1())
	}

	// 重启：已输出的 80 条不能重复。
	stop2, _ := runPipeline(t, cfgPath)
	batch = nil
	for i := 81; i <= 100; i++ {
		batch = append(batch, fmt.Sprintf(`{"n":%d}`, i))
	}
	appendLines(t, src, batch...)
	waitFor(t, "100 records", func() bool { return len(outputNS(t, out)) == 100 })
	stop2()

	ns := outputNS(t, out)
	sort.Ints(ns)
	if len(ns) != 100 {
		t.Fatalf("want exactly 100 records, got %d (duplicates on restart)", len(ns))
	}
	for i, n := range ns {
		if n != i+1 {
			t.Fatalf("record %d missing or duplicated: %v", i+1, ns)
		}
	}
}

// TestPendingLineAcrossRestart 覆盖：半行 + 终止信号 → 重启后补完。
// 断言：完整行恰好输出一次，不产生碎片。
func TestPendingLineAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "app.log")
	out := filepath.Join(dir, "out.log")
	off := filepath.Join(dir, "offset.json")
	cfgPath := writeConfig(t, dir, src, out, off)

	appendLines(t, src, `{"n":1}`, `{"n":2}`)
	// 半行，没有换行。
	f, _ := os.OpenFile(src, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, `{"n":3`)
	f.Close()

	stop1, m1 := runPipeline(t, cfgPath)
	waitFor(t, "2 records", func() bool { return len(outputNS(t, out)) == 2 })
	stop1() // 优雅退出时半行被持久化

	if got := m1()["pending_lines_persisted"]; got != 1 {
		t.Fatalf("pending not persisted: %v", m1())
	}

	// 停机期间写者补完了这一行，并继续写。
	f, _ = os.OpenFile(src, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprint(f, `,"tail":true}`+"\n")
	f.Close()
	appendLines(t, src, `{"n":4}`)

	stop2, m2 := runPipeline(t, cfgPath)
	waitFor(t, "4 records", func() bool { return len(outputNS(t, out)) == 4 })
	stop2()

	if got := m2()["pending_lines_resumed"]; got != 1 {
		t.Fatalf("pending not resumed: %v", m2())
	}

	// 第三条记录必须是拼完整的 {"n":3,"tail":true}。
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	lines := splitLines(string(raw))
	var third map[string]any
	if err := json.Unmarshal([]byte(lines[2]), &third); err != nil {
		t.Fatal(err)
	}
	if third["n"].(float64) != 3 || third["tail"] != true {
		t.Fatalf("line 3 was not joined across restart: %v", third)
	}
	ns := outputNS(t, out)
	if len(ns) != 4 {
		t.Fatalf("want 4 records, got %d", len(ns))
	}
}
