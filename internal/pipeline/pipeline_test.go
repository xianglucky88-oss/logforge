package pipeline

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/internal-platform/logforge/internal/config"
	"github.com/internal-platform/logforge/internal/plugin"
)

// captureSink 记录收到的内容；fail 为真时 Write 返回错误。
type captureSink struct {
	mu       sync.Mutex
	name     string
	fail     bool
	received []map[string]any
}

func (s *captureSink) Name() string { return s.name }
func (s *captureSink) Write(rec map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return fmt.Errorf("boom")
	}
	s.received = append(s.received, rec)
	return nil
}
func (s *captureSink) Flush() error { return nil }
func (s *captureSink) Close() error { return nil }

func (s *captureSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

var (
	capturedMu sync.Mutex
	captured   = map[string]*captureSink{}
)

func init() {
	plugin.RegisterSink("capture", func(cfg map[string]any) (plugin.Sink, error) {
		name, _ := cfg["name"].(string)
		if name == "" {
			return nil, fmt.Errorf("capture sink requires name")
		}
		fail, _ := cfg["fail"].(bool)
		s := &captureSink{name: name, fail: fail}
		capturedMu.Lock()
		captured[name] = s
		capturedMu.Unlock()
		return s, nil
	})
}

func captureByName(t *testing.T, name string) *captureSink {
	t.Helper()
	capturedMu.Lock()
	defer capturedMu.Unlock()
	s, ok := captured[name]
	if !ok {
		t.Fatalf("capture sink %q not created", name)
	}
	return s
}

func buildPipeline(t *testing.T, yamlBody string) *Pipeline {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "logforge.yaml")
	if err := os.WriteFile(path, []byte(yamlBody), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("new pipeline: %v", err)
	}
	p.SetErrorWriter(&strings.Builder{})
	return p
}

func cfgWith(rules, sinks string) string {
	return `
source: {path: /tmp/app.log}
route:
  rules:
` + rules + `
  default: [archive]
sinks:
  - {name: archive, type: capture, config: {name: archive}}
  - {name: audit, type: capture, config: {name: audit}}
  - {name: alert, type: capture, config: {name: alert}}
` + sinks
}

func TestDeliverFanOutMergesRules(t *testing.T) {
	p := buildPipeline(t, cfgWith(`
    - {name: errors, match: {level: ERROR}, sinks: [alert]}
    - {name: billing, match: {service: billing}, sinks: [audit]}
`, ""))
	rec := map[string]any{"level": "ERROR", "service": "billing"}
	p.deliver(rec)

	if got := captureByName(t, "alert").count(); got != 1 {
		t.Fatalf("alert got %d records, want 1", got)
	}
	if got := captureByName(t, "audit").count(); got != 1 {
		t.Fatalf("audit got %d records, want 1", got)
	}
	if got := captureByName(t, "archive").count(); got != 0 {
		t.Fatalf("archive got %d records, want 0", got)
	}

	m := p.Metrics()
	// 一条记录、两个出口：记录维度只计一次，投递维度计两次。
	if m["records_routed"] != 1 || m["records_matched"] != 1 || m["records_default"] != 0 {
		t.Fatalf("record counters wrong: %v", m)
	}
	if m["emitted"] != 2 || m["records_delivered"] != 1 || m["records_lost"] != 0 {
		t.Fatalf("delivery counters wrong: %v", m)
	}
	if m["write_failed"] != 0 {
		t.Fatalf("write_failed = %d", m["write_failed"])
	}
}

func TestDeliverSameSinkFromTwoRulesOnce(t *testing.T) {
	p := buildPipeline(t, cfgWith(`
    - {name: errors, match: {level: ERROR}, sinks: [audit]}
    - {name: billing, match: {service: billing}, sinks: [audit]}
`, ""))
	p.deliver(map[string]any{"level": "ERROR", "service": "billing"})

	if got := captureByName(t, "audit").count(); got != 1 {
		t.Fatalf("audit got %d records, want 1 (dedup)", got)
	}
	if got := p.Metrics()["emitted"]; got != 1 {
		t.Fatalf("emitted = %d, want 1", got)
	}
}

func TestDeliverFallsBackToDefault(t *testing.T) {
	p := buildPipeline(t, cfgWith(`
    - {name: errors, match: {level: ERROR}, sinks: [alert]}
`, ""))
	p.deliver(map[string]any{"level": "INFO"})

	if got := captureByName(t, "archive").count(); got != 1 {
		t.Fatalf("archive got %d records, want 1", got)
	}
	if got := captureByName(t, "alert").count(); got != 0 {
		t.Fatalf("alert got %d records, want 0", got)
	}
	m := p.Metrics()
	if m["records_matched"] != 0 || m["records_default"] != 1 {
		t.Fatalf("counters wrong: %v", m)
	}
}

// 一个出口写失败：其他出口照常收到，失败被报告而不是静默吞掉。
func TestDeliverReportsWriteFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "logforge.yaml")
	body := `
source: {path: /tmp/app.log}
route:
  rules:
    - {name: errors, match: {level: ERROR}, sinks: [broken, audit]}
  default: [archive]
sinks:
  - {name: archive, type: capture, config: {name: archive}}
  - {name: audit, type: capture, config: {name: audit}}
  - {name: broken, type: capture, config: {name: broken, fail: true}}
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var logs strings.Builder
	p.SetErrorWriter(&logs)
	p.deliver(map[string]any{"level": "ERROR"})

	if got := captureByName(t, "audit").count(); got != 1 {
		t.Fatalf("audit got %d records, want 1 despite broken sink", got)
	}
	if !strings.Contains(logs.String(), `sink "broken"`) {
		t.Fatalf("write failure was not reported: %q", logs.String())
	}
}

// 一个出口写失败：其他出口照常收到，计数区分记录与投递。
func TestDeliverIsolatesWriteFailure(t *testing.T) {
	p := buildPipeline(t, `
source: {path: /tmp/app.log}
route:
  rules:
    - {name: errors, match: {level: ERROR}, sinks: [broken, audit]}
  default: [archive]
sinks:
  - {name: archive, type: capture, config: {name: archive}}
  - {name: audit, type: capture, config: {name: audit}}
  - {name: broken, type: capture, config: {name: broken, fail: true}}
`)
	p.deliver(map[string]any{"level": "ERROR"})

	if got := captureByName(t, "audit").count(); got != 1 {
		t.Fatalf("audit got %d records, want 1 despite broken sink", got)
	}
	m := p.Metrics()
	if m["write_failed"] != 1 {
		t.Fatalf("write_failed = %d, want 1", m["write_failed"])
	}
	if m["emitted"] != 1 {
		t.Fatalf("emitted = %d, want 1", m["emitted"])
	}
	if m["records_delivered"] != 1 || m["records_lost"] != 0 {
		t.Fatalf("delivery counters = %v", m)
	}
	if m["sink_write_failed.broken"] != 1 || m["sink_written.audit"] != 1 {
		t.Fatalf("per-sink counters = %v", m)
	}
}

func TestDeliverReportsRecordLostWhenAllSinksFail(t *testing.T) {
	p := buildPipeline(t, `
source: {path: /tmp/app.log}
route:
  rules: []
  default: [broken]
sinks:
  - {name: broken, type: capture, config: {name: broken, fail: true}}
`)
	p.deliver(map[string]any{"level": "ERROR"})

	m := p.Metrics()
	if m["records_lost"] != 1 || m["records_delivered"] != 0 {
		t.Fatalf("counters = %v", m)
	}
	if m["write_failed"] != 1 || m["emitted"] != 0 {
		t.Fatalf("counters = %v", m)
	}
}

func TestDeliverRecordsTypeMismatchMetric(t *testing.T) {
	p := buildPipeline(t, cfgWith(`
    - {name: code, match: {code: 507}, sinks: [alert]}
`, ""))
	// 字段存在但类型不同（字符串 vs 数字）：不命中，记一次 mismatch。
	p.deliver(map[string]any{"code": "507"})
	m := p.Metrics()
	if m["route_type_mismatch"] != 1 {
		t.Fatalf("route_type_mismatch = %d, want 1", m["route_type_mismatch"])
	}
	if m["records_default"] != 1 {
		t.Fatalf("records_default = %d, want 1", m["records_default"])
	}
}

// 配置错误在启动期暴露：未知输出端不应等到运行期才发现。
func TestNewRejectsBadRoute(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	body := `
source: {path: /tmp/app.log}
route:
  rules:
    - {name: r, match: {level: ERROR}, sinks: [nope]}
  default: [archive]
sinks:
  - {name: archive, type: capture, config: {name: archive}}
`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := config.Load(path); err == nil {
		t.Fatal("expected config.Load to reject unknown sink")
	}
}
