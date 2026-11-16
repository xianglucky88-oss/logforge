package pipeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/internal-platform/logforge/internal/config"
	_ "github.com/internal-platform/logforge/internal/sink" // 注册 file/faulty sink
)

// baseCfg 构造一份只依赖临时目录、可独立运行的最小配置。
func baseCfg(t *testing.T, policy string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	c := &config.Config{}
	c.Source.Path = filepath.Join(dir, "app.log")
	c.Source.PollIntervalMS = 50
	c.Buffer.Capacity = 8
	c.Buffer.Policy = policy
	c.Buffer.SpillDir = filepath.Join(dir, "spill")
	c.Buffer.SpillSegmentBytes = 4096
	c.Buffer.BlockTimeoutMS = 500
	c.Buffer.BlockTimeoutAction = "drop_new"
	c.Buffer.SpillFullAction = "drop_new"
	c.Output.MaxRetries = 2
	c.Output.RetryBackoffMS = 5
	c.Output.WriteTimeoutMS = 200
	c.Output.DrainTimeoutMS = 1000
	c.Offset.Path = filepath.Join(dir, "offset.json")
	c.Route.Default = []string{"all"}
	return c
}

// writeFileSinkCfg 给配置挂一个 file sink。
func writeFileSinkCfg(c *config.Config, path string) {
	c.Sinks = append(c.Sinks, struct {
		Name   string         `yaml:"name"`
		Type   string         `yaml:"type"`
		Config map[string]any `yaml:"config"`
	}{Name: "all", Type: "file", Config: map[string]any{"path": path}})
}

// feed 直接走 handleBlock 注入 N 条记录，绕过 tail。
func feed(p *Pipeline, n int) {
	for i := 0; i < n; i++ {
		p.handleBlock(`{"msg":"hello","i":` + itoa(i) + `}`)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestPipelineEndToEndEmission(t *testing.T) {
	cfg := baseCfg(t, "drop_oldest")
	cfg.Buffer.Capacity = 64 // 足够大，避免消费者启动前的瞬时满队导致丢弃
	out := filepath.Join(t.TempDir(), "out.log")
	writeFileSinkCfg(cfg, out)

	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go p.consume()
	feed(p, 50)

	// 等待全部发出。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Metrics()["emitted"] >= 50 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := p.buf.Close(); err != nil {
		t.Fatal(err)
	}
	m := p.Metrics()
	if m["emitted"] != 50 {
		t.Fatalf("emitted=%d want 50", m["emitted"])
	}
	if lines := countLines(t, out); lines != 50 {
		t.Fatalf("output lines=%d want 50", lines)
	}
}

func TestPipelineSlowSinkDoesNotStall(t *testing.T) {
	// 慢 sink（每次 Write 阻塞）+ 写超时，验证消费协程不会被永久卡死，
	// 且失败记录走有界重试后进入 dead-letter，缓冲最终能关闭排空。
	cfg := baseCfg(t, "block")
	cfg.Buffer.Capacity = 32
	cfg.Output.MaxRetries = 1
	cfg.Output.RetryBackoffMS = 2
	cfg.Sinks = append(cfg.Sinks, struct {
		Name   string         `yaml:"name"`
		Type   string         `yaml:"type"`
		Config map[string]any `yaml:"config"`
	}{
		Name: "all", Type: "faulty",
		Config: map[string]any{
			"path":         filepath.Join(t.TempDir(), "out.log"),
			"block_ms":     500, // 远超 200ms 写超时
			"fail_first_n": 0,
		},
	})

	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go p.consume()
	feed(p, 5)

	// 每条记录：1 次尝试 + 1 次重试，每次受 200ms 写超时约束后转死信。
	// 以 dead-letter 计数到达为准轮询，而不是假设 consume 会自行退出。
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if p.Metrics()["dead_lettered"] >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := p.buf.Close(); err != nil {
		t.Fatal(err)
	}

	m := p.Metrics()
	if m["dead_lettered"] < 5 {
		t.Fatalf("dead_lettered=%d want >=5, write_failed=%d", m["dead_lettered"], m["write_failed"])
	}
	// dead-letter 段文件应当落盘，可人工检查。
	matches, _ := filepath.Glob(filepath.Join(cfg.Buffer.SpillDir, "dead-*"))
	if len(matches) == 0 {
		t.Fatal("dead-letter segment not written")
	}
}

func TestPipelineFailingSinkRetriesThenDeadLetters(t *testing.T) {
	cfg := baseCfg(t, "drop_new")
	cfg.Sinks = append(cfg.Sinks, struct {
		Name   string         `yaml:"name"`
		Type   string         `yaml:"type"`
		Config map[string]any `yaml:"config"`
	}{
		Name: "all", Type: "faulty",
		Config: map[string]any{
			"path":         filepath.Join(t.TempDir(), "out.log"),
			"fail_first_n": 100, // 永远失败
		},
	})

	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go p.consume()
	feed(p, 3)

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if p.Metrics()["dead_lettered"] >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	_ = p.buf.Close()
	m := p.Metrics()
	if m["dead_lettered"] != 3 {
		t.Fatalf("dead_lettered=%d want 3", m["dead_lettered"])
	}
	if m["write_failed"] == 0 {
		t.Fatal("expected write_failed to be counted")
	}
	if m["requeued"] == 0 {
		t.Fatal("expected requeue attempts to be counted")
	}
}

func TestPipelineMetricsIdentityAfterRun(t *testing.T) {
	cfg := baseCfg(t, "spill")
	out := filepath.Join(t.TempDir(), "out.log")
	writeFileSinkCfg(cfg, out)

	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	go p.consume()
	feed(p, 40)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Metrics()["emitted"] >= 40 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = p.buf.Close()

	m := p.Metrics()
	// 三态对账：pushed == emitted + buffered + dropped_total。
	pushed := m["pushed"]
	accounted := m["emitted"] + m["buffered"] + m["dropped"]
	if pushed != accounted {
		t.Fatalf("identity broken: pushed=%d emitted=%d buffered=%d dropped=%d",
			pushed, m["emitted"], m["buffered"], m["dropped"])
	}
}

func TestPipelineBlockPolicyBoundedPush(t *testing.T) {
	// 无消费者时，block 策略的 Push 必须在 BlockTimeout 后走兜底而不是永久阻塞。
	cfg := baseCfg(t, "block")
	cfg.Buffer.BlockTimeoutMS = 60
	cfg.Buffer.BlockTimeoutAction = "drop_new"
	out := filepath.Join(t.TempDir(), "out.log")
	writeFileSinkCfg(cfg, out)

	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 不启动 consume，故意让缓冲堆积。
	start := time.Now()
	feed(p, 40)
	if time.Since(start) > 3*time.Second {
		t.Fatal("Push blocked far beyond configured timeout")
	}
	_ = p.buf.Close()
	m := p.Metrics()
	// 容量 8，超出部分要么在缓冲要么被兜底丢弃，总数守恒。
	if m["pushed"] != 40 {
		t.Fatalf("pushed=%d want 40", m["pushed"])
	}
	if m["buffered"]+m["dropped"] != 40 {
		t.Fatalf("buffered(%d)+dropped(%d) != 40", m["buffered"], m["dropped"])
	}
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, b := range raw {
		if b == '\n' {
			n++
		}
	}
	return n
}
