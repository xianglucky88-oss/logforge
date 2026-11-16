// Package pipeline 把各个模块串起来。
//
// 数据流：tail → multiline → parser → buffer（背压/溢写）→ router → sinks
//
// 关键交互边界：
//
//   - 生产侧 handleBlock 调 Buffer.Push，block 策略下受 ctx 与超时保护；
//   - 消费侧单协程 Pop，sink.Write 带超时，失败有界重试，超限进 dead-letter；
//   - 关闭时先停 tail，再在 DrainTimeout 内排空缓冲，最后 Close 唤醒所有等待；
//   - 指标快照直接取自 Buffer 的同锁 Stats，丢弃数与队列状态不会错位。
package pipeline

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/internal-platform/logforge/internal/buffer"
	"github.com/internal-platform/logforge/internal/config"
	"github.com/internal-platform/logforge/internal/metrics"
	"github.com/internal-platform/logforge/internal/multiline"
	"github.com/internal-platform/logforge/internal/offset"
	"github.com/internal-platform/logforge/internal/plugin"
	"github.com/internal-platform/logforge/internal/router"
	"github.com/internal-platform/logforge/internal/tail"
)

// Pipeline 是一次运行实例。
type Pipeline struct {
	cfg     *config.Config
	metrics *metrics.Counters

	merger  *multiline.Merger
	parsers []plugin.Parser
	router  *router.Router
	buf     *buffer.Buffer

	offsetStore *offset.Store
	offsetFile  *offset.File

	sinks map[string]plugin.Sink

	runCtx       context.Context
	writeTimeout time.Duration
	maxRetries   int
	retryBackoff time.Duration
	drainTimeout time.Duration
}

// New 按配置装配管线。
func New(cfg *config.Config) (*Pipeline, error) {
	p := &Pipeline{cfg: cfg, metrics: &metrics.Counters{}, sinks: map[string]plugin.Sink{}}

	// 输出端
	for _, sc := range cfg.Sinks {
		s, err := plugin.NewSink(sc.Type, sc.Config)
		if err != nil {
			return nil, fmt.Errorf("sink %s: %w", sc.Name, err)
		}
		p.sinks[sc.Name] = s
	}

	// 解析器链
	for _, pc := range cfg.Parsers {
		parser, err := plugin.NewParser(pc.Name, pc.Config)
		if err != nil {
			return nil, fmt.Errorf("parser %s: %w", pc.Name, err)
		}
		p.parsers = append(p.parsers, parser)
	}

	// 路由
	var rules []router.Rule
	for _, r := range cfg.Route.Rules {
		rules = append(rules, router.Rule{Name: r.Name, Match: r.Match, Sinks: r.Sinks})
	}
	rt, err := router.New(rules, p.sinks, cfg.Route.Default)
	if err != nil {
		return nil, err
	}
	p.router = rt

	// 合并器
	merger, err := multiline.New(multiline.Options{
		StartPattern: cfg.Multiline.StartPattern,
		MaxLines:     cfg.Multiline.MaxLines,
		FlushAfter:   time.Duration(cfg.Multiline.FlushAfterMS) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	p.merger = merger

	// 缓冲（含崩溃恢复）
	bcfg := buffer.Config{
		Capacity:           cfg.Buffer.Capacity,
		Policy:             buffer.Policy(cfg.Buffer.Policy),
		SpillDir:           cfg.Buffer.SpillDir,
		SpillSegmentBytes:  cfg.Buffer.SpillSegmentBytes,
		SpillMaxSegments:   cfg.Buffer.SpillMaxSegments,
		SpillMaxBytes:      cfg.Buffer.SpillMaxBytes,
		BlockTimeout:       time.Duration(cfg.Buffer.BlockTimeoutMS) * time.Millisecond,
		BlockTimeoutAction: buffer.FallbackAction(cfg.Buffer.BlockTimeoutAction),
		SpillFullAction:    buffer.FallbackAction(cfg.Buffer.SpillFullAction),
		ManifestEveryAcks:  cfg.Buffer.ManifestEveryAcks,
	}
	buf, rec, err := buffer.New(bcfg)
	if err != nil {
		return nil, fmt.Errorf("buffer: %w", err)
	}
	p.buf = buf
	if rec.Found {
		fmt.Fprintf(os.Stderr, "buffer recovery: generation=%d on_disk=%d dupes=%d corrupt=%d quarantined=%d committed=%d next=%d\n",
			rec.Generation, rec.ReplayedOnDisk, rec.DuplicateFrames, rec.CorruptFrames,
			rec.QuarantinedSegs, rec.CommittedSeq, rec.NextSeq)
	}

	p.writeTimeout = time.Duration(cfg.Output.WriteTimeoutMS) * time.Millisecond
	p.maxRetries = cfg.Output.MaxRetries
	p.retryBackoff = time.Duration(cfg.Output.RetryBackoffMS) * time.Millisecond
	p.drainTimeout = time.Duration(cfg.Output.DrainTimeoutMS) * time.Millisecond

	// 位点
	if cfg.Offset.Path != "" {
		p.offsetStore = offset.New(cfg.Offset.Path)
		f, err := p.offsetStore.Load()
		if err != nil {
			return nil, err
		}
		p.offsetFile = f
	} else {
		p.offsetFile = &offset.File{Version: 1, Offsets: map[string]int64{}}
	}

	return p, nil
}

// Run 阻塞运行直到 ctx 结束。返回退出前的位点。
func (p *Pipeline) Run(ctx context.Context) (int64, error) {
	p.runCtx = ctx
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.consume()
	}()

	start := p.offsetFile.Offsets[p.cfg.Source.Path]
	t := tail.New(tail.Options{
		Path:         p.cfg.Source.Path,
		PollInterval: time.Duration(p.cfg.Source.PollIntervalMS) * time.Millisecond,
		StartOffset:  start,
	}, p.handleLine, func(err error) {
		fmt.Fprintf(os.Stderr, "tail error: %v\n", err)
	})

	off, err := t.Run(ctx)
	// 收尾：把合并器里没发出的记录刷出去（用独立上下文，避免 ctx 已取消被拒）。
	if block := p.merger.Flush(); block != "" {
		p.handleBlock(block)
	}

	// 关闭排空：给消费协程 bounded 时间把剩余记录送出去，然后强制 Close 唤醒。
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	if p.drainTimeout > 0 {
		select {
		case <-done:
		case <-time.After(p.drainTimeout):
		}
	}
	if cerr := p.buf.Close(); cerr != nil {
		fmt.Fprintf(os.Stderr, "buffer close: %v\n", cerr)
	}
	wg.Wait()

	p.offsetFile.Offsets[p.cfg.Source.Path] = off
	if p.offsetStore != nil {
		if saveErr := p.offsetStore.Save(p.offsetFile); saveErr != nil && err == nil {
			err = saveErr
		}
	}

	for _, s := range p.sinks {
		_ = s.Flush()
		_ = s.Close()
	}
	return off, err
}

func (p *Pipeline) handleLine(line string) {
	p.metrics.Inc("lines_read", 1)
	if block := p.merger.Feed(line); block != "" {
		p.handleBlock(block)
	}
}

func (p *Pipeline) handleBlock(block string) {
	p.metrics.Inc("blocks_formed", 1)
	record := p.parse(block)
	res, pushErr := p.buf.Push(p.pushCtx(), record)
	if pushErr != nil {
		// 关闭期间无法入队：计入丢弃，避免记录无声消失。
		p.metrics.Inc("dropped", 1)
		return
	}
	if res.Dropped {
		p.metrics.Inc("dropped", 1)
	}
}

// pushCtx 返回 Push 使用的上下文：运行期用 Run 的 ctx，
// 收尾阶段（Run 返回后 Flush 触发的最后一条）用背景上下文配合 Close 唤醒。
func (p *Pipeline) pushCtx() context.Context {
	if p.runCtx != nil {
		return p.runCtx
	}
	return context.Background()
}

// parse 按注册顺序尝试解析器，全部失败时保留原文。
func (p *Pipeline) parse(block string) map[string]any {
	for _, parser := range p.parsers {
		if rec, err := parser.Parse(block); err == nil {
			p.metrics.Inc("parse_ok", 1)
			return rec
		}
	}
	p.metrics.Inc("parse_failed", 1)
	return map[string]any{"raw": block, "parse_error": true}
}

// consume 是唯一的消费协程。
//
// 退出条件：Pop 返回 ok=false（缓冲已 Close 且排空）。
// sink 写失败 → 有界重试（指数退避）→ 超限 DeadLetter；
// 单次 Write 受 WriteTimeout 约束，慢/挂死 sink 不会永久卡住消费。
func (p *Pipeline) consume() {
	for {
		env, ok := p.buf.Pop()
		if !ok {
			return
		}
		if p.deliver(env) {
			p.buf.Ack(env)
			p.metrics.Inc("emitted", 1)
			continue
		}
		// deliver 已用尽重试：转 dead-letter，记录显式损失而不是静默丢弃。
		if dlErr := p.buf.DeadLetter(env); dlErr != nil {
			fmt.Fprintf(os.Stderr, "dead-letter persist failed seq=%d: %v\n", env.Seq, dlErr)
		}
	}
}

// deliver 尝试把一条记录写入其全部目标 sink，返回是否整体成功。
func (p *Pipeline) deliver(env *buffer.Envelope) bool {
	names := p.router.Route(env.Record)
	for attempt := 0; ; attempt++ {
		failed := false
		for _, name := range names {
			s, ok := p.router.Sink(name)
			if !ok {
				continue
			}
			if err := p.writeOne(s, env.Record); err != nil {
				p.metrics.Inc("write_failed", 1)
				fmt.Fprintf(os.Stderr, "sink %s write failed seq=%d attempt=%d: %v\n",
					name, env.Seq, attempt, err)
				failed = true
			}
		}
		if !failed {
			return true
		}
		if attempt >= p.maxRetries {
			return false
		}
		// 退避期间让出缓冲：Requeue 后记录从队首回来，顺序不被破坏。
		p.buf.Requeue(env)
		p.backoff(attempt)
		// 重新 Pop 同一条记录（Requeue 保证它在重试队首）。
		re, ok := p.buf.Pop()
		if !ok {
			return false
		}
		env = re
	}
}

// writeOne 给不带 context 的 Sink 接口套一层超时与 panic 兜底。
//
// goroutine 在超时后可能仍被挂死的 sink 阻塞，但它不再能阻止消费协程
// 前进：这是“不永久卡死”的边界，goroutine 泄漏量等于挂死调用数，有界。
func (p *Pipeline) writeOne(s plugin.Sink, record map[string]any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("sink panic recovered: %v", r)
		}
	}()
	if p.writeTimeout <= 0 {
		return s.Write(record)
	}
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("sink panic recovered: %v", r)}
			}
		}()
		ch <- result{err: s.Write(record)}
	}()
	select {
	case r := <-ch:
		return r.err
	case <-time.After(p.writeTimeout):
		return fmt.Errorf("sink write timeout after %s", p.writeTimeout)
	}
}

// backoff 指数退避：base * 2^attempt，封顶 5*base，并响应关闭。
func (p *Pipeline) backoff(attempt int) {
	d := p.retryBackoff
	for i := 0; i < attempt && d < 5*p.retryBackoff; i++ {
		d *= 2
	}
	if d > 5*p.retryBackoff {
		d = 5 * p.retryBackoff
	}
	t := time.NewTimer(d)
	defer t.Stop()
	// 关闭时无需精确中断睡眠：drain 超时后 Close 会让下一次 Pop 返回 false。
	<-t.C
}

// Metrics 返回计数器快照，缓冲部分与 Buffer 同锁同源。
func (p *Pipeline) Metrics() map[string]int64 {
	s := p.buf.Stats()
	p.metrics.ApplyBufferStats(
		s.Pushed, s.Buffered(), s.InMemory, s.InRetry, s.OnDisk,
		s.DroppedNew, s.DroppedOldest, s.DroppedBlocked,
		s.DroppedSpillFull, s.DroppedSpillIO, s.DroppedCorrupt, s.DeadLettered,
		s.SpilledWrites, s.SpillReads, s.Requeued,
		s.CorruptFrames, s.QuarantinedSegs, s.ReplayedDupes,
	)
	return p.metrics.Snapshot()
}
