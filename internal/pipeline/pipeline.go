// Package pipeline 把各个模块串起来。
//
// 数据流：tail → multiline → parser → buffer → router → sinks
//
// 位点提交协议：记录被"解决"（写入所有路由 sink 成功，或被缓冲策略
// 丢弃）后才推进已确认位点；位点按周期 + 退出时持久化。因此：
//   - 优雅退出：全部排空后才做最终提交，重启零重复、零丢失；
//   - 被 kill：重放窗口不超过一个提交间隔，且只重复"已输出但未提交"
//     的尾部，已提交的记录不会再次输出。
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

// item 是缓冲队列里的元素：一条记录及其来源坐标。
type item struct {
	rec map[string]any
	id  offset.FileID
	end int64
}

// Pipeline 是一次运行实例。
type Pipeline struct {
	cfg     *config.Config
	metrics *metrics.Counters

	// mu 保护 merger 与 lastLine*（tailer  goroutine 与超时冲刷 goroutine 共用）。
	mu      sync.Mutex
	merger  *multiline.Merger
	parsers []plugin.Parser
	router  *router.Router
	buf     *buffer.Buffer

	offsetStore *offset.Store
	tracker     *offset.Tracker

	sinks map[string]plugin.Sink

	lastLineID  offset.FileID
	lastLineEnd int64
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

	// 缓冲
	p.buf = buffer.New(cfg.Buffer.Capacity, buffer.Policy(cfg.Buffer.Policy), cfg.Buffer.SpillDir)

	// 位点：加载（含备份回退与 v1 迁移），损坏时隔离后从空状态开始。
	var loaded *offset.File
	if cfg.Offset.Path != "" {
		p.offsetStore = offset.New(cfg.Offset.Path)
		f, info, err := p.offsetStore.Load()
		switch {
		case err == offset.ErrCorrupt:
			p.metrics.Inc("offset_corrupt_total", 1)
			fmt.Fprintf(os.Stderr, "offset file corrupt, quarantined and starting fresh: %s\n", p.offsetStore.Path())
			p.offsetStore.Quarantine()
			f = &offset.File{Version: 2, Paths: map[string]*offset.PathState{}}
		case err != nil:
			return nil, fmt.Errorf("load offsets: %w", err)
		}
		if info.RecoveredFromBak {
			p.metrics.Inc("offset_recovered_from_bak", 1)
		}
		if info.Migrated {
			p.metrics.Inc("offset_migrated", 1)
		}
		if info.NewerVersion {
			p.metrics.Inc("offset_newer_version", 1)
		}
		loaded = f
	} else {
		loaded = &offset.File{Version: 2, Paths: map[string]*offset.PathState{}}
	}
	p.tracker = offset.NewTracker(cfg.Source.Path, loaded)

	return p, nil
}

// Run 阻塞运行直到 ctx 结束，然后按协议收尾：排空管线、附着半行缓存、
// 最终提交位点、关闭输出端。
func (p *Pipeline) Run(ctx context.Context) error {
	var consumeWg sync.WaitGroup
	consumeWg.Add(1)
	go func() {
		defer consumeWg.Done()
		p.consume()
	}()

	stopBg := make(chan struct{})
	var bgWg sync.WaitGroup
	if p.offsetStore != nil {
		bgWg.Add(1)
		go func() {
			defer bgWg.Done()
			p.persistLoop(stopBg)
		}()
	}
	if p.cfg.Multiline.FlushAfterMS > 0 {
		bgWg.Add(1)
		go func() {
			defer bgWg.Done()
			p.flushLoop(stopBg)
		}()
	}

	t := tail.New(tail.Options{
		Path:            p.cfg.Source.Path,
		PollInterval:    time.Duration(p.cfg.Source.PollIntervalMS) * time.Millisecond,
		Resume:          p.tracker.Resume(),
		LegacyOffset:    p.tracker.LegacyOffset(),
		DrainQuietPolls: p.cfg.Source.RotateDrainQuietPolls,
		DrainMax:        time.Duration(p.cfg.Source.RotateDrainMaxMS) * time.Millisecond,
		BacklogGlob:     p.cfg.Source.BacklogGlob,
		OnEvent:         p.onTailEvent,
	}, p.handleLine, func(err error) {
		fmt.Fprintf(os.Stderr, "tail error: %v\n", err)
	})
	// legacy 位点已交给 tailer 一次性使用，提交状态里不再保留。
	p.tracker.ConsumeLegacy()

	reports, err := t.Run(ctx)

	// 收尾：把合并器里没发出的记录刷出去。
	p.mu.Lock()
	if block := p.merger.Flush(); block != "" {
		p.handleBlockLocked(block, p.lastLineID, p.lastLineEnd)
	}
	p.mu.Unlock()

	p.buf.Close()
	consumeWg.Wait() // 消费者排空：所有记录都已 resolve
	close(stopBg)
	bgWg.Wait()

	// 先 flush 输出端，再做最终位点提交：提交的内容必须已经落盘。
	for _, s := range p.sinks {
		_ = s.Flush()
	}

	attached, dropped, pendingBytes := p.tracker.AttachReports(reports)
	p.metrics.Inc("pending_lines_persisted", int64(attached))
	p.metrics.Inc("pending_bytes_persisted", pendingBytes)
	p.metrics.Inc("pending_dropped", int64(dropped))
	p.metrics.Set("open_sources", 0)
	p.updateLag()

	if p.offsetStore != nil {
		if saveErr := p.offsetStore.Save(p.tracker.Snapshot()); saveErr != nil {
			p.metrics.Inc("offset_commit_failed", 1)
			p.tracker.MarkDirty()
			if err == nil {
				err = saveErr
			}
		} else {
			p.metrics.Inc("offset_commits_total", 1)
		}
	}

	for _, s := range p.sinks {
		_ = s.Close()
	}
	return err
}

// onTailEvent 把采集侧事件接入指标与提交状态。
func (p *Pipeline) onTailEvent(ev tail.Event) {
	switch ev.Kind {
	case tail.EventOpen:
		p.metrics.Inc("source_opened", 1)
		p.metrics.Set("open_sources", p.metrics.Get("open_sources")+1)
	case tail.EventCurrent:
		p.tracker.MarkCurrent(ev.ID)
	case tail.EventRotation:
		p.metrics.Inc("rotations_detected", 1)
	case tail.EventTruncate:
		// 截断后该身份的已确认位点失效，归零重新增长。
		p.metrics.Inc("truncations_detected", 1)
		p.tracker.Reset(ev.ID)
	case tail.EventBacklogOpen:
		p.metrics.Inc("backlog_files_recovered", 1)
		p.metrics.Set("open_sources", p.metrics.Get("open_sources")+1)
	case tail.EventBacklogLost:
		p.metrics.Inc("backlog_bytes_lost", ev.Detail)
	case tail.EventDrainSeal:
		p.metrics.Inc("rotated_sources_sealed", 1)
		p.metrics.Set("open_sources", p.metrics.Get("open_sources")-1)
		if ev.Detail >= 0 {
			p.tracker.SetSize(ev.ID, ev.Detail)
		}
	case tail.EventPendingResumed:
		p.metrics.Inc("pending_lines_resumed", 1)
	case tail.EventPendingDropped:
		p.metrics.Inc("pending_dropped", 1)
	case tail.EventResumed:
		p.metrics.Inc("offset_resumes", 1)
	}
}

func (p *Pipeline) handleLine(l tail.Line) {
	p.metrics.Inc("lines_read", 1)
	p.metrics.Inc("bytes_read", int64(len(l.Text))+1)

	p.mu.Lock()
	defer p.mu.Unlock()
	block, closedPrev := p.merger.Feed(l.Text)
	if block != "" {
		if closedPrev {
			// 块由本行之前的行组成，结束坐标是上一行的。
			p.handleBlockLocked(block, p.lastLineID, p.lastLineEnd)
		} else {
			p.handleBlockLocked(block, l.ID, l.End)
		}
	}
	p.lastLineID, p.lastLineEnd = l.ID, l.End
}

// handleBlockLocked 把一条完整记录送入缓冲。调用方必须持有 p.mu。
// 被缓冲策略丢弃的记录立即 resolve，避免位点停滞。
func (p *Pipeline) handleBlockLocked(block string, id offset.FileID, end int64) {
	p.metrics.Inc("blocks_formed", 1)
	record := p.parse(block)
	dropped, err := p.buf.Push(item{rec: record, id: id, end: end})
	if err == nil && dropped {
		p.metrics.Inc("dropped", 1)
		p.tracker.Resolve(id, end)
	}
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

func (p *Pipeline) consume() {
	for {
		raw, ok := p.buf.Pop()
		if !ok {
			return
		}
		it := raw.(item)
		for _, name := range p.router.Route(it.rec) {
			s, ok := p.router.Sink(name)
			if !ok {
				continue
			}
			if err := s.Write(it.rec); err != nil {
				p.metrics.Inc("write_failed", 1)
				continue
			}
			p.metrics.Inc("emitted", 1)
		}
		// 记录已解决：推进已确认位点。
		p.tracker.Resolve(it.id, it.end)
		p.metrics.Inc("records_resolved", 1)
	}
}

// persistLoop 按周期持久化已确认位点。崩溃的重放窗口由这个间隔界定。
func (p *Pipeline) persistLoop(stop <-chan struct{}) {
	interval := time.Duration(p.cfg.Offset.CommitIntervalMS) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.updateLag()
			if !p.tracker.Dirty() {
				continue
			}
			if err := p.offsetStore.Save(p.tracker.Snapshot()); err != nil {
				p.metrics.Inc("offset_commit_failed", 1)
				p.tracker.MarkDirty()
				fmt.Fprintf(os.Stderr, "commit offsets: %v\n", err)
			} else {
				p.metrics.Inc("offset_commits_total", 1)
			}
		}
	}
}

// flushLoop 把超时未合拢的多行记录刷出去，避免它们无限期压住位点。
func (p *Pipeline) flushLoop(stop <-chan struct{}) {
	interval := time.Duration(p.cfg.Multiline.FlushAfterMS) * time.Millisecond / 2
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			p.mu.Lock()
			if block := p.merger.FlushIfStale(time.Now()); block != "" {
				p.handleBlockLocked(block, p.lastLineID, p.lastLineEnd)
			}
			p.mu.Unlock()
		}
	}
}

// updateLag 记录"已成块但未解决"的记录数（提交滞后）。
func (p *Pipeline) updateLag() {
	p.metrics.Set("commit_lag_records", p.metrics.Get("blocks_formed")-p.metrics.Get("records_resolved"))
}

// Metrics 返回计数器快照。
func (p *Pipeline) Metrics() map[string]int64 { return p.metrics.Snapshot() }
