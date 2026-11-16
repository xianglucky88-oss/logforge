// Package pipeline 把各个模块串起来。
//
// 数据流：tail → multiline → parser → buffer → router → sinks
//
// 计数对账关系（verify.sh 会断言）：
//
//	blocks_formed == Σ blocks_closed_<reason>
//	blocks_formed == parse_ok + parse_failed
//	parse_ok      == Σ parse_ok_<parser>
//	单出口时 blocks_formed == emitted + write_failed + dropped
package pipeline

import (
	"context"
	"fmt"
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

	merger      *multiline.Merger
	parsers     []plugin.Parser
	router      *router.Router
	buf         *buffer.Buffer
	annotateKey string
	flushAfter  time.Duration

	offsetStore *offset.Store
	offsetFile  *offset.File

	sinks map[string]plugin.Sink

	stopTimeout chan struct{}
}

// New 按配置装配管线。
func New(cfg *config.Config) (*Pipeline, error) {
	p := &Pipeline{
		cfg:         cfg,
		metrics:     &metrics.Counters{},
		sinks:       map[string]plugin.Sink{},
		stopTimeout: make(chan struct{}),
	}

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

	// 解析成功记录默认标注胜出的解析器，便于解释字段来源；
	// 配置 parse.annotate_parser: "" 可关闭（避免与业务字段冲突）。
	p.annotateKey = "_parser"
	if cfg.Parse.AnnotateParser != nil {
		p.annotateKey = *cfg.Parse.AnnotateParser
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
	p.flushAfter = time.Duration(cfg.Multiline.FlushAfterMS) * time.Millisecond
	mopts := multiline.Options{
		StartPattern:        cfg.Multiline.StartPattern,
		ContinuationPattern: cfg.Multiline.ContinuationPattern,
		MaxLines:            cfg.Multiline.MaxLines,
		FlushAfter:          p.flushAfter,
		FlushOnComplete:     cfg.Multiline.FlushOnComplete,
		Orphan:              multiline.OrphanPolicy(cfg.Multiline.Orphan),
		OnOverflow:          multiline.OverflowPolicy(cfg.Multiline.OnOverflow),
	}
	if mopts.FlushOnComplete {
		// 解析器链的"这条记录是否完整"反馈给边界判断。
		mopts.Complete = p.parsesCleanly
	}
	merger, err := multiline.New(mopts)
	if err != nil {
		return nil, err
	}
	p.merger = merger

	// 缓冲
	p.buf = buffer.New(cfg.Buffer.Capacity, buffer.Policy(cfg.Buffer.Policy), cfg.Buffer.SpillDir)

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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.consume()
	}()

	// 超时收尾：合并器等不到新起始行时，按 flush_after 主动收尾，
	// 否则文件末尾的记录会一直卡住直到进程退出。
	var tickWg sync.WaitGroup
	if p.flushAfter > 0 {
		tickWg.Add(1)
		go func() {
			defer tickWg.Done()
			p.timeoutLoop()
		}()
	}

	start := p.offsetFile.Offsets[p.cfg.Source.Path]
	t := tail.New(tail.Options{
		Path:         p.cfg.Source.Path,
		PollInterval: time.Duration(p.cfg.Source.PollIntervalMS) * time.Millisecond,
		StartOffset:  start,
	}, p.handleLine, func(err error) {
		fmt.Printf("tail error: %v\n", err)
	})

	off, err := t.Run(ctx)

	// 收尾顺序：先停超时循环（保证不再有并发的块输出），
	// 再刷合并器里没发出的记录，最后关闭缓冲让消费端排空。
	if p.flushAfter > 0 {
		close(p.stopTimeout)
		tickWg.Wait()
	}
	for _, res := range p.merger.Flush() {
		p.handleBlock(res.Block, string(res.Reason))
	}
	p.syncMergerStats()
	p.buf.Close()
	wg.Wait()

	p.offsetFile.Offsets[p.cfg.Source.Path] = off
	if p.offsetStore != nil {
		// 当前只在退出时保存一次。
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

// timeoutLoop 周期性检查合并器里等待过久的记录。
func (p *Pipeline) timeoutLoop() {
	interval := p.flushAfter / 2
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	if interval > time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopTimeout:
			return
		case now := <-ticker.C:
			for _, res := range p.merger.FlushTimeout(now) {
				p.handleBlock(res.Block, string(res.Reason))
			}
			p.syncMergerStats()
		}
	}
}

func (p *Pipeline) syncMergerStats() {
	p.metrics.Set("overflow_lines_dropped", p.merger.OverflowDropped())
}

func (p *Pipeline) handleLine(line string) {
	p.metrics.Inc("lines_read", 1)
	for _, res := range p.merger.Feed(line) {
		p.handleBlock(res.Block, string(res.Reason))
	}
}

func (p *Pipeline) handleBlock(block, reason string) {
	p.metrics.Inc("blocks_formed", 1)
	p.metrics.Inc("blocks_closed_"+reason, 1)
	record := p.parse(block)
	if dropped, err := p.buf.Push(record); err == nil && dropped {
		p.metrics.Inc("dropped", 1)
	}
}

// parsesCleanly 判断一个块能否被解析链完整解析，供合并器做边界反馈。
func (p *Pipeline) parsesCleanly(block string) bool {
	for _, parser := range p.parsers {
		if _, err := parser.Parse(block); err == nil {
			return true
		}
	}
	return false
}

// parse 按注册顺序尝试解析器，第一个成功即生效并标注胜出者；
// 全部失败时保留完整原文，供死信路由与事后排查。
func (p *Pipeline) parse(block string) map[string]any {
	for _, parser := range p.parsers {
		rec, err := parser.Parse(block)
		if err == nil {
			p.metrics.Inc("parse_ok", 1)
			p.metrics.Inc("parse_ok_"+parser.Name(), 1)
			if p.annotateKey != "" {
				rec[p.annotateKey] = parser.Name()
			}
			return rec
		}
	}
	p.metrics.Inc("parse_failed", 1)
	return map[string]any{"raw": block, "parse_error": true}
}

func (p *Pipeline) consume() {
	for {
		rec, ok := p.buf.Pop()
		if !ok {
			return
		}
		for _, name := range p.router.Route(rec) {
			s, ok := p.router.Sink(name)
			if !ok {
				continue
			}
			if err := s.Write(rec); err != nil {
				p.metrics.Inc("write_failed", 1)
				continue
			}
			p.metrics.Inc("emitted", 1)
		}
	}
}

// Metrics 返回计数器快照。
func (p *Pipeline) Metrics() map[string]int64 { return p.metrics.Snapshot() }
