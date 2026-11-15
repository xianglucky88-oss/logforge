// Package pipeline 把各个模块串起来。
//
// 数据流：tail → multiline → parser → buffer → router → sinks
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

	merger  *multiline.Merger
	parsers []plugin.Parser
	router  *router.Router
	buf     *buffer.Buffer

	offsetStore *offset.Store
	offsetFile  *offset.File

	sinks map[string]plugin.Sink
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

	start := p.offsetFile.Offsets[p.cfg.Source.Path]
	t := tail.New(tail.Options{
		Path:         p.cfg.Source.Path,
		PollInterval: time.Duration(p.cfg.Source.PollIntervalMS) * time.Millisecond,
		StartOffset:  start,
	}, p.handleLine, func(err error) {
		fmt.Printf("tail error: %v\n", err)
	})

	off, err := t.Run(ctx)
	// 收尾：把合并器里没发出的记录刷出去。
	if block := p.merger.Flush(); block != "" {
		p.handleBlock(block)
	}
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

func (p *Pipeline) handleLine(line string) {
	p.metrics.Inc("lines_read", 1)
	if block := p.merger.Feed(line); block != "" {
		p.handleBlock(block)
	}
}

func (p *Pipeline) handleBlock(block string) {
	p.metrics.Inc("blocks_formed", 1)
	record := p.parse(block)
	if dropped, err := p.buf.Push(record); err == nil && dropped {
		p.metrics.Inc("dropped", 1)
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
