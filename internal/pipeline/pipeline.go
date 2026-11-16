// Package pipeline 把各个模块串起来。
//
// 数据流：tail → multiline → parser → buffer → router → sinks
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	// errOut 接收运行期错误（写失败、关闭失败等）。默认 stderr；
	// 运行期错误不会静默吞掉，也不会改变其他输出端的结果。
	errOut io.Writer
}

// New 按配置装配管线。
func New(cfg *config.Config) (*Pipeline, error) {
	p := &Pipeline{cfg: cfg, metrics: &metrics.Counters{}, sinks: map[string]plugin.Sink{}, errOut: os.Stderr}

	// 输出端
	for _, sc := range cfg.Sinks {
		s, err := plugin.NewSink(sc.Type, sc.Config)
		if err != nil {
			return nil, fmt.Errorf("sink %q (type %q): %w", sc.Name, sc.Type, err)
		}
		p.sinks[sc.Name] = s
	}

	// 解析器链
	for _, pc := range cfg.Parsers {
		parser, err := plugin.NewParser(pc.Name, pc.Config)
		if err != nil {
			return nil, fmt.Errorf("parser %q: %w", pc.Name, err)
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
		return nil, fmt.Errorf("route: %w", err)
	}
	p.router = rt

	// 合并器
	merger, err := multiline.New(multiline.Options{
		StartPattern: cfg.Multiline.StartPattern,
		MaxLines:     cfg.Multiline.MaxLines,
		FlushAfter:   time.Duration(cfg.Multiline.FlushAfterMS) * time.Millisecond,
	})
	if err != nil {
		return nil, fmt.Errorf("multiline: %w", err)
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

// SetErrorWriter 替换运行期错误输出，主要供测试使用。传 nil 恢复 stderr。
func (p *Pipeline) SetErrorWriter(w io.Writer) {
	if w == nil {
		w = os.Stderr
	}
	p.errOut = w
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
		p.reportError(fmt.Errorf("tail: %w", err))
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

	// 关闭所有输出端：按名字排序，保证顺序确定；每个出口单独
	// flush/close，一个失败不影响其他出口，但错误会被报告。
	// 即使用 ctx 取消导致的正常退出，也不会掩盖 flush/close 失败。
	if closeErr := p.closeSinks(); closeErr != nil {
		err = errors.Join(err, closeErr)
	}
	return off, err
}

// closeSinks 依次 flush 并 close 所有输出端，返回合并后的错误。
func (p *Pipeline) closeSinks() error {
	names := router.SinkNames(p.sinks)
	var errs []error
	for _, name := range names {
		s := p.sinks[name]
		if err := s.Flush(); err != nil {
			p.metrics.Inc("write_failed", 1)
			p.metrics.IncSinkFailed(name)
			p.reportError(fmt.Errorf("sink %q: flush: %w", name, err))
			errs = append(errs, fmt.Errorf("sink %q: flush: %w", name, err))
		}
		if err := s.Close(); err != nil {
			p.reportError(fmt.Errorf("sink %q: close: %w", name, err))
			errs = append(errs, fmt.Errorf("sink %q: close: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Pipeline) reportError(err error) {
	if p.errOut == nil {
		return
	}
	fmt.Fprintf(p.errOut, "logforge: %v\n", err)
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
	if dropped, err := p.buf.Push(record); err != nil {
		if !errors.Is(err, buffer.ErrClosed) {
			p.reportError(fmt.Errorf("buffer: %w", err))
		}
	} else if dropped {
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

// consume 从缓冲取记录并按路由结果投递。
//
// 每条记录独立处理：一个输出端写失败只增加该出口的失败计数并报告错误，
// 不会中断同一条记录发往其他出口的投递，也不会影响后续记录。
func (p *Pipeline) consume() {
	for {
		rec, ok := p.buf.Pop()
		if !ok {
			return
		}
		p.deliver(rec)
	}
}

func (p *Pipeline) deliver(rec map[string]any) {
	p.metrics.Inc("records_routed", 1)
	decision := p.router.Route(rec)
	if decision.Matched {
		p.metrics.Inc("records_matched", 1)
	} else {
		p.metrics.Inc("records_default", 1)
	}
	if decision.TypeMismatches > 0 {
		p.metrics.Inc("route_type_mismatch", int64(decision.TypeMismatches))
	}

	delivered := 0
	for _, name := range decision.Sinks {
		s, ok := p.router.Sink(name)
		if !ok {
			// 启动期已校验过，正常不会发生；真发生了也按写失败处理，
			// 不让记录在无声无息中少投一个出口。
			p.metrics.Inc("write_failed", 1)
			p.metrics.IncSinkFailed(name)
			p.reportError(fmt.Errorf("sink %q: not found at delivery time", name))
			continue
		}
		if err := s.Write(rec); err != nil {
			p.metrics.Inc("write_failed", 1)
			p.metrics.IncSinkFailed(name)
			p.reportError(fmt.Errorf("sink %q: write: %w", name, err))
			continue
		}
		p.metrics.Inc("emitted", 1)
		p.metrics.IncSinkWritten(name)
		delivered++
	}
	if delivered > 0 {
		p.metrics.Inc("records_delivered", 1)
	} else {
		p.metrics.Inc("records_lost", 1)
	}
}

// Metrics 返回计数器快照。
func (p *Pipeline) Metrics() map[string]int64 { return p.metrics.Snapshot() }
