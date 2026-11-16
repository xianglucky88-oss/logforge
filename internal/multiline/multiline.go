// Package multiline 把多行日志合并成一条记录。
//
// 典型场景是堆栈跟踪：首行带时间戳，后面若干行是缩进的调用栈。
// 每一行先被分类，分类结果决定合并行为：
//
//   - 起始行（匹配 start_pattern）：收尾当前记录，开启新记录；
//   - 续行（匹配 continuation_pattern）：并入当前记录；
//   - 外来行（两者都不匹配）：当前记录必然已经结束，先收尾，
//     该行再按 Orphan 策略处理。
//
// 未设置 continuation_pattern 时，所有非起始行都视为续行（旧行为）；
// 此时可以打开 FlushOnComplete，让解析器链参与边界判断：当前块已经
// 能被完整解析时，遇到非起始行就先收尾，避免孤立行粘坏上一条记录。
//
// 每条收尾的记录都带一个 Reason，调用方按原因计数即可对账。
package multiline

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Reason 说明一条记录为什么被收尾，用于指标对账。
type Reason string

const (
	// ReasonStart 是看到下一条记录的起始行。
	ReasonStart Reason = "start"
	// ReasonForeign 是看到既非起始又非续行的外来行（需设置 continuation_pattern）。
	ReasonForeign Reason = "foreign"
	// ReasonComplete 是解析器链判定当前块已完整（需打开 flush_on_complete）。
	ReasonComplete Reason = "complete"
	// ReasonMaxLines 是达到最大行数被强制切分。
	ReasonMaxLines Reason = "max_lines"
	// ReasonTimeout 是超过 FlushAfter 没有等到新行。
	ReasonTimeout Reason = "timeout"
	// ReasonShutdown 是进程退出前的最终收尾。
	ReasonShutdown Reason = "shutdown"
	// ReasonOrphan 是没有可归属记录的孤立行，单独成条。
	ReasonOrphan Reason = "orphan"
	// ReasonPassthrough 是未配置 start_pattern 时的逐行透传。
	ReasonPassthrough Reason = "passthrough"
)

// OrphanPolicy 决定没有打开的记录时，续行/外来行如何处理。
type OrphanPolicy string

const (
	// OrphanEmit 让孤立行单独成条，立即输出。坏行各自成条，便于计数对账。
	OrphanEmit OrphanPolicy = "emit"
	// OrphanBuffer 让孤立行开启一个新块，等待后续行并入。
	// 适合位点恢复在堆栈中间的场景：残余的续行会重新聚成一条。
	OrphanBuffer OrphanPolicy = "buffer"
)

// OverflowPolicy 决定达到 MaxLines 之后，同一条记录的剩余续行怎么办。
type OverflowPolicy string

const (
	// OverflowTruncate 丢弃剩余续行并计数（OverflowDropped），
	// 记录边界保持干净，但被截掉的行不再出现。
	OverflowTruncate OverflowPolicy = "truncate"
	// OverflowSplit 让剩余续行开启新块。数据全部保留，
	// 但会产生没有起始行的"幻影记录"（通常解析失败，进入死信）。
	OverflowSplit OverflowPolicy = "split"
)

// Options 控制合并行为。
type Options struct {
	// StartPattern 匹配"新记录的第一行"。为空表示不合并，逐行透传。
	StartPattern string
	// ContinuationPattern 匹配"当前记录的续行"。设置之后，既不匹配
	// 起始也不匹配续行的外来行会收尾当前记录，而不是被并进去。
	ContinuationPattern string
	// MaxLines 是一条记录的最大行数，超出后强制切分。
	MaxLines int
	// FlushAfter 是距离首行多久没有新行就强制输出。<= 0 表示不按时间收尾。
	FlushAfter time.Duration
	// FlushOnComplete 打开后，遇到非起始行时会先用 Complete 检查当前块
	// 是否已经是一条完整记录，是则先收尾。仅在未设置
	// ContinuationPattern 时生效（有续行模式时外来行一律收尾）。
	//
	// 注意：解析链不能把多行记录的首行判为完整（例如 regex 恰好匹配
	// 堆栈首行），否则堆栈会在首行后就被劈开。堆栈场景请改用
	// ContinuationPattern 保护续行。
	FlushOnComplete bool
	// Complete 由调用方提供，判断一个块是否能被解析链完整解析。
	// FlushOnComplete 打开时必填。
	Complete func(block string) bool
	// Orphan 是孤立行策略，默认 OrphanEmit。
	Orphan OrphanPolicy
	// OnOverflow 是超长记录的收尾策略，默认 OverflowTruncate。
	OnOverflow OverflowPolicy
}

// Result 是一条收尾的记录及其原因。
type Result struct {
	Block  string
	Reason Reason
}

// Merger 是合并器。方法可并发调用（采集循环喂行、定时器做超时收尾）。
type Merger struct {
	opts Options

	start *regexp.Regexp
	cont  *regexp.Regexp

	mu       sync.Mutex
	lines    []string
	started  time.Time
	overflow bool
	// overflowDropped 是被 OverflowTruncate 丢弃的续行数。
	overflowDropped int64
}

// New 构造合并器。正则非法或策略未知时返回 error。
func New(opts Options) (*Merger, error) {
	m := &Merger{opts: opts}
	if opts.StartPattern != "" {
		re, err := regexp.Compile(opts.StartPattern)
		if err != nil {
			return nil, fmt.Errorf("bad start_pattern: %w", err)
		}
		m.start = re
	}
	if opts.ContinuationPattern != "" {
		if m.start == nil {
			return nil, fmt.Errorf("continuation_pattern requires start_pattern")
		}
		re, err := regexp.Compile(opts.ContinuationPattern)
		if err != nil {
			return nil, fmt.Errorf("bad continuation_pattern: %w", err)
		}
		m.cont = re
	}
	if m.opts.MaxLines <= 0 {
		m.opts.MaxLines = 200
	}
	if m.opts.Orphan == "" {
		m.opts.Orphan = OrphanEmit
	}
	switch m.opts.Orphan {
	case OrphanEmit, OrphanBuffer:
	default:
		return nil, fmt.Errorf("unknown orphan policy %q", m.opts.Orphan)
	}
	if m.opts.OnOverflow == "" {
		m.opts.OnOverflow = OverflowTruncate
	}
	switch m.opts.OnOverflow {
	case OverflowTruncate, OverflowSplit:
	default:
		return nil, fmt.Errorf("unknown on_overflow policy %q", m.opts.OnOverflow)
	}
	if m.opts.FlushOnComplete && m.opts.Complete == nil {
		return nil, fmt.Errorf("flush_on_complete requires a Complete callback")
	}
	return m, nil
}

// Feed 送入一行，返回因此收尾的记录（可能同时收尾旧块并吐出孤立行，
// 所以是切片）。空切片表示还没有完整记录。
func (m *Merger) Feed(line string) []Result {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.start == nil {
		return []Result{{Block: line, Reason: ReasonPassthrough}}
	}

	// 起始行：收尾旧块，开新块。起始行本身不做完整性判断——
	// 堆栈首行可能恰好能被某个 regex 解析，提前收尾会劈开堆栈。
	if m.start.MatchString(line) {
		res := m.closeLocked(ReasonStart)
		m.openLocked(line)
		return res
	}

	if len(m.lines) == 0 {
		// 没有打开的记录：文件中间启动、上一块刚被截断、或上一块已收尾。
		if m.overflow {
			if m.isContinuation(line) {
				if m.opts.OnOverflow == OverflowTruncate {
					// 保持 overflow 状态：同一条记录的剩余续行全部丢弃并计数。
					m.overflowDropped++
					return nil
				}
				// OverflowSplit：溢出的续行开启新块，数据保留。
				m.overflow = false
				m.openLocked(line)
				return nil
			}
			m.overflow = false
		}
		return m.orphanLocked(line)
	}

	// 有打开的记录，且该行不是新起始行。
	if m.cont == nil && m.opts.FlushOnComplete && m.opts.Complete(strings.Join(m.lines, "\n")) {
		// 解析器链判定当前块已完整：外来行不能并进去，先收尾。
		res := m.closeLocked(ReasonComplete)
		return append(res, m.orphanLocked(line)...)
	}
	if m.isContinuation(line) {
		m.lines = append(m.lines, line)
		if len(m.lines) >= m.opts.MaxLines {
			res := m.closeLocked(ReasonMaxLines)
			m.overflow = true
			return res
		}
		return nil
	}
	// 外来行：当前记录必然已结束，先收尾，该行按孤立行处理。
	res := m.closeLocked(ReasonForeign)
	return append(res, m.orphanLocked(line)...)
}

// FlushTimeout 把等待超过 FlushAfter 的未完成记录收尾，由定时器调用。
func (m *Merger) FlushTimeout(now time.Time) []Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.lines) == 0 || m.opts.FlushAfter <= 0 {
		return nil
	}
	if now.Sub(m.started) < m.opts.FlushAfter {
		return nil
	}
	return m.closeLocked(ReasonTimeout)
}

// Flush 输出尚未完成的记录，用于退出时收尾。
func (m *Merger) Flush() []Result {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeLocked(ReasonShutdown)
}

// OverflowDropped 返回被 OverflowTruncate 丢弃的续行总数。
func (m *Merger) OverflowDropped() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overflowDropped
}

// isContinuation 判断是否为续行。未设置续行模式时，非起始行一律视为续行。
func (m *Merger) isContinuation(line string) bool {
	if m.cont == nil {
		return true
	}
	return m.cont.MatchString(line)
}

func (m *Merger) openLocked(line string) {
	m.lines = []string{line}
	m.started = time.Now()
	m.overflow = false
}

func (m *Merger) closeLocked(reason Reason) []Result {
	if len(m.lines) == 0 {
		return nil
	}
	block := strings.Join(m.lines, "\n")
	m.lines = nil
	return []Result{{Block: block, Reason: reason}}
}

func (m *Merger) orphanLocked(line string) []Result {
	if m.opts.Orphan == OrphanBuffer {
		m.openLocked(line)
		return nil
	}
	return []Result{{Block: line, Reason: ReasonOrphan}}
}
