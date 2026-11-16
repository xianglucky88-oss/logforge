// Package multiline 把多行日志合并成一条记录。
//
// 典型场景是堆栈跟踪：首行带时间戳，后面若干行是缩进的调用栈。
package multiline

import (
	"regexp"
	"strings"
	"time"
)

// Options 控制合并行为。
type Options struct {
	// StartPattern 匹配"新记录的第一行"。为空表示不合并。
	StartPattern string
	// MaxLines 是一条记录的最大行数，超出后强制切分。
	MaxLines int
	// FlushAfter 是距离首行多久没有新行就强制输出。
	FlushAfter time.Duration
}

// Merger 是合并器。Feed 是单线程调用，不需要加锁。
type Merger struct {
	opts  Options
	start *regexp.Regexp

	lines   []string
	started time.Time
}

// New 构造合并器。正则非法时返回 error。
func New(opts Options) (*Merger, error) {
	m := &Merger{opts: opts}
	if opts.StartPattern != "" {
		re, err := regexp.Compile(opts.StartPattern)
		if err != nil {
			return nil, err
		}
		m.start = re
	}
	if m.opts.MaxLines <= 0 {
		m.opts.MaxLines = 200
	}
	return m, nil
}

// Feed 送入一行。done 是因此完成的记录（空串表示还没有完整记录）。
//
// closedPrev 说明 done 的字节范围归属：
//   - true：done 由本行之前的若干行组成（本行开启了下一条记录），
//     其结束偏移是"上一行"的结束偏移；
//   - false：done 就是本行自己，其结束偏移是本行的结束偏移。
//
// 调用方用这个区分把记录映射回文件偏移，位点提交才不会错位。
func (m *Merger) Feed(line string) (done string, closedPrev bool) {
	if m.start == nil {
		return line, false
	}
	if m.start.MatchString(line) {
		if len(m.lines) > 0 {
			done = strings.Join(m.lines, "\n")
		}
		m.lines = []string{line}
		m.started = time.Now()
		return done, true
	}
	if len(m.lines) == 0 {
		// 文件开头就是续行，单独成条，避免丢数据。
		return line, false
	}
	m.lines = append(m.lines, line)
	if len(m.lines) >= m.opts.MaxLines {
		done := strings.Join(m.lines, "\n")
		m.lines = nil
		return done, true
	}
	return "", false
}

// FlushIfStale 在累积的记录超过 FlushAfter 没有新行时输出它。
// 没有累积或计时未启用时返回空串。
func (m *Merger) FlushIfStale(now time.Time) string {
	if len(m.lines) == 0 || m.opts.FlushAfter <= 0 {
		return ""
	}
	if now.Sub(m.started) < m.opts.FlushAfter {
		return ""
	}
	return m.Flush()
}

// Flush 输出尚未完成的记录，用于退出时收尾。
func (m *Merger) Flush() string {
	if len(m.lines) == 0 {
		return ""
	}
	done := strings.Join(m.lines, "\n")
	m.lines = nil
	return done
}
