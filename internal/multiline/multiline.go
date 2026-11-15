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

// Feed 送入一行，返回因"遇到新的起始行"而完成的上一条记录。
// 返回空串表示还没有完整记录。
func (m *Merger) Feed(line string) string {
	if m.start == nil {
		return line
	}
	if m.start.MatchString(line) {
		var done string
		if len(m.lines) > 0 {
			done = strings.Join(m.lines, "\n")
		}
		m.lines = []string{line}
		m.started = time.Now()
		return done
	}
	if len(m.lines) == 0 {
		// 文件开头就是续行，单独成条，避免丢数据。
		return line
	}
	m.lines = append(m.lines, line)
	if len(m.lines) >= m.opts.MaxLines {
		done := strings.Join(m.lines, "\n")
		m.lines = nil
		return done
	}
	return ""
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
