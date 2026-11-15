// Package metrics 是进程内计数器。
//
// 没有依赖，没有导出器；Snapshot 返回一份可序列化的快照，
// 由调用方决定写到哪里。
package metrics

import "sync"

// Counters 是采集链路的计数集合。
type Counters struct {
	mu sync.Mutex

	LinesRead    int64
	BlocksFormed int64
	ParseOK      int64
	ParseFailed  int64
	Emitted      int64
	Dropped      int64
	WriteFailed  int64
}

// Inc 增加某个计数器。name 必须是结构体字段名之一。
func (c *Counters) Inc(name string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch name {
	case "lines_read":
		c.LinesRead += n
	case "blocks_formed":
		c.BlocksFormed += n
	case "parse_ok":
		c.ParseOK += n
	case "parse_failed":
		c.ParseFailed += n
	case "emitted":
		c.Emitted += n
	case "dropped":
		c.Dropped += n
	case "write_failed":
		c.WriteFailed += n
	}
}

// Snapshot 返回当前计数。
func (c *Counters) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]int64{
		"lines_read":    c.LinesRead,
		"blocks_formed": c.BlocksFormed,
		"parse_ok":      c.ParseOK,
		"parse_failed":  c.ParseFailed,
		"emitted":       c.Emitted,
		"dropped":       c.Dropped,
		"write_failed":  c.WriteFailed,
	}
}
