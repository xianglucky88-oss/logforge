// Package metrics 是进程内计数器。
//
// 没有依赖，没有导出器；Snapshot 返回一份可序列化的快照，
// 由调用方决定写到哪里。
//
// # 计数语义
//
// 一条记录可能被路由到多个输出端（fan-out）。为避免歧义，计数分为
// "按记录"和"按投递"两类：
//
//   - records_routed：进入路由器的记录数。每条记录只计一次，
//     与它最终有几个出口无关。
//   - records_matched / records_default：按是否命中规则分别计数，
//     两者之和等于 records_routed。
//   - emitted：成功的"输出端投递"次数。一条记录 fan-out 到 N 个出口
//     且全部成功时 +N，因此 emitted 可能大于 records_routed。
//   - write_failed：失败的"输出端投递"次数。某个出口写失败只影响它
//     自己，同一条记录发往其他出口的成功投递照常计入 emitted。
//   - records_delivered：至少有一个出口写成功的记录数。
//   - records_lost：所有出口都写失败的记录数。它只在整条记录彻底
//     没能落到任何地方时才增加。
//
// 另有按输出端拆分的 sink_written.<name> / sink_write_failed.<name>，
// 用来定位是哪个出口在失败，而不必翻日志。
package metrics

import "sync"

// Counters 是采集链路的计数集合。
type Counters struct {
	mu sync.Mutex

	LinesRead         int64
	BlocksFormed      int64
	ParseOK           int64
	ParseFailed       int64
	RecordsRouted     int64
	RecordsMatched    int64
	RecordsDefault    int64
	RecordsDelivered  int64
	RecordsLost       int64
	RouteTypeMismatch int64
	Emitted           int64
	Dropped           int64
	WriteFailed       int64

	// 按输出端名拆分的计数，便于定位单个出口的问题。
	sinkWritten map[string]int64
	sinkFailed  map[string]int64
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
	case "records_routed":
		c.RecordsRouted += n
	case "records_matched":
		c.RecordsMatched += n
	case "records_default":
		c.RecordsDefault += n
	case "records_delivered":
		c.RecordsDelivered += n
	case "records_lost":
		c.RecordsLost += n
	case "route_type_mismatch":
		c.RouteTypeMismatch += n
	case "emitted":
		c.Emitted += n
	case "dropped":
		c.Dropped += n
	case "write_failed":
		c.WriteFailed += n
	}
}

// IncSinkWritten 记录一次成功的输出端投递。
func (c *Counters) IncSinkWritten(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sinkWritten == nil {
		c.sinkWritten = map[string]int64{}
	}
	c.sinkWritten[name]++
}

// IncSinkFailed 记录一次失败的输出端投递。
func (c *Counters) IncSinkFailed(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sinkFailed == nil {
		c.sinkFailed = map[string]int64{}
	}
	c.sinkFailed[name]++
}

// Snapshot 返回当前计数。
func (c *Counters) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int64{
		"lines_read":          c.LinesRead,
		"blocks_formed":       c.BlocksFormed,
		"parse_ok":            c.ParseOK,
		"parse_failed":        c.ParseFailed,
		"records_routed":      c.RecordsRouted,
		"records_matched":     c.RecordsMatched,
		"records_default":     c.RecordsDefault,
		"records_delivered":   c.RecordsDelivered,
		"records_lost":        c.RecordsLost,
		"route_type_mismatch": c.RouteTypeMismatch,
		"emitted":             c.Emitted,
		"dropped":             c.Dropped,
		"write_failed":        c.WriteFailed,
	}
	for name, n := range c.sinkWritten {
		out["sink_written."+name] = n
	}
	for name, n := range c.sinkFailed {
		out["sink_write_failed."+name] = n
	}
	return out
}
