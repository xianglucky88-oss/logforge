// Package metrics 是进程内计数器。
//
// 没有依赖，没有导出器；Snapshot 返回一份可序列化的快照，
// 由调用方决定写到哪里。
//
// 计数器名是动态字符串（如 blocks_closed_timeout、parse_ok_json），
// 这样合并器收尾原因、解析器命中情况都能各自计数并互相对账：
//
//	blocks_formed == Σ blocks_closed_<reason>
//	blocks_formed == parse_ok + parse_failed
//	parse_ok      == Σ parse_ok_<parser>
package metrics

import "sync"

// Counters 是采集链路的计数集合。零值即可用。
type Counters struct {
	mu   sync.Mutex
	vals map[string]int64
}

// Inc 给 name 计数器加 n。
func (c *Counters) Inc(name string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.vals == nil {
		c.vals = map[string]int64{}
	}
	c.vals[name] += n
}

// Set 把 name 设置为 v，用于上报别处维护的累计值（如合并器丢弃的溢出行）。
func (c *Counters) Set(name string, v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.vals == nil {
		c.vals = map[string]int64{}
	}
	c.vals[name] = v
}

// Snapshot 返回当前计数的副本。
func (c *Counters) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.vals))
	for k, v := range c.vals {
		out[k] = v
	}
	return out
}
