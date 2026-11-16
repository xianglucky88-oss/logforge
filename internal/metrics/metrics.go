// Package metrics 是进程内计数器。
//
// 没有依赖，没有导出器；Snapshot 返回一份可序列化的快照，
// 由调用方决定写到哪里。计数器按名字动态创建，新增指标不需要改这里。
package metrics

import "sync"

// Counters 是采集链路的计数集合。Inc 用于单调计数器，Set 用于瞬时 gauge。
type Counters struct {
	mu sync.Mutex
	m  map[string]int64
}

// Inc 增加某个计数器，计数器不存在时自动创建。
func (c *Counters) Inc(name string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]int64{}
	}
	c.m[name] += n
}

// Set 记录某个 gauge 的当前值。
func (c *Counters) Set(name string, v int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]int64{}
	}
	c.m[name] = v
}

// Get 返回某个计数器的当前值。
func (c *Counters) Get(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[name]
}

// Snapshot 返回当前计数的副本。
func (c *Counters) Snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int64, len(c.m))
	for k, v := range c.m {
		out[k] = v
	}
	return out
}
