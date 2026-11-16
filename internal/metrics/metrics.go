// Package metrics 是进程内计数器。
//
// 没有依赖，没有导出器；Snapshot 返回一份可序列化的快照，
// 由调用方决定写到哪里。
//
// 缓冲相关的权威计数由 internal/buffer.Stats 在同一把锁内维护，
// Pipeline 每次做 Snapshot 时把 Buffer 的最新 Stats 合并进来，
// 因此“指标快照里的丢弃数”和“缓冲真实状态”不会出现不同步窗口。
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

	// 缓冲背压/溢写相关（由 buffer.Stats 合并填充）。
	Pushed            int64
	Buffered          int64
	InMemory          int64
	InRetry           int64
	OnDisk            int64
	DroppedNew        int64
	DroppedOldest     int64
	DroppedBlocked    int64
	DroppedSpillFull  int64
	DroppedSpillIO    int64
	DroppedCorrupt    int64
	DeadLettered      int64
	SpilledWrites     int64
	SpillReads        int64
	Requeued          int64
	CorruptFrames     int64
	QuarantinedSegs   int64
	ReplayedDupes     int64
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

		"pushed":             c.Pushed,
		"buffered":           c.Buffered,
		"in_memory":          c.InMemory,
		"in_retry":           c.InRetry,
		"on_disk":            c.OnDisk,
		"dropped_new":        c.DroppedNew,
		"dropped_oldest":     c.DroppedOldest,
		"dropped_blocked":    c.DroppedBlocked,
		"dropped_spill_full": c.DroppedSpillFull,
		"dropped_spill_io":   c.DroppedSpillIO,
		"dropped_corrupt":    c.DroppedCorrupt,
		"dead_lettered":      c.DeadLettered,
		"spilled_writes":     c.SpilledWrites,
		"spill_reads":        c.SpillReads,
		"requeued":           c.Requeued,
		"corrupt_frames":     c.CorruptFrames,
		"quarantined_segs":   c.QuarantinedSegs,
		"replayed_dupes":     c.ReplayedDupes,
	}
}

// ApplyBufferStats 在同一把锁内把缓冲权威计数覆盖到快照字段，
// 保证调用方拿到的 map 里缓冲部分与 Buffer.Stats 严格同源。
func (c *Counters) ApplyBufferStats(pushed, buffered, inMemory, inRetry, onDisk,
	droppedNew, droppedOldest, droppedBlocked, droppedSpillFull, droppedSpillIO,
	droppedCorrupt, deadLettered, spilledWrites, spillReads, requeued,
	corruptFrames, quarantinedSegs, replayedDupes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Pushed = pushed
	c.Buffered = buffered
	c.InMemory = inMemory
	c.InRetry = inRetry
	c.OnDisk = onDisk
	c.DroppedNew = droppedNew
	c.DroppedOldest = droppedOldest
	c.DroppedBlocked = droppedBlocked
	c.DroppedSpillFull = droppedSpillFull
	c.DroppedSpillIO = droppedSpillIO
	c.DroppedCorrupt = droppedCorrupt
	c.DeadLettered = deadLettered
	c.SpilledWrites = spilledWrites
	c.SpillReads = spillReads
	c.Requeued = requeued
	c.CorruptFrames = corruptFrames
	c.QuarantinedSegs = quarantinedSegs
	c.ReplayedDupes = replayedDupes
	// Dropped 保留为“丢弃总数”的兼容口径。
	c.Dropped = droppedNew + droppedOldest + droppedBlocked +
		droppedSpillFull + droppedSpillIO + droppedCorrupt + deadLettered
}
