// 三态计数：每条进入 Buffer 的记录在任意时刻恰好属于
// 已发出、缓冲中、已丢弃三者之一，因此恒等式
//
//	pushed == emitted + buffered + dropped_total
//
// 始终成立。所有字段只在 Buffer 的互斥锁内修改，
// Snapshot 也在同一把锁内拷贝，指标快照与真实丢弃数不会错位。
package buffer

// Stats 是 Buffer 的全量计数。除 buffered 类仪表量外都是单调累计量。
type Stats struct {
	// Pushed 是被接受进入缓冲的记录数（拿到了 Seq 且没有在入口被拒）。
	Pushed int64 `json:"pushed"`
	// Emitted 是已经被消费者 Ack 的记录数（成功写入全部目标 sink）。
	Emitted int64 `json:"emitted"`

	// 丢弃按原因拆分，DroppedTotal 是它们的和。
	DroppedNew       int64 `json:"dropped_new"`        // drop_new：满了直接拒新
	DroppedOldest    int64 `json:"dropped_oldest"`     // drop_oldest：踢出最老
	DroppedBlocked   int64 `json:"dropped_blocked"`    // block 超时/关闭后的兜底丢弃
	DroppedSpillFull int64 `json:"dropped_spill_full"` // 溢写预算耗尽后的降级丢弃
	DroppedSpillIO   int64 `json:"dropped_spill_io"`   // 溢写 I/O 错误（含磁盘满）后的降级丢弃
	DroppedCorrupt   int64 `json:"dropped_corrupt"`    // 溢写帧损坏/坏段隔离造成的记录损失
	DeadLettered     int64 `json:"dead_lettered"`      // sink 反复失败转入 dead-letter 段

	// 缓冲中（仪表量，非单调）：内存环 + 重试队列 + 磁盘活段。
	InMemory int64 `json:"in_memory"`
	InRetry  int64 `json:"in_retry"`
	OnDisk   int64 `json:"on_disk"`

	// 溢写与恢复相关指标。
	SpilledWrites   int64 `json:"spilled_writes"`    // 累计写入磁盘的帧数
	SpillReads      int64 `json:"spill_reads"`       // 累计从磁盘回放的帧数
	Requeued        int64 `json:"requeued"`          // sink 失败后重排队次数
	CorruptFrames   int64 `json:"corrupt_frames"`    // 校验失败并被跳过/重同步的帧
	QuarantinedSegs int64 `json:"quarantined_segs"`  // 被隔离的坏段数
	ReplayedDupes   int64 `json:"replayed_dupes"`    // 恢复时因 seq<=committed 跳过的帧
}

func newStats() Stats { return Stats{} }

// IsZero 判断是否为零值，用于旧 manifest 兼容。
func (s Stats) IsZero() bool {
	return s == Stats{}
}

// DroppedTotal 汇总所有丢弃（含 dead-letter）。
func (s Stats) DroppedTotal() int64 {
	return s.DroppedNew + s.DroppedOldest + s.DroppedBlocked +
		s.DroppedSpillFull + s.DroppedSpillIO + s.DroppedCorrupt + s.DeadLettered
}

// Buffered 是当前仍在缓冲中的记录总数。
func (s Stats) Buffered() int64 { return s.InMemory + s.InRetry + s.OnDisk }

// Identity 校验三态恒等式，返回不匹配的差额（0 表示自洽）。
func (s Stats) Identity() int64 {
	return s.Pushed - s.Emitted - s.Buffered() - s.DroppedTotal()
}

// sub 返回两个累计快照之差，用于计算本世代增量。仪表量字段不参与，
// 由调用方按当前队列状态重新填充。
func (s Stats) sub(base Stats) Stats {
	return Stats{
		Pushed:            s.Pushed - base.Pushed,
		Emitted:           s.Emitted - base.Emitted,
		DroppedNew:        s.DroppedNew - base.DroppedNew,
		DroppedOldest:     s.DroppedOldest - base.DroppedOldest,
		DroppedBlocked:    s.DroppedBlocked - base.DroppedBlocked,
		DroppedSpillFull:  s.DroppedSpillFull - base.DroppedSpillFull,
		DroppedSpillIO:    s.DroppedSpillIO - base.DroppedSpillIO,
		DroppedCorrupt:    s.DroppedCorrupt - base.DroppedCorrupt,
		DeadLettered:      s.DeadLettered - base.DeadLettered,
		SpilledWrites:     s.SpilledWrites - base.SpilledWrites,
		SpillReads:        s.SpillReads - base.SpillReads,
		Requeued:          s.Requeued - base.Requeued,
		CorruptFrames:     s.CorruptFrames - base.CorruptFrames,
		QuarantinedSegs:   s.QuarantinedSegs - base.QuarantinedSegs,
		ReplayedDupes:     s.ReplayedDupes - base.ReplayedDupes,
	}
}
