// Package buffer 是采集与输出之间的有界缓冲，提供可验证的背压与溢写。
//
// 满载策略（Config.Policy）：
//
//   - drop_new：直接拒绝新记录，保护已有数据；
//   - drop_oldest：踢出最老一条，保留最新数据（“保新”）；
//   - block：阻塞生产者直到有空位，带超时与兜底动作，Close 必定唤醒，
//     消费协程不会被慢 sink 永久卡死；
//   - spill：内存满后把整环数据落到磁盘段文件，磁盘预算/I/O 失败时
//     按 SpillFullAction 分级降级为 drop_new 或 drop_oldest。
//
// 每条记录分配单调递增的 Seq，任意时刻恰好处于已发出、缓冲中、已丢弃
// 三态之一，恒等式 pushed == emitted + buffered + dropped_total 始终成立
// （见 stats.go），进程重启后从 manifest 恢复历史计数与磁盘段。
package buffer

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// ErrClosed 表示缓冲已关闭。
var ErrClosed = errors.New("buffer closed")

// Policy 决定容量满时的行为。
type Policy string

const (
	// PolicyDropNew 满载时拒绝新记录。
	PolicyDropNew Policy = "drop_new"
	// PolicyDropOldest 满载时丢弃最老记录，保留新数据。
	PolicyDropOldest Policy = "drop_oldest"
	// PolicyBlock 满载时阻塞生产者，直到有空位、超时或关闭。
	PolicyBlock Policy = "block"
	// PolicySpill 满载时溢写到磁盘，磁盘受预算约束。
	PolicySpill Policy = "spill"
)

// DropReason 解释一次丢弃的来源。
type DropReason string

const (
	DropNone        DropReason = ""
	DropReasonNew   DropReason = "new"        // drop_new
	DropReasonOld   DropReason = "oldest"     // drop_oldest
	DropReasonBlock DropReason = "blocked"    // block 超时/关闭兜底
	DropReasonDisk DropReason = "spill_full"  // 溢写预算耗尽
	DropReasonIO    DropReason = "spill_io"   // 溢写 I/O 失败（含磁盘满）
)

// FallbackAction 是 block 超时或溢写失败后的兜底动作。
type FallbackAction string

const (
	// FallbackDropNew 兜底为拒绝新记录（默认，内存严格有界）。
	FallbackDropNew FallbackAction = "drop_new"
	// FallbackDropOldest 兜底为踢出最老记录。
	FallbackDropOldest FallbackAction = "drop_oldest"
)

// Config 是缓冲配置。
type Config struct {
	// Capacity 是内存环容量，必须 > 0。
	Capacity int
	// Policy 是满载策略，默认 drop_oldest（与历史行为兼容）。
	Policy Policy
	// SpillDir 是溢写目录；policy=spill 时必填，其他策略可选（用于 dead-letter）。
	SpillDir string
	// SpillSegmentBytes 是单个溢写段的滚动字节数，默认 8 MiB。
	SpillSegmentBytes int64
	// SpillMaxSegments 是活段数量上限，默认 64。
	SpillMaxSegments int
	// SpillMaxBytes 是溢写总字节预算，默认 1 GiB。
	SpillMaxBytes int64

	// BlockTimeout 是 block 策略的最大等待时间；0 表示无限等，
	// 但 Close 仍然一定唤醒（关闭语义不受影响）。
	BlockTimeout time.Duration
	// BlockTimeoutAction 是超时后的兜底动作，默认 drop_new。
	BlockTimeoutAction FallbackAction
	// SpillFullAction 是溢写失败后的兜底动作，默认 drop_new。
	SpillFullAction FallbackAction

	// ManifestEveryAcks 是每 Ack 多少条刷一次 manifest，默认 256。
	// 崩溃窗口内的记录会在重启后依赖 Seq 幂等去重，按 at-least-once 重放。
	ManifestEveryAcks int
}

// Envelope 是缓冲内部的记录包装，携带全局序号与尝试次数。
type Envelope struct {
	Seq      uint64
	Attempts uint32
	Record   map[string]any
}

// PushResult 汇报一次 Push 的处置结果。
type PushResult struct {
	// Dropped 为 true 时记录未进入缓冲（或触发了一次弃旧）。
	Dropped bool
	Reason  DropReason
	// Spilled 为 true 时本次 Push 触发了内存到磁盘的溢写。
	Spilled bool
	Seq     uint64
}

// Recovery 汇报 New 时从溢写目录恢复出的状态，供启动日志与指标对接。
type Recovery struct {
	Found           bool   // 目录里是否存在上一世代的 manifest 或段文件
	Generation      uint64
	ReplayedOnDisk  int64  // 仍在磁盘上、待消费的帧数（去重后）
	DuplicateFrames int64  // seq<=committed，将被跳过的重复帧
	CorruptFrames   int64  // 校验失败被重同步/跳过的帧
	QuarantinedSegs int    // 被隔离的坏段
	CommittedSeq    uint64
	NextSeq         uint64
}

// Buffer 是并发安全的有界 FIFO，支持内存环、重试队列与磁盘溢写。
type Buffer struct {
	mu sync.Mutex

	cfg Config

	// 内存环
	ring  []*Envelope
	head  int
	count int
	// spillArmed 为 false 时，即使环满也先在内存保留，直到消费到低水位
	// 重新布防，用于抑制溢写抖动（见 Push 中 spill 分支）。
	spillArmed bool

	// 重试队列：sink 失败的记录从队首插回，保证 Seq 顺序。
	retry []*Envelope

	// 等待者信号：每个挂起的等待者持有一个容量为 1 的信号 channel。
	// 通知方向 channel 发一个非阻塞信号；等待者在 select 前已完成注册，
	// 并在被唤醒后于锁内复查条件，因此注册窗口竞态与虚假唤醒都被覆盖。
	roomWaiters []chan struct{}
	dataWaiters []chan struct{}

	// 序号与水位
	nextSeq      uint64
	committedSeq uint64
	// hasCommitted 标记本进程（或上一世代）是否成功 Ack 过至少一条，
	// 用于区分“水位初值 0”与“真的提交到了 seq=0”，避免首帧被重复判定误杀。
	hasCommitted bool
	// deadSeq 是已转入 dead-letter 的序号，恢复回放时跳过；
	// 持久化在 manifest 中，小于水位的条目定期压缩。
	deadSeq map[uint64]struct{}

	closed bool
	stats  Stats
	// base 是打开时从上一世代继承的累计量基线。
	// 对外暴露的 stats 含历史累计（便于观测总量），但三态恒等式
	// 只在“本世代增量”（stats - base）上校验，避免跨世代计数互相污染。
	base Stats
	// openingBuffered 是本世代开始时从磁盘恢复出的在途帧数（期初存货）。
	openingBuffered int64
	// openingDropped 是恢复扫描时确认已损坏/隔离、属于历史 Pushed 的帧数。
	openingDropped int64

	spill *spillStore
	mani  *manifestData

	ackSinceManifest int
}

// New 构造缓冲并执行崩溃恢复。spill 目录不可访问时返回错误。
func New(cfg Config) (*Buffer, *Recovery, error) {
	if cfg.Capacity <= 0 {
		return nil, nil, errors.New("buffer capacity must be > 0")
	}
	if cfg.Policy == "" {
		cfg.Policy = PolicyDropOldest
	}
	switch cfg.Policy {
	case PolicyDropNew, PolicyDropOldest, PolicyBlock, PolicySpill:
	default:
		return nil, nil, errors.New("unknown buffer policy: " + string(cfg.Policy))
	}
	if cfg.Policy == PolicySpill && cfg.SpillDir == "" {
		return nil, nil, errors.New("policy spill requires spill_dir")
	}
	if cfg.BlockTimeoutAction == "" {
		cfg.BlockTimeoutAction = FallbackDropNew
	}
	if cfg.SpillFullAction == "" {
		cfg.SpillFullAction = FallbackDropNew
	}
	if cfg.ManifestEveryAcks <= 0 {
		cfg.ManifestEveryAcks = 256
	}
	if cfg.SpillSegmentBytes <= 0 {
		cfg.SpillSegmentBytes = 8 << 20
	}

	b := &Buffer{
		cfg:        cfg,
		ring:       make([]*Envelope, cfg.Capacity),
		deadSeq:    map[uint64]struct{}{},
		spillArmed: true,
	}

	rec := &Recovery{}
	if cfg.SpillDir != "" {
		store, mani, err := openSpill(cfg.SpillDir, cfg.SpillMaxSegments, cfg.SpillMaxBytes)
		if err != nil {
			return nil, nil, err
		}
		b.spill = store
		b.mani = mani
		// 只有 manifest 或磁盘段确实承载了上一世代数据才算“发现恢复现场”，
		// 空目录（哪怕目录已存在）不算，避免误报恢复。
		rec.Found = store.manifestExisted || store.hadOrphans ||
			len(mani.Segments) > 0 || len(mani.DeadSegments) > 0
		rec.Generation = mani.Generation
		rec.CommittedSeq = mani.CommittedSeq

		// 接上一世代的累计计数作为观测基线；本世代对账只看增量（见 DeltaStats）。
		b.base = mani.Stats
		b.base.InMemory, b.base.InRetry, b.base.OnDisk = 0, 0, 0
		// 恢复期确认的损坏/隔离损失：历史 Pushed 已含这些帧，
		// 记到独立的“期初已丢弃”里（不进 base，避免被 sub 抵消）。
		openingDropped := int64(store.corruptLost) + int64(store.quarantinedFrames)
		b.base.CorruptFrames += int64(store.corruptFrames)
		b.stats = b.base
		// 期初已丢弃并入当前累计观测值。
		b.stats.DroppedCorrupt += openingDropped
		b.stats.QuarantinedSegs += int64(store.quarantined)

		b.nextSeq = mani.NextSeq
		b.committedSeq = mani.CommittedSeq
		b.hasCommitted = mani.HasCommitted
		for _, s := range mani.DeadSeq {
			b.deadSeq[s] = struct{}{}
		}

		// 盘点活段：区分待消费帧、重复帧与损坏损失，重算 OnDisk 仪表量。
		onDisk, dupes, corrupt, maxSeq, sawFrame := b.accountSegments()
		b.stats.OnDisk = onDisk
		// 期初存货只含仍可读的在途帧；损坏帧走 openingDropped，二者不重叠。
		b.openingBuffered = onDisk
		b.openingDropped = openingDropped
		b.stats.ReplayedDupes = b.base.ReplayedDupes + dupes
		b.stats.CorruptFrames = b.base.CorruptFrames + corrupt
		rec.ReplayedOnDisk = onDisk
		rec.DuplicateFrames = dupes
		rec.CorruptFrames = int64(store.corruptFrames) + corrupt
		rec.QuarantinedSegs = store.quarantined
		if sawFrame && maxSeq >= b.nextSeq {
			b.nextSeq = maxSeq + 1
		}
		rec.NextSeq = b.nextSeq
	}
	return b, rec, nil
}

// accountSegments 扫描活段，返回（待消费帧数, 重复帧数, 新发现损坏帧,
// 最大 seq, 是否扫到过帧）。
func (b *Buffer) accountSegments() (onDisk, dupes, corrupt int64, maxSeq uint64, sawFrame bool) {
	if b.spill == nil {
		return 0, 0, 0, 0, false
	}
	for _, meta := range b.spill.segments {
		f, err := openSegFile(b.spill.dir, meta.Name)
		if err != nil {
			continue
		}
		fr := newFrameReader(f)
		var lastSeq uint64
		for {
			frame, resynced, err := fr.readFrame()
			if err != nil {
				break
			}
			sawFrame = true
			if resynced {
				corrupt++
			}
			if frame.Seq > maxSeq {
				maxSeq = frame.Seq
			}
			lastSeq = frame.Seq
			if frame.Dead {
				continue
			}
			if _, dead := b.deadSeq[frame.Seq]; dead {
				continue
			}
			if b.hasCommitted && frame.Seq <= b.committedSeq {
				dupes++
			} else {
				onDisk++
			}
		}
		_ = f.Close()
		b.fillLastSeq(meta.ID, lastSeq)
	}
	return onDisk, dupes, corrupt, maxSeq, sawFrame
}

func (b *Buffer) fillLastSeq(id int, lastSeq uint64) {
	for i := range b.spill.segments {
		if b.spill.segments[i].ID == id {
			b.spill.segments[i].LastSeq = lastSeq
			return
		}
	}
}

// notifyRoom 给所有等待空位的生产者发信号（非阻塞，容量 1 保证不丢）。
func (b *Buffer) notifyRoom() {
	for _, ch := range b.roomWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// notifyData 给等待数据的消费者发信号。
func (b *Buffer) notifyData() {
	for _, ch := range b.dataWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// Push 入队一条记录。ctx 只在 block 策略下生效，用于取消等待。
func (b *Buffer) Push(ctx context.Context, item map[string]any) (PushResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return PushResult{}, ErrClosed
	}

	// spill 策略优先处理。spillArmed 实现低水位防抖：
	// 一次溢写后暂时“撤防”，直到内存环被消费到低水位以下才重新“布防”。
	// 否则消费者持续取空、生产者每次凑满都整环落盘，会造成溢写抖动，
	// 磁盘帧只增不减、吞吐被锁内磁盘 I/O 压垮。
	if b.cfg.Policy == PolicySpill && b.spillArmed && b.count >= b.cfg.Capacity {
		spilled, err := b.spillMemory()
		if err == nil {
			b.spillArmed = false // 撤防，等 popRing 把环消费到低水位再布防
			res := b.accept(item)
			res.Spilled = spilled
			return res, nil
		}
		return b.fallbackSpill(item, err), nil
	}

	if b.count < b.cfg.Capacity {
		return b.accept(item), nil
	}

	switch b.cfg.Policy {
	case PolicyDropNew:
		return b.dropEntry(DropReasonNew, func() { b.stats.DroppedNew++ }), nil

	case PolicyDropOldest:
		b.evictOldest()
		res := b.accept(item)
		res.Dropped = true
		res.Reason = DropReasonOld
		return res, nil

	case PolicyBlock:
		if err := b.waitRoom(ctx); err != nil {
			if errors.Is(err, ErrClosed) {
				return PushResult{}, ErrClosed
			}
			// 超时或 ctx 取消：执行兜底动作，保证调用方不会无限阻塞。
			return b.fallbackBlocked(item), nil
		}
		return b.accept(item), nil

	case PolicySpill:
		// 撤防期环仍被填满（消费者很慢）：强制溢写，内存严格有界不丢数据。
		spilled, err := b.spillMemory()
		if err != nil {
			return b.fallbackSpill(item, err), nil
		}
		res := b.accept(item)
		res.Spilled = spilled
		return res, nil
	}

	return PushResult{}, errors.New("unreachable policy")
}

// accept 把记录放入内存环并分配 Seq。调用时环必须有空位。
func (b *Buffer) accept(item map[string]any) PushResult {
	seq := b.nextSeq
	b.nextSeq++
	idx := (b.head + b.count) % b.cfg.Capacity
	b.ring[idx] = &Envelope{Seq: seq, Record: item}
	b.count++
	b.stats.Pushed++
	b.stats.InMemory++
	b.notifyData()
	return PushResult{Seq: seq}
}

// dropEntry 处理入口拒绝：占号并计入对应丢弃原因，记录不进缓冲。
func (b *Buffer) dropEntry(reason DropReason, inc func()) PushResult {
	seq := b.nextSeq
	b.nextSeq++
	b.stats.Pushed++
	inc()
	return PushResult{Dropped: true, Reason: reason, Seq: seq}
}

// evictOldest 踢出内存环最老的一条：它在 accept 时已计入 Pushed，
// 这里只做状态迁移（buffered -> dropped），不能重复增加 Pushed。
func (b *Buffer) evictOldest() {
	b.ring[b.head] = nil
	b.head = (b.head + 1) % b.cfg.Capacity
	b.count--
	b.stats.InMemory--
	b.stats.DroppedOldest++
	b.notifyRoom()
}

// fallbackBlocked 是 block 超时/取消后的兜底。
func (b *Buffer) fallbackBlocked(item map[string]any) PushResult {
	if b.cfg.BlockTimeoutAction == FallbackDropOldest {
		b.evictOldest()
		res := b.accept(item)
		res.Dropped = true
		res.Reason = DropReasonBlock
		// evictOldest 记的是 oldest 原因，这里按真实触发原因重新归类。
		b.stats.DroppedOldest--
		b.stats.DroppedBlocked++
		return res
	}
	return b.dropEntry(DropReasonBlock, func() { b.stats.DroppedBlocked++ })
}

// fallbackSpill 是溢写失败后的分级降级。
func (b *Buffer) fallbackSpill(item map[string]any, spillErr error) PushResult {
	reason := DropReasonIO
	inc := func() { b.stats.DroppedSpillIO++ }
	if errors.Is(spillErr, ErrSpillFull) {
		reason = DropReasonDisk
		inc = func() { b.stats.DroppedSpillFull++ }
	}
	if b.cfg.SpillFullAction == FallbackDropOldest {
		b.evictOldest()
		res := b.accept(item)
		res.Dropped = true
		res.Reason = reason
		b.stats.DroppedOldest--
		inc()
		return res
	}
	return b.dropEntry(reason, inc)
}

// waitRoom 阻塞直到内存环出现空位、超时或关闭。
//
// 基于 sync.Cond：调用时已持有 mu。超时与 ctx 取消各启动一个一次性
// 看门狗，触发时 Broadcast roomCond 把等待者唤醒；醒来后在锁内重新
// 检查真实条件与 deadline，虚假唤醒无害。看门狗用 stop 通道回收，
// 正常被 Pop/Close 唤醒时不会泄漏。
// waitRoom 阻塞直到内存环出现空位、超时或关闭。
//
// 标准等待模式（全程围绕 mu）：
//  1. 循环检查条件，不满足则注册一个容量 1 的信号 channel；
//  2. 释放锁，在 select 中等待信号、超时或 ctx 取消；
//  3. 重新持锁，回到循环顶部复查真实条件，虚假唤醒自然被过滤。
//
// 信号 channel 容量为 1，且通知只在持锁改状态后发出：等待者注册发生在
// 持锁临界区内，要么注册先于后续状态变更（能收到信号），要么复查时条件
// 已满足（直接返回），不存在“信号早到且永久丢失”的窗口。
func (b *Buffer) waitRoom(ctx context.Context) error {
	deadline := time.Time{}
	if b.cfg.BlockTimeout > 0 {
		deadline = time.Now().Add(b.cfg.BlockTimeout)
	}

	for b.count >= b.cfg.Capacity && !b.closed {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		ch := make(chan struct{}, 1)
		b.roomWaiters = append(b.roomWaiters, ch)

		var timerC <-chan time.Time
		var timer *time.Timer
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerC = timer.C
		}
		b.mu.Unlock()
		select {
		case <-ch:
		case <-timerC:
		case <-ctx.Done():
		}
		if timer != nil {
			timer.Stop()
		}
		b.mu.Lock()
		b.removeRoomWaiter(ch)
		// 回到顶部以真实条件/超时/关闭为准，不依赖唤醒来源。
	}
	if b.closed {
		return ErrClosed
	}
	return nil
}

// removeRoomWaiter 注销一个空位等待者，避免向已返回的等待者重复发信号。
func (b *Buffer) removeRoomWaiter(ch chan struct{}) {
	out := b.roomWaiters[:0]
	for _, w := range b.roomWaiters {
		if w != ch {
			out = append(out, w)
		}
	}
	b.roomWaiters = out
}

// removeDataWaiter 注销一个数据等待者。
func (b *Buffer) removeDataWaiter(ch chan struct{}) {
	out := b.dataWaiters[:0]
	for _, w := range b.dataWaiters {
		if w != ch {
			out = append(out, w)
		}
	}
	b.dataWaiters = out
}

// spillMemory 把整个内存环按 FIFO 写成磁盘帧，全部成功后才清空环，
// 不会留下“半溢写”的队列状态；失败时已落盘的帧由下次 manifest 对账。
func (b *Buffer) spillMemory() (bool, error) {
	if b.spill == nil {
		return false, errors.New("spill_dir not configured")
	}
	frames := make([]diskFrame, 0, b.count)
	for i := 0; i < b.count; i++ {
		env := b.ring[(b.head+i)%b.cfg.Capacity]
		raw, err := marshalRecord(env.Record)
		if err != nil {
			return false, err
		}
		frames = append(frames, diskFrame{Seq: env.Seq, Attempts: env.Attempts, Payload: raw})
	}

	var written int
	for _, f := range frames {
		if err := b.spill.writeFrame(f, b.cfg.SpillSegmentBytes); err != nil {
			return false, err
		}
		written++
	}
	// 显式结束本批溢写：把活动段 finalize 并滚动出新活动段。
	// 否则在段字节上限很大时活动段长期不滚动，而活动段“只写不读”，
	// 消费者将读不到这些帧。滚动后刚写的段立即成为可读段。
	if err := b.spill.rollActive(); err != nil {
		return false, err
	}
	// 不在每次溢写都 fsync：高频满负荷下逐批 sync 会把吞吐压垮（抖动）。
	// 持久性边界由 manifest 的周期原子落盘与 Close 时的 sync+flush 保证；
	// 崩溃窗口内丢失的只是尚未刷盘的尾部帧，重启后按 at-least-once 重放。

	for i := 0; i < b.count; i++ {
		b.ring[(b.head+i)%b.cfg.Capacity] = nil
	}
	b.head = 0
	b.count = 0
	b.stats.InMemory = 0
	b.stats.OnDisk += int64(written)
	b.stats.SpilledWrites += int64(written)
	b.persistManifest()
	return true, nil
}

// Pop 阻塞式出队，缓冲关闭且排空后返回 ok=false。
func (b *Buffer) Pop() (*Envelope, bool) {
	return b.pop(0)
}

// PopDeadline 是带截止时间的 Pop，用于关闭时限速排空；
// 超时返回 ok=false 但不改变关闭状态，避免关闭流程被慢生产拖住。
func (b *Buffer) PopDeadline(d time.Duration) (*Envelope, bool) {
	return b.pop(d)
}

func (b *Buffer) pop(timeout time.Duration) (*Envelope, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for {
		if env := b.popRetry(); env != nil {
			return env, true
		}
		if env, ok := b.popSpill(); ok {
			return env, true
		}
		if env, ok := b.popRing(); ok {
			return env, true
		}
		if b.closed {
			return nil, false
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return nil, false
		}

		ch := make(chan struct{}, 1)
		b.dataWaiters = append(b.dataWaiters, ch)
		var timerC <-chan time.Time
		var timer *time.Timer
		if !deadline.IsZero() {
			timer = time.NewTimer(time.Until(deadline))
			timerC = timer.C
		}
		b.mu.Unlock()
		if timer == nil {
			<-ch
		} else {
			select {
			case <-ch:
			case <-timerC:
			}
			timer.Stop()
		}
		b.mu.Lock()
		b.removeDataWaiter(ch)
		// 回到顶部统一复查：取数、关闭、截止时间，过滤虚假唤醒。
	}
}

func (b *Buffer) empty() bool {
	return b.count == 0 && len(b.retry) == 0 &&
		(b.spill == nil || !b.spill.hasReadable())
}

// popRetry 从重试队首取（重试项 Seq 一定最老）。
func (b *Buffer) popRetry() *Envelope {
	if len(b.retry) == 0 {
		return nil
	}
	env := b.retry[0]
	b.retry = b.retry[1:]
	b.stats.InRetry--
	return env
}

// popSpill 从磁盘活段按 FIFO 取下一条可消费记录。
//
// 内部循环负责：跳过 dead/重复帧、跳过损坏帧、在段耗尽时推进到下一段，
// 直到取到一条记录或确认磁盘已空。这样调用方不会因为“本段恰好读完”
// 就错误地回退到内存环，保证 磁盘(更老) → 内存(更新) 的全局 FIFO。
func (b *Buffer) popSpill() (*Envelope, bool) {
	if b.spill == nil || len(b.spill.segments) == 0 {
		return nil, false
	}
	// 所有活段都已读尽（等水位清理）时，磁盘侧没有可消费数据。
	if !b.spill.hasReadable() {
		return nil, false
	}
	for b.spill.hasReadable() {
		reader := b.spill.oldestReader()
		if reader == nil {
			return nil, false
		}
		frame, _, err := reader.next()
		if err == nil {
			if frame.Dead {
				continue
			}
			if _, dead := b.deadSeq[frame.Seq]; dead {
				continue
			}
			// committedSeq 是“已成功发出的最大 Seq”高水位。
			// 仅当确实发生过 Ack（hasCommitted）且帧序号不高于水位时，
			// 才判定为崩溃重放的重复帧；seq=0 在首次 Ack 前不会被误杀。
			if b.hasCommitted && frame.Seq <= b.committedSeq {
				b.stats.ReplayedDupes++
				continue
			}
			rec, err := unmarshalRecord(frame.Payload)
			if err != nil {
				b.stats.CorruptFrames++
				b.stats.DroppedCorrupt++
				if b.stats.OnDisk > 0 {
					b.stats.OnDisk--
				}
				continue
			}
			b.stats.OnDisk--
			b.stats.SpillReads++
			return &Envelope{Seq: frame.Seq, Attempts: frame.Attempts, Record: rec}, true
		}

		// 本段耗尽：精确标记该段读尽并关闭读取器，继续下一段。
		// 段文件本身不删除，等 Ack 水位越过其 LastSeq 再 retire。
		if errors.Is(err, io.EOF) {
			reader.close()
			delete(b.spill.readers, reader.meta.ID)
			b.spill.markDrained(reader.meta.ID)
			continue
		}

		// 运行期损坏：隔离整段，段内未读帧计入损坏损失。
		b.stats.CorruptFrames++
		lost := int64(reader.meta.Frames - reader.read)
		if lost < 0 {
			lost = 0
		}
		b.stats.OnDisk -= lost
		b.stats.DroppedCorrupt += lost
		if qerr := b.spill.quarantine(reader.meta.Name); qerr == nil {
			b.spill.removeMeta(reader.meta.ID)
			b.stats.QuarantinedSegs++
		}
	}
	return nil, false
}

// popRing 从内存环取一条。
func (b *Buffer) popRing() (*Envelope, bool) {
	if b.count == 0 {
		return nil, false
	}
	env := b.ring[b.head]
	b.ring[b.head] = nil
	b.head = (b.head + 1) % b.cfg.Capacity
	b.count--
	b.stats.InMemory--
	// 消费到低水位（容量的一半）以下时重新布防，允许下一次满负荷溢写。
	if b.cfg.Policy == PolicySpill && !b.spillArmed && b.count <= b.cfg.Capacity/2 {
		b.spillArmed = true
	}
	b.notifyRoom()
	return env, true
}

// Ack 确认一条记录已成功写入全部目标 sink，推进提交水位。
func (b *Buffer) Ack(env *Envelope) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stats.Emitted++
	b.hasCommitted = true
	// 单消费者按 FIFO 处理，Ack 天然连续；这里仍取 max 防御乱序调用。
	if env.Seq > b.committedSeq {
		b.committedSeq = env.Seq
	}
	b.ackSinceManifest++
	b.retireSegments()
	if b.ackSinceManifest >= b.cfg.ManifestEveryAcks {
		b.ackSinceManifest = 0
		b.persistManifest()
	}
	b.notifyRoom()
}

// retireSegments 删除 LastSeq 已被提交水位覆盖的活段。
func (b *Buffer) retireSegments() {
	if b.spill == nil {
		return
	}
	changed := false
	for len(b.spill.segments) > 0 {
		first := b.spill.segments[0]
		// 必须同时满足：段已被读尽（drained）且提交水位越过段内最大 Seq。
		// 只看水位会删掉仍在读取的活动段；只看 drained 会删掉尚未确认的段。
		if !b.spill.drained[first.ID] {
			break
		}
		if first.LastSeq > b.committedSeq {
			break
		}
		if err := b.spill.retireOldest(); err != nil {
			break
		}
		changed = true
	}
	if changed {
		b.persistManifest()
	}
}

// Requeue 把 sink 写失败的记录插回重试队列，尝试次数 +1。
// 重试队列按 Seq 升序维护，下一次 Pop 仍然先取最老记录。
func (b *Buffer) Requeue(env *Envelope) {
	b.mu.Lock()
	defer b.mu.Unlock()
	env.Attempts++
	pos := len(b.retry)
	for pos > 0 && b.retry[pos-1].Seq > env.Seq {
		pos--
	}
	b.retry = append(b.retry, nil)
	copy(b.retry[pos+1:], b.retry[pos:])
	b.retry[pos] = env
	b.stats.InRetry++
	b.stats.Requeued++
	b.notifyData()
}

// DeadLetter 把超过重试上限的记录转入 dead-letter。
//
// 配置了 spill 目录时持久化到 dead-*.log（永不回放、可人工检查），
// 序号同时记入 manifest 的 dead 列表，防止原始帧在崩溃后被重复回放；
// 没有 spill 目录时只做计数。两种情况下损失都显式计入 dead_lettered，
// 数据不会悄悄消失。注意不推进 committedSeq，水位连续性由 Ack 保证。
func (b *Buffer) DeadLetter(env *Envelope) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var err error
	if b.spill != nil {
		raw, mErr := marshalRecord(env.Record)
		if mErr == nil {
			err = b.spill.writeFrame(diskFrame{
				Seq:      env.Seq,
				Attempts: env.Attempts,
				Dead:     true,
				Payload:  raw,
			}, b.cfg.SpillSegmentBytes)
		} else {
			err = mErr
		}
		b.deadSeq[env.Seq] = struct{}{}
		b.persistManifest()
	}
	b.stats.DeadLettered++
	b.retireSegments()
	b.notifyRoom()
	return err
}

// Len 返回当前缓冲中的记录总数（内存 + 重试 + 磁盘）。
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return int(b.stats.Buffered())
}

// Stats 返回绝对计数快照（含历史世代累计）。快照与状态迁移在同一把
// 互斥锁内完成，因此快照里的丢弃数与队列真实状态严格同步。
func (b *Buffer) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stats
	s.InMemory = int64(b.count)
	s.InRetry = int64(len(b.retry))
	return s
}

// DeltaStats 返回本世代（本次 New 之后）的增量计数，三态恒等式
//
//	(pushed增量 + 期初存货) ==
//	    emitted增量 + 当前缓冲 + 丢弃增量 + 期初已损坏
//
// 在该快照上严格成立。期初存货是恢复出的在途帧、期初已损坏是恢复时
// 确认的损失，二者都在历史世代计入过 Pushed，本世代只确定去向，
// 因此作为恒等式左侧加项、右侧补一项，避免与历史基线互相抵消。
func (b *Buffer) DeltaStats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	d := b.stats.sub(b.base)
	d.InMemory = int64(b.count)
	d.InRetry = int64(len(b.retry))
	// OnDisk 是仪表量，直接取当前值而非历史差值。
	d.OnDisk = b.stats.OnDisk
	// 期初存货与期初损坏折进 Pushed 侧，调用方仍可用标准 Identity() 校验。
	d.Pushed += b.openingBuffered + b.openingDropped
	return d
}

// Close 关闭缓冲并唤醒所有等待者，可重复调用。
//
// 顺序：先把内存环与重试队列刷到磁盘（spill 策略），再标记关闭并唤醒，
// 最后持久化 manifest 与关闭段文件。这样即使关闭后 manifest 丢失，
// 孤儿段也能完整收养，不会漏掉关闭时仍在内存里的记录。
// 无 spill 时内存数据随进程退出，其规模与丢弃数仍可在 Stats 中查到。
func (b *Buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}

	var flushErr error
	if b.spill != nil {
		// 先刷重试队列（更老）再刷内存环，保持 FIFO。
		if len(b.retry) > 0 || b.count > 0 {
			flushErr = b.flushAllToDisk()
		}
	}

	b.closed = true
	b.notifyRoom()
	b.notifyData()
	if b.spill != nil {
		b.persistManifest()
		if cerr := b.spill.close(); flushErr == nil {
			flushErr = cerr
		}
	}
	return flushErr
}

// flushAllToDisk 把重试队列与内存环全部落盘并清空，调用时持有 mu。
func (b *Buffer) flushAllToDisk() error {
	envs := make([]*Envelope, 0, len(b.retry)+b.count)
	envs = append(envs, b.retry...)
	for i := 0; i < b.count; i++ {
		envs = append(envs, b.ring[(b.head+i)%b.cfg.Capacity])
	}
	written := 0
	for _, env := range envs {
		raw, err := marshalRecord(env.Record)
		if err != nil {
			return err
		}
		if err := b.spill.writeFrame(diskFrame{
			Seq: env.Seq, Attempts: env.Attempts, Payload: raw,
		}, b.cfg.SpillSegmentBytes); err != nil {
			return err
		}
		written++
	}
	if err := b.spill.sync(); err != nil {
		return err
	}
	for i := range b.retry {
		b.retry[i] = nil
	}
	b.retry = nil
	for i := 0; i < b.count; i++ {
		b.ring[(b.head+i)%b.cfg.Capacity] = nil
	}
	b.head, b.count = 0, 0
	b.stats.InMemory, b.stats.InRetry = 0, 0
	b.stats.OnDisk += int64(written)
	b.stats.SpilledWrites += int64(written)
	return nil
}

// persistManifest 把当前水位、段清单、dead 序号与计数刷到 manifest（原子 rename）。
// 调用时持有 mu。
func (b *Buffer) persistManifest() {
	if b.spill == nil || b.mani == nil {
		return
	}
	b.mani.NextSeq = b.nextSeq
	b.mani.CommittedSeq = b.committedSeq
	b.mani.HasCommitted = b.hasCommitted
	b.mani.Segments = append([]segmentMeta(nil), b.spill.segments...)
	b.mani.DeadSegments = append([]segmentMeta(nil), b.spill.deadSegments...)
	b.mani.Stats = b.stats
	dead := make([]uint64, 0, len(b.deadSeq))
	for s := range b.deadSeq {
		if s > b.committedSeq { // 水位之下的序号已由 replay 去重覆盖，压缩掉
			dead = append(dead, s)
		}
	}
	b.mani.DeadSeq = dead
	_ = b.spill.saveManifest(b.mani) // 失败不致命：段文件仍在，重启走孤儿收养
}
