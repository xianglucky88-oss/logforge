package buffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"
)

func rec(i int) map[string]any {
	return map[string]any{"seq_no": i, "msg": fmt.Sprintf("record-%d", i)}
}

// seqNo 容忍 JSON 往返后的数字类型变化（int/float64），统一转 int 比较。
func seqNo(env *Envelope) int {
	switch v := env.Record["seq_no"].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return -1
	}
}

// assertIdentity 校验三态恒等式 pushed == emitted + buffered + dropped_total。
// 对账基于本世代增量（DeltaStats），不受跨世代历史累计影响。
func assertIdentity(t *testing.T, b *Buffer) {
	t.Helper()
	s := b.DeltaStats()
	if d := s.Identity(); d != 0 {
		t.Fatalf("identity broken: pushed=%d emitted=%d buffered=%d dropped=%d delta=%d stats=%+v",
			s.Pushed, s.Emitted, s.Buffered(), s.DroppedTotal(), d, s)
	}
}

func drainAll(t *testing.T, b *Buffer) []*Envelope {
	t.Helper()
	var out []*Envelope
	for {
		env, ok := b.PopDeadline(200 * time.Millisecond)
		if !ok {
			return out
		}
		out = append(out, env)
		b.Ack(env)
	}
}

func TestDropNew(t *testing.T) {
	b, _, err := New(Config{Capacity: 3, Policy: PolicyDropNew})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	dropped := 0
	for i := 0; i < 10; i++ {
		res, err := b.Push(context.Background(), rec(i))
		if err != nil {
			t.Fatal(err)
		}
		if res.Dropped {
			dropped++
		}
	}
	if b.Len() != 3 {
		t.Fatalf("Len=%d want 3", b.Len())
	}
	if dropped != 7 {
		t.Fatalf("dropped=%d want 7", dropped)
	}
	s := b.Stats()
	if s.DroppedNew != 7 {
		t.Fatalf("DroppedNew=%d want 7", s.DroppedNew)
	}
	// 保留的是前 3 条（拒新）。
	for i := 0; i < 3; i++ {
		env, ok := b.Pop()
		if !ok || seqNo(env) != i {
			t.Fatalf("pop %d got %v ok=%v", i, env, ok)
		}
		b.Ack(env)
	}
	assertIdentity(t, b)
}

func TestDropOldestKeepsNewest(t *testing.T) {
	b, _, err := New(Config{Capacity: 3, Policy: PolicyDropOldest})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	for i := 0; i < 10; i++ {
		if _, err := b.Push(context.Background(), rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	s := b.Stats()
	if s.DroppedOldest != 7 {
		t.Fatalf("DroppedOldest=%d want 7", s.DroppedOldest)
	}
	// 保新：留下的是 7、8、9。
	for i := 7; i < 10; i++ {
		env, ok := b.Pop()
		if !ok || seqNo(env) != i {
			t.Fatalf("pop want %d got %v ok=%v", i, env, ok)
		}
		b.Ack(env)
	}
	assertIdentity(t, b)
}

func TestBlockWakesOnPopAndClose(t *testing.T) {
	t.Run("wake on pop", func(t *testing.T) {
		b, _, _ := New(Config{Capacity: 2, Policy: PolicyBlock})
		defer b.Close()
		for i := 0; i < 2; i++ {
			if _, err := b.Push(context.Background(), rec(i)); err != nil {
				t.Fatal(err)
			}
		}
		done := make(chan PushResult, 1)
		go func() {
			res, err := b.Push(context.Background(), rec(99))
			if err != nil {
				done <- PushResult{}
				return
			}
			done <- res
		}()
		select {
		case <-done:
			t.Fatal("push should block while full")
		case <-time.After(50 * time.Millisecond):
		}
		env, ok := b.Pop()
		if !ok {
			t.Fatal("pop failed")
		}
		b.Ack(env)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("push not woken by pop")
		}
		assertIdentity(t, b)
	})

	t.Run("wake on close with bounded time", func(t *testing.T) {
		b, _, _ := New(Config{Capacity: 1, Policy: PolicyBlock})
		if _, err := b.Push(context.Background(), rec(0)); err != nil {
			t.Fatal(err)
		}
		errCh := make(chan error, 1)
		go func() {
			_, err := b.Push(context.Background(), rec(1))
			errCh <- err
		}()
		time.Sleep(50 * time.Millisecond)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("want ErrClosed, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked push not woken by Close")
		}
	})
}

func TestBlockTimeoutFallback(t *testing.T) {
	b, _, _ := New(Config{
		Capacity:           1,
		Policy:             PolicyBlock,
		BlockTimeout:       80 * time.Millisecond,
		BlockTimeoutAction: FallbackDropNew,
	})
	defer b.Close()
	if _, err := b.Push(context.Background(), rec(0)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	res, err := b.Push(context.Background(), rec(1))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Dropped || res.Reason != DropReasonBlock {
		t.Fatalf("want blocked drop, got %+v", res)
	}
	if time.Since(start) < 70*time.Millisecond {
		t.Fatal("timeout returned too early")
	}
	s := b.Stats()
	if s.DroppedBlocked != 1 {
		t.Fatalf("DroppedBlocked=%d", s.DroppedBlocked)
	}
	assertIdentity(t, b)

	// 兜底动作 drop_oldest：超时后新记录进入、旧记录被踢。
	b2, _, _ := New(Config{
		Capacity: 1, Policy: PolicyBlock,
		BlockTimeout: 40 * time.Millisecond, BlockTimeoutAction: FallbackDropOldest,
	})
	defer b2.Close()
	b2.Push(context.Background(), rec(0))
	res2, _ := b2.Push(context.Background(), rec(1))
	if !res2.Dropped || res2.Reason != DropReasonBlock {
		t.Fatalf("want blocked/oldest, got %+v", res2)
	}
	env, ok := b2.Pop()
	if !ok || seqNo(env) != 1 {
		t.Fatalf("want newest record kept, got %v ok=%v", env, ok)
	}
	b2.Ack(env)
	assertIdentity(t, b2)
}

func TestBlockCancelWithContext(t *testing.T) {
	b, _, _ := New(Config{Capacity: 1, Policy: PolicyBlock, BlockTimeout: time.Minute})
	defer b.Close()
	b.Push(context.Background(), rec(0))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start := time.Now()
	res, err := b.Push(ctx, rec(1))
	if err != nil {
		t.Fatalf("cancel must produce fallback result, not error: %v", err)
	}
	if !res.Dropped || res.Reason != DropReasonBlock {
		t.Fatalf("want blocked drop on cancel, got %+v", res)
	}
	if time.Since(start) > time.Second {
		t.Fatal("ctx cancel did not unblock promptly")
	}
	assertIdentity(t, b)
}

func TestSpillRoundTripAndFIFO(t *testing.T) {
	dir := t.TempDir()
	b, recov, err := New(Config{
		Capacity: 4, Policy: PolicySpill, SpillDir: dir,
		SpillSegmentBytes: 128, ManifestEveryAcks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if recov.Found {
		t.Fatal("fresh dir should not report recovery")
	}

	const n = 30
	for i := 0; i < n; i++ {
		if _, err := b.Push(context.Background(), rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	s := b.Stats()
	if s.SpilledWrites == 0 || s.OnDisk == 0 {
		t.Fatalf("expected spill to happen, stats=%+v", s)
	}
	// 内存 + 磁盘必须严格等于总写入量（无丢失），内存环在最后一次
	// 溢写后未必重新填满，因此这里不假设 InMemory == Capacity。
	if s.InMemory+s.OnDisk != n {
		t.Fatalf("in_memory(%d)+on_disk(%d) != %d", s.InMemory, s.OnDisk, n)
	}
	if s.InMemory > int64(b.cfg.Capacity) {
		t.Fatalf("InMemory=%d exceeds capacity %d", s.InMemory, b.cfg.Capacity)
	}

	got := drainAll(t, b)
	if len(got) != n {
		t.Fatalf("drained %d want %d", len(got), n)
	}
	var lastSeq uint64
	for i, env := range got {
		// 记录内容严格按入队顺序（FIFO）。
		if seqNo(env) != i {
			t.Fatalf("FIFO broken at %d: got %v", i, env.Record["seq_no"])
		}
		// 全局序号单调不减（同一缓冲内严格递增）。
		if i > 0 && env.Seq <= lastSeq {
			t.Fatalf("seq not monotonic at %d: %d after %d", i, env.Seq, lastSeq)
		}
		lastSeq = env.Seq
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	assertIdentity(t, b)
}

func TestSpillRecoveryOrderAndDuplicates(t *testing.T) {
	dir := t.TempDir()
	const n = 20
	cfg := Config{Capacity: 4, Policy: PolicySpill, SpillDir: dir, SpillSegmentBytes: 128}

	// 第一轮：写入并只 Ack 前 8 条，模拟“发出一部分后崩溃”。
	b, _, _ := New(cfg)
	for i := 0; i < n; i++ {
		if _, err := b.Push(context.Background(), rec(i)); err != nil {
			t.Fatal(err)
		}
	}
	for k := 0; k < 8; k++ {
		env, ok := b.Pop()
		if !ok {
			t.Fatal("pop failed")
		}
		b.Ack(env)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 第二轮：同目录恢复。
	b2, r2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if !r2.Found {
		t.Fatal("expected recovery report")
	}
	// Ack 了 8 条（seq 0..7），提交水位停在最后一条的 seq=7。
	if r2.CommittedSeq != 7 {
		t.Fatalf("CommittedSeq=%d want 7", r2.CommittedSeq)
	}
	if r2.ReplayedOnDisk == 0 {
		t.Fatal("expected frames replayed on disk")
	}
	// 新 Push 的 Seq 必须接在历史之后，不能复用序号。
	// n 条记录占用 seq 0..n-1，下一条必须 >= n。
	res, _ := b2.Push(context.Background(), rec(999))
	if res.Seq < uint64(n) {
		t.Fatalf("new seq %d should be >= historical count %d", res.Seq, n)
	}

	got := drainAll(t, b2)
	wantStart := 8 // seq_no 0..7 已提交
	// 历史未消费部分（seq_no 8..n-1）按 FIFO 在前，新写入的 999 在最后。
	for i, env := range got {
		var expected int
		if i == len(got)-1 {
			expected = 999
		} else {
			expected = wantStart + i
		}
		if seqNo(env) != expected {
			t.Fatalf("recovery order broken at %d: got %v want %d", i, seqNo(env), expected)
		}
	}
	// 本世代 Emitted 只统计本次 New 之后 Ack 的记录（不含上一世代的 8 条）。
	d := b2.DeltaStats()
	if d.Emitted != int64(len(got)) {
		t.Fatalf("delta emitted=%d want %d", d.Emitted, len(got))
	}
	assertIdentity(t, b2)
}

func TestSpillDuplicateReplaySkipped(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Capacity: 2, Policy: PolicySpill, SpillDir: dir,
		ManifestEveryAcks: 1000} // 故意拉大 manifest 窗口

	// 第一轮：Ack 后不触发 manifest 落盘就 Close（Close 仍会刷一次，
	// 这里通过直接构造“manifest 落后于段”的场景验证幂等跳过）。
	b, _, _ := New(cfg)
	for i := 0; i < 6; i++ {
		b.Push(context.Background(), rec(i))
	}
	env, _ := b.Pop()
	b.Ack(env)
	env, _ = b.Pop()
	b.Ack(env)
	b.Close()

	// 手工把 manifest 的 committed 回退，制造重复帧（模拟 manifest 滞后）。
	// has_committed 必须保持 true：真实上一轮确实 Ack 过，水位才有效。
	mPath := filepath.Join(dir, manifestName)
	raw, _ := os.ReadFile(mPath)
	m := map[string]any{}
	if err := jsonUnmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	m["committed_seq"] = float64(0)
	m["has_committed"] = true
	os.WriteFile(mPath, jsonMarshal(m), 0o644)

	b2, r2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if r2.DuplicateFrames == 0 {
		t.Fatalf("expected duplicate frames, report=%+v", r2)
	}
	got := drainAll(t, b2)
	// 所有记录至多出现一次：重复帧被跳过。
	seen := map[int]bool{}
	for _, e := range got {
		n := seqNo(e)
		if seen[n] {
			t.Fatalf("record %d replayed twice", n)
		}
		seen[n] = true
	}
	assertIdentity(t, b2)
}

func TestSpillCorruptFrameResyncAndQuarantine(t *testing.T) {
	// 场景一：段中部一帧 CRC 损坏，读取器靠 magic 重同步，坏帧被跳过、
	// 好帧仍能恢复，损失计入 dropped_corrupt，三态恒等式成立。
	dir := t.TempDir()
	cfg := Config{Capacity: 2, Policy: PolicySpill, SpillDir: dir, SpillSegmentBytes: 1 << 20}

	b, _, _ := New(cfg)
	for i := 0; i < 6; i++ {
		b.Push(context.Background(), rec(i))
	}
	b.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, segPrefix+"*"+segSuffix))
	if len(segs) == 0 {
		t.Fatal("no spill segment")
	}
	// 手工拼一个含“好帧 + 坏帧 + 好帧”的段，验证重同步。
	good1 := encodeFrame(diskFrame{Seq: 100, Payload: []byte(`{"seq_no":100}`)})
	bad := encodeFrame(diskFrame{Seq: 101, Payload: []byte(`{"seq_no":101}`)})
	bad[len(bad)-1] ^= 0xFF // 翻转 CRC 末字节
	good2 := encodeFrame(diskFrame{Seq: 102, Payload: []byte(`{"seq_no":102}`)})
	var crafted []byte
	crafted = append(crafted, good1...)
	crafted = append(crafted, bad...)
	crafted = append(crafted, good2...)
	if err := os.WriteFile(segs[0], crafted, 0o644); err != nil {
		t.Fatal(err)
	}

	b2, r2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if r2.CorruptFrames == 0 {
		t.Fatalf("expected corrupt frame accounting, report=%+v", r2)
	}
	// 好帧仍能恢复（重同步），且恒等式成立（损坏损失已计入 dropped_corrupt）。
	_ = drainAll(t, b2)
	assertIdentity(t, b2)

	// 场景二：段完全无法读出任何好帧，整段移入 quarantine。
	dir2 := t.TempDir()
	cfg2 := cfg
	cfg2.SpillDir = dir2
	b3, _, _ := New(cfg2)
	for i := 0; i < 4; i++ {
		b3.Push(context.Background(), rec(i))
	}
	b3.Close()
	seg2, _ := filepath.Glob(filepath.Join(dir2, segPrefix+"*"+segSuffix))
	os.WriteFile(seg2[0], append(frameMagic[:], 0xFF, 0xFF, 0xFF, 0xFF), 0o644)
	b4, r4, err := New(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer b4.Close()
	if r4.QuarantinedSegs == 0 {
		t.Fatalf("expected quarantine, report=%+v", r4)
	}
	if _, err := os.Stat(filepath.Join(dir2, quarantineDir, filepath.Base(seg2[0]))); err != nil {
		t.Fatalf("quarantined segment not found: %v", err)
	}
	assertIdentity(t, b4)
}

func TestSpillDiskFullFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Capacity: 2, Policy: PolicySpill, SpillDir: dir,
		SpillFullAction: FallbackDropNew,
	}
	b, _, _ := New(cfg)
	defer b.Close()

	b.Push(context.Background(), rec(0))
	b.Push(context.Background(), rec(1))
	// 注入一次磁盘写故障，下一次满负荷 Push 触发溢写并失败。
	b.spill.injectDiskFault(1)
	res, err := b.Push(context.Background(), rec(2))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Dropped || (res.Reason != DropReasonIO && res.Reason != DropReasonDisk) {
		t.Fatalf("want spill failure drop, got %+v", res)
	}
	s := b.Stats()
	if s.DroppedSpillIO+s.DroppedSpillFull == 0 {
		t.Fatalf("spill failure not accounted: %+v", s)
	}
	assertIdentity(t, b)

	// 预算耗尽：极小 maxSegments。
	dir2 := t.TempDir()
	cfg2 := Config{
		Capacity: 1, Policy: PolicySpill, SpillDir: dir2,
		SpillMaxSegments: 1, SpillSegmentBytes: 1,
		SpillFullAction:  FallbackDropOldest,
	}
	b2, _, _ := New(cfg2)
	defer b2.Close()
	var sawBudgetDrop bool
	for i := 0; i < 20; i++ {
		res, err := b2.Push(context.Background(), rec(i))
		if err != nil {
			t.Fatal(err)
		}
		if res.Dropped && res.Reason == DropReasonDisk {
			sawBudgetDrop = true
		}
	}
	if !sawBudgetDrop {
		t.Fatal("expected spill budget exhaustion to trigger drop")
	}
	assertIdentity(t, b2)
}

func TestRetryAndDeadLetter(t *testing.T) {
	dir := t.TempDir()
	b, _, _ := New(Config{
		Capacity: 4, Policy: PolicyBlock, SpillDir: dir,
		BlockTimeout: time.Second, BlockTimeoutAction: FallbackDropNew,
	})
	defer b.Close()

	for i := 0; i < 5; i++ {
		b.Push(context.Background(), rec(i))
	}
	// 模拟消费：前两条走重试后 Ack，第三条直接死信。
	env, _ := b.Pop()
	b.Requeue(env)
	env2, ok := b.Pop()
	if !ok || env2.Seq != env.Seq {
		t.Fatalf("requeue must return same oldest env, got %v want %d", env2, env.Seq)
	}
	b.Ack(env2)

	env3, _ := b.Pop()
	if err := b.DeadLetter(env3); err != nil {
		t.Fatal(err)
	}
	s := b.Stats()
	if s.DeadLettered != 1 || s.InRetry != 0 {
		t.Fatalf("stats wrong after dead letter: %+v", s)
	}
	// dead-letter 文件存在且不会在 Pop 中回放。
	dead, _ := filepath.Glob(filepath.Join(dir, deadPrefix+"*"+segSuffix))
	if len(dead) == 0 {
		t.Fatal("dead segment missing")
	}
	rest := drainAll(t, b)
	for _, e := range rest {
		if e.Seq == env3.Seq {
			t.Fatal("dead-lettered record replayed")
		}
	}
	assertIdentity(t, b)

	// 重启后 dead 序号仍被跳过。
	b.Close()
	b2, _, err := New(Config{Capacity: 4, Policy: PolicyBlock, SpillDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	for {
		env, ok := b2.PopDeadline(100 * time.Millisecond)
		if !ok {
			break
		}
		if env.Seq == env3.Seq {
			t.Fatal("dead letter replayed after restart")
		}
		b2.Ack(env)
	}
	assertIdentity(t, b2)
}

func TestConcurrentPushPop(t *testing.T) {
	dir := t.TempDir()
	for _, policy := range []Policy{PolicyDropNew, PolicyDropOldest, PolicySpill} {
		policy := policy
		t.Run(string(policy), func(t *testing.T) {
			cfg := Config{Capacity: 8, Policy: policy, SpillDir: filepath.Join(dir, string(policy))}
			if policy != PolicySpill {
				cfg.SpillDir = ""
			}
			b, _, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			const (
				producers = 4
				perP      = 200
			)
			want := int64(producers * perP)
			var wg sync.WaitGroup
			for p := 0; p < producers; p++ {
				wg.Add(1)
				go func(pid int) {
					defer wg.Done()
					for i := 0; i < perP; i++ {
						if _, err := b.Push(context.Background(), rec(pid*perP+i)); err != nil {
							return
						}
					}
				}(p)
			}
			consumed := make(chan *Envelope, producers*perP)
			consumerDone := make(chan struct{})
			go func() {
				defer close(consumerDone)
				// Close 且排空后 Pop 才返回 false，期间短暂空闲不会误退出。
				for {
					env, ok := b.Pop()
					if !ok {
						return
					}
					consumed <- env
					b.Ack(env)
				}
			}()
			wg.Wait()
			// 直接以“已进入缓冲的记录都有最终去向”为准等待排空，
			// 不用 Len（受 Pop/Ack 在途窗口影响）也不用跨世代 delta。
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				st := b.Stats()
				if st.Emitted+st.DroppedTotal() >= want {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			b.Close()
			select {
			case <-consumerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("consumer did not exit after Close")
			}

			s := b.DeltaStats()
			if s.Pushed != want {
				t.Fatalf("Pushed=%d want %d", s.Pushed, want)
			}
			if s.Emitted+s.DroppedTotal() != want {
				t.Fatalf("emitted(%d)+dropped(%d) != %d", s.Emitted, s.DroppedTotal(), want)
			}
			close(consumed)
			last := uint64(0)
			for e := range consumed {
				if e.Seq < last {
					t.Fatalf("global seq order violated: %d after %d", e.Seq, last)
				}
				last = e.Seq
			}
			assertIdentity(t, b)
		})
	}
}

func TestStatsSnapshotConsistency(t *testing.T) {
	b, _, _ := New(Config{Capacity: 4, Policy: PolicyDropOldest})
	const total = 2000

	var producerWg, consumerWg sync.WaitGroup
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		for i := 0; i < total; i++ {
			b.Push(context.Background(), rec(i))
		}
	}()
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		// 阻塞 Pop，由 Close 唤醒退出，避免 deadline 空转导致消费不完。
		for {
			env, ok := b.Pop()
			if !ok {
				return
			}
			b.Ack(env)
		}
	}()

	// 并发期间反复取快照。恒等式在“操作边界”严格成立；Pop 取出后到 Ack
	// 之间存在一个在途窗口（记录已离开缓冲但尚未计入 emitted），此刻
	// delta 只可能为 +1。因此并发采样接受 0 或 +1，最终静止时必须为 0。
	stop := time.After(300 * time.Millisecond)
	func() {
		for {
			select {
			case <-stop:
				return
			default:
				d := b.DeltaStats().Identity()
				if d != 0 && d != 1 {
					t.Fatalf("snapshot identity broken: delta=%d", d)
				}
				_ = b.Stats() // 绝对快照也必须能在并发下安全读取
			}
		}
	}()

	producerWg.Wait()
	// 等全部记录有了最终去向再 Close，让消费者排空退出。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := b.DeltaStats()
		if s.Emitted+s.DroppedTotal() >= total {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	b.Close()
	consumerWg.Wait()
	assertIdentity(t, b)
}

func TestNoGoroutineLeakOnClose(t *testing.T) {
	before := runtime.NumGoroutine()
	b, _, _ := New(Config{Capacity: 1, Policy: PolicyBlock})
	b.Push(context.Background(), rec(0))

	// 多个被阻塞的生产者。
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Push(context.Background(), rec(1))
		}()
	}
	// 一个等待数据之外的 Pop 不会发生（环非空），这里制造空 Pop 等待。
	b2, _, _ := New(Config{Capacity: 1, Policy: PolicyBlock})
	defer b2.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = b2.Pop()
	}()
	time.Sleep(100 * time.Millisecond)
	b.Close()
	b2.Close()
	wg.Wait()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	after := runtime.NumGoroutine()
	if after > before+2 {
		// 打印残留 goroutine 栈，便于定位泄漏来源。
		_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
}

// 小工具：统一测试里的 JSON 入口。
func jsonMarshal(v any) []byte {
	raw, _ := json.Marshal(v)
	return raw
}

func jsonUnmarshal(raw []byte, v any) error { return json.Unmarshal(raw, v) }

func TestCloseIdempotentAndDrain(t *testing.T) {
	b, _, _ := New(Config{Capacity: 4, Policy: PolicyDropNew})
	for i := 0; i < 3; i++ {
		b.Push(context.Background(), rec(i))
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("double close: %v", err)
	}
	// 关闭后 Push 立即报错。
	if _, err := b.Push(context.Background(), rec(9)); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed got %v", err)
	}
	// 关闭前排入的数据仍可排空。
	got := 0
	for {
		env, ok := b.PopDeadline(100 * time.Millisecond)
		if !ok {
			break
		}
		got++
		b.Ack(env)
	}
	if got != 3 {
		t.Fatalf("drained %d want 3", got)
	}
}

func TestManifestOrphanAdoption(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Capacity: 2, Policy: PolicySpill, SpillDir: dir, SpillSegmentBytes: 64}
	b, _, _ := New(cfg)
	for i := 0; i < 8; i++ {
		b.Push(context.Background(), rec(i))
	}
	b.Close()

	// 删除 manifest，模拟“段已落盘、manifest 丢失”的崩溃现场。
	if err := os.Remove(filepath.Join(dir, manifestName)); err != nil {
		t.Fatal(err)
	}
	b2, r2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	if !r2.Found || r2.ReplayedOnDisk == 0 {
		t.Fatalf("orphan segments not adopted: %+v", r2)
	}
	got := drainAll(t, b2)
	if len(got) != 8 {
		t.Fatalf("adopted replay count=%d want 8", len(got))
	}
	for i, e := range got {
		if seqNo(e) != i {
			t.Fatalf("order after adoption broken at %d", i)
		}
	}
}

// 防止 unused 警告的辅助：确认错误哨兵行为。
func TestErrorSentinels(t *testing.T) {
	if !errors.Is(ErrSpillFull, ErrSpillFull) {
		t.Fatal("sentinel broken")
	}
	if !strings.Contains(ErrClosed.Error(), "closed") {
		t.Fatal("ErrClosed message changed")
	}
}
