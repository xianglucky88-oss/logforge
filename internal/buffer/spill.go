// 磁盘溢写存储：滚动段文件 + 原子更新的 manifest。
//
// 目录布局（dir 由配置 spill_dir 指定）：
//
//	spill-000001.log   数据段，帧格式见 frame.go
//	spill-000002.log
//	dead-000001.log    dead-letter 段，回放时跳过，但保留可检查
//	manifest.json      已提交位点、段清单与三态计数（原子 rename 落盘）
//	quarantine/        无法读取的坏段隔离到这里，不直接删除
//
// 所有方法都在 Buffer 的互斥锁内被调用，因此 store 自身不再加锁；
// 磁盘 I/O 与队列操作串行化，换取明确的背压语义（磁盘慢=Push 慢=生产者被限速）。
package buffer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
)

const (
	manifestName = "manifest.json"
	segPrefix    = "spill-"
	deadPrefix   = "dead-"
	segSuffix    = ".log"
	quarantineDir = "quarantine"
	manifestVersion = 1
)

// ErrSpillFull 表示达到溢写预算（段数或磁盘字节上限）。
var ErrSpillFull = errors.New("spill budget exhausted")

// segmentMeta 是 manifest 中的一个段条目。
type segmentMeta struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Dead    bool   `json:"dead"`
	Frames  int    `json:"frames"` // 已写入的帧（含 dead 帧）
	Bytes   int64  `json:"bytes"`
	LastSeq uint64 `json:"last_seq"` // 段内最大 Seq，用于按提交水位删段
}

// manifestData 是 manifest.json 的磁盘格式。
type manifestData struct {
	Version      int           `json:"version"`
	Generation   uint64        `json:"generation"`
	NextSeq      uint64        `json:"next_seq"`
	CommittedSeq uint64        `json:"committed_seq"`
	// HasCommitted 区分“水位初值 0”与“确实提交过 seq=0”。
	HasCommitted bool          `json:"has_committed"`
	Segments     []segmentMeta `json:"segments"`
	DeadSegments []segmentMeta `json:"dead_segments"`
	// DeadSeq 是已转入 dead-letter、回放必须跳过的序号（已压缩到水位之上）。
	DeadSeq []uint64 `json:"dead_seq"`
	Stats   Stats    `json:"stats"`
}

// spillStore 管理溢写目录。
type spillStore struct {
	dir string

	// failWrites > 0 时，下一次写段文件返回 ENOSPC 风格错误，
	// 用于在测试中确定性地注入“磁盘空间不足”。
	failWrites atomic.Int32

	nextID      int
	active      *os.File
	activeMeta  segmentMeta
	readers     map[int]*segReader
	segments    []segmentMeta // 按 id 升序，含活动段；dead 段不在此列
	drained     map[int]bool  // 已读尽、等待 Ack 水位清理的段 id
	deadSegments []segmentMeta

	// 预算
	maxSegments int
	maxBytes    int64
	spilledBytes int64 // 当前所有活段字节数

	// hadOrphans 标记 recover 是否在 manifest 之外发现了段文件。
	hadOrphans bool
	// manifestExisted 标记打开时 manifest.json 是否真实存在。
	manifestExisted bool

	// 帧损坏累计（恢复与运行期共用）
	corruptFrames     int
	corruptLost       int // 损坏/不可读而损失的帧数（按 manifest 基线对账）
	quarantined       int
	quarantinedFrames int
}

// segReader 是一个段的延迟打开读取器。
type segReader struct {
	store   *spillStore
	meta    segmentMeta
	f       *os.File
	fr      *frameReader
	closed  bool
	drained bool // 已读到 EOF
	read    int  // 已成功读出的帧数
}

func (sr *segReader) open() error {
	if sr.fr != nil {
		return nil
	}
	f, err := os.Open(filepath.Join(sr.store.dir, sr.meta.Name))
	if err != nil {
		return err
	}
	sr.f = f
	sr.fr = newFrameReader(f)
	return nil
}

// next 返回该段下一帧；干净读完返回 io.EOF。
func (sr *segReader) next() (diskFrame, bool, error) {
	if sr.drained {
		return diskFrame{}, false, io.EOF
	}
	if err := sr.open(); err != nil {
		return diskFrame{}, false, err
	}
	f, resynced, err := sr.fr.readFrame()
	if err != nil {
		if errors.Is(err, io.EOF) {
			sr.drained = true
		}
		return diskFrame{}, resynced, err
	}
	sr.read++
	if resynced {
		sr.store.corruptFrames++
	}
	return f, resynced, nil
}

func (sr *segReader) close() {
	if sr.closed {
		return
	}
	sr.closed = true
	if sr.f != nil {
		_ = sr.f.Close()
	}
}

// openSpill 打开（必要时创建）溢写目录，并执行崩溃恢复扫描。
// 没有目录权限等问题时返回错误；目录不存在会被创建。
func openSpill(dir string, maxSegments int, maxBytes int64) (*spillStore, *manifestData, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create spill dir: %w", err)
	}
	if maxSegments <= 0 {
		maxSegments = 64
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 30 // 1 GiB
	}
	s := &spillStore{
		dir:         dir,
		maxSegments: maxSegments,
		maxBytes:    maxBytes,
		readers:     map[int]*segReader{},
		drained:     map[int]bool{},
	}

	loaded, manifestExisted, err := s.loadManifest()
	if err != nil {
		return nil, nil, err
	}
	s.manifestExisted = manifestExisted
	report, err := s.recover(loaded)
	if err != nil {
		return nil, nil, err
	}
	return s, report, nil
}

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }

func (s *spillStore) loadManifest() (*manifestData, bool, error) {
	raw, err := os.ReadFile(manifestPath(s.dir))
	if errors.Is(err, os.ErrNotExist) {
		return &manifestData{Version: manifestVersion, Stats: newStats()}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("read manifest: %w", err)
	}
	var m manifestData
	if err := json.Unmarshal(raw, &m); err != nil {
		// manifest 损坏：不直接致命，改名隔离后按空 manifest 恢复段文件。
		_ = os.Rename(manifestPath(s.dir), manifestPath(s.dir)+".corrupt")
		return &manifestData{Version: manifestVersion, Stats: newStats()}, true, nil
	}
	if m.Stats.IsZero() {
		m.Stats = newStats()
	}
	return &m, true, nil
}

// recover 对照目录里的实际段文件与 manifest 做对账：
//   - manifest 里有、磁盘上没有的段：从清单剔除（已被删除的正常结果）；
//   - 磁盘上有、manifest 里没有的 spill-*.log：作为孤儿段收养，校验帧；
//   - 无法打开/读取的段：移入 quarantine 并计数；
//   - 每帧做 CRC 校验，损坏帧触发重同步，残余坏字节计入 corrupt 指标；
//   - seq <= committed 的帧标记为重复，交由 Buffer 跳过。
func (s *spillStore) recover(m *manifestData) (*manifestData, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("scan spill dir: %w", err)
	}

	onDisk := map[string]bool{}
	var orphanSpill, orphanDead []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		onDisk[name] = true
		switch {
		case strings.HasPrefix(name, segPrefix):
			if !metaContains(m.Segments, name) && !metaContains(m.DeadSegments, name) {
				orphanSpill = append(orphanSpill, name)
			}
		case strings.HasPrefix(name, deadPrefix):
			if !metaContains(m.DeadSegments, name) {
				orphanDead = append(orphanDead, name)
			}
		}
	}

	// 收养孤儿段（典型来源：段已 finalize 但 manifest 还没来得及 rename 就崩溃）。
	m.Segments = append(m.Segments, metasFromNames(orphanSpill, false)...)
	m.DeadSegments = append(m.DeadSegments, metasFromNames(orphanDead, true)...)

	// 剔除磁盘上已不存在的清单条目。
	m.Segments = filterMetas(m.Segments, onDisk)
	m.DeadSegments = filterMetas(m.DeadSegments, onDisk)

	// 校验所有活段，坏段隔离；以 manifest 里的 Frames 为基线，
	// 实际能读出的好帧数若少于基线，差额就是损坏造成的记录损失。
	// 注意用独立切片收集结果，不能原地复用 m.Segments 的底层数组，
	// 否则回写好帧数会破坏尚未处理元素的基线 Frames。
	kept := make([]segmentMeta, 0, len(m.Segments))
	for _, meta := range m.Segments {
		expected := meta.Frames // manifest 基线；孤儿段此时为 0
		frames, _, bytes, err := s.verifySegment(meta)
		if err != nil {
			if qerr := s.quarantine(meta.Name); qerr == nil {
				s.quarantined++
				s.quarantinedFrames += expected
				// 整段隔离：基线内的帧全部损失（孤儿段无基线，记 0）。
				s.corruptLost += expected
			}
			continue
		}
		if expected > frames {
			s.corruptLost += expected - frames
		}
		meta.Frames = frames
		meta.Bytes = bytes
		kept = append(kept, meta)
		s.spilledBytes += bytes
		if meta.ID >= s.nextID {
			s.nextID = meta.ID + 1
		}
	}
	m.Segments = sortMetas(kept)

	// dead 段不做内容校验（永远不会回放），仅登记大小。
	for i := range m.DeadSegments {
		if info, err := os.Stat(filepath.Join(s.dir, m.DeadSegments[i].Name)); err == nil {
			m.DeadSegments[i].Bytes = info.Size()
		}
		if m.DeadSegments[i].ID >= s.nextID {
			s.nextID = m.DeadSegments[i].ID + 1
		}
	}
	m.DeadSegments = sortMetas(m.DeadSegments)

	s.segments = m.Segments
	s.deadSegments = m.DeadSegments
	s.hadOrphans = len(orphanSpill) > 0 || len(orphanDead) > 0
	// 空目录首次打开不推进 Generation：没有历史数据就不算一个恢复世代。
	// 仅当存在 manifest 或收养了段时，才把世代号 +1。
	if s.manifestExisted || s.hadOrphans {
		m.Generation++
	}
	m.Version = manifestVersion
	return m, nil
}

func metaContains(metas []segmentMeta, name string) bool {
	for _, mm := range metas {
		if mm.Name == name {
			return true
		}
	}
	return false
}

func metasFromNames(names []string, dead bool) []segmentMeta {
	out := make([]segmentMeta, 0, len(names))
	for _, n := range names {
		out = append(out, segmentMeta{ID: idFromName(n), Name: n, Dead: dead})
	}
	return sortMetas(out)
}

func filterMetas(metas []segmentMeta, onDisk map[string]bool) []segmentMeta {
	out := metas[:0]
	for _, mm := range metas {
		if onDisk[mm.Name] {
			out = append(out, mm)
		}
	}
	return out
}

func sortMetas(metas []segmentMeta) []segmentMeta {
	sort.Slice(metas, func(i, j int) bool { return metas[i].ID < metas[j].ID })
	return metas
}

// idFromName 从 spill-000123.log / dead-000123.log 解析数字 id，解析失败给 -1。
func idFromName(name string) int {
	stem := strings.TrimSuffix(name, segSuffix)
	stem = strings.TrimPrefix(stem, segPrefix)
	stem = strings.TrimPrefix(stem, deadPrefix)
	var id int
	if _, err := fmt.Sscanf(stem, "%d", &id); err != nil {
		return -1
	}
	return id
}

// verifySegment 顺序读完整个段并校验，返回（好帧数, 重同步次数, 文件字节数, 错误）。
// 打开失败，或文件非空却一个好帧都读不出来时返回错误，由调用方隔离。
func (s *spillStore) verifySegment(meta segmentMeta) (int, int, int64, error) {
	path := filepath.Join(s.dir, meta.Name)
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, 0, 0, err
	}

	fr := newFrameReader(f)
	frames, resyncs := 0, 0
	for {
		_, resynced, err := fr.readFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return frames, resyncs, info.Size(), err
		}
		if resynced {
			resyncs++
			s.corruptFrames++
		}
		frames++
	}
	// 非空段却读不出任何好帧：无可信内容，按坏段处理。
	if frames == 0 && info.Size() > 0 {
		return 0, resyncs, info.Size(), fmt.Errorf("%w: no readable frame in %s", ErrCorruptFrame, meta.Name)
	}
	return frames, resyncs, info.Size(), nil
}

func (s *spillStore) quarantine(name string) error {
	qdir := filepath.Join(s.dir, quarantineDir)
	if err := os.MkdirAll(qdir, 0o755); err != nil {
		return err
	}
	return os.Rename(filepath.Join(s.dir, name), filepath.Join(qdir, name))
}

// startSegment 滚动出新的活动段。
func (s *spillStore) startSegment(dead bool) error {
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			return err
		}
		if err := s.active.Close(); err != nil {
			return err
		}
		s.active = nil
	}
	if !dead && len(s.segments) >= s.maxSegments {
		return fmt.Errorf("%w: max segments %d", ErrSpillFull, s.maxSegments)
	}
	id := s.nextID
	s.nextID++
	prefix := segPrefix
	if dead {
		prefix = deadPrefix
	}
	name := fmt.Sprintf("%s%06d%s", prefix, id, segSuffix)
	f, err := os.OpenFile(filepath.Join(s.dir, name),
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		s.nextID--
		return err
	}
	meta := segmentMeta{ID: id, Name: name, Dead: dead}
	s.active = f
	s.activeMeta = meta
	if dead {
		s.deadSegments = append(s.deadSegments, meta)
	} else {
		s.segments = append(s.segments, meta)
	}
	return nil
}

// rollActive 结束当前活动段（关闭写句柄但保留文件），下次 writeFrame
// 会按需 startSegment 开新段。被 finalize 的段从此只参与读取，不再追加。
func (s *spillStore) rollActive() error {
	if s.active == nil {
		return nil
	}
	if err := s.active.Sync(); err != nil {
		s.active.Close()
		s.active = nil
		return err
	}
	if err := s.active.Close(); err != nil {
		s.active = nil
		return err
	}
	s.active = nil
	return nil
}

// writeFrame 向活动段追加一帧，必要时先滚动。
// 返回 ErrSpillFull 表示预算耗尽；注入故障或真实 ENOSPC 时返回包装错误。
func (s *spillStore) writeFrame(f diskFrame, segmentBytes int64) error {
	if n := s.failWrites.Load(); n > 0 {
		s.failWrites.Add(-1)
		return fmt.Errorf("injected disk failure: %w", errNoSpace)
	}
	dead := f.Dead
	if s.active == nil || s.activeMeta.Dead != dead ||
		(!dead && segmentBytes > 0 && s.activeMeta.Bytes >= segmentBytes) {
		if err := s.startSegment(dead); err != nil {
			return err
		}
	}
	if !dead && s.spilledBytes+int64(len(encodeFrame(f))) > s.maxBytes {
		return fmt.Errorf("%w: max bytes %d", ErrSpillFull, s.maxBytes)
	}
	raw := encodeFrame(f)
	n, err := s.active.Write(raw)
	if err != nil {
		return fmt.Errorf("spill write: %w", err)
	}
	s.activeMeta.Frames++
	s.activeMeta.Bytes += int64(n)
	s.activeMeta.LastSeq = f.Seq
	if !dead {
		s.spilledBytes += int64(n)
	}
	s.upsertMeta(s.activeMeta)
	return nil
}

// errNoSpace 模拟真实磁盘满的错误哨兵。
var errNoSpace = errors.New("no space left on device")

func (s *spillStore) upsertMeta(meta segmentMeta) {
	list := s.segments
	if meta.Dead {
		list = s.deadSegments
	}
	for i := range list {
		if list[i].ID == meta.ID {
			list[i] = meta
			return
		}
	}
}

// oldestReader 返回最老“可读”活段的读取器（延迟打开）。
// 已读尽（drained）的段在队首等待水位清理，读取时跳过它们。
// isActive 报告该段是否为当前正在追加的活动段。
// 活动段只写不读：读取器只访问已 finalize 的段，避免在同一个文件上
// 边追加边顺序读，读到 EOF 后漏掉后续帧。
func (s *spillStore) isActive(id int) bool {
	return s.active != nil && s.activeMeta.ID == id
}

func (s *spillStore) oldestReader() *segReader {
	for _, meta := range s.segments {
		if s.drained[meta.ID] || s.isActive(meta.ID) {
			continue
		}
		if sr, ok := s.readers[meta.ID]; ok {
			return sr
		}
		sr := &segReader{store: s, meta: meta}
		s.readers[meta.ID] = sr
		return sr
	}
	return nil
}

// markDrained 精确把指定 id 的段标记为读尽。
func (s *spillStore) markDrained(id int) {
	s.drained[id] = true
}

// hasReadable 报告是否还有未读尽、且非活动的可读段。
func (s *spillStore) hasReadable() bool {
	for _, meta := range s.segments {
		if !s.drained[meta.ID] && !s.isActive(meta.ID) {
			return true
		}
	}
	return false
}

// retireOldest 删除最老的活段（其所有帧都已被 Ack）。
func (s *spillStore) retireOldest() error {
	if len(s.segments) == 0 {
		return nil
	}
	meta := s.segments[0]
	if sr, ok := s.readers[meta.ID]; ok {
		sr.close()
		delete(s.readers, meta.ID)
	}
	err := os.Remove(filepath.Join(s.dir, meta.Name))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.spilledBytes -= meta.Bytes
	if s.spilledBytes < 0 {
		s.spilledBytes = 0
	}
	delete(s.drained, meta.ID)
	s.segments = s.segments[1:]
	return nil
}

// removeMeta 从活段清单中摘除指定 id（运行期损坏隔离后调用）。
func (s *spillStore) removeMeta(id int) {
	out := s.segments[:0]
	for _, meta := range s.segments {
		if meta.ID != id {
			out = append(out, meta)
		}
	}
	s.segments = out
	delete(s.drained, id)
}

// openSegFile 打开段文件供恢复扫描，独立于延迟读取器。
func openSegFile(dir, name string) (*os.File, error) {
	return os.Open(filepath.Join(dir, name))
}

// sync 持久化活动段数据（不强制每帧调用）。
func (s *spillStore) sync() error {
	if s.active != nil {
		return s.active.Sync()
	}
	return nil
}

// close 收尾活动段。
func (s *spillStore) close() error {
	for _, sr := range s.readers {
		sr.close()
	}
	if s.active != nil {
		if err := s.active.Sync(); err != nil {
			s.active.Close()
			s.active = nil
			return err
		}
		err := s.active.Close()
		s.active = nil
		return err
	}
	return nil
}

// saveManifest 原子写 manifest：临时文件 + fsync + rename。
func (s *spillStore) saveManifest(m *manifestData) error {
	m.Version = manifestVersion
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, manifestPath(s.dir))
}

// injectDiskFault 让接下来 n 次 writeFrame 失败，测试专用。
func (s *spillStore) injectDiskFault(n int) { s.failWrites.Store(int32(n)) }
