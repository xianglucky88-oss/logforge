// Package tail 负责跟踪单个文件的新增内容。
//
// 跟踪协议围绕"文件身份"（dev:inode）而不是路径：
//
//   - 路径指向的身份变化 = 轮转：旧文件句柄保留为 rotated source 继续
//     读取（覆盖"轮转后旧文件仍被写入"），连续静默若干轮或超时后封闭；
//     新身份从 0 或其持久化位点开始。
//   - fstat 发现 size < 已读位置 = 截断（copytruncate）：回到 0 重读，
//     并通知上层重置该身份的已确认位点。
//   - 读到 EOF 的半行（没有换行结尾）缓存在内存 pending 里，退出时
//     交给上层持久化；重启时预填，跨重启拼成完整记录。
//   - 启动时按 path* 扫描目录，用 inode 找回停机期间被轮转的物理文件，
//     从持久化位点把未读内容读完（backlog），找不到的计入丢失。
package tail

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/internal-platform/logforge/internal/offset"
)

// EventKind 是 tailer 上报的可观察事件类型。
type EventKind string

const (
	// EventOpen 打开了一个新来源。Detail 是起始偏移。
	EventOpen EventKind = "open"
	// EventCurrent 路径当前指向的身份变了（新打开或提升 drain 中的来源）。
	EventCurrent EventKind = "current"
	// EventRotation 路径指向了新的身份，旧来源转入 drain。
	EventRotation EventKind = "rotation"
	// EventTruncate 检测到截断（size < 已读位置），来源已回到 0。
	EventTruncate EventKind = "truncate"
	// EventBacklogOpen 启动扫描找回了停机期间被轮转的文件。
	EventBacklogOpen EventKind = "backlog_open"
	// EventBacklogLost 有身份的未读内容已无法找回（文件被删除）。
	// Detail 是丢失的字节数估计。
	EventBacklogLost EventKind = "backlog_lost"
	// EventDrainSeal 一个 rotated source 静默期满被封闭。Detail 是最终大小。
	EventDrainSeal EventKind = "drain_seal"
	// EventPendingResumed 持久化的半行校验通过并被预填。
	EventPendingResumed EventKind = "pending_resumed"
	// EventPendingDropped 持久化的半行校验失败（内容已被改写）。
	EventPendingDropped EventKind = "pending_dropped"
	// EventResumed 从持久化位点续读。Detail 是位点。
	EventResumed EventKind = "resumed"
)

// Event 是 tailer 上报给上层（指标/日志）的事件。
type Event struct {
	Kind   EventKind
	ID     offset.FileID
	Detail int64
}

// Line 是一条完整（以换行结尾）的行及其来源坐标。
type Line struct {
	ID   offset.FileID
	Text string
	// End 是这行换行符之后的字节偏移。
	End int64
}

// Options 是 Tailer 的构造参数。
type Options struct {
	Path         string
	PollInterval time.Duration
	// Resume 是各身份的持久化状态（来自位点文件）。
	Resume map[offset.FileID]offset.State
	// LegacyOffset 是 v1 迁移来的未验证位点，只应用于启动时
	// 的当前文件，且仅当其大小足够。
	LegacyOffset int64
	// DrainQuietPolls 是 rotated source 连续静默多少轮后被封闭。
	DrainQuietPolls int
	// DrainMax 是 rotated source 最长的 drain 时间。
	DrainMax time.Duration
	// BacklogGlob 覆盖启动扫描用的匹配模式；空表示 path+"*"。
	BacklogGlob string
	// OnEvent 接收可观察事件，可以为 nil。
	OnEvent func(Event)
}

// source 是一个打开的物理文件。
type source struct {
	id      offset.FileID
	f       *os.File
	reader  *bufio.Reader
	pos     int64  // 已读位置（含 pending）
	pending []byte // 未换行的半行字节
	rotated bool
	quiet   int
	since   time.Time // 进入 rotated 状态的时间
}

// Tailer 按行读取文件，读到末尾后轮询等待新内容，
// 同时跟踪路径身份变化与所有未封闭的 rotated source。
type Tailer struct {
	opts    Options
	onLine  func(Line)
	onError func(error)

	sources map[offset.FileID]*source
	current offset.FileID // "" 表示路径当前不存在
}

// New 构造 Tailer。onLine 不能为 nil。
func New(opts Options, onLine func(Line), onError func(error)) *Tailer {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 200 * time.Millisecond
	}
	if opts.DrainQuietPolls <= 0 {
		opts.DrainQuietPolls = 3
	}
	if opts.DrainMax <= 0 {
		opts.DrainMax = 30 * time.Second
	}
	if opts.Resume == nil {
		opts.Resume = map[offset.FileID]offset.State{}
	}
	return &Tailer{
		opts:    opts,
		onLine:  onLine,
		onError: onError,
		sources: map[offset.FileID]*source{},
	}
}

// Run 阻塞运行直到 ctx 结束。返回所有未封闭来源的最终观察，
// 供上层把半行缓存与文件大小并入位点状态。
func (t *Tailer) Run(ctx context.Context) ([]offset.ReadReport, error) {
	t.startup()

	for {
		if ctx.Err() != nil {
			return t.reports(), nil
		}

		progressed := false
		// 按身份排序遍历，让行为可复现（测试友好）。
		for _, s := range t.orderedSources() {
			if t.drain(ctx, s) {
				progressed = true
			}
			if ctx.Err() != nil {
				break
			}
		}

		t.checkPath()
		t.sealQuiet()

		if !progressed {
			select {
			case <-ctx.Done():
			case <-time.After(t.opts.PollInterval):
			}
		}
	}
}

// startup 打开启动时能找到的所有来源：当前路径 + 停机期间被轮转的 backlog。
func (t *Tailer) startup() {
	liveID, liveOK := identify(t.opts.Path)

	// 1. 启动扫描：在 path* 里找回持久化身份对应的物理文件。
	found := map[offset.FileID]bool{}
	for _, name := range t.backlogCandidates() {
		if name == t.opts.Path {
			continue // 当前路径单独处理
		}
		id, ok := identify(name)
		if !ok {
			continue
		}
		st, known := t.opts.Resume[id]
		if !known {
			continue // 不是我们的历史，可能是无关文件
		}
		found[id] = true
		if liveOK && id == liveID {
			continue // 就是当前文件，下面统一打开
		}
		// 停机期间被轮转的文件：内容有变化就打开续读
		// （变大 = 有未读追加；变小 = 被截断，openSource 会归零重读）。
		if size := fileSize(name); size >= 0 && size != st.Offset {
			if s, err := t.openSource(id, st, name); err == nil {
				s.rotated, s.since = true, time.Now()
				t.sources[id] = s
				t.emit(Event{Kind: EventBacklogOpen, ID: id, Detail: st.Offset})
			} else {
				t.onError(fmt.Errorf("open backlog %s: %w", name, err))
			}
		}
	}

	// 2. 有未读内容但找不到文件 = 物理丢失（被删除）。
	for id, st := range t.opts.Resume {
		if found[id] || (liveOK && id == liveID) {
			continue
		}
		if st.Size > st.Offset {
			t.emit(Event{Kind: EventBacklogLost, ID: id, Detail: st.Size - st.Offset})
		}
	}

	// 3. 打开当前路径。不存在就等它出现（轮询循环里会重试）。
	if liveOK {
		t.openCurrent(liveID)
	}
}

// backlogCandidates 返回启动扫描匹配到的常规文件路径（排序保证确定性）。
func (t *Tailer) backlogCandidates() []string {
	pattern := t.opts.BacklogGlob
	if pattern == "" {
		pattern = t.opts.Path + "*"
	}
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil
	}
	out := matches[:0]
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil && fi.Mode().IsRegular() {
			out = append(out, m)
		}
	}
	sort.Strings(out)
	return out
}

// openCurrent 打开路径当前指向的身份。
func (t *Tailer) openCurrent(id offset.FileID) {
	// 路径又指回某个还在 drain 的身份（快速复用/改名回来）：直接提升。
	if s, ok := t.sources[id]; ok {
		s.rotated, s.quiet = false, 0
		t.current = id
		t.emit(Event{Kind: EventCurrent, ID: id})
		return
	}
	st, known := t.opts.Resume[id]
	if !known && t.opts.LegacyOffset > 0 {
		// v1 迁移：未验证位点只在大小足够时采用，且只用一次。
		if size := fileSize(t.opts.Path); size >= t.opts.LegacyOffset {
			st = offset.State{Offset: t.opts.LegacyOffset}
			known = true
		}
		t.opts.LegacyOffset = 0
	}
	s, err := t.openSource(id, st, t.opts.Path)
	if err != nil {
		t.onError(fmt.Errorf("open %s: %w", t.opts.Path, err))
		return
	}
	t.sources[id] = s
	t.current = id
	t.emit(Event{Kind: EventOpen, ID: id, Detail: s.pos})
	t.emit(Event{Kind: EventCurrent, ID: id})
	if known && s.pos > 0 {
		t.emit(Event{Kind: EventResumed, ID: id, Detail: s.pos})
	}
}

// openSource 按持久化状态打开一个物理文件：
// 校验截断、校验并预填半行，返回定位好的 source。
func (t *Tailer) openSource(id offset.FileID, st offset.State, path string) (*source, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()
	start := st.Offset
	var pending []byte

	switch {
	case size < start:
		// 停机期间被截断：位点已经越界，从头开始。
		start = 0
		t.emit(Event{Kind: EventTruncate, ID: id, Detail: size})
	case len(st.Pending) > 0:
		// 半行缓存：逐字节比对文件内容，确认它确实还躺在 Offset 处。
		if size >= start+int64(len(st.Pending)) && t.verifyPending(f, start, st.Pending) {
			pending = append([]byte(nil), st.Pending...)
			start += int64(len(st.Pending))
			t.emit(Event{Kind: EventPendingResumed, ID: id, Detail: int64(len(pending))})
		} else {
			// 内容已被改写：半行不可信，丢弃并从头读，避免错位拼接。
			start = 0
			t.emit(Event{Kind: EventPendingDropped, ID: id})
			t.emit(Event{Kind: EventTruncate, ID: id, Detail: size})
		}
	}

	if _, err := f.Seek(start, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &source{id: id, f: f, reader: bufio.NewReader(f), pos: start, pending: pending}, nil
}

// verifyPending 读取 f 在 [off, off+len(pending)) 的内容并与 pending 比对。
func (t *Tailer) verifyPending(f *os.File, off int64, pending []byte) bool {
	buf := make([]byte, len(pending))
	if _, err := f.ReadAt(buf, off); err != nil {
		return false
	}
	return string(buf) == string(pending)
}

// drain 把一个 source 读到 EOF，产出完整行。返回是否读到了字节。
func (t *Tailer) drain(ctx context.Context, s *source) bool {
	progressed := false
	for {
		chunk, err := s.reader.ReadBytes('\n')
		if len(chunk) > 0 {
			progressed = true
			s.pos += int64(len(chunk))
			s.quiet = 0
			if chunk[len(chunk)-1] == '\n' {
				line := string(append(s.pending, chunk[:len(chunk)-1]...))
				s.pending = nil
				t.onLine(Line{ID: s.id, Text: line, End: s.pos})
			} else {
				// 半行：缓存起来，等后续写入补齐（或退出时持久化）。
				s.pending = append(s.pending, chunk...)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.onError(fmt.Errorf("read %s: %w", t.opts.Path, err))
			break
		}
		if ctx.Err() != nil {
			break
		}
	}

	// 截断检测：文件比已读位置还短 = copytruncate。
	if fi, err := s.f.Stat(); err == nil && fi.Size() < s.pos {
		if _, err := s.f.Seek(0, io.SeekStart); err == nil {
			s.reader = bufio.NewReader(s.f)
			s.pos = 0
			s.pending = nil
			t.emit(Event{Kind: EventTruncate, ID: s.id, Detail: fi.Size()})
		}
	}
	return progressed
}

// checkPath 比较路径当前指向的身份与正在跟踪的身份。
func (t *Tailer) checkPath() {
	id, ok := identify(t.opts.Path)
	if !ok {
		// 路径消失（被轮转走还没重建，或被删除）：当前来源转入 drain。
		if t.current != "" {
			if s, ok := t.sources[t.current]; ok && !s.rotated {
				s.rotated, s.since = true, time.Now()
			}
			t.current = ""
		}
		return
	}
	if id == t.current {
		return
	}
	// 轮转：旧来源转入 drain，新身份成为当前。
	if t.current != "" {
		if s, ok := t.sources[t.current]; ok && !s.rotated {
			s.rotated, s.since = true, time.Now()
		}
	}
	t.current = "" // openCurrent 成功后才会重新赋值
	t.emit(Event{Kind: EventRotation, ID: id})
	t.openCurrent(id)
}

// sealQuiet 封闭静默期满或超时的 rotated source。
func (t *Tailer) sealQuiet() {
	for id, s := range t.sources {
		if !s.rotated {
			continue
		}
		// 本轮没读到字节的来源累计静默轮数。
		s.quiet++
		expired := t.opts.DrainMax > 0 && time.Since(s.since) > t.opts.DrainMax
		if s.quiet < t.opts.DrainQuietPolls && !expired {
			continue
		}
		size := int64(-1)
		if fi, err := s.f.Stat(); err == nil {
			size = fi.Size()
		}
		s.f.Close()
		delete(t.sources, id)
		t.emit(Event{Kind: EventDrainSeal, ID: id, Detail: size})
	}
}

// reports 汇总所有未封闭来源的最终观察。
func (t *Tailer) reports() []offset.ReadReport {
	out := make([]offset.ReadReport, 0, len(t.sources))
	for _, s := range t.orderedSources() {
		rep := offset.ReadReport{
			ID:           s.id,
			ReadPos:      s.pos,
			PendingStart: s.pos - int64(len(s.pending)),
			Pending:      append([]byte(nil), s.pending...),
		}
		if fi, err := s.f.Stat(); err == nil {
			rep.Size = fi.Size()
		}
		out = append(out, rep)
		s.f.Close()
	}
	return out
}

// orderedSources 按身份排序返回来源，保证遍历顺序确定。
func (t *Tailer) orderedSources() []*source {
	out := make([]*source, 0, len(t.sources))
	for _, s := range t.sources {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func (t *Tailer) emit(ev Event) {
	if t.opts.OnEvent != nil {
		t.opts.OnEvent(ev)
	}
}

// identify 返回路径当前指向的文件身份。
func identify(path string) (offset.FileID, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return "", false
	}
	return fileID(fi), true
}

// fileID 从 FileInfo 提取 dev:inode 身份。
func fileID(fi os.FileInfo) offset.FileID {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return offset.FileID(fmt.Sprintf("%x:%x", uint64(st.Dev), uint64(st.Ino)))
	}
	// 非 Unix 系统的兜底：路径+大小+修改时间，弱身份但好过没有。
	return offset.FileID(fmt.Sprintf("~%x-%x", fi.Size(), fi.ModTime().UnixNano()))
}

func fileSize(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return -1
}
