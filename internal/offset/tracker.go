package offset

import (
	"sort"
	"sync"
	"time"
)

// maxIdentitiesPerPath 限制每个路径记住的身份数量，防止位点文件无限增长。
// 超出后按最后提交时间淘汰最旧的。
const maxIdentitiesPerPath = 8

// Tracker 是一个逻辑路径在运行期的提交状态。
//
// 它把两侧的信息汇合起来：
//   - 消费侧：Resolve() 报告某身份的字节已经确认输出，推进可持久化位点；
//   - 采集侧：退出时 AttachReports() 把半行缓存对齐后并入状态。
//
// 持久化不变式：Offset 永远只指向"已确认输出"的记录边界；
// Pending 只在"半行起点 == Offset"对齐时保存。
type Tracker struct {
	mu      sync.Mutex
	path    string
	current FileID
	legacy  int64
	states  map[FileID]*State
	dirty   bool
}

// NewTracker 从已加载的位点文件恢复某路径的运行时状态。
func NewTracker(path string, f *File) *Tracker {
	t := &Tracker{
		path:   path,
		states: map[FileID]*State{},
	}
	if ps, ok := f.Paths[path]; ok && ps != nil {
		t.current = ps.Current
		t.legacy = ps.LegacyOffset
		for id, st := range ps.Identities {
			if st == nil {
				continue
			}
			cp := *st
			t.states[id] = &cp
		}
	}
	return t
}

// CurrentID 返回路径当前的身份（"" 表示未知）。
func (t *Tracker) CurrentID() FileID {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.current
}

// LegacyOffset 返回 v1 迁移来的未验证位点（0 表示没有）。
func (t *Tracker) LegacyOffset() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.legacy
}

// MarkCurrent 记录路径当前指向的身份。
func (t *Tracker) MarkCurrent(id FileID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.current != id {
		t.current = id
		t.dirty = true
	}
}

// Resume 返回供 tailer 续读的状态副本。
func (t *Tracker) Resume() map[FileID]State {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[FileID]State, len(t.states))
	for id, st := range t.states {
		out[id] = *st
	}
	return out
}

// Resolve 报告 id 的 [已确认] 前缀推进到 end。
// 记录按顺序处理，end 单调不减；截断重置后从小值重新增长。
func (t *Tracker) Resolve(id FileID, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(id)
	if end > st.Offset {
		st.Offset = end
		// 位点推进后，旧的半行缓存必然已被覆盖（要么已拼成记录，
		// 要么不再对齐），清除等待退出时重新附着。
		st.Pending = nil
		st.Updated = time.Now().UnixNano()
		t.dirty = true
	}
}

// Reset 在检测到截断（或内容被改写）后把身份的位点归零。
func (t *Tracker) Reset(id FileID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.states[id] = &State{Updated: time.Now().UnixNano()}
	t.dirty = true
}

// SetSize 记录某身份在封闭（rotate drain 结束）时的大小，
// 供下次启动判断它是否还有未读内容。
func (t *Tracker) SetSize(id FileID, size int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st := t.state(id)
	if size > st.Size {
		st.Size = size
		t.dirty = true
	}
}

// AttachReports 在优雅退出时并入采集侧的最终观察。
// 只有"半行起点 == 已确认位点"的来源才会持久化 Pending；
// 不对齐的半行不保存——重启后会从 Offset 重新读到它们。
// 返回 (附着的半行条数, 丢弃的半行条数, 附着的半行字节数)。
func (t *Tracker) AttachReports(reps []ReadReport) (attached, dropped int, attachedBytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, r := range reps {
		st := t.state(r.ID)
		st.Size = r.Size
		st.Updated = time.Now().UnixNano()
		if len(r.Pending) == 0 {
			continue
		}
		if r.PendingStart == st.Offset {
			st.Pending = append([]byte(nil), r.Pending...)
			attached++
			attachedBytes += int64(len(r.Pending))
		} else {
			dropped++
		}
		t.dirty = true
	}
	return attached, dropped, attachedBytes
}

// Dirty 报告是否有未持久化的变更。
func (t *Tracker) Dirty() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dirty
}

// Snapshot 返回当前状态的深拷贝用于持久化，并清除 dirty 标记。
// 调用方负责保存；保存失败时应当 MarkDirty() 以便下个周期重试。
func (t *Tracker) Snapshot() *File {
	t.mu.Lock()
	defer t.mu.Unlock()
	ps := &PathState{
		Current:      t.current,
		LegacyOffset: t.legacy,
		Identities:   map[FileID]*State{},
	}
	for id, st := range t.states {
		cp := *st
		if len(st.Pending) > 0 {
			cp.Pending = append([]byte(nil), st.Pending...)
		}
		ps.Identities[id] = &cp
	}
	t.prune(ps)
	t.dirty = false
	return &File{
		Version: currentVersion,
		Paths:   map[string]*PathState{t.path: ps},
	}
}

// MarkDirty 在持久化失败后恢复 dirty 标记。
func (t *Tracker) MarkDirty() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dirty = true
}

// ConsumeLegacy 在 tailer 采用（或拒绝）了 legacy 位点后清除它，
// 避免下次启动重复迁移。
func (t *Tracker) ConsumeLegacy() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.legacy != 0 {
		t.legacy = 0
		t.dirty = true
	}
}

func (t *Tracker) state(id FileID) *State {
	st, ok := t.states[id]
	if !ok {
		st = &State{}
		t.states[id] = st
	}
	return st
}

// prune 限制身份数量，淘汰最久未提交的。
func (t *Tracker) prune(ps *PathState) {
	if len(ps.Identities) <= maxIdentitiesPerPath {
		return
	}
	ids := make([]FileID, 0, len(ps.Identities))
	for id := range ps.Identities {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ps.Identities[ids[i]].Updated < ps.Identities[ids[j]].Updated
	})
	for _, id := range ids[:len(ids)-maxIdentitiesPerPath] {
		if id == ps.Current {
			continue
		}
		delete(ps.Identities, id)
	}
}
