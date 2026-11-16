// Package offset 负责持久化每个文件的读取位点。
//
// 位点协议的核心思想：位点按"文件身份"（dev:inode）而不是路径记录。
// 同一路径被轮转、截断、快速复用后会产生不同的物理文件，它们各自
// 拥有独立的位点；只有身份匹配，位点才有意义。
//
// 磁盘格式是带 CRC32 校验的信封（v2）：
//
//	{"version":2, "crc32":N, "data":{...File...}}
//
// 每次保存先原子 rename 主文件，再把同一份字节写成 .bak 镜像。
// 两个文件是独立 inode 的同代副本：任何一个损坏都能从另一个
// 零重放恢复；两个都损坏时返回 ErrCorrupt，由调用方决定恢复策略
// （隔离损坏文件并从空状态开始，同时暴露指标）。
//
// v1 格式（{"version":1,"offsets":{"path":N}}）在加载时自动迁移：
// 旧位点没有身份信息，只能作为"未验证"的 LegacyOffset 挂到路径下，
// 由 tail 包在确认当前文件大小足够后才采用。
package offset

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

// FileID 唯一标识一个物理文件（设备号:inode）。
// 路径可以被复用，身份不会（在文件存活期内）。
type FileID string

// State 是一个物理文件的恢复状态。
type State struct {
	// Offset 是已确认输出的字节位点：<= Offset 的字节所属的记录
	// 都已经被 sink 确认（或被缓冲策略丢弃）。重启后从这里继续。
	Offset int64 `json:"offset"`
	// Pending 是恰好从 Offset 开始的半行字节（没有换行结尾）。
	// 只在"半行起点 == 已确认位点"对齐时才会持久化；
	// 重启时逐字节校验后预填，让跨重启的半行拼成完整记录。
	Pending []byte `json:"pending,omitempty"`
	// Size 是最后一次优雅退出时观察到的文件大小（0 表示未知，
	// 例如进程是被 kill 的）。用于发现"停机期间被截断/删除"。
	Size int64 `json:"size,omitempty"`
	// Updated 是这次状态提交的时间（UnixNano）。
	Updated int64 `json:"updated"`
}

// ReadReport 是 tailer 退出时对某个仍在监听的来源的最终观察。
type ReadReport struct {
	ID FileID
	// ReadPos 是已读到的字节位置（含 Pending）。
	ReadPos int64
	// PendingStart 是 Pending 在文件中的起始偏移。
	PendingStart int64
	// Pending 是尚未换行的半行字节。
	Pending []byte
	// Size 是退出时 fstat 到的文件大小。
	Size int64
}

// PathState 记录一个逻辑路径当前及历史的物理文件身份。
type PathState struct {
	// Current 是路径当前指向的身份（"" 表示未知）。
	Current FileID `json:"current,omitempty"`
	// LegacyOffset 是 v1 迁移来的未验证位点；没有身份信息，
	// 仅在当前文件大小足够时被采用一次，随后清除。
	LegacyOffset int64 `json:"legacy_offset,omitempty"`
	// Identities 是该路径已知各物理文件的恢复状态。
	Identities map[FileID]*State `json:"identities,omitempty"`
}

// File 是位点文件的数据模型（信封里 data 字段的内容）。
type File struct {
	Version int                   `json:"version"`
	Paths   map[string]*PathState `json:"paths"`
}

const currentVersion = 2

// ErrCorrupt 表示主文件和备份都无法恢复。
var ErrCorrupt = errors.New("offset file corrupt (main and backup)")

// LoadInfo 描述一次加载的恢复路径，供调用方暴露指标。
type LoadInfo struct {
	// RecoveredFromBak 为 true 表示主文件不可用，从 .bak 恢复。
	RecoveredFromBak bool
	// Migrated 为 true 表示从 v1 格式迁移。
	Migrated bool
	// NewerVersion 为 true 表示文件由更新版本写入，
	// 未知字段被容忍性忽略（下次保存会丢弃它们）。
	NewerVersion bool
}

// Store 读写位点文件。
type Store struct {
	path string
}

// New 构造位点存储。
func New(path string) *Store { return &Store{path: path} }

// Path 返回位点文件路径。
func (s *Store) Path() string { return s.path }

func (s *Store) bakPath() string { return s.path + ".bak" }

// Load 读取位点。文件不存在时返回空表。
// 主文件损坏或缺失时回退到 .bak；两者都不可用时返回 ErrCorrupt。
func (s *Store) Load() (*File, *LoadInfo, error) {
	info := &LoadInfo{}
	f, err := s.loadOne(s.path, info)
	if err == nil {
		return f, info, nil
	}
	if !os.IsNotExist(err) {
		// 主文件存在但损坏：尝试备份。
		f, bakErr := s.loadOne(s.bakPath(), info)
		if bakErr == nil {
			info.RecoveredFromBak = true
			return f, info, nil
		}
		return nil, info, ErrCorrupt
	}
	// 主文件不存在：可能是上次保存在 rename 之间崩溃，尝试备份。
	f, bakErr := s.loadOne(s.bakPath(), info)
	if bakErr == nil {
		info.RecoveredFromBak = true
		return f, info, nil
	}
	if os.IsNotExist(bakErr) {
		// 全新启动。
		return &File{Version: currentVersion, Paths: map[string]*PathState{}}, info, nil
	}
	return nil, info, ErrCorrupt
}

func (s *Store) loadOne(path string, info *LoadInfo) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// 先识别格式：v1 是扁平的 {version, offsets}；v2 是信封。
	var head struct {
		Version int              `json:"version"`
		Offsets map[string]int64 `json:"offsets"`
		CRC32   *uint32          `json:"crc32"`
		Data    json.RawMessage  `json:"data"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	switch {
	case head.Data != nil && head.CRC32 != nil:
		// v2 信封：先校验，再解析数据。
		if crc32.ChecksumIEEE(head.Data) != *head.CRC32 {
			return nil, fmt.Errorf("%s: crc mismatch", path)
		}
		var f File
		if err := json.Unmarshal(head.Data, &f); err != nil {
			return nil, fmt.Errorf("parse %s data: %w", path, err)
		}
		if f.Version > currentVersion {
			// 来自更新版本的文件：容忍未知字段，但标记出来。
			info.NewerVersion = true
		}
		if f.Paths == nil {
			f.Paths = map[string]*PathState{}
		}
		return &f, nil

	case head.Offsets != nil:
		// v1：迁移为"未验证"的 legacy 位点。
		f := &File{Version: currentVersion, Paths: map[string]*PathState{}}
		for p, off := range head.Offsets {
			f.Paths[p] = &PathState{LegacyOffset: off}
		}
		info.Migrated = true
		return f, nil

	default:
		return nil, fmt.Errorf("%s: unrecognized format", path)
	}
}

// Save 原子写入位点：先写临时文件再 rename 覆盖主文件，然后把同一份
// 字节写成 .bak 镜像（独立 inode，不能用硬链接——否则对主文件的原地
// 写坏会同时毁掉备份）。每次 rename 后 fsync 目录保证其持久。
func (s *Store) Save(f *File) error {
	f.Version = currentVersion
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	// 信封手工拼接：data 以紧凑 JSON 原样内嵌，保证 Load 时
	// 取到的字节与这里计算 CRC 的字节完全一致。
	crc := crc32.ChecksumIEEE(data)
	raw := []byte(fmt.Sprintf("{\n  \"version\": %d,\n  \"crc32\": %d,\n  \"data\": %s\n}\n",
		currentVersion, crc, data))

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := writeAtomic(s.path, raw, dir); err != nil {
		return err
	}
	// 同代镜像：主文件损坏时 Load 回退到 .bak，零重放。
	// 镜像失败不致命（主文件已写好），下次保存会补上。
	_ = writeAtomic(s.bakPath(), raw, dir)
	return nil
}

// writeAtomic 写临时文件、fsync、rename 到目标，最后 fsync 目录。
func writeAtomic(path string, raw []byte, dir string) error {
	tmp, err := os.CreateTemp(dir, ".offset-*")
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
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync 目录，让 rename 在崩溃后仍然成立。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Quarantine 把损坏的位点文件（及 .bak）挪到带时间戳的名字，
// 避免下次启动反复撞上同一份坏数据，同时保留现场供排查。
func (s *Store) Quarantine() {
	stamp := time.Now().Format("20060102-150405")
	for _, p := range []string{s.path, s.bakPath()} {
		if _, err := os.Stat(p); err == nil {
			_ = os.Rename(p, p+".corrupt."+stamp)
		}
	}
}
