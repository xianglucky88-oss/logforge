// Package offset 负责持久化每个文件的读取位点。
//
// 位点文件是 JSON，带版本号，便于以后改格式时做迁移。
package offset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// File 是位点文件的格式。
type File struct {
	Version int              `json:"version"`
	Offsets map[string]int64 `json:"offsets"`
}

const currentVersion = 1

// Store 读写位点文件。
type Store struct {
	path string
}

// New 构造位点存储。
func New(path string) *Store { return &Store{path: path} }

// Load 读取位点。文件不存在时返回空表。
func (s *Store) Load() (*File, error) {
	raw, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &File{Version: currentVersion, Offsets: map[string]int64{}}, nil
	}
	if err != nil {
		return nil, err
	}
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse offset file: %w", err)
	}
	if f.Offsets == nil {
		f.Offsets = map[string]int64{}
	}
	return &f, nil
}

// Save 原子写入位点：先写临时文件再 rename。
func (s *Store) Save(f *File) error {
	f.Version = currentVersion
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".offset-*")
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
	return os.Rename(tmpName, s.path)
}
