// Package sink 提供内置输出端。新增输出类型时在这里加文件并注册。
package sink

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/internal-platform/logforge/internal/plugin"
)

func init() {
	plugin.RegisterSink("file", func(cfg map[string]any) (plugin.Sink, error) {
		path, _ := cfg["path"].(string)
		if path == "" {
			return nil, fmt.Errorf("file sink requires path")
		}
		return NewFile(path)
	})
	plugin.RegisterSink("stdout", func(map[string]any) (plugin.Sink, error) {
		return &stdoutSink{}, nil
	})
}

// File 按行追加写 JSON。
type File struct {
	mu   sync.Mutex
	path string
	f    *os.File
}

// NewFile 打开（或创建）输出文件。
func NewFile(path string) (*File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &File{path: path, f: f}, nil
}

func (s *File) Name() string { return "file:" + s.path }

// Write 序列化一条记录并写入。当前实现逐条写入，没有攒批。
func (s *File) Write(record map[string]any) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.f.Write(append(raw, '\n'))
	return err
}

func (s *File) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Sync()
}

func (s *File) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

type stdoutSink struct{ mu sync.Mutex }

func (s *stdoutSink) Name() string { return "stdout" }

func (s *stdoutSink) Write(record map[string]any) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = fmt.Fprintln(os.Stdout, string(raw))
	return err
}

func (s *stdoutSink) Flush() error { return nil }
func (s *stdoutSink) Close() error { return nil }
