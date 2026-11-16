// faulty 是用于验证背压与 sink 失败处理的可注入故障输出端。
//
// 它在普通文件 sink 之外提供三个开关：
//
//	fail_first_n   前 N 次 Write 返回错误（验证重试与 dead-letter）；
//	block_ms       每次 Write 人为阻塞，模拟慢 sink（验证 block/超时不卡死）；
//	panic_nth      第 N 次 Write 触发 panic，由消费侧 recover 兜底。
//
// 该类型只服务于测试与 scripts 下的验证场景，不参与正常采集链路。
package sink

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/internal-platform/logforge/internal/plugin"
)

func init() {
	plugin.RegisterSink("faulty", func(cfg map[string]any) (plugin.Sink, error) {
		path, _ := cfg["path"].(string)
		if path == "" {
			return nil, fmt.Errorf("faulty sink requires path")
		}
		f, err := NewFile(path)
		if err != nil {
			return nil, err
		}
		return &Faulty{
			file:      f,
			failFirst: int(asInt(cfg["fail_first_n"])),
			blockMS:   int(asInt(cfg["block_ms"])),
			panicNth:  int(asInt(cfg["panic_nth"])),
		}, nil
	})
}

func asInt(v any) int64 {
	switch x := v.(type) {
	case int:
		return int64(x)
	case int64:
		return x
	case float64:
		return int64(x)
	}
	return 0
}

// Faulty 在文件 sink 上叠加故障注入。
type Faulty struct {
	file *File

	mu        sync.Mutex
	calls     int
	failFirst int
	blockMS   int
	panicNth  int
}

// Name 返回 sink 名。
func (s *Faulty) Name() string { return "faulty:" + s.file.path }

// Write 按注入配置失败、阻塞或 panic，成功时落盘。
func (s *Faulty) Write(record map[string]any) (err error) {
	s.mu.Lock()
	s.calls++
	calls := s.calls
	failFirst := s.failFirst
	blockMS := s.blockMS
	panicNth := s.panicNth
	s.mu.Unlock()

	if panicNth > 0 && calls == panicNth {
		panic(fmt.Sprintf("faulty sink injected panic at call %d", calls))
	}
	if blockMS > 0 {
		time.Sleep(time.Duration(blockMS) * time.Millisecond)
	}
	if calls <= failFirst {
		return fmt.Errorf("faulty sink injected failure at call %d", calls)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	s.file.mu.Lock()
	defer s.file.mu.Unlock()
	_, err = s.file.f.Write(append(raw, '\n'))
	return err
}

// Flush 透传。
func (s *Faulty) Flush() error { return s.file.Flush() }

// Close 透传。
func (s *Faulty) Close() error { return s.file.Close() }
