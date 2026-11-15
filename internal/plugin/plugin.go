// Package plugin 定义了可扩展点：解析器与输出端都以工厂形式注册。
//
// 新增一种日志格式或一种输出目标时，只需实现接口并注册工厂，
// 配置里按名字引用即可，不需要改动 pipeline。
package plugin

import (
	"fmt"
	"sort"
	"sync"
)

// Parser 把一条原始记录解析成结构化字段。
type Parser interface {
	// Name 是配置中引用它的名字。
	Name() string
	// Parse 返回解析结果。返回 error 表示这条记录无法解析，
	// 由调用方决定丢弃还是原样保留。
	Parse(line string) (map[string]any, error)
}

// Sink 是输出端。
type Sink interface {
	Name() string
	Write(record map[string]any) error
	Flush() error
	Close() error
}

// ParserFactory 根据配置创建解析器。
type ParserFactory func(cfg map[string]any) (Parser, error)

// SinkFactory 根据配置创建输出端。
type SinkFactory func(cfg map[string]any) (Sink, error)

var (
	mu        sync.RWMutex
	parsers   = map[string]ParserFactory{}
	sinkTypes = map[string]SinkFactory{}
)

// RegisterParser 注册解析器工厂。重复注册同名项会 panic，
// 在启动期暴露配置错误比运行期静默覆盖更安全。
func RegisterParser(name string, f ParserFactory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := parsers[name]; dup {
		panic(fmt.Sprintf("parser %q already registered", name))
	}
	parsers[name] = f
}

// RegisterSink 注册输出端工厂。
func RegisterSink(name string, f SinkFactory) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := sinkTypes[name]; dup {
		panic(fmt.Sprintf("sink %q already registered", name))
	}
	sinkTypes[name] = f
}

// NewParser 按名字创建解析器。
func NewParser(name string, cfg map[string]any) (Parser, error) {
	mu.RLock()
	f, ok := parsers[name]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown parser %q", name)
	}
	return f(cfg)
}

// NewSink 按类型创建输出端。
func NewSink(kind string, cfg map[string]any) (Sink, error) {
	mu.RLock()
	f, ok := sinkTypes[kind]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown sink type %q", kind)
	}
	return f(cfg)
}

// ParserNames 返回已注册的解析器名，按字典序。
func ParserNames() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(parsers))
	for k := range parsers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// SinkNames 返回已注册的输出端类型名，按字典序。
func SinkNames() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(sinkTypes))
	for k := range sinkTypes {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
