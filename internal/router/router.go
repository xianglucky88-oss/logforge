// Package router 按规则把记录分发给一个或多个输出端。
package router

import (
	"fmt"

	"github.com/internal-platform/logforge/internal/plugin"
)

// Rule 是一条路由规则：所有 match 字段都相等时命中。
type Rule struct {
	Name  string            `yaml:"name"`
	Match map[string]string `yaml:"match"`
	Sinks []string          `yaml:"sinks"`
}

// Router 持有规则与已创建的输出端。
type Router struct {
	rules []Rule
	sinks map[string]plugin.Sink
	// 默认出口：没有任何规则命中时使用。
	defaults []string
}

// New 创建路由器。sinks 是已按名字创建好的输出端。
func New(rules []Rule, sinks map[string]plugin.Sink, defaults []string) (*Router, error) {
	for _, r := range rules {
		for _, s := range r.Sinks {
			if _, ok := sinks[s]; !ok {
				return nil, fmt.Errorf("rule %q references unknown sink %q", r.Name, s)
			}
		}
	}
	for _, s := range defaults {
		if _, ok := sinks[s]; !ok {
			return nil, fmt.Errorf("default sink %q not found", s)
		}
	}
	return &Router{rules: rules, sinks: sinks, defaults: defaults}, nil
}

// Route 返回该记录应当写入的所有输出端名。
//
// 当前实现只返回默认出口——规则匹配逻辑尚未接入。
func (r *Router) Route(record map[string]any) []string {
	return r.defaults
}

// Sink 按名字取输出端。
func (r *Router) Sink(name string) (plugin.Sink, bool) {
	s, ok := r.sinks[name]
	return s, ok
}

// All 返回全部输出端，供关闭时统一 flush。
func (r *Router) All() map[string]plugin.Sink { return r.sinks }
