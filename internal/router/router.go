// Package router 按规则把记录分发给一个或多个输出端。
package router

import (
	"fmt"
	"strconv"

	"github.com/internal-platform/logforge/internal/plugin"
)

// Rule 是一条路由规则：所有 match 字段都相等时命中。
// 规则按配置顺序评估，第一条命中的规则生效；都不命中走默认出口。
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
// 第一条全部字段命中的规则生效；没有命中时返回默认出口。
func (r *Router) Route(record map[string]any) []string {
	for _, rule := range r.rules {
		if matchAll(rule.Match, record) {
			return rule.Sinks
		}
	}
	return r.defaults
}

func matchAll(match map[string]string, record map[string]any) bool {
	for k, want := range match {
		got, ok := record[k]
		if !ok {
			return false
		}
		if stringify(got) != want {
			return false
		}
	}
	return true
}

// stringify 把不同解析器产出的标量统一成文本再比较：
// JSON 的数字是 float64，logfmt/regex 经 coerce 可能是 int64，
// 因此 500 与 500.0 在路由匹配中等价，true 与 "true" 等价。
func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case int64:
		return strconv.FormatInt(t, 10)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", t)
	}
}

// Sink 按名字取输出端。
func (r *Router) Sink(name string) (plugin.Sink, bool) {
	s, ok := r.sinks[name]
	return s, ok
}

// All 返回全部输出端，供关闭时统一 flush。
func (r *Router) All() map[string]plugin.Sink { return r.sinks }
