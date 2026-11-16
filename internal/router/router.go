// Package router 按规则把记录分发给一个或多个输出端。
//
// # 匹配语义
//
// 一条规则由若干 match 字段组成：记录的对应字段必须全部与配置值
// 相等，规则才算命中。比较是"类型严格"的：
//
//   - 配置值是字符串 → 记录值必须也是字符串，按字节相等比较；
//   - 配置值是布尔   → 记录值必须也是布尔；
//   - 配置值是数字   → 记录值必须也是数字，按数值相等比较
//     （int64 与 float64 之间可以互相比较，例如 JSON 解析出的
//     507.0 与配置里的 507 相等）。
//
// 字段缺失视为"不命中"，但不是类型错误。字段存在而类型与配置值
// 不一致时同样不命中，并在 Decision.TypeMismatches 中记录一次。
//
// # 多规则命中：合并
//
// 一条记录命中多条规则时采用"合并"（fan-out）语义：所有命中规则的
// 输出端都会收到这条记录。输出端按规则在配置中的顺序、规则内 sinks
// 的书写顺序去重合并——同一个输出端只会收到一次，位置取首次出现处。
// 这使 service=billing 且 level=ERROR 的记录能同时进入审计与告警出口。
//
// 只有一条规则都没命中时，才使用默认出口（route.default）。
// 命中任何规则都不会再回落到默认出口。
package router

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/internal-platform/logforge/internal/plugin"
)

// Rule 是一条路由规则：所有 match 字段都相等时命中。
type Rule struct {
	Name  string         `yaml:"name"`
	Match map[string]any `yaml:"match"`
	Sinks []string       `yaml:"sinks"`
}

// Decision 是一条记录的路由结果。
type Decision struct {
	// Sinks 是这条记录应当写入的输出端，已按规则顺序去重。
	// 未命中任何规则时是默认出口列表。
	Sinks []string
	// Matched 表示是否命中至少一条规则。
	Matched bool
	// TypeMismatches 是本次路由中"字段存在但类型与配置值不一致"的
	// 比较次数。它只用于观测，不影响记录进入其他规则或默认出口。
	TypeMismatches int
}

// Router 持有规则与已创建的输出端。
type Router struct {
	rules []Rule
	sinks map[string]plugin.Sink
	// 默认出口：没有任何规则命中时使用。
	defaults []string
}

// New 创建路由器。sinks 是已按名字创建好的输出端。
//
// 这里再次校验规则结构，保证即使调用方绕过 config.Load 直接构造
// 路由器，也不会出现"规则引用不存在的输出端"或"未命中即被静默丢弃"
// 的情况。配置层面的错误应当在启动期暴露，而不是等到运行期。
func New(rules []Rule, sinks map[string]plugin.Sink, defaults []string) (*Router, error) {
	seenRules := map[string]int{}
	for i, r := range rules {
		if strings.TrimSpace(r.Name) == "" {
			return nil, fmt.Errorf("route.rules[%d]: rule name is required", i)
		}
		if first, dup := seenRules[r.Name]; dup {
			return nil, fmt.Errorf("route.rules[%d] %q: duplicate rule name (first defined at route.rules[%d])", i, r.Name, first)
		}
		seenRules[r.Name] = i
		if len(r.Match) == 0 {
			return nil, fmt.Errorf("route.rules[%d] %q: match must not be empty (it would match every record)", i, r.Name)
		}
		if len(r.Sinks) == 0 {
			return nil, fmt.Errorf("route.rules[%d] %q: at least one sink is required", i, r.Name)
		}
		seenSinks := map[string]struct{}{}
		for j, name := range r.Sinks {
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("route.rules[%d] %q: sinks[%d]: sink name must not be empty", i, r.Name, j)
			}
			if _, dup := seenSinks[name]; dup {
				return nil, fmt.Errorf("route.rules[%d] %q: sinks[%d]: duplicate sink %q", i, r.Name, j, name)
			}
			seenSinks[name] = struct{}{}
			if _, ok := sinks[name]; !ok {
				return nil, fmt.Errorf("route.rules[%d] %q: sinks[%d]: unknown sink %q", i, r.Name, j, name)
			}
		}
	}
	if len(defaults) == 0 {
		return nil, fmt.Errorf("route.default is required: records that match no rule would be silently dropped")
	}
	seenDefaults := map[string]struct{}{}
	for i, name := range defaults {
		if strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("route.default[%d]: sink name must not be empty", i)
		}
		if _, dup := seenDefaults[name]; dup {
			return nil, fmt.Errorf("route.default[%d]: duplicate sink %q", i, name)
		}
		seenDefaults[name] = struct{}{}
		if _, ok := sinks[name]; !ok {
			return nil, fmt.Errorf("route.default[%d]: unknown sink %q", i, name)
		}
	}
	return &Router{rules: rules, sinks: sinks, defaults: defaults}, nil
}

// Route 返回该记录应当写入的所有输出端名以及匹配信息。
//
// 命中多条规则时合并所有规则的输出端，按规则顺序去重；一条都不命中
// 时返回默认出口。
func (r *Router) Route(record map[string]any) Decision {
	var d Decision
	seen := make(map[string]struct{}, len(r.defaults))
	for _, rule := range r.rules {
		matched, mismatches := matchRule(rule, record)
		d.TypeMismatches += mismatches
		if !matched {
			continue
		}
		d.Matched = true
		for _, name := range rule.Sinks {
			// New 已保证规则引用的输出端都存在；这里只做去重。
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			d.Sinks = append(d.Sinks, name)
		}
	}
	if d.Matched {
		return d
	}
	for _, name := range r.defaults {
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		d.Sinks = append(d.Sinks, name)
	}
	return d
}

// Sink 按名字取输出端。
func (r *Router) Sink(name string) (plugin.Sink, bool) {
	s, ok := r.sinks[name]
	return s, ok
}

// All 返回全部输出端，供关闭时统一 flush。
func (r *Router) All() map[string]plugin.Sink { return r.sinks }

// matchRule 返回规则是否命中，以及字段类型不一致的比较次数。
//
// 这里不短路：即使已经有一个字段不相等，也会继续比较其余字段，
// 使 TypeMismatches 的计数不受 map 迭代顺序影响。
func matchRule(rule Rule, record map[string]any) (matched bool, typeMismatches int) {
	matched = true
	for field, want := range rule.Match {
		got, ok := record[field]
		if !ok {
			// 字段缺失：不命中，但不算类型错误。
			matched = false
			continue
		}
		eq, typeMismatch := valueEqual(want, got)
		if typeMismatch {
			typeMismatches++
			matched = false
			continue
		}
		if !eq {
			matched = false
		}
	}
	return matched, typeMismatches
}

// valueEqual 按类型严格比较配置值与记录值。
// 第二个返回值表示"存在但类型不一致"（而不是值不相等）。
func valueEqual(want, got any) (equal bool, typeMismatch bool) {
	switch w := want.(type) {
	case string:
		g, ok := got.(string)
		if !ok {
			return false, true
		}
		return g == w, false
	case bool:
		g, ok := got.(bool)
		if !ok {
			return false, true
		}
		return g == w, false
	default:
		wv := reflect.ValueOf(want)
		gv := reflect.ValueOf(got)
		if !isNumericKind(wv.Kind()) || !isNumericKind(gv.Kind()) {
			// 配置值类型已在启动期校验过；运行期遇到无法比较的值
			// 按类型不一致处理，绝不会让记录因此命中。
			return false, true
		}
		return numericEqual(wv, gv), false
	}
}

func isNumericKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func isSignedKind(k reflect.Kind) bool {
	return k >= reflect.Int && k <= reflect.Int64
}

func isUnsignedKind(k reflect.Kind) bool {
	return k >= reflect.Uint && k <= reflect.Uint64
}

// numericEqual 比较两个数字。整数与整数精确比较；只要有一侧是浮点，
// 就按 float64 比较（日志里的数值远小于 2^53，精度足够）。
func numericEqual(a, b reflect.Value) bool {
	if isSignedKind(a.Kind()) && isSignedKind(b.Kind()) {
		return a.Int() == b.Int()
	}
	if isUnsignedKind(a.Kind()) && isUnsignedKind(b.Kind()) {
		return a.Uint() == b.Uint()
	}
	return numericFloat(a) == numericFloat(b)
}

func numericFloat(v reflect.Value) float64 {
	switch {
	case isSignedKind(v.Kind()):
		return float64(v.Int())
	case isUnsignedKind(v.Kind()):
		return float64(v.Uint())
	default:
		return v.Float()
	}
}

// SinkNames 返回已配置的输出端名（按字典序），用于错误信息。
func SinkNames(sinks map[string]plugin.Sink) []string {
	out := make([]string, 0, len(sinks))
	for name := range sinks {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
