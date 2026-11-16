// Package config 定义配置文件结构。
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 是 logforge 的完整配置。
type Config struct {
	Source struct {
		Path string `yaml:"path"`
		// PollIntervalMS 是读到 EOF 后的轮询间隔。
		PollIntervalMS int `yaml:"poll_interval_ms"`
	} `yaml:"source"`

	Multiline struct {
		StartPattern string `yaml:"start_pattern"`
		MaxLines     int    `yaml:"max_lines"`
		FlushAfterMS int    `yaml:"flush_after_ms"`
	} `yaml:"multiline"`

	// Parsers 按顺序尝试，第一个成功的生效。
	Parsers []struct {
		Name   string         `yaml:"name"`
		Config map[string]any `yaml:"config"`
	} `yaml:"parsers"`

	Buffer struct {
		Capacity int    `yaml:"capacity"`
		Policy   string `yaml:"policy"`
		SpillDir string `yaml:"spill_dir"`
	} `yaml:"buffer"`

	Route struct {
		// Rules 按书写顺序匹配；一条记录命中多条规则时合并它们的
		// 输出端（见 internal/router）。Match 的值可以是字符串、布尔
		// 或数字，比较时类型必须与记录字段一致。
		Rules []struct {
			Name  string         `yaml:"name"`
			Match map[string]any `yaml:"match"`
			Sinks []string       `yaml:"sinks"`
		} `yaml:"rules"`
		Default []string `yaml:"default"`
	} `yaml:"route"`

	Sinks []struct {
		Name   string         `yaml:"name"`
		Type   string         `yaml:"type"`
		Config map[string]any `yaml:"config"`
	} `yaml:"sinks"`

	Offset struct {
		Path string `yaml:"path"`
	} `yaml:"offset"`
}

// Load 读取并做基本校验。
//
// 所有能静态判定的配置错误都在这里（启动期）暴露，并尽量带上行号，
// 避免等到运行期才因为某条记录走进某个分支而暴露。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	var c Config
	if err := doc.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.Source.Path == "" {
		return nil, fmt.Errorf("source.path is required")
	}
	if len(c.Sinks) == 0 {
		return nil, fmt.Errorf("at least one sink is required")
	}
	if c.Source.PollIntervalMS <= 0 {
		c.Source.PollIntervalMS = 200
	}
	if c.Multiline.MaxLines <= 0 {
		c.Multiline.MaxLines = 200
	}
	if c.Buffer.Policy == "" {
		c.Buffer.Policy = "drop_oldest"
	}
	if err := c.validate(doc.Content); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) validate(doc []*yaml.Node) error {
	// 输出端：名字必须存在且唯一——重名会让后一个定义静默覆盖前一个，
	// 规则引用的到底是谁就说不清了。
	sinkNames := make(map[string]struct{}, len(c.Sinks))
	for i, s := range c.Sinks {
		if strings.TrimSpace(s.Name) == "" {
			return fmt.Errorf("sinks[%d]: name is required", i)
		}
		if strings.TrimSpace(s.Type) == "" {
			return fmt.Errorf("sinks[%d] %q: type is required", i, s.Name)
		}
		if _, dup := sinkNames[s.Name]; dup {
			return fmt.Errorf("sinks[%d]: duplicate sink name %q (sink names must be unique)", i, s.Name)
		}
		sinkNames[s.Name] = struct{}{}
	}

	// 解析器：名字必填且唯一。
	parserNames := make(map[string]struct{}, len(c.Parsers))
	for i, p := range c.Parsers {
		if strings.TrimSpace(p.Name) == "" {
			return fmt.Errorf("parsers[%d]: name is required", i)
		}
		if _, dup := parserNames[p.Name]; dup {
			return fmt.Errorf("parsers[%d]: duplicate parser name %q", i, p.Name)
		}
		parserNames[p.Name] = struct{}{}
	}

	ruleLines, defaultLine := routeLines(doc)

	ruleNames := make(map[string]int, len(c.Route.Rules))
	for i, r := range c.Route.Rules {
		at := linePrefix(ruleLines, i)
		if strings.TrimSpace(r.Name) == "" {
			return fmt.Errorf("%sroute.rules[%d]: name is required", at, i)
		}
		if first, dup := ruleNames[r.Name]; dup {
			return fmt.Errorf("%sroute.rules[%d] %q: duplicate rule name (first defined at route.rules[%d])",
				at, i, r.Name, first)
		}
		ruleNames[r.Name] = i

		if len(r.Match) == 0 {
			return fmt.Errorf("%sroute.rules[%d] %q: match must not be empty (it would match every record)",
				at, i, r.Name)
		}
		for field, v := range r.Match {
			if strings.TrimSpace(field) == "" {
				return fmt.Errorf("%sroute.rules[%d] %q: match field name must not be empty", at, i, r.Name)
			}
			if v == nil {
				return fmt.Errorf("%sroute.rules[%d] %q: match.%s is null; use an explicit string/bool/number",
					at, i, r.Name, field)
			}
			if !isScalar(v) {
				return fmt.Errorf("%sroute.rules[%d] %q: match.%s must be a string, bool or number, got %T (quote the value to compare it as literal text)",
					at, i, r.Name, field, v)
			}
		}

		if len(r.Sinks) == 0 {
			return fmt.Errorf("%sroute.rules[%d] %q: at least one sink is required", at, i, r.Name)
		}
		seen := make(map[string]struct{}, len(r.Sinks))
		for j, name := range r.Sinks {
			if strings.TrimSpace(name) == "" {
				return fmt.Errorf("%sroute.rules[%d] %q: sinks[%d]: sink name must not be empty", at, i, r.Name, j)
			}
			if _, dup := seen[name]; dup {
				return fmt.Errorf("%sroute.rules[%d] %q: sinks[%d]: duplicate sink %q", at, i, r.Name, j, name)
			}
			seen[name] = struct{}{}
			if _, ok := sinkNames[name]; !ok {
				return fmt.Errorf("%sroute.rules[%d] %q: sinks[%d]: unknown sink %q (configured sinks: %s)",
					at, i, r.Name, j, name, sortedNames(sinkNames))
			}
		}
	}

	if len(c.Route.Default) == 0 {
		return fmt.Errorf("%sroute.default: at least one sink is required (otherwise records matching no rule would be silently dropped)",
			linePrefixAt(defaultLine))
	}
	seen := make(map[string]struct{}, len(c.Route.Default))
	for i, name := range c.Route.Default {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("%sroute.default[%d]: sink name must not be empty", linePrefixAt(defaultLine), i)
		}
		if _, dup := seen[name]; dup {
			return fmt.Errorf("%sroute.default[%d]: duplicate sink %q", linePrefixAt(defaultLine), i, name)
		}
		seen[name] = struct{}{}
		if _, ok := sinkNames[name]; !ok {
			return fmt.Errorf("%sroute.default[%d]: unknown sink %q (configured sinks: %s)",
				linePrefixAt(defaultLine), i, name, sortedNames(sinkNames))
		}
	}
	return nil
}

// isScalar 判断路由匹配值是否是允许的标量类型。
func isScalar(v any) bool {
	switch v.(type) {
	case string, bool, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	}
	return false
}

func sortedNames(m map[string]struct{}) string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	// 少量元素，插入排序即可，避免为一行错误信息引入额外依赖。
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return strings.Join(names, ", ")
}

func linePrefix(lines []int, i int) string {
	if i < len(lines) && lines[i] > 0 {
		return fmt.Sprintf("line %d: ", lines[i])
	}
	return ""
}

func linePrefixAt(line int) string {
	if line > 0 {
		return fmt.Sprintf("line %d: ", line)
	}
	return ""
}

// routeLines 从原始 YAML 节点里取出每条规则与 default 的行号，
// 让校验错误能直接定位到配置文件。找不到时返回零值，不影响校验。
func routeLines(doc []*yaml.Node) (ruleLines []int, defaultLine int) {
	if len(doc) == 0 || doc[0].Kind != yaml.MappingNode {
		return nil, 0
	}
	root := doc[0]
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "route" {
			continue
		}
		route := root.Content[i+1]
		if route.Kind != yaml.MappingNode {
			return nil, 0
		}
		for j := 0; j+1 < len(route.Content); j += 2 {
			switch route.Content[j].Value {
			case "rules":
				rules := route.Content[j+1]
				if rules.Kind != yaml.SequenceNode {
					continue
				}
				for _, rn := range rules.Content {
					ruleLines = append(ruleLines, rn.Line)
				}
			case "default":
				defaultLine = route.Content[j+1].Line
			}
		}
	}
	return ruleLines, defaultLine
}
