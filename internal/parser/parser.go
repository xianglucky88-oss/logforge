// Package parser 提供内置解析器。新增格式时在这里加一个文件即可。
package parser

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/internal-platform/logforge/internal/plugin"
)

func init() {
	plugin.RegisterParser("json", func(map[string]any) (plugin.Parser, error) {
		return jsonParser{}, nil
	})
	plugin.RegisterParser("logfmt", func(map[string]any) (plugin.Parser, error) {
		return logfmtParser{}, nil
	})
	plugin.RegisterParser("regex", func(cfg map[string]any) (plugin.Parser, error) {
		pattern, _ := cfg["pattern"].(string)
		if pattern == "" {
			return nil, fmt.Errorf("regex parser requires a pattern")
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("bad regex: %w", err)
		}
		return &regexParser{re: re}, nil
	})
}

type jsonParser struct{}

func (jsonParser) Name() string { return "json" }

func (jsonParser) Parse(line string) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal([]byte(line), &out); err != nil {
		return nil, err
	}
	return out, nil
}

type logfmtParser struct{}

func (logfmtParser) Name() string { return "logfmt" }

// Parse 解析 k=v 形式的日志，值可以带双引号。
func (logfmtParser) Parse(line string) (map[string]any, error) {
	out := map[string]any{}
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		start := i
		for i < len(line) && line[i] != '=' && line[i] != ' ' {
			i++
		}
		if i >= len(line) || line[i] != '=' {
			if start < i {
				return nil, fmt.Errorf("malformed pair %q", line[start:i])
			}
			continue
		}
		key := line[start:i]
		i++ // skip '='
		var val string
		if i < len(line) && line[i] == '"' {
			i++
			var sb strings.Builder
			for i < len(line) && line[i] != '"' {
				if line[i] == '\\' && i+1 < len(line) {
					i++
				}
				sb.WriteByte(line[i])
				i++
			}
			i++
			val = sb.String()
		} else {
			start = i
			for i < len(line) && line[i] != ' ' {
				i++
			}
			val = line[start:i]
		}
		out[key] = coerce(val)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no fields parsed")
	}
	return out, nil
}

func coerce(v string) any {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	if v == "true" {
		return true
	}
	if v == "false" {
		return false
	}
	return v
}

type regexParser struct{ re *regexp.Regexp }

func (p *regexParser) Name() string { return "regex" }

func (p *regexParser) Parse(line string) (map[string]any, error) {
	m := p.re.FindStringSubmatch(line)
	if m == nil {
		return nil, fmt.Errorf("pattern did not match")
	}
	out := map[string]any{}
	for i, name := range p.re.SubexpNames() {
		if i == 0 || name == "" {
			continue
		}
		out[name] = coerce(m[i])
	}
	return out, nil
}
