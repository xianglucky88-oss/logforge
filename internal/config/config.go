// Package config 定义配置文件结构。
package config

import (
	"fmt"
	"os"

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
		// ContinuationPattern 匹配续行（如缩进的堆栈帧）。设置后，
		// 既不匹配起始也不匹配续行的外来行会收尾当前记录。
		ContinuationPattern string `yaml:"continuation_pattern"`
		MaxLines            int    `yaml:"max_lines"`
		FlushAfterMS        int    `yaml:"flush_after_ms"`
		// FlushOnComplete 为 true 时，解析器链参与边界判断：
		// 当前块已能完整解析，遇到非起始行就先收尾。
		// 仅在未设置 continuation_pattern 时生效。
		FlushOnComplete bool `yaml:"flush_on_complete"`
		// Orphan 是孤立行策略：emit（单独成条）或 buffer（聚成一块）。
		Orphan string `yaml:"orphan"`
		// OnOverflow 是超长记录策略：truncate（丢弃剩余续行并计数）
		// 或 split（剩余续行开启新块）。
		OnOverflow string `yaml:"on_overflow"`
	} `yaml:"multiline"`

	Parse struct {
		// AnnotateParser 把胜出的解析器名写入记录的该字段；
		// 缺省为 "_parser"，显式置空字符串则关闭标注。
		AnnotateParser *string `yaml:"annotate_parser"`
	} `yaml:"parse"`

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
		Rules []struct {
			Name  string            `yaml:"name"`
			Match map[string]string `yaml:"match"`
			Sinks []string          `yaml:"sinks"`
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
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
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
	if c.Multiline.FlushAfterMS < 0 {
		return nil, fmt.Errorf("multiline.flush_after_ms must be >= 0 (0 disables the timeout)")
	}
	if c.Multiline.Orphan == "" {
		c.Multiline.Orphan = "emit"
	}
	switch c.Multiline.Orphan {
	case "emit", "buffer":
	default:
		return nil, fmt.Errorf("multiline.orphan must be emit or buffer, got %q", c.Multiline.Orphan)
	}
	if c.Multiline.OnOverflow == "" {
		c.Multiline.OnOverflow = "truncate"
	}
	switch c.Multiline.OnOverflow {
	case "truncate", "split":
	default:
		return nil, fmt.Errorf("multiline.on_overflow must be truncate or split, got %q", c.Multiline.OnOverflow)
	}
	if c.Buffer.Policy == "" {
		c.Buffer.Policy = "drop_oldest"
	}
	return &c, nil
}
