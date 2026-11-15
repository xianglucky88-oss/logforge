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
	if c.Buffer.Policy == "" {
		c.Buffer.Policy = "drop_oldest"
	}
	return &c, nil
}
