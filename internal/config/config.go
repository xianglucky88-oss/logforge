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
		// 溢写段滚动与预算。
		SpillSegmentBytes int64 `yaml:"spill_segment_bytes"`
		SpillMaxSegments  int   `yaml:"spill_max_segments"`
		SpillMaxBytes     int64 `yaml:"spill_max_bytes"`
		// block 策略的超时（毫秒，0=无限等但关闭必唤醒）与兜底动作。
		BlockTimeoutMS       int    `yaml:"block_timeout_ms"`
		BlockTimeoutAction   string `yaml:"block_timeout_action"`
		// 溢写失败（磁盘满/预算耗尽）后的兜底动作。
		SpillFullAction      string `yaml:"spill_full_action"`
		// 每 Ack 多少条持久化一次 manifest。
		ManifestEveryAcks    int    `yaml:"manifest_every_acks"`
	} `yaml:"buffer"`

	// Output 控制消费协程与 sink 交互的保护参数。
	Output struct {
		// WriteTimeoutMS 限制单次 sink.Write 的最长耗时，0=不限制。
		WriteTimeoutMS int `yaml:"write_timeout_ms"`
		// MaxRetries 是单条记录写失败后的最大重试次数，超过进 dead-letter。
		MaxRetries int `yaml:"max_retries"`
		// RetryBackoffMS 是重试之间的基础退避（指数退避，上限 5 倍）。
		RetryBackoffMS int `yaml:"retry_backoff_ms"`
		// DrainTimeoutMS 是关闭时排空缓冲的最长时间，0=排完为止。
		DrainTimeoutMS int `yaml:"drain_timeout_ms"`
	} `yaml:"output"`

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
	switch c.Buffer.Policy {
	case "drop_new", "drop_oldest", "block", "spill":
	default:
		return nil, fmt.Errorf("unknown buffer.policy %q", c.Buffer.Policy)
	}
	if c.Buffer.Capacity <= 0 {
		c.Buffer.Capacity = 1024
	}
	if c.Buffer.BlockTimeoutAction == "" {
		c.Buffer.BlockTimeoutAction = "drop_new"
	}
	if c.Buffer.SpillFullAction == "" {
		c.Buffer.SpillFullAction = "drop_new"
	}
	if c.Output.MaxRetries <= 0 {
		c.Output.MaxRetries = 3
	}
	if c.Output.RetryBackoffMS <= 0 {
		c.Output.RetryBackoffMS = 50
	}
	if c.Output.WriteTimeoutMS < 0 {
		c.Output.WriteTimeoutMS = 0
	}
	return &c, nil
}
