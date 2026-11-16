package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "logforge.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

const baseConfig = `
source:
  path: /tmp/app.log
sinks:
  - name: archive
    type: file
    config: {path: /tmp/archive.log}
  - name: audit
    type: file
    config: {path: /tmp/audit.log}
  - name: alert
    type: file
    config: {path: /tmp/alert.log}
route:
  rules:
    - name: errors
      match: {level: ERROR}
      sinks: [alert]
  default: [archive]
`

func TestLoadValid(t *testing.T) {
	c, err := Load(writeConfig(t, baseConfig))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Route.Rules) != 1 || c.Route.Rules[0].Name != "errors" {
		t.Fatalf("rules = %+v", c.Route.Rules)
	}
	if c.Route.Rules[0].Match["level"] != "ERROR" {
		t.Fatalf("match = %#v", c.Route.Rules[0].Match)
	}
}

// match 值应保留 YAML 标量的原始类型，而不是全部转成字符串。
func TestLoadMatchKeepsScalarTypes(t *testing.T) {
	cfg := strings.Replace(baseConfig, "match: {level: ERROR}", "match: {level: ERROR, code: 507, ok: true, ratio: 1.5}", 1)
	c, err := Load(writeConfig(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	m := c.Route.Rules[0].Match
	if m["level"] != "ERROR" {
		t.Fatalf("level = %#v", m["level"])
	}
	if _, ok := m["code"].(int); !ok {
		t.Fatalf("code should decode as int, got %T", m["code"])
	}
	if _, ok := m["ok"].(bool); !ok {
		t.Fatalf("ok should decode as bool, got %T", m["ok"])
	}
	if _, ok := m["ratio"].(float64); !ok {
		t.Fatalf("ratio should decode as float64, got %T", m["ratio"])
	}
}

func TestLoadRejectsNullMatchValue(t *testing.T) {
	cfg := strings.Replace(baseConfig, "match: {level: ERROR}", "match: {level: null}", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "match.level is null") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsNonScalarMatchValue(t *testing.T) {
	cfg := strings.Replace(baseConfig, "match: {level: ERROR}", "match:\n        level:\n          nested: x", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "match.level must be a string, bool or number") {
		t.Fatalf("err = %v", err)
	}
}

// 引号包裹的数字/布尔应保留为字符串，从而按字符串严格比较。
func TestLoadQuotedScalarStaysString(t *testing.T) {
	cfg := strings.Replace(baseConfig, "match: {level: ERROR}", `match: {level: ERROR, code: "507"}`, 1)
	c, err := Load(writeConfig(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := c.Route.Rules[0].Match["code"].(string); !ok || got != "507" {
		t.Fatalf("quoted code = %#v (%T), want string \"507\"", c.Route.Rules[0].Match["code"], c.Route.Rules[0].Match["code"])
	}
}

func TestLoadRejectsUnknownSinkInRule(t *testing.T) {
	cfg := strings.Replace(baseConfig, "sinks: [alert]", "sinks: [nope]", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unknown sink") || !strings.Contains(err.Error(), "line ") {
		t.Fatalf("error should mention unknown sink and a line number: %v", err)
	}
}

func TestLoadRejectsUnknownDefaultSink(t *testing.T) {
	cfg := strings.Replace(baseConfig, "default: [archive]", "default: [nope]", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "route.default") || !strings.Contains(err.Error(), "unknown sink") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsMissingDefault(t *testing.T) {
	cfg := strings.Replace(baseConfig, "  default: [archive]\n", "", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "route.default") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsEmptyMatch(t *testing.T) {
	cfg := strings.Replace(baseConfig, "match: {level: ERROR}", "match: {}", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "match must not be empty") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsDuplicateRuleName(t *testing.T) {
	cfg := strings.Replace(baseConfig, "  default: [archive]",
		`    - name: errors
      match: {level: WARN}
      sinks: [audit]
  default: [archive]`, 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "duplicate rule name") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsDuplicateSinkName(t *testing.T) {
	cfg := strings.Replace(baseConfig, "  - name: alert\n    type: file\n    config: {path: /tmp/alert.log}", "  - name: archive\n    type: file\n    config: {path: /tmp/dup.log}", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "duplicate sink name") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadRejectsRuleWithoutSinks(t *testing.T) {
	cfg := strings.Replace(baseConfig, "      sinks: [alert]\n", "", 1)
	_, err := Load(writeConfig(t, cfg))
	if err == nil || !strings.Contains(err.Error(), "at least one sink is required") {
		t.Fatalf("err = %v", err)
	}
}
