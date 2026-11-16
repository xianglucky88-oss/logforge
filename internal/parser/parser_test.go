package parser

import (
	"testing"

	"github.com/internal-platform/logforge/internal/plugin"
)

func TestJSON(t *testing.T) {
	p, err := plugin.NewParser("json", nil)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := p.Parse(`{"level":"ERROR","code":500}`)
	if err != nil {
		t.Fatal(err)
	}
	if rec["level"] != "ERROR" {
		t.Fatalf("level mismatch: %v", rec["level"])
	}
}

func TestLogfmt(t *testing.T) {
	p, _ := plugin.NewParser("logfmt", nil)
	rec, err := p.Parse(`ts=1 level=WARN msg="disk almost full" code=507`)
	if err != nil {
		t.Fatal(err)
	}
	if rec["msg"] != "disk almost full" {
		t.Fatalf("quoted value mismatch: %v", rec["msg"])
	}
	if rec["code"] != int64(507) {
		t.Fatalf("numeric coercion failed: %T %v", rec["code"], rec["code"])
	}
}

func TestRegexNamedGroups(t *testing.T) {
	p, err := plugin.NewParser("regex", map[string]any{"pattern": `^(?P<level>\w+) (?P<msg>.*)$`})
	if err != nil {
		t.Fatal(err)
	}
	rec, err := p.Parse("INFO started")
	if err != nil {
		t.Fatal(err)
	}
	if rec["level"] != "INFO" || rec["msg"] != "started" {
		t.Fatalf("unexpected: %v", rec)
	}
}

func TestUnknownParser(t *testing.T) {
	if _, err := plugin.NewParser("nope", nil); err == nil {
		t.Fatal("expected error")
	}
}

// 粘连了半个 JSON 的行不应被 logfmt 侥幸解析：键里带 { " : 直接判失败。
func TestLogfmtRejectsGluedLine(t *testing.T) {
	p, _ := plugin.NewParser("logfmt", nil)
	glued := `{"ts":"2026-11-15T10:00:0ts=2026-11-15T10:00:01Z level=INFO msg="handled"`
	if _, err := p.Parse(glued); err == nil {
		t.Fatal("glued line should not parse as logfmt")
	}
}

// 合法的非常规键（带点、横线）仍然可以解析。
func TestLogfmtKeyShapes(t *testing.T) {
	p, _ := plugin.NewParser("logfmt", nil)
	rec, err := p.Parse(`http.status=200 trace-id=abc`)
	if err != nil {
		t.Fatal(err)
	}
	if rec["http.status"] != int64(200) || rec["trace-id"] != "abc" {
		t.Fatalf("unexpected: %v", rec)
	}
}
