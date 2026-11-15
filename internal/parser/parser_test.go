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
