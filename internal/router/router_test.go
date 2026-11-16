package router

import (
	"testing"

	"github.com/internal-platform/logforge/internal/plugin"
)

type fakeSink struct{ name string }

func (s *fakeSink) Name() string               { return s.name }
func (s *fakeSink) Write(map[string]any) error { return nil }
func (s *fakeSink) Flush() error               { return nil }
func (s *fakeSink) Close() error               { return nil }

func newRouter(t *testing.T, rules []Rule, defaults []string) *Router {
	t.Helper()
	sinks := map[string]plugin.Sink{
		"all": &fakeSink{name: "all"},
		"bad": &fakeSink{name: "bad"},
	}
	rt, err := New(rules, sinks, defaults)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func TestFirstMatchingRuleWins(t *testing.T) {
	rt := newRouter(t, []Rule{
		{Name: "dead", Match: map[string]string{"parse_error": "true"}, Sinks: []string{"bad"}},
		{Name: "err", Match: map[string]string{"level": "ERROR"}, Sinks: []string{"all"}},
	}, []string{"all"})

	got := rt.Route(map[string]any{"parse_error": true, "level": "ERROR"})
	if len(got) != 1 || got[0] != "bad" {
		t.Fatalf("first rule should win, got %v", got)
	}
}

func TestDefaultWhenNoRuleMatches(t *testing.T) {
	rt := newRouter(t, []Rule{
		{Name: "dead", Match: map[string]string{"parse_error": "true"}, Sinks: []string{"bad"}},
	}, []string{"all"})
	got := rt.Route(map[string]any{"level": "INFO"})
	if len(got) != 1 || got[0] != "all" {
		t.Fatalf("want default sink, got %v", got)
	}
}

// 不同解析器产出的数字类型不同（JSON 是 float64，logfmt 是 int64），
// 路由比较前统一字符串化，500 与 500.0 等价。
func TestMatchNormalizesTypes(t *testing.T) {
	rt := newRouter(t, []Rule{
		{Name: "slow", Match: map[string]string{"code": "500", "cached": "true"}, Sinks: []string{"bad"}},
	}, []string{"all"})

	cases := []map[string]any{
		{"code": int64(500), "cached": true},   // logfmt/regex coerce 结果
		{"code": float64(500), "cached": true}, // JSON 解析结果
		{"code": "500", "cached": "true"},      // 原始字符串
	}
	for i, rec := range cases {
		if got := rt.Route(rec); len(got) != 1 || got[0] != "bad" {
			t.Fatalf("case %d: want bad, got %v", i, got)
		}
	}
}

func TestMissingKeyNeverMatches(t *testing.T) {
	rt := newRouter(t, []Rule{
		{Name: "dead", Match: map[string]string{"parse_error": "true"}, Sinks: []string{"bad"}},
	}, []string{"all"})
	if got := rt.Route(map[string]any{"level": "INFO"}); got[0] != "all" {
		t.Fatalf("missing key should fall through to default, got %v", got)
	}
}

func TestUnknownSinkInRule(t *testing.T) {
	sinks := map[string]plugin.Sink{"all": &fakeSink{name: "all"}}
	_, err := New([]Rule{{Name: "x", Match: map[string]string{"a": "b"}, Sinks: []string{"nope"}}}, sinks, nil)
	if err == nil {
		t.Fatal("rule referencing unknown sink should error")
	}
}
