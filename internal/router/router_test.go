package router

import (
	"reflect"
	"strings"
	"testing"

	"github.com/internal-platform/logforge/internal/plugin"
)

// fakeSink 只用于占位，路由逻辑不调用它的方法。
type fakeSink struct{ name string }

func (f fakeSink) Name() string               { return f.name }
func (f fakeSink) Write(map[string]any) error { return nil }
func (f fakeSink) Flush() error               { return nil }
func (f fakeSink) Close() error               { return nil }

func sinks(names ...string) map[string]plugin.Sink {
	m := map[string]plugin.Sink{}
	for _, n := range names {
		m[n] = fakeSink{name: n}
	}
	return m
}

func mustRouter(t *testing.T, rules []Rule, defaults []string, names ...string) *Router {
	t.Helper()
	r, err := New(rules, sinks(names...), defaults)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func TestRouteSingleRule(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "errors", Match: map[string]any{"level": "ERROR"}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	d := r.Route(map[string]any{"level": "ERROR", "service": "orders"})
	if !d.Matched {
		t.Fatal("expected match")
	}
	if !reflect.DeepEqual(d.Sinks, []string{"alert"}) {
		t.Fatalf("sinks = %v", d.Sinks)
	}
}

func TestRouteMissUsesDefault(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "errors", Match: map[string]any{"level": "ERROR"}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	d := r.Route(map[string]any{"level": "INFO"})
	if d.Matched {
		t.Fatal("expected no match")
	}
	if !reflect.DeepEqual(d.Sinks, []string{"archive"}) {
		t.Fatalf("sinks = %v", d.Sinks)
	}
}

func TestRouteAllFieldsMustMatch(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "billing-errors", Match: map[string]any{"service": "billing", "level": "ERROR"}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	if d := r.Route(map[string]any{"service": "billing", "level": "INFO"}); d.Matched {
		t.Fatal("expected partial match to fail")
	}
	if d := r.Route(map[string]any{"service": "billing"}); d.Matched {
		t.Fatal("expected missing field to fail")
	}
	if d := r.Route(map[string]any{"service": "billing", "level": "ERROR"}); !d.Matched {
		t.Fatal("expected full match")
	}
}

// 多条规则命中时合并（fan-out），且输出端去重、顺序稳定。
func TestRouteMultipleRulesMergeAndDedup(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "errors", Match: map[string]any{"level": "ERROR"}, Sinks: []string{"alert", "audit"}},
		{Name: "billing", Match: map[string]any{"service": "billing"}, Sinks: []string{"audit", "billing"}},
	}, []string{"archive"}, "alert", "audit", "billing", "archive")

	d := r.Route(map[string]any{"level": "ERROR", "service": "billing"})
	if !d.Matched {
		t.Fatal("expected match")
	}
	want := []string{"alert", "audit", "billing"}
	if !reflect.DeepEqual(d.Sinks, want) {
		t.Fatalf("sinks = %v, want %v", d.Sinks, want)
	}
}

// 合并语义下，命中任何规则都不会再追加默认出口。
func TestRouteMatchDoesNotFallBackToDefault(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "billing", Match: map[string]any{"service": "billing"}, Sinks: []string{"audit"}},
	}, []string{"archive"}, "audit", "archive")

	d := r.Route(map[string]any{"service": "billing"})
	if !reflect.DeepEqual(d.Sinks, []string{"audit"}) {
		t.Fatalf("sinks = %v", d.Sinks)
	}
}

func TestRouteNumericMatching(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "5xx", Match: map[string]any{"code": 507}, Sinks: []string{"alert"}},
		{Name: "ratio", Match: map[string]any{"ratio": 1.5}, Sinks: []string{"audit"}},
	}, []string{"archive"}, "alert", "audit", "archive")

	// logfmt/regex 解析器产出 int64；JSON 产出 float64。
	if d := r.Route(map[string]any{"code": int64(507)}); !d.Matched {
		t.Fatal("int64 should match int literal")
	}
	if d := r.Route(map[string]any{"code": float64(507)}); !d.Matched {
		t.Fatal("float64 507 should match int literal")
	}
	if d := r.Route(map[string]any{"ratio": float64(1.5)}); !d.Matched {
		t.Fatal("float should match float literal")
	}
	if d := r.Route(map[string]any{"code": int64(500)}); d.Matched {
		t.Fatal("500 should not match 507")
	}
}

// 数字与字符串不相等，且记为类型不一致。
func TestRouteTypeMismatch(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "code", Match: map[string]any{"code": 507}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	d := r.Route(map[string]any{"code": "507"})
	if d.Matched {
		t.Fatal("string 507 must not match number 507")
	}
	if d.TypeMismatches != 1 {
		t.Fatalf("TypeMismatches = %d, want 1", d.TypeMismatches)
	}
	if !reflect.DeepEqual(d.Sinks, []string{"archive"}) {
		t.Fatalf("sinks = %v", d.Sinks)
	}
}

func TestRouteMissingFieldIsNotTypeMismatch(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "code", Match: map[string]any{"code": 507}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	d := r.Route(map[string]any{"level": "ERROR"})
	if d.Matched || d.TypeMismatches != 0 {
		t.Fatalf("matched=%v mismatches=%d", d.Matched, d.TypeMismatches)
	}
}

// 类型不一致只影响比较结果，不会让同一条记录丢掉其他规则或默认出口。
func TestRouteTypeMismatchDoesNotBlockOtherRules(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "code", Match: map[string]any{"code": 507}, Sinks: []string{"alert"}},
		{Name: "errors", Match: map[string]any{"level": "ERROR"}, Sinks: []string{"audit"}},
	}, []string{"archive"}, "alert", "audit", "archive")

	d := r.Route(map[string]any{"code": "507", "level": "ERROR"})
	if !d.Matched {
		t.Fatal("expected errors rule to match")
	}
	if !reflect.DeepEqual(d.Sinks, []string{"audit"}) {
		t.Fatalf("sinks = %v", d.Sinks)
	}
	if d.TypeMismatches != 1 {
		t.Fatalf("TypeMismatches = %d, want 1", d.TypeMismatches)
	}
}

func TestRouteBoolMatching(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "fatal", Match: map[string]any{"fatal": true}, Sinks: []string{"alert"}},
	}, []string{"archive"}, "alert", "archive")

	if d := r.Route(map[string]any{"fatal": true}); !d.Matched {
		t.Fatal("bool true should match")
	}
	d := r.Route(map[string]any{"fatal": "true"})
	if d.Matched || d.TypeMismatches != 1 {
		t.Fatalf("string \"true\" must be a type mismatch: matched=%v mismatches=%d", d.Matched, d.TypeMismatches)
	}
}

func TestRouteRuleOrderPreserved(t *testing.T) {
	r := mustRouter(t, []Rule{
		{Name: "first", Match: map[string]any{"a": "1"}, Sinks: []string{"s1"}},
		{Name: "second", Match: map[string]any{"b": "2"}, Sinks: []string{"s2"}},
	}, []string{"d"}, "s1", "s2", "d")

	d := r.Route(map[string]any{"a": "1", "b": "2"})
	if !reflect.DeepEqual(d.Sinks, []string{"s1", "s2"}) {
		t.Fatalf("sinks = %v, want rule order [s1 s2]", d.Sinks)
	}
}

func TestNewRejectsUnknownSink(t *testing.T) {
	_, err := New([]Rule{{Name: "r", Match: map[string]any{"a": "1"}, Sinks: []string{"missing"}}},
		sinks("s1"), []string{"s1"})
	if err == nil || !strings.Contains(err.Error(), `unknown sink "missing"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRejectsUnknownDefaultSink(t *testing.T) {
	_, err := New(nil, sinks("s1"), []string{"missing"})
	if err == nil || !strings.Contains(err.Error(), `unknown sink "missing"`) {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRejectsEmptyDefault(t *testing.T) {
	_, err := New(nil, sinks("s1"), nil)
	if err == nil || !strings.Contains(err.Error(), "route.default") {
		t.Fatalf("err = %v", err)
	}
}

func TestNewRejectsBadRules(t *testing.T) {
	cases := []struct {
		name string
		rule Rule
		want string
	}{
		{"empty name", Rule{Name: "", Match: map[string]any{"a": "1"}, Sinks: []string{"s1"}}, "name is required"},
		{"empty match", Rule{Name: "r", Match: nil, Sinks: []string{"s1"}}, "match must not be empty"},
		{"empty sinks", Rule{Name: "r", Match: map[string]any{"a": "1"}}, "at least one sink"},
		{"dup sink", Rule{Name: "r", Match: map[string]any{"a": "1"}, Sinks: []string{"s1", "s1"}}, "duplicate sink"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New([]Rule{tc.rule}, sinks("s1"), []string{"s1"})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestNewRejectsDuplicateRuleNames(t *testing.T) {
	_, err := New([]Rule{
		{Name: "r", Match: map[string]any{"a": "1"}, Sinks: []string{"s1"}},
		{Name: "r", Match: map[string]any{"b": "2"}, Sinks: []string{"s1"}},
	}, sinks("s1"), []string{"s1"})
	if err == nil || !strings.Contains(err.Error(), "duplicate rule name") {
		t.Fatalf("err = %v", err)
	}
}
