package main

import "testing"

// 执行超时要翻译成人话，不能把驱动的英文原文直接甩给用户
func TestExplainDBErrorTimeout(t *testing.T) {
	cases := []struct{ in, want string }{
		{"context deadline exceeded", "语句超时或已中断"},
		{"context canceled", "语句超时或已中断"},
		{"Error 1064 (42000): You have an error in your SQL syntax", "SQL 语法错误"},
		{"Error 1062 (23000): Duplicate entry", "主键重复"},
	}
	for _, c := range cases {
		got := explainDBError(c.in)
		if !containsAny(got, []string{c.want}) {
			t.Errorf("%q → %q，期望包含 %q", c.in, got, c.want)
		}
	}
	// 无关错误不该被套上超时提示
	if got := explainDBError("Error 1146 (42S02): Table 'x' doesn't exist"); containsAny(got, []string{"语句超时"}) {
		t.Errorf("表不存在被误判成超时: %q", got)
	}
	// withHint 必须把原文和提示都带上
	out := withHint("context deadline exceeded")
	if !containsAny(out, []string{"context deadline exceeded", "执行超时"}) {
		t.Errorf("withHint 丢了原文或提示: %q", out)
	}
}
