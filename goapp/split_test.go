package main

import (
	"strings"
	"testing"
)

// 拆句必须和 MySQL 的注释规则一致：-- 后面要跟空白才是注释，# 到行尾也是注释
func TestSplitStatementsComments(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want int
		has  string // 第一条语句里必须包含的片段
	}{
		{"两个减号不是注释", "UPDATE t SET a=1--2 WHERE id=1", 1, "--2"},
		{"注释带空格", "SELECT 1 -- 说明\n; SELECT 2", 2, "SELECT 1"},
		{"注释里的分号不拆句", "SELECT 1 -- a;b\n; SELECT 2", 2, "SELECT 1"},
		{"井号注释", "# 说明; 带分号\nSELECT 1; SELECT 2", 2, "SELECT 1"},
		{"字符串里的分号不拆句", "SELECT 'a;b'; SELECT 2", 2, "'a;b'"},
		{"块注释", "SELECT 1 /* a;b */; SELECT 2", 2, "SELECT 1"},
	}
	for _, c := range cases {
		got := splitStatements(c.in)
		if len(got) != c.want {
			t.Fatalf("%s: 拆出 %d 条，期望 %d 条（%q）", c.name, len(got), c.want, got)
		}
		if c.has != "" && !strings.Contains(got[0], c.has) {
			t.Fatalf("%s: 第一条 %q 应包含 %q", c.name, got[0], c.has)
		}
	}
}
