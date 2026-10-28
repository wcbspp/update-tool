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

// 顶层 WHERE：子查询里的 WHERE、字符串里的 where 都不算；DELETE 的 LIMIT 要单独认
func TestWhereAndLimit(t *testing.T) {
	cases := []struct{ in, want string }{
		{"DELETE FROM t WHERE id IN (SELECT id FROM x WHERE v=1)", "id IN (SELECT id FROM x WHERE v=1)"},
		{"UPDATE t SET a=1 WHERE note='a where b'", "note='a where b'"},
		{"DELETE FROM t WHERE a=1 ORDER BY id LIMIT 10", "a=1"},
		{"DELETE FROM t", ""},
	}
	for _, c := range cases {
		if got := whereRaw(c.in); got != c.want {
			t.Fatalf("whereRaw(%q)=%q，期望 %q", c.in, got, c.want)
		}
	}

	limCases := []struct {
		in   string
		want int64
	}{
		{"DELETE FROM t WHERE a=1 LIMIT 100", 100},
		{"delete from t limit 5;", 5},
		{"DELETE FROM t WHERE a=1", 0},
		{"DELETE FROM t WHERE note='limit 3'", 0},
	}
	for _, c := range limCases {
		if got := deleteLimit(c.in); got != c.want {
			t.Fatalf("deleteLimit(%q)=%d，期望 %d", c.in, got, c.want)
		}
	}
}
