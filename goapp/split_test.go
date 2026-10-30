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

// SET 另起一行时也必须能解析：脚本里几乎都是这种写法，
// 之前只认 " set "（前后空格），换行就匹配不上，整条 UPDATE 被标成「预检未覆盖」。
func TestSplitUpdateSetOnNewLine(t *testing.T) {
	cases := []struct {
		name       string
		sql        string
		wantTables string
	}{
		{"SET 换行", "UPDATE hilist_b A ,hilist_b_update_2107 B\nSET a.ENDDATE = DATE_SUB(b.begndate, INTERVAL 1 day)\nWHERE a.POOLAREA_NO='000000'",
			"hilist_b A ,hilist_b_update_2107 B"},
		{"SET 同行的老写法", "UPDATE t SET a=1 WHERE id=1", "t"},
		{"多行 + STRAIGHT_JOIN", "UPDATE hilist_b_update_2107 B\n  STRAIGHT_JOIN hilist_b A\n     ON A.HILIST_CODE = B.HILIST_CODE_FWXM_OLD\nSET A.ENDDATE = 1",
			"hilist_b_update_2107 B\n  STRAIGHT_JOIN hilist_b A\n     ON A.HILIST_CODE = B.HILIST_CODE_FWXM_OLD"},
	}
	for _, c := range cases {
		tables, setPart, ok := splitUpdate(c.sql)
		if !ok {
			t.Errorf("%s: 解析失败", c.name)
			continue
		}
		if tables != c.wantTables {
			t.Errorf("%s: tables=%q，期望 %q", c.name, tables, c.wantTables)
		}
		if !strings.HasPrefix(strings.TrimSpace(setPart), "A.ENDDATE") && !strings.HasPrefix(strings.TrimSpace(setPart), "a.ENDDATE") && !strings.HasPrefix(strings.TrimSpace(setPart), "a=1") {
			t.Errorf("%s: set 部分不对：%q", c.name, setPart)
		}
	}

	// 非 UPDATE 不该被误认
	if _, _, ok := splitUpdate("SELECT 1"); ok {
		t.Error("SELECT 被当成 UPDATE 了")
	}
	// 字符串里的 set 不能被当成 SET 关键字：set 部分应从 memo 开始
	tables, setPart, ok := splitUpdate("UPDATE t SET memo = 'set me' WHERE id=1")
	if !ok {
		t.Fatal("正常语句解析失败")
	}
	if tables != "t" {
		t.Errorf("表名解析错: %q", tables)
	}
	if !strings.HasPrefix(strings.TrimSpace(setPart), "memo") {
		t.Errorf("SET 被字符串里的 set 干扰了: %q", setPart)
	}
}

// 逗号连接（UPDATE a, b）必须拆成两张表，否则单表分支会漏掉关联表
func TestUpdateTablePartsCommaJoin(t *testing.T) {
	tables, _, ok := splitUpdate("UPDATE hilist_b A ,hilist_b_update_2107 B\nSET a.x=1")
	if !ok {
		t.Fatal("解析失败")
	}
	parts := updateTableParts(tables)
	if len(parts) != 2 {
		t.Fatalf("应拆成 2 张表，实际 %d: %q", len(parts), parts)
	}
	if tb, al := parseTableRef(parts[0]); tb != "hilist_b" || al != "A" {
		t.Errorf("第一张表解析错: %q %q", tb, al)
	}
	if tb, al := parseTableRef(parts[1]); tb != "hilist_b_update_2107" || al != "B" {
		t.Errorf("第二张表解析错: %q %q", tb, al)
	}
}

// 注释里的 set 不能被当成 SET 关键字
func TestSplitUpdateIgnoresSetInComment(t *testing.T) {
	cases := []struct{ name, sql, wantTables string }{
		{"行注释含 set", "UPDATE hilist_b A -- 这里要 set 一下\nSET a.x=1", "hilist_b A"},
		{"块注释含 set", "UPDATE hilist_b A /* set 说明 */ SET a.x=1", "hilist_b A"},
		{"井号注释含 set", "UPDATE hilist_b A # set\nSET a.x=1", "hilist_b A"},
		{"中文注释无 set", "UPDATE hilist_b A -- 服务项目专用\nSET a.x=1", "hilist_b A"},
	}
	for _, c := range cases {
		tables, setPart, ok := splitUpdate(c.sql)
		if !ok {
			t.Errorf("%s: 解析失败", c.name)
			continue
		}
		if tables != c.wantTables {
			t.Errorf("%s: tables=%q，期望 %q", c.name, tables, c.wantTables)
		}
		if !strings.HasPrefix(strings.TrimSpace(setPart), "a.x") && !strings.HasPrefix(strings.TrimSpace(setPart), "SET a.x") {
			t.Errorf("%s: set 部分不对：%q", c.name, setPart)
		}
	}
}
