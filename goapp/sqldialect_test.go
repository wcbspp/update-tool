package main

import (
	"strings"
	"testing"
)

func TestTonightEdgeCases(t *testing.T) {
	t.Run("stripCTE", func(t *testing.T) {
		cases := []struct{ in, wantPrefix string }{
			{"WITH a AS (SELECT 1) SELECT * FROM a", "SELECT * FROM a"},
			{"with recursive a(n) as (select 1 union all select n+1 from a where n<3) select * from a", "select * from a"},
			{"WITH a AS (SELECT 'update delete' AS s) UPDATE t SET x=1", "UPDATE t SET x=1"},
			{"WITH `update` AS (SELECT 1) DELETE FROM t", "DELETE FROM t"},
			{"UPDATE t SET x=1", "UPDATE t SET x=1"},
			{"WITHOUT_PREFIX", "WITHOUT_PREFIX"},
		}
		for _, c := range cases {
			got := stripCTE(c.in)
			if len(got) < len(c.wantPrefix) || got[:len(c.wantPrefix)] != c.wantPrefix {
				t.Fatalf("stripCTE(%q) = %q，期望以 %q 开头", c.in, got, c.wantPrefix)
			}
		}
	})

	t.Run("usesMySQL8Only", func(t *testing.T) {
		shouldBe8 := []string{"WITH a AS (SELECT 1) SELECT * FROM a", "TABLE t", "VALUES ROW(1,2)"}
		for _, s := range shouldBe8 {
			if _, _, _, _, ok := usesMySQL8Only(s); !ok {
				t.Fatalf("%q 应判为 8.0 专属", s)
			}
		}
		normal := []string{
			"UPDATE t SET a=1 WHERE id=1",
			"INSERT INTO t (a) VALUES (1)",
			"DELETE FROM t WHERE id=1",
			"SELECT * FROM with_x",
			"UPDATE table_stat SET a=1",
		}
		for _, s := range normal {
			if feat, _, _, _, ok := usesMySQL8Only(s); ok {
				t.Fatalf("%q 被误判为 8.0 专属（%s）", s, feat)
			}
		}
	})

	t.Run("updateTableParts", func(t *testing.T) {
		cases := []struct {
			in   string
			want int
		}{
			{"t1 A, t2 B", 2},
			{"t1 A JOIN t2 B ON A.id=B.id", 2},
			{"t1 A STRAIGHT_JOIN t2 B ON A.id=B.id", 2},
			{"t1", 1},
		}
		for _, c := range cases {
			if got := len(updateTableParts(c.in)); got != c.want {
				t.Fatalf("updateTableParts(%q) = %d，期望 %d", c.in, got, c.want)
			}
		}
	})

	t.Run("parseSetAssignments", func(t *testing.T) {
		cols, rows := parseSetAssignments("SET a=1, b='x,y', c=(SELECT 1)")
		if len(cols) != 3 || len(rows) != 1 || len(rows[0]) != 3 {
			t.Fatalf("解析错：cols=%v rows=%v", cols, rows)
		}
		if cols[1] != "b" || rows[0][1] != "'x,y'" {
			t.Fatalf("带逗号的字符串没处理对：%v", rows[0])
		}
	})

	t.Run("parseServerVer", func(t *testing.T) {
		for _, c := range []struct {
			raw          string
			major, minor int
			ok           bool
		}{
			{"5.7.41-230700-log", 5, 7, true},
			{"10.11.6-MariaDB-1:10.11.6+maria~ubu2204", 10, 11, true},
			{"8.0.36", 8, 0, true},
			{"unknown", 0, 0, false},
		} {
			v := parseServerVer(c.raw)
			if v.ok != c.ok || (c.ok && (v.major != c.major || v.minor != c.minor)) {
				t.Fatalf("%q → %+v", c.raw, v)
			}
		}
	})

	t.Run("cteNames", func(t *testing.T) {
		names := cteNames("WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b")
		if !names["a"] || !names["b"] || len(names) != 2 {
			t.Fatalf("CTE 名解析错：%v", names)
		}
		if names2 := cteNames("UPDATE t SET a=1"); len(names2) != 0 {
			t.Fatalf("普通 UPDATE 不该有 CTE 名：%v", names2)
		}
	})
}

// 审计回归：DELETE 修饰词必须归一化，否则两道防线都漏
func TestDeleteModifiers(t *testing.T) {
	for _, st := range []string{
		"DELETE LOW_PRIORITY FROM t",
		"delete quick from t",
		"DELETE IGNORE FROM t",
	} {
		plain := reDeleteMods.ReplaceAllString(st, "delete ")
		m := reDelete.FindStringSubmatch(plain)
		if m == nil || normTable(m[1]) != "t" {
			t.Fatalf("%q 归一化后仍取不到表名：%q", st, plain)
		}
	}
}

// 审计回归：字符串里的 on duplicate 不能当成 ODKU
func TestInsertTolerantStringLiteral(t *testing.T) {
	if ok, how := insertTolerant(`INSERT INTO t (a,memo) VALUES (1,'on duplicate key update x')`); ok {
		t.Fatalf("字符串里的 on duplicate 被误判为 %s", how)
	}
	if ok, _ := insertTolerant(`INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a=2`); !ok {
		t.Fatal("真正的 ODKU 没识别出来")
	}
}

// 审计回归：ON 条件里的 'a set b' 不能把 UPDATE 切坏
func TestSplitUpdateWithSetInString(t *testing.T) {
	tables, set, ok := splitUpdate("UPDATE t1 a JOIN t2 b ON a.id=b.id AND b.tab='a set b' SET a.x=1 WHERE a.id=1")
	if !ok {
		t.Fatal("splitUpdate 失败")
	}
	if len(updateTableParts(tables)) != 2 {
		t.Fatalf("表部分切错：%q", tables)
	}
	if !strings.HasPrefix(strings.ToLower(set), "a.x=1") {
		t.Fatalf("SET 部分切错：%q", set)
	}
}

// 审计回归：派生表不能被当成真实表
func TestUpdateTablePartsDerived(t *testing.T) {
	parts := updateTableParts("t1 a JOIN (SELECT id FROM t2 JOIN t3 ON t2.id=t3.id) b ON a.id=b.id")
	if len(parts) != 1 || !strings.HasPrefix(parts[0], "t1") {
		t.Fatalf("派生表被当成表名了：%#v", parts)
	}
}

// 审计回归：MariaDB 的 5.5.5- 前缀不能骗到版本判断
func TestMariaDBVersion(t *testing.T) {
	v := parseServerVer("5.5.5-10.11.6-MariaDB-1:10.11.6+maria~ubu2204")
	if !v.ok || v.major != 10 || v.minor != 11 {
		t.Fatalf("MariaDB 版本解析错：%+v", v)
	}
	if !v.atLeast(8, 0, 1) {
		t.Fatal("MariaDB 10.11 应支持 CTE")
	}
}

// 执行入口兜底：哪些 DELETE 必须拦、哪些必须放行
func TestFindUnsafeStatement(t *testing.T) {
	blocked := []string{
		"DELETE FROM t",
		"DELETE FROM t;",
		"DELETE LOW_PRIORITY FROM t",
		"delete quick from t",
		"DELETE IGNORE FROM t",
		"WITH c AS (SELECT 1) DELETE FROM t",
		"DELETE FROM t PARTITION (p0)",
	}
	for _, st := range blocked {
		if _, _, bad := findUnsafeStatement([]string{st}); !bad {
			t.Fatalf("%q 应该被拦", st)
		}
	}
	allowed := []string{
		"DELETE FROM t WHERE 1=1",
		"DELETE FROM t ORDER BY id LIMIT 5",
		"DELETE a FROM t1 a JOIN t2 b ON a.id=b.id WHERE b.x=1",
		"DELETE FROM t1 USING t1 JOIN t2 ON t1.id=t2.id",
		"UPDATE t SET a=1",
		"INSERT INTO t (a) VALUES (1)",
	}
	for _, st := range allowed {
		if _, _, bad := findUnsafeStatement([]string{st}); bad {
			t.Fatalf("%q 不该被拦", st)
		}
	}
	// 分区提示要准确
	if _, hint, _ := findUnsafeStatement([]string{"DELETE FROM t PARTITION (p0)"}); !strings.Contains(hint, "PARTITION") {
		t.Fatalf("分区提示不对：%q", hint)
	}
	// 序号是 1-based，且能定位到第几条
	if idx, _, bad := findUnsafeStatement([]string{"SELECT 1", "DELETE FROM t"}); !bad || idx != 2 {
		t.Fatalf("定位错：idx=%d bad=%v", idx, bad)
	}
}
