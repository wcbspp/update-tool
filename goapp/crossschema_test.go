package main

import (
	"strings"
	"testing"
)

// 跨库写非事务引擎：预演必须拒绝，不能假称「已回滚」。
// 需要 MYSQL_TEST_DSN（会在里面建一个临时库，跑完 DROP）。
func TestDryRunRejectsCrossSchemaMyISAM(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	const other = "medupdate_xdb"
	for _, st := range []string{
		"DROP DATABASE IF EXISTS " + other,
		"CREATE DATABASE " + other,
		"CREATE TABLE " + other + ".x_t (id INT PRIMARY KEY, v INT) ENGINE=MyISAM",
		"INSERT INTO " + other + ".x_t VALUES (1, 1)",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	t.Cleanup(func() {
		if c, err := openDB(ds); err == nil {
			_, _ = c.Exec("DROP DATABASE IF EXISTS " + other)
			c.Close()
		}
	})

	_, failedAt, msg, _ := runStatements(ds, []string{
		"UPDATE " + other + ".x_t SET v = 2 WHERE id = 1",
	}, true)
	if failedAt >= 0 || !strings.Contains(msg, "事务引擎") {
		t.Fatalf("跨库 MyISAM 预演应被拒绝，实际 failedAt=%d msg=%q", failedAt, msg)
	}

	var v int
	if err := db.QueryRow("SELECT v FROM " + other + ".x_t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("读回失败: %v", err)
	}
	if v != 1 {
		t.Fatalf("预演不应改动数据，实际 v=%d", v)
	}
}

// CTE 前缀与反引号限定名：三种写法都必须能解析出真实目标表，
// 否则「非事务引擎不能回滚」这条防线会被整条绕过。
func TestWriteTargetRefsHandlesCteAndBackticks(t *testing.T) {
	stmts := []string{
		"WITH x AS (SELECT 1) UPDATE otherdb.t SET v = 1 WHERE id = 1",
		"DELETE FROM `shop`.`u` WHERE id = 2",
		"INSERT INTO `dst` VALUES (1)",
	}
	got := map[string]bool{}
	for _, r := range writeTargetRefs(stmts, "cur") {
		got[r.schema+"."+r.name] = true
	}
	for _, want := range []string{"otherdb.t", "shop.u", "cur.dst"} {
		if !got[want] {
			t.Fatalf("缺少目标表 %s，实际 %v", want, got)
		}
	}
}

func TestIsUseStmt(t *testing.T) {
	yes := []string{"USE otherdb", "use `db2`", "  USE x", "/*c*/ USE y"}
	no := []string{"UPDATE t SET v=1", "SELECT 'use x'", "USEFUL"}
	for _, s := range yes {
		if !isUseStmt(s) {
			t.Fatalf("应识别为 USE：%q", s)
		}
	}
	for _, s := range no {
		if isUseStmt(s) {
			t.Fatalf("不应识别为 USE：%q", s)
		}
	}
}

// 块注释当分隔符用：'x'/*c*/'y' 在 MySQL 里等于 'xy'，
// 把注释连同位置一起删掉会变成 'x”y'（x'y），静默改写要写入的值。
func TestSplitStatementsKeepsCommentSeparator(t *testing.T) {
	got := splitStatements("SELECT 'x'/*c*/'y';")
	if len(got) != 1 {
		t.Fatalf("应当拆成 1 条，实际 %d 条：%v", len(got), got)
	}
	if strings.Contains(got[0], "'x''y'") {
		t.Fatalf("注释被无空格吞掉，值会被改写：%q", got[0])
	}
}
