package main

import (
	"strings"
	"testing"
)

// 跨库写非事务引擎：预演必须拒绝，不能假称「已回滚」。// 跨库写非事务引擎：执行失败时必须在错误里点名哪些表改不回来，
// 不能只说「已回滚」。需要 MYSQL_TEST_DSN（会在里面建临时库，跑完 DROP）。
func TestRunNamesNonTransactionalTablesOnFailure(t *testing.T) {
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
			if isPrivilegeErr(err) {
				t.Skipf("该数据库不允许建库（%v），跨库用例无法准备", err)
			}
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
		"UPDATE 不存在的表 SET x = 1",
	})
	if failedAt != 1 {
		t.Fatalf("应停在第 2 条，实际 failedAt=%d", failedAt)
	}
	if !strings.Contains(msg, "非事务引擎") {
		t.Fatalf("错误里必须点名非事务引擎，实际：%q", msg)
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

// 逗号多表 UPDATE：只有 SET 左边的别名才是写入目标，
// 只读参与的关联表不能算进去（否则会误报「非事务引擎、改不回来」）。
func TestWriteTargetRefsSkipsReadOnlyJoinTable(t *testing.T) {
	got := map[string]bool{}
	for _, r := range writeTargetRefs([]string{
		"UPDATE inn t, dim d SET t.v = d.v WHERE t.id = d.id",
	}, "cur") {
		got[r.schema+"."+r.name] = true
	}
	if !got["cur.inn"] {
		t.Fatalf("应包含写入表 cur.inn，实际 %v", got)
	}
	if got["cur.dim"] {
		t.Fatalf("dim 只读参与，不应算写入目标：%v", got)
	}
}

// 两张表都被 SET 赋值时，两张都要算
func TestWriteTargetRefsMultiTableBothWritten(t *testing.T) {
	got := map[string]bool{}
	for _, r := range writeTargetRefs([]string{
		"UPDATE a x, b y SET x.v = 1, y.v = 2 WHERE x.id = y.id",
	}, "cur") {
		got[r.schema+"."+r.name] = true
	}
	for _, want := range []string{"cur.a", "cur.b"} {
		if !got[want] {
			t.Fatalf("缺少写入表 %s，实际 %v", want, got)
		}
	}
}

// autocommit 是能绕过 COMMIT/ROLLBACK 拦截的写法：
// SET SESSION / SET @@ 前缀都对不上，必须在语句扫描阶段整类拒绝。
func TestFindAutocommitStmt(t *testing.T) {
	bad := []string{
		"SET SESSION autocommit = 0",
		"SET @@autocommit = 1",
		"SET autocommit=1",
		"set session AUTOCOMMIT = 1",
	}
	for _, s := range bad {
		if len(findAutocommitStmt([]string{s})) != 1 {
			t.Fatalf("应识别为改 autocommit：%q", s)
		}
	}
	ok := []string{
		"UPDATE t SET v = 1 WHERE id = 1",
		"SELECT 'autocommit' AS x",
		"INSERT INTO t VALUES (1)",
	}
	for _, s := range ok {
		if len(findAutocommitStmt([]string{s})) != 0 {
			t.Fatalf("不应拦截：%q", s)
		}
	}
}

// autocommit 的等价写法：SET 后没空格、混写用户变量、带注释
func TestFindAutocommitStmtVariants(t *testing.T) {
	bad := []string{
		"SET@@autocommit=1",
		"SET @x=1,@@session.autocommit=1",
		"SET @@session.autocommit = 1",
		"SET /*c*/ autocommit=1",
		"SET @@autocommit:=1",
	}
	for _, s := range bad {
		if len(findAutocommitStmt([]string{s})) != 1 {
			t.Fatalf("应识别为改 autocommit：%q", s)
		}
	}
	ok := []string{
		"SET @old=@@autocommit",
		"SET @autocommit=1",
		"SET @x='autocommit=1'",
	}
	for _, s := range ok {
		if len(findAutocommitStmt([]string{s})) != 0 {
			t.Fatalf("不应拦截：%q", s)
		}
	}
}

// 结尾是字符串/括号的语句不该被判成「没写完」；末行是注释也不该误报
func TestValidateTextNoFalseTruncation(t *testing.T) {
	cases := []string{
		"UPDATE t SET v='x' WHERE v='a'",
		"UPDATE t SET v='x' WHERE id IN (1,2)",
		"UPDATE t SET v=1 WHERE id=1;\n-- 待确认,",
		"UPDATE t SET v=1 WHERE id=1;\n-- 注意 and",
	}
	for _, c := range cases {
		for _, chk := range validateText(c) {
			if chk["level"] == "error" && chk["title"] == "语句疑似不完整" {
				t.Fatalf("不该报截断：%q -> %v", c, chk)
			}
		}
	}
}

// 多表 DELETE 的别名必须还原成真实表名，否则引擎检查会去查一张叫 "a" 的表，
// 查不到就静默放过——「这些表改不回来」的提示整条消失，界面却仍说已回滚。
func TestWriteTargetRefsResolvesDeleteAliases(t *testing.T) {
	cases := map[string]string{
		"DELETE a FROM zz_m1 a JOIN zz_m2 b ON a.id = b.id":             "zz_m1",
		"DELETE FROM a USING zz_m1 a JOIN zz_m2 b ON a.id = b.id":       "zz_m1",
		"DELETE FROM zz_m1 USING zz_m1 JOIN zz_m2 b ON zz_m1.id = b.id": "zz_m1",
	}
	for st, want := range cases {
		got := map[string]bool{}
		for _, r := range writeTargetRefs([]string{st}, "cur") {
			got[r.name] = true
		}
		if !got[want] {
			t.Fatalf("%s\n  应包含目标表 %s，实际 %v", st, want, got)
		}
	}
}
