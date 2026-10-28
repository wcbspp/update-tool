package main

import (
	"strings"
	"testing"
)

// 预检分支的集成测试：需要真实 MySQL（只建自己的临时表）。
// MYSQL_TEST_DSN='root:pass@tcp(127.0.0.1:3306)/medupdate_test' go test -run TestPreflight -v
func setupPfTables(t *testing.T, ds DataSource) {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	stmts := []string{
		"DROP TABLE IF EXISTS pf_target",
		"DROP TABLE IF EXISTS pf_update",
		"CREATE TABLE pf_target (HILIST_CODE VARCHAR(32) NOT NULL, BEGNDATE DATE NOT NULL, INSU_ADMDVS VARCHAR(16) NOT NULL, ENDDATE DATE NULL, POOLAREA_NO VARCHAR(16), LIST_TYPE VARCHAR(8), PRIMARY KEY (HILIST_CODE, BEGNDATE, INSU_ADMDVS))",
		"CREATE TABLE pf_update (HILIST_CODE VARCHAR(32) NOT NULL, BEGNDATE DATE NOT NULL, INSU_ADMDVS VARCHAR(16) NOT NULL, ENDDATE DATE NULL, POOLAREA_NO VARCHAR(16), LIST_TYPE VARCHAR(8), CRTER_NAME VARCHAR(32), KEY i_code (HILIST_CODE))",
		"INSERT INTO pf_target VALUES ('A','2025-01-01','000000',NULL,'000000','201'),('B','2025-01-01','000000',NULL,'000000','201')",
		"INSERT INTO pf_update VALUES ('A','2026-01-01','000000',NULL,'000000','201','X')",
	}
	for _, st := range stmts {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备数据失败(%s): %v", st, err)
		}
	}
}

func titles(checks []map[string]any) []string {
	out := make([]string, 0, len(checks))
	for _, c := range checks {
		if s, ok := c["title"].(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func hasTitle(checks []map[string]any, want string) bool {
	for _, t := range titles(checks) {
		if strings.Contains(t, want) {
			return true
		}
	}
	return false
}

func runPf(t *testing.T, ds DataSource, sql string) []map[string]any {
	checks, _, err := preflight(ds, splitStatements(sql), "CRTER_ID", []string{"NV", "LC"})
	if err != nil {
		t.Fatalf("预检失败: %v", err)
	}
	return checks
}

func TestPreflightMultiUpdateNoWhere(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	checks := runPf(t, ds, "UPDATE pf_target a, pf_update b SET a.ENDDATE = b.BEGNDATE WHERE a.HILIST_CODE=b.HILIST_CODE")
	if !hasTitle(checks, "被更新表") {
		t.Fatalf("多表 UPDATE 应识别被更新表，实际检查项: %v", titles(checks))
	}
	checks = runPf(t, ds, "UPDATE pf_target a, pf_update b SET a.ENDDATE = b.BEGNDATE")
	if !hasTitle(checks, "没有 WHERE 条件") {
		t.Fatalf("缺 WHERE 应报错，实际检查项: %v", titles(checks))
	}
}

// 回归：单表 UPDATE 曾被整段跳过，一条检查都不出
func TestPreflightSingleUpdateNotSkipped(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	checks := runPf(t, ds, "UPDATE pf_target SET ENDDATE='2026-01-01' WHERE HILIST_CODE='A'")
	if len(checks) == 0 {
		t.Fatal("单表 UPDATE 不应被跳过")
	}
	if !hasTitle(checks, "被更新表") {
		t.Fatalf("单表 UPDATE 应识别被更新表，实际: %v", titles(checks))
	}
	if !hasTitle(checks, "预计影响行数") {
		t.Fatalf("单表 UPDATE 应给影响行数，实际: %v", titles(checks))
	}
}

func TestPreflightBogusSetColumn(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	checks := runPf(t, ds, "UPDATE pf_target SET NO_SUCH_COL='x' WHERE 1=1")
	if !hasTitle(checks, "SET 列不存在") {
		t.Fatalf("应报 SET 列不存在，实际: %v", titles(checks))
	}
}

func TestPreflightInsertSelect(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	checks := runPf(t, ds, "INSERT INTO pf_target SELECT * FROM pf_update")
	if !hasTitle(checks, "目标表") {
		t.Fatalf("INSERT 应报目标表，实际: %v", titles(checks))
	}
	if !hasTitle(checks, "源表") {
		t.Fatalf("INSERT 应检查源表，实际: %v", titles(checks))
	}
}

func TestPreflightDeleteNoWhere(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	checks := runPf(t, ds, "DELETE FROM pf_target")
	if !hasTitle(checks, "没有 WHERE 条件") {
		t.Fatalf("无 WHERE 的 DELETE 应告警，实际: %v", titles(checks))
	}
	if !hasTitle(checks, "被删除表") {
		t.Fatalf("DELETE 应识别被删除表，实际: %v", titles(checks))
	}
}

func TestPreflightMultiStatementSameTable(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	setupPfTables(t, ds)
	sql := "UPDATE pf_target SET ENDDATE='2026-01-01' WHERE HILIST_CODE='A';\n" +
		"UPDATE pf_target SET ENDDATE='2026-02-01' WHERE HILIST_CODE='B';"
	checks := runPf(t, ds, sql)
	if !hasTitle(checks, "同一张表被") {
		t.Fatalf("应给出同表多语句的结论，实际: %v", titles(checks))
	}
}
