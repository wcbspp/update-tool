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

// 并发化之后结果必须不变：有冲突、无冲突、冲突数到上限就停
func TestPreflightInsertConflictParallel(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	setup := []string{
		"DROP TABLE IF EXISTS pf_tgt2",
		"DROP TABLE IF EXISTS pf_src2",
		"DROP TABLE IF EXISTS pf_src_big",
		"DROP TABLE IF EXISTS pf_tgt_big",
		"CREATE TABLE pf_tgt2 (id INT NOT NULL PRIMARY KEY, code VARCHAR(8))",
		"CREATE TABLE pf_src2 (id INT NOT NULL PRIMARY KEY, code VARCHAR(8))",
		"DROP TABLE IF EXISTS pf_src3",
		"CREATE TABLE pf_src3 (id INT NOT NULL PRIMARY KEY, code VARCHAR(8))",
		"INSERT INTO pf_tgt2 VALUES (1,'A')",
		"INSERT INTO pf_src2 VALUES (1,'A'),(2,'B')",
		"INSERT INTO pf_src3 VALUES (101,'A'),(102,'B')",
		"CREATE TABLE pf_src_big (id INT NOT NULL PRIMARY KEY, code VARCHAR(8))",
		"INSERT INTO pf_src_big VALUES (1,'X')",
	}
	for i := 0; i < 11; i++ {
		setup = append(setup, "INSERT INTO pf_src_big SELECT id + (SELECT MAX(id) FROM pf_src_big), 'X' FROM pf_src_big")
	}
	setup = append(setup,
		"CREATE TABLE pf_tgt_big (id INT NOT NULL PRIMARY KEY, code VARCHAR(8))",
		"INSERT INTO pf_tgt_big SELECT id, code FROM pf_src_big WHERE id <= 1500",
	)
	for _, st := range setup {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备数据失败(%s): %v", st, err)
		}
	}
	defer func() {
		for _, st := range []string{"DROP TABLE IF EXISTS pf_tgt2", "DROP TABLE IF EXISTS pf_src2", "DROP TABLE IF EXISTS pf_src3", "DROP TABLE IF EXISTS pf_src_big", "DROP TABLE IF EXISTS pf_tgt_big"} {
			db.Exec(st)
		}
	}()

	checks := runPf(t, ds, "INSERT INTO pf_tgt2 (id, code) SELECT id, code FROM pf_src2")
	if !hasTitle(checks, "主键冲突") {
		t.Fatalf("应报主键冲突，实际: %v", titles(checks))
	}

	// 用一张本来就不冲突的源表（不是靠 SELECT 里改值：判重只比源表主键列）
	checks = runPf(t, ds, "INSERT INTO pf_tgt2 (id, code) SELECT id, code FROM pf_src3")
	if !hasTitle(checks, "无主键冲突") {
		t.Fatalf("应报无主键冲突，实际: %v", titles(checks))
	}

	checks = runPf(t, ds, "INSERT INTO pf_tgt_big (id, code) SELECT id, code FROM pf_src_big")
	found := false
	for _, c := range checks {
		if s, _ := c["title"].(string); strings.Contains(s, "主键冲突") {
			d, _ := c["detail"].(string)
			if !strings.Contains(d, "至少") || !strings.Contains(d, "1000") {
				t.Fatalf("冲突超过上限时应说明数到 1000 就停，实际: %s", d)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("大表冲突未检出，实际: %v", titles(checks))
	}
}
