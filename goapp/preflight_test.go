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
	t.Cleanup(func() {
		if c, err := openDB(ds); err == nil {
			_, _ = c.Exec("DROP TABLE IF EXISTS pf_target")
			_, _ = c.Exec("DROP TABLE IF EXISTS pf_update")
			c.Close()
		}
	})
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

// INSERT ... SELECT 的列对应关系：普通列要认，换顺序要认，表达式一律不下结论
func TestInsertKeyMap(t *testing.T) {
	pk := []string{"X", "Y"}
	cases := []struct {
		name    string
		sel     string
		colList string
		want    map[string]string
		ok      bool
	}{
		{"同名直取", "select x, y from s", "x, y", map[string]string{"X": "X", "Y": "Y"}, true},
		{"换顺序", "select y, x from s", "x, y", map[string]string{"X": "Y", "Y": "X"}, true},
		{"带别名前缀", "select a.x as xx, a.y from s a", "x, y", map[string]string{"X": "X", "Y": "Y"}, true},
		{"整表投影", "select * from s", "", map[string]string{"X": "X", "Y": "Y"}, true},
		{"整表投影换行", "select *\nfrom s", "", map[string]string{"X": "X", "Y": "Y"}, true},
		{"整表投影带别名", "select a.* from s a", "", map[string]string{"X": "X", "Y": "Y"}, true},
		{"表达式不给结论", "select x+1, y from s", "x, y", map[string]string{}, false},
		{"常量不给结论", "select 'A', y from s", "x, y", map[string]string{}, false},
		{"列数对不上", "select x from s", "x, y", map[string]string{}, false},
		{"列清单缺判重列", "select x from s", "x", map[string]string{}, false},
		{"映射到别的源列", "select x, z from s", "x, y", map[string]string{"X": "X", "Y": "Z"}, true},
	}
	for _, c := range cases {
		got, ok := insertKeyMap(c.sel, c.colList, pk)
		if ok != c.ok {
			t.Fatalf("%s: ok=%v 期望 %v（got=%v）", c.name, ok, c.ok, got)
		}
		if !ok {
			continue
		}
		for k, v := range c.want {
			if got[k] != v {
				t.Fatalf("%s: 映射 %s→%s，期望 %s", c.name, k, got[k], v)
			}
		}
	}
}

// 回归：SELECT 里把两列调换顺序时，冲突判断必须按映射后的列比，不能按同名列比
func TestPreflightInsertReorderedColumns(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	setup := []string{
		"DROP TABLE IF EXISTS pf_swap_tgt",
		"DROP TABLE IF EXISTS pf_swap_src",
		"CREATE TABLE pf_swap_tgt (x VARCHAR(8) NOT NULL, y VARCHAR(8) NOT NULL, PRIMARY KEY (x, y))",
		"CREATE TABLE pf_swap_src (x VARCHAR(8) NOT NULL, y VARCHAR(8) NOT NULL)",
		"INSERT INTO pf_swap_tgt VALUES ('q','p')",
		"INSERT INTO pf_swap_src VALUES ('p','q')",
	}
	for _, st := range setup {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备数据失败(%s): %v", st, err)
		}
	}
	defer func() {
		db.Exec("DROP TABLE IF EXISTS pf_swap_tgt")
		db.Exec("DROP TABLE IF EXISTS pf_swap_src")
	}()

	// 实际插入的是 (y,x) = ('q','p')，与目标表已有行冲突；按同名列比会漏报
	checks := runPf(t, ds, "INSERT INTO pf_swap_tgt (x, y) SELECT y, x FROM pf_swap_src")
	if !hasTitle(checks, "主键冲突") {
		t.Fatalf("调换列顺序后应检出主键冲突，实际: %v", titles(checks))
	}

	// SELECT 里写表达式 → 不给冲突结论，改为提示人工核对
	checks = runPf(t, ds, "INSERT INTO pf_swap_tgt (x, y) SELECT CONCAT(x,''), y FROM pf_swap_src")
	if hasTitle(checks, "无主键冲突") || hasTitle(checks, "主键冲突") {
		t.Fatalf("表达式无法判断时不应给冲突结论，实际: %v", titles(checks))
	}
	if !hasTitle(checks, "判重未做自动比对") {
		t.Fatalf("应提示无法自动比对，实际: %v", titles(checks))
	}
}

// DELETE 走有界精确计数（不是执行计划估算）
func TestPreflightDeleteExactCount(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, st := range []string{
		"DROP TABLE IF EXISTS pf_del",
		"CREATE TABLE pf_del (id INT NOT NULL PRIMARY KEY, flag INT)",
		"INSERT INTO pf_del VALUES (1,1),(2,1),(3,1),(4,0),(5,0)",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	defer db.Exec("DROP TABLE IF EXISTS pf_del")

	// 带 LIMIT：实际只删 2 行，不能报 3 行
	checksLim := runPf(t, ds, "DELETE FROM pf_del WHERE flag=1 LIMIT 2")
	if !hasTitle(checksLim, "预计影响行数") {
		t.Fatalf("应输出预计影响行数，实际: %v", titles(checksLim))
	}
	for _, c := range checksLim {
		if s, _ := c["title"].(string); strings.Contains(s, "预计影响行数") {
			d, _ := c["detail"].(string)
			// 匹配 3 行、LIMIT 2：实际删 2 行，是精确的
			if !strings.Contains(d, "预计删除 2 行") || !strings.Contains(d, "LIMIT 2") {
				t.Fatalf("LIMIT 2 应报删除 2 行，实际: %s", d)
			}
		}
	}

	checks := runPf(t, ds, "DELETE FROM pf_del WHERE flag=1")
	found := false
	for _, c := range checks {
		if s, _ := c["title"].(string); strings.Contains(s, "预计影响行数") {
			d, _ := c["detail"].(string)
			if !strings.Contains(d, "预计删除 3 行") || !strings.Contains(d, "精确计数") {
				t.Fatalf("应给出精确行数 3，实际: %s", d)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("应输出预计影响行数，实际: %v", titles(checks))
	}
}

// 没配版本前缀时，列出实际值并说明“未配置”，不能拼出 WHERE ()
func TestPreflightVersionWithoutPrefixes(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, st := range []string{
		"DROP TABLE IF EXISTS pf_batch_x_update_1",
		"CREATE TABLE pf_batch_x_update_1 (CODE VARCHAR(8), CRTER_ID VARCHAR(16))",
		"INSERT INTO pf_batch_x_update_1 VALUES ('A','NV1'),('B','LC2')",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	defer db.Exec("DROP TABLE IF EXISTS pf_batch_x_update_1")

	// 语句里要引用批次表，预检才会把它当版本核对对象
	checks, versions, err := preflight(ds, splitStatements("UPDATE pf_batch_x_update_1 SET CODE='A' WHERE CODE='A'"), "CRTER_ID", nil)
	if err != nil {
		t.Fatalf("预检失败: %v", err)
	}
	_ = checks
	if len(versions) != 1 {
		t.Fatalf("应返回 1 张批次表，实际 %d", len(versions))
	}
	v := versions[0]
	if s, _ := v["note"].(string); !strings.Contains(s, "未配置") {
		t.Fatalf("应说明未配置前缀，实际: %v", v)
	}
	if vals, _ := v["ok"].([]string); len(vals) != 2 {
		t.Fatalf("应列出 2 个实际值，实际: %v", v["ok"])
	}
}

// 估算时按表大小重排连接顺序：小表在前 + STRAIGHT_JOIN，且结果不变
func TestReorderForCount(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	setup := []string{
		"DROP TABLE IF EXISTS r_ord_big",
		"DROP TABLE IF EXISTS r_ord_small",
		"CREATE TABLE r_ord_big (id INT NOT NULL PRIMARY KEY, v INT)",
		"CREATE TABLE r_ord_small (id INT NOT NULL PRIMARY KEY, v INT)",
	}
	for _, st := range setup {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	// 大表 8192 行、小表 50 行（递归 CTE 有 1000 次上限，用倍增造数）
	for _, tb := range []string{"r_ord_big", "r_ord_small"} {
		if _, err := db.Exec("INSERT INTO " + tb + " (id, v) VALUES (1,1)"); err != nil {
			t.Fatalf("造数失败: %v", err)
		}
		for i := 0; i < 13; i++ {
			if _, err := db.Exec("INSERT INTO " + tb + " (id, v) SELECT id + (SELECT MAX(id) FROM (SELECT id FROM " + tb + ") x), 1 FROM " + tb); err != nil {
				t.Fatalf("造数失败: %v", err)
			}
		}
	}
	if _, err := db.Exec("DELETE FROM r_ord_big WHERE id > 8192"); err != nil {
		t.Fatalf("修剪失败: %v", err)
	}
	if _, err := db.Exec("DELETE FROM r_ord_small WHERE id > 50"); err != nil {
		t.Fatalf("修剪失败: %v", err)
	}
	for _, tb := range []string{"r_ord_big", "r_ord_small"} {
		if _, err := db.Exec("UPDATE " + tb + " SET v = id % 7"); err != nil {
			t.Fatalf("造数失败: %v", err)
		}
	}
	defer func() {
		db.Exec("DROP TABLE IF EXISTS r_ord_big")
		db.Exec("DROP TABLE IF EXISTS r_ord_small")
	}()
	if _, err := db.Exec("ANALYZE TABLE r_ord_big, r_ord_small"); err != nil {
		t.Fatalf("统计信息失败: %v", err)
	}

	out, ok := reorderForCount(db, ds.Database, "r_ord_big a, r_ord_small b")
	if !ok {
		t.Fatalf("应能重排，实际放弃")
	}
	if !strings.Contains(out, "STRAIGHT_JOIN") {
		t.Fatalf("应加 STRAIGHT_JOIN，实际: %s", out)
	}
	if !strings.HasPrefix(out, "r_ord_small") {
		t.Fatalf("小表应在前，实际: %s", out)
	}

	// 重排不改变结果
	var n1, n2 int64
	if err := db.QueryRow("SELECT COUNT(*) FROM r_ord_big a, r_ord_small b WHERE a.v = b.v").Scan(&n1); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	q2 := "SELECT COUNT(*) FROM " + out + " WHERE a.v = b.v"
	if err := db.QueryRow(q2).Scan(&n2); err != nil {
		t.Fatalf("重排后查询失败: %v", err)
	}
	if n1 != n2 {
		t.Fatalf("重排改变了结果：%d vs %d", n1, n2)
	}
}

// 没配版本字段时不应产生版本核对块（否则界面显示一堆“无  字段”）
func TestPreflightNoVersionColumnSkipsCheck(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS pf_nv_update_1"); err != nil {
		t.Fatalf("准备失败: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE pf_nv_update_1 (CODE VARCHAR(8))"); err != nil {
		t.Fatalf("准备失败: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS pf_nv_update_1")

	_, versions, err := preflight(ds, splitStatements("UPDATE pf_nv_update_1 SET CODE='A' WHERE CODE='A'"), "", nil)
	if err != nil {
		t.Fatalf("预检失败: %v", err)
	}
	if len(versions) != 0 {
		t.Fatalf("没配版本字段不应有版本块，实际 %v", versions)
	}
}

// 回归：WHERE 里带子查询的单表 DELETE 不能被当成多表 DELETE
func TestPreflightDeleteWithSubquery(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, st := range []string{
		"DROP TABLE IF EXISTS pf_dq", "DROP TABLE IF EXISTS pf_dq_src",
		"CREATE TABLE pf_dq (id INT NOT NULL PRIMARY KEY)",
		"CREATE TABLE pf_dq_src (id INT NOT NULL PRIMARY KEY)",
		"INSERT INTO pf_dq VALUES (1),(2),(3)",
		"INSERT INTO pf_dq_src VALUES (1)",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	defer func() {
		db.Exec("DROP TABLE IF EXISTS pf_dq")
		db.Exec("DROP TABLE IF EXISTS pf_dq_src")
	}()

	checks := runPf(t, ds, "DELETE FROM pf_dq WHERE id IN (SELECT id FROM pf_dq_src)")
	if hasTitle(checks, "多表 DELETE") {
		t.Fatalf("单表 + 子查询不该判成多表，实际: %v", titles(checks))
	}
	foundTbl, foundCnt := false, false
	for _, c := range checks {
		if s, _ := c["title"].(string); s == "被删除表" {
			if d, _ := c["detail"].(string); d == "pf_dq" {
				foundTbl = true
			}
		}
		if s, _ := c["title"].(string); strings.Contains(s, "预计影响行数") {
			foundCnt = true
		}
	}
	if !foundTbl || !foundCnt {
		t.Fatalf("应识别表名并给行数，实际: %v / %v", titles(checks), checks)
	}

	// 修饰词不能当成表名
	checks = runPf(t, ds, "DELETE IGNORE FROM pf_dq WHERE id=1")
	for _, c := range checks {
		if s, _ := c["title"].(string); s == "被删除表" {
			if d, _ := c["detail"].(string); d != "pf_dq" {
				t.Fatalf("DELETE IGNORE 的表名应解析为 pf_dq，实际 %q", d)
			}
		}
	}
}

// 回归：SET 里带子查询、但没有外层 WHERE 的 UPDATE 必须报错
func TestPreflightUpdateSubqueryNoWhere(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, st := range []string{
		"DROP TABLE IF EXISTS pf_uw", "DROP TABLE IF EXISTS pf_uw_src",
		"CREATE TABLE pf_uw (id INT NOT NULL PRIMARY KEY, v VARCHAR(8))",
		"CREATE TABLE pf_uw_src (id INT NOT NULL PRIMARY KEY)",
		"INSERT INTO pf_uw VALUES (1,'a')",
		"INSERT INTO pf_uw_src VALUES (1)",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	defer func() {
		db.Exec("DROP TABLE IF EXISTS pf_uw")
		db.Exec("DROP TABLE IF EXISTS pf_uw_src")
	}()

	checks := runPf(t, ds, "UPDATE pf_uw SET v=(SELECT 'x' FROM pf_uw_src WHERE pf_uw_src.id=pf_uw.id)")
	if !hasTitle(checks, "没有 WHERE 条件") {
		t.Fatalf("SET 里的子查询不能算 WHERE，实际: %v", titles(checks))
	}
}
