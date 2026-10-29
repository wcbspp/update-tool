package main

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// 大数据量多场景测试：只在设置 MYSQL_PERF_DSN 时运行，自己建表、自己删表。
// MYSQL_PERF_DSN='root:pass@tcp(127.0.0.1:3306)/medupdate_perf' go test -run TestPerf -v -timeout 30m
func perfRows() int {
	if v := os.Getenv("MYSQL_PERF_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 3000000
}

func perfSetup(t *testing.T, ds DataSource) {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	stmts := []string{
		"DROP TABLE IF EXISTS perf_seq",
		"DROP TABLE IF EXISTS perf_target",
		"DROP TABLE IF EXISTS perf_batch",
		"CREATE TABLE perf_seq (n INT PRIMARY KEY)",
		"CREATE TABLE perf_target (" +
			"HILIST_CODE VARCHAR(32) NOT NULL, BEGNDATE DATE NOT NULL, INSU_ADMDVS VARCHAR(16) NOT NULL," +
			"ENDDATE DATE NULL, HILIST_NAME VARCHAR(64), LIST_TYPE VARCHAR(8), POOLAREA_NO VARCHAR(16)," +
			"CRTER_ID VARCHAR(16), UPDT_TIME DATETIME, WUBI VARCHAR(32), PINYIN VARCHAR(32)," +
			"PRIMARY KEY (HILIST_CODE, BEGNDATE, INSU_ADMDVS))",
		"CREATE TABLE perf_batch (" +
			"HILIST_CODE VARCHAR(32) NOT NULL, BEGNDATE DATE NOT NULL, INSU_ADMDVS VARCHAR(16) NOT NULL," +
			"ENDDATE DATE NULL, HILIST_NAME VARCHAR(64), LIST_TYPE VARCHAR(8), POOLAREA_NO VARCHAR(16)," +
			"CRTER_ID VARCHAR(16), UPDT_TIME DATETIME, WUBI VARCHAR(32), PINYIN VARCHAR(32)," +
			"KEY idx_code (HILIST_CODE))",
		"INSERT INTO perf_seq VALUES (1)",
	}
	for _, st := range stmts {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	// 翻倍生成序号
	rows := perfRows()
	start := time.Now()
	// 用 information_schema 的笛卡尔积生成序号，避免自插入翻倍产生重复
	_, err = db.Exec(fmt.Sprintf(`INSERT INTO perf_target
		(HILIST_CODE,BEGNDATE,INSU_ADMDVS,ENDDATE,HILIST_NAME,LIST_TYPE,POOLAREA_NO,CRTER_ID,UPDT_TIME,WUBI,PINYIN)
		SELECT CONCAT('C',LPAD(@r:=@r+1,10,'0')), '2025-01-01', '000000', NULL, CONCAT('名称',@r), '201', '000000', 'NV2.9', NOW(), CONCAT('W',@r), CONCAT('P',@r)
		FROM (SELECT @r:=0) init, information_schema.COLUMNS c1, information_schema.COLUMNS c2, information_schema.COLUMNS c3
		LIMIT %d`, rows))
	if err != nil {
		t.Fatalf("灌入 %d 行失败: %v", rows, err)
	}
	t.Logf("目标表灌入 %d 行，用时 %s", rows, time.Since(start).Round(time.Millisecond))
	idxStart := time.Now()
	for _, ddl := range []string{
		"ALTER TABLE perf_target ADD INDEX idx_updt (UPDT_TIME)",
		"ALTER TABLE perf_target ADD INDEX idx_code (HILIST_CODE)",
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatalf("补索引失败: %v", err)
		}
	}
	t.Logf("补建两个索引，用时 %s", time.Since(idxStart).Round(time.Millisecond))
	// 批次表 300 行：前 250 行命中已有编码，后 50 行是新编码
	_, err = db.Exec(`INSERT INTO perf_batch
		(HILIST_CODE,BEGNDATE,INSU_ADMDVS,ENDDATE,HILIST_NAME,LIST_TYPE,POOLAREA_NO,CRTER_ID,UPDT_TIME,WUBI,PINYIN)
		SELECT CONCAT('C',LPAD(@m:=@m+1,10,'0')), '2026-01-01', '000000', NULL, CONCAT('新名称',@m), '201', '000000', 'NV2.10', NOW(), CONCAT('W',@m), CONCAT('P',@m)
		FROM (SELECT @m:=0) init2, information_schema.COLUMNS d1 LIMIT 250`)
	if err != nil {
		t.Fatalf("灌入批次表失败: %v", err)
	}
	_, err = db.Exec(`INSERT INTO perf_batch
		(HILIST_CODE,BEGNDATE,INSU_ADMDVS,ENDDATE,HILIST_NAME,LIST_TYPE,POOLAREA_NO,CRTER_ID,UPDT_TIME,WUBI,PINYIN)
		SELECT CONCAT('N',LPAD(@k:=@k+1,10,'0')), '2026-01-01', '000000', NULL, CONCAT('新增',@k), '201', '000000', 'NV2.10', NOW(), CONCAT('W',@k), CONCAT('P',@k)
		FROM (SELECT @k:=0) init3, information_schema.COLUMNS e1 LIMIT 50`)
	if err != nil {
		t.Fatalf("灌入批次表(新增)失败: %v", err)
	}
}

func perfTeardown(ds DataSource) {
	if db, err := openDB(ds); err == nil {
		for _, t := range []string{"perf_seq", "perf_target", "perf_batch"} {
			_, _ = db.Exec("DROP TABLE IF EXISTS " + t)
		}
		db.Close()
	}
}

func perfRun(t *testing.T, ds DataSource, name, sql string) []map[string]any {
	t0 := time.Now()
	checks, _, err := preflight(ds, splitStatements(sql), "CRTER_ID", []string{"NV", "LC"})
	d := time.Since(t0)
	if err != nil {
		t.Fatalf("%s 预检失败: %v", name, err)
	}
	ms := 0
	hasRowEstimate := false
	for _, c := range checks {
		if c["title"] == "预计影响行数" {
			hasRowEstimate = true
			if s, ok := c["detail"].(string); ok {
				t.Logf("[%s] %s（%s）", name, s, d.Round(time.Millisecond))
			}
		}
		ms++
	}
	if ms == 0 {
		t.Fatalf("%s 预检没有返回任何检查项", name)
	}
	if !hasRowEstimate {
		t.Errorf("%s 缺少「预计影响行数」检查项，预检可能没跑起来", name)
	}
	if ms == 0 {
		t.Fatalf("%s 没有任何检查项", name)
	}
	return checks
}

func TestPerfScenarios(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_PERF_DSN")
	perfSetup(t, ds)
	defer perfTeardown(ds)

	// 1. 多表 UPDATE：走主键等值连接
	perfRun(t, ds, "多表UPDATE", `UPDATE perf_target a, perf_batch b
		SET a.ENDDATE = DATE_SUB(b.BEGNDATE, INTERVAL 1 DAY)
		WHERE a.HILIST_CODE = b.HILIST_CODE AND a.POOLAREA_NO='000000'
		  AND (a.ENDDATE IS NULL OR a.ENDDATE >= b.BEGNDATE)`)

	// 2. 服务项目专用：CONCAT 等值（关键：300 万行还能不能走索引）
	perfRun(t, ds, "CONCAT等值", `UPDATE perf_target a, perf_batch b
		SET a.ENDDATE = DATE_SUB(b.BEGNDATE, INTERVAL 1 DAY)
		WHERE a.HILIST_CODE = CONCAT(b.HILIST_CODE,'-',b.HILIST_CODE)
		  AND a.POOLAREA_NO='000000'
		  AND (a.ENDDATE IS NULL OR a.ENDDATE >= b.BEGNDATE)`)

	// 3. INSERT ... SELECT：主键冲突比对 + 索引判定
	perfRun(t, ds, "INSERT-SELECT", `INSERT INTO perf_target
		(HILIST_CODE,BEGNDATE,INSU_ADMDVS,ENDDATE,HILIST_NAME,LIST_TYPE,POOLAREA_NO,CRTER_ID,UPDT_TIME,WUBI,PINYIN)
		SELECT HILIST_CODE,BEGNDATE,INSU_ADMDVS,ENDDATE,HILIST_NAME,LIST_TYPE,POOLAREA_NO,CRTER_ID,UPDT_TIME,WUBI,PINYIN
		FROM perf_batch`)

	// 4. 单表 UPDATE（大表全表更新，应报错并给估算）
	checks := perfRun(t, ds, "单表UPDATE无WHERE", `UPDATE perf_target SET ENDDATE='2026-01-01'`)
	if !hasTitle(checks, "没有 WHERE 条件") {
		t.Fatalf("大表无 WHERE 应报错")
	}

	// 5. DELETE 无 WHERE
	perfRun(t, ds, "DELETE无WHERE", `DELETE FROM perf_target`)

	// 6. 同表多语句
	perfRun(t, ds, "同表多语句", `UPDATE perf_target SET ENDDATE='2026-01-01' WHERE HILIST_CODE='C0000001';
		UPDATE perf_target SET ENDDATE='2026-02-01' WHERE HILIST_CODE='C0000002';`)
}

// 执行阶段：小批量写入，验证事务在 300 万行表上仍然可用
func TestPerfExecute(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_PERF_DSN")
	perfSetup(t, ds)
	defer perfTeardown(ds)

	stmts := splitStatements(`UPDATE perf_target a, perf_batch b
		SET a.ENDDATE = DATE_SUB(b.BEGNDATE, INTERVAL 1 DAY)
		WHERE a.HILIST_CODE = b.HILIST_CODE AND a.POOLAREA_NO='000000'
		  AND (a.ENDDATE IS NULL OR a.ENDDATE >= b.BEGNDATE)`)

	t0 := time.Now()
	_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if failedAt != -1 || errMsg != "" {
		t.Fatalf("执行失败 failedAt=%d err=%s", failedAt, errMsg)
	}
	t.Logf("大表 UPDATE 执行完成，用时 %s", time.Since(t0).Round(time.Millisecond))
}
