package main

import (
	"strings"
	"testing"
	"time"
)

// 执行语义测试：跟直接在客户端里跑应该是同一个结果。
// MYSQL_TEST_DSN='root:pass@tcp(127.0.0.1:3306)/medupdate_test' go test -run TestExec -v
func prepareExecTable(t *testing.T, ds DataSource) {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, st := range []string{
		"DROP TABLE IF EXISTS exec_probe",
		"CREATE TABLE exec_probe (id INT NOT NULL PRIMARY KEY, v VARCHAR(20) NULL)",
	} {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备失败(%s): %v", st, err)
		}
	}
	t.Cleanup(func() {
		if db, err := openDB(ds); err == nil {
			_, _ = db.Exec("DROP TABLE IF EXISTS exec_probe")
			db.Close()
		}
	})
}

func rowsOf(t *testing.T, ds DataSource, q string) []string {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id int
		var v *string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatalf("扫描失败: %v", err)
		}
		s := "-"
		if v != nil {
			s = *v
		}
		out = append(out, strings.Join([]string{itoa(id), s}, ":"))
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

// 与客户端一致：全部成功才落库，affected 是「真正改变的行数」
func TestExecAffectedAndCommit(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	prepareExecTable(t, ds)

	stmts := []string{
		"INSERT INTO exec_probe (id, v) VALUES (1,'a'),(2,'b')",
		"UPDATE exec_probe SET v='a' WHERE id=1", // 值没变 → affected 0（与客户端相同）
		"UPDATE exec_probe SET v='c' WHERE id=2", // 真改 1 行
		"DELETE FROM exec_probe WHERE id=1",      // 删 1 行
	}
	res, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if failedAt != -1 || errMsg != "" {
		t.Fatalf("应全部成功，实际 failedAt=%d err=%q", failedAt, errMsg)
	}
	want := map[string]int64{"0": 2, "1": 0, "2": 1, "3": 1}
	for k, v := range want {
		if got := res[k].Affected; got != v {
			t.Fatalf("语句 %s affected=%d，期望 %d", k, got, v)
		}
	}
	if got := rowsOf(t, ds, "SELECT id, v FROM exec_probe ORDER BY id"); len(got) != 1 || got[0] != "2:c" {
		t.Fatalf("最终数据不对: %v", got)
	}
}

// 预演只回滚，不留痕
func TestExecDryRunLeavesNothing(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	prepareExecTable(t, ds)

	_, failedAt, errMsg, _ := runStatements(ds, []string{"INSERT INTO exec_probe (id, v) VALUES (9,'x')"}, true)
	if failedAt != -1 || errMsg != "" {
		t.Fatalf("预演不应报错，实际 %d %q", failedAt, errMsg)
	}
	if got := rowsOf(t, ds, "SELECT id, v FROM exec_probe"); len(got) != 0 {
		t.Fatalf("预演后不应留数据，实际 %v", got)
	}
}

// 超时按「每条语句」算：两条各 0.6 秒的语句，在 1 秒上限下都应通过
func TestExecTimeoutIsPerStatement(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	ds.DMLSec = 1

	stmts := []string{"SELECT SLEEP(0.6)", "SELECT SLEEP(0.6)"}
	t0 := time.Now()
	_, failedAt, errMsg, _ := runStatements(ds, stmts, true)
	if failedAt != -1 || errMsg != "" {
		t.Fatalf("每条语句都应在 1 秒内完成，实际 failedAt=%d err=%q", failedAt, errMsg)
	}
	if d := time.Since(t0); d > 3*time.Second {
		t.Fatalf("两条语句应串行但各自计时，总耗时异常: %v", d)
	}
}

// 隐式提交/自控事务的语句必须被挡在执行之外
func TestExecGuardRejectsImplicitCommit(t *testing.T) {
	bad := []string{
		"LOCK TABLES exec_probe WRITE",
		"UNLOCK TABLES",
		"ANALYZE TABLE exec_probe",
		"OPTIMIZE TABLE exec_probe",
		"REPAIR TABLE exec_probe",
		"CALL some_proc()",
		"CREATE INDEX i ON exec_probe(v)",
		"TRUNCATE TABLE exec_probe",
	}
	for _, s := range bad {
		if !isDDL(s) {
			t.Fatalf("应被拦截: %s", s)
		}
	}
	good := []string{
		"UPDATE exec_probe SET v='x' WHERE id=1",
		"INSERT INTO exec_probe (id) SELECT id FROM exec_probe",
		"DELETE FROM exec_probe WHERE id=1",
	}
	for _, s := range good {
		if isDDL(s) {
			t.Fatalf("不应被拦截: %s", s)
		}
	}
}

// 预检查询超时的取值：0/未填 → 20 秒，-1 → 不限，正数按配置
func TestDsPFSec(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{{0, 20}, {-1, 0}, {7, 7}, {300, 300}}
	for _, c := range cases {
		if got := dsPFSec(DataSource{PFSec: c.in}); got != c.want {
			t.Fatalf("PFSec=%d → %d，期望 %d", c.in, got, c.want)
		}
	}
}
