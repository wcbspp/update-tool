package main

import (
	"database/sql"
	"fmt"
	"testing"
)

// 执行的硬约束：整段脚本是一个事务。
//   - 全部成功 → 每一条都必须真的生效（不能出现「报成功但少做了一条」）
//   - 任何一条失败 → 前面做的全部回滚，库里一个字都不能留
// 另外验证驱动没有开启 multiStatements：否则一条字符串能偷偷执行多条，
// 事务边界就不再可靠。

func execSetup(t *testing.T, ds DataSource) {
	t.Helper()
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, q := range []string{
		`DROP TABLE IF EXISTS exec_a`,
		`DROP TABLE IF EXISTS exec_b`,
		`CREATE TABLE exec_a (id INT PRIMARY KEY, v VARCHAR(20), n INT) ENGINE=InnoDB`,
		`CREATE TABLE exec_b (id INT PRIMARY KEY, note VARCHAR(20)) ENGINE=InnoDB`,
		`INSERT INTO exec_a VALUES (1,'a',10),(2,'b',20),(3,'c',30)`,
		`INSERT INTO exec_b VALUES (1,'x')`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("准备数据失败（%s）: %v", q, err)
		}
	}
}

func execDump(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	return dumpTable(t, db, table)
}

// 五条语句跨两张表，全部必须生效
func TestExecAppliesEveryStatement(t *testing.T) {
	ds := smokeDS(t)
	execSetup(t, ds)
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	stmts := []string{
		`UPDATE exec_a SET v='A1' WHERE id=1`,
		`INSERT INTO exec_a VALUES (4,'d',40)`,
		`DELETE FROM exec_a WHERE id=3`,
		`INSERT INTO exec_b VALUES (2,'y')`,
		`UPDATE exec_b SET note='z' WHERE id=1`,
	}
	results, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if errMsg != "" {
		t.Fatalf("执行失败于第 %d 条: %s", failedAt+1, errMsg)
	}
	if len(results) != len(stmts) {
		t.Fatalf("只返回了 %d 条结果，应为 %d 条", len(results), len(stmts))
	}

	// 逐条核对：漏做任何一条都会在这里暴露
	var v string
	var n int
	if err := db.QueryRow(`SELECT v,n FROM exec_a WHERE id=1`).Scan(&v, &n); err != nil || v != "A1" {
		t.Fatalf("第 1 条没生效: v=%q err=%v", v, err)
	}
	var cnt int
	if err := db.QueryRow(`SELECT COUNT(*) FROM exec_a WHERE id=4`).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("第 2 条没生效: cnt=%d err=%v", cnt, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM exec_a WHERE id=3`).Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("第 3 条没生效: cnt=%d err=%v", cnt, err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM exec_b WHERE id=2`).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("第 4 条没生效: cnt=%d err=%v", cnt, err)
	}
	var note string
	if err := db.QueryRow(`SELECT note FROM exec_b WHERE id=1`).Scan(&note); err != nil || note != "z" {
		t.Fatalf("第 5 条没生效: note=%q err=%v", note, err)
	}

	// 返回的受影响行数也要对得上
	total := int64(0)
	for _, r := range results {
		total += r.Affected
	}
	if total != 5 {
		t.Fatalf("受影响行数合计应为 5，实际 %d", total)
	}
}

// rowLockFree: 这一行是不是没人锁着。
// 用 SELECT ... FOR UPDATE + 1 秒锁等待来判断：能拿到锁说明确实没有别的事务
// 还压着这把锁（也就是那条失败的事务真的结束了）。
// 不用 INNODB_TRX：它需要 PROCESS 权限，权限不够时只能看到自己的事务，
// 结果永远是 0，等于什么也没验证。
func rowLockFree(t *testing.T, ds DataSource, table string, id int) bool {
	t.Helper()
	conn, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Exec("SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
		t.Fatalf("设置锁等待超时失败: %v", err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatalf("开事务失败: %v", err)
	}
	defer tx.Rollback()
	var n int
	err = tx.QueryRow("SELECT n FROM " + table + " WHERE id = " + itoa(id) + " FOR UPDATE").Scan(&n)
	return err == nil
}

// proveLockDetection: 正控。先故意锁住那一行，确认检测能看出「被锁」；
// 看不出就说明这个手段无效，后面的「没锁」也不可信。
func proveLockDetection(t *testing.T, ds DataSource) {
	t.Helper()
	holder, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer holder.Close()
	tx, err := holder.Begin()
	if err != nil {
		t.Fatalf("开事务失败: %v", err)
	}
	if _, err := tx.Exec(`UPDATE exec_a SET n = n + 1 WHERE id = 1`); err != nil {
		tx.Rollback()
		t.Fatalf("正控写入失败: %v", err)
	}
	if rowLockFree(t, ds, "exec_a", 1) {
		tx.Rollback()
		t.Fatal("行明明被锁着却判定为「没锁」，这个检测手段无效")
	}
	tx.Rollback()
	if !rowLockFree(t, ds, "exec_a", 1) {
		t.Fatal("正控回滚后仍然判定为「被锁」，检测不可靠")
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	if neg {
		return "-" + string(b)
	}
	return string(b)
}

// 把失败语句插到每一个位置，每次都必须零残留
func TestExecFailureAtAnyPositionLeavesNothing(t *testing.T) {
	ds := smokeDS(t)
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	// 先证明「能看出行被锁住」，否则后面的「没锁」是空话
	proveLockDetection(t, ds)

	good := []string{
		`UPDATE exec_a SET v='CHANGED' WHERE id=1`,
		`INSERT INTO exec_a VALUES (7,'g',70)`,
		`DELETE FROM exec_a WHERE id=2`,
		`UPDATE exec_b SET note='CHANGED' WHERE id=1`,
	}
	bad := `INSERT INTO exec_a VALUES (1,'dup',0)` // 主键冲突，必然失败

	for pos := 0; pos <= len(good); pos++ {
		execSetup(t, ds)
		beforeA := execDump(t, db, "exec_a")
		beforeB := execDump(t, db, "exec_b")

		stmts := make([]string, 0, len(good)+1)
		stmts = append(stmts, good[:pos]...)
		stmts = append(stmts, bad)
		stmts = append(stmts, good[pos:]...)

		_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
		if errMsg == "" {
			t.Fatalf("失败语句插在第 %d 位，却没有报错", pos+1)
		}
		if failedAt != pos {
			t.Fatalf("应失败在第 %d 位，实际第 %d 位", pos+1, failedAt+1)
		}
		if a := execDump(t, db, "exec_a"); a != beforeA {
			t.Fatalf("失败于第 %d 位时 exec_a 被改动了\n前：%s\n后：%s", pos+1, beforeA, a)
		}
		if b := execDump(t, db, "exec_b"); b != beforeB {
			t.Fatalf("失败于第 %d 位时 exec_b 被改动了\n前：%s\n后：%s", pos+1, beforeB, b)
		}
		if containsAny(errMsg, []string{"回滚可能未完成"}) {
			t.Fatalf("失败于第 %d 位时回滚失败，数据可能留在库里: %s", pos+1, errMsg)
		}
		// 不能只看「另一个连接读不到」：事务若挂在那里没提交也没回滚，
		// 读起来同样看不到。这里直接查还有没有未结束的事务。
		// 失败后那行必须没人锁着：锁还在就说明事务既没提交也没回滚
		if !rowLockFree(t, ds, "exec_a", 1) {
			t.Fatalf("失败于第 %d 位后 exec_a(1) 仍被锁着，事务可能没回滚", pos+1)
		}
	}
}

// 驱动不能开启 multiStatements：否则一条字符串能塞多条语句，事务边界会失真
func TestDriverRejectsMultiStatements(t *testing.T) {
	ds := smokeDS(t)
	execSetup(t, ds)
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("UPDATE exec_a SET v='x' WHERE id=1; UPDATE exec_a SET v='y' WHERE id=1")
	if err == nil {
		t.Fatal("驱动允许一条 Exec 执行多条语句（multiStatements 被打开了），事务边界不可靠")
	}
	// 光报错不够：还要确认第一条没被执行掉
	var v string
	if e := db.QueryRow(`SELECT v FROM exec_a WHERE id=1`).Scan(&v); e != nil {
		t.Fatalf("回查失败: %v", e)
	}
	if v != "a" {
		t.Fatalf("虽然报错，但第一条语句被执行了（v=%q），多条语句仍在暗中生效", v)
	}
	if !containsAny(err.Error(), []string{"syntax", "语法", "1064"}) {
		t.Fatalf("报错原因不像语法错误，无法确认是 multiStatements 被拒: %v", err)
	}
}

// 演练模式：多条语句全部「执行」过，但库里不能有任何变化
func TestExecDryRunTouchesNothing(t *testing.T) {
	ds := smokeDS(t)
	execSetup(t, ds)
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	beforeA := execDump(t, db, "exec_a")
	beforeB := execDump(t, db, "exec_b")

	stmts := []string{
		`UPDATE exec_a SET v='DRY' WHERE id=1`,
		`INSERT INTO exec_a VALUES (8,'h',80)`,
		`DELETE FROM exec_a WHERE id=2`,
		`UPDATE exec_b SET note='DRY' WHERE id=1`,
	}
	results, _, errMsg, _ := runStatements(ds, stmts, true)
	if errMsg != "" {
		t.Fatalf("演练报错: %s", errMsg)
	}
	if len(results) != len(stmts) {
		t.Fatalf("演练只返回 %d 条结果，应为 %d 条", len(results), len(stmts))
	}
	if a := execDump(t, db, "exec_a"); a != beforeA {
		t.Fatalf("演练改动了 exec_a\n前：%s\n后：%s", beforeA, a)
	}
	if b := execDump(t, db, "exec_b"); b != beforeB {
		t.Fatalf("演练改动了 exec_b\n前：%s\n后：%s", beforeB, b)
	}
}

// 失败时返回的错误里要能定位到第几条
func TestExecFailureReportsPosition(t *testing.T) {
	ds := smokeDS(t)
	execSetup(t, ds)
	stmts := []string{
		`UPDATE exec_a SET v='x' WHERE id=1`,
		`INSERT INTO 不存在的表 VALUES (1)`,
	}
	_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if errMsg == "" {
		t.Fatal("应该报错")
	}
	if failedAt != 1 {
		t.Fatalf("应指向第 2 条，实际第 %d 条", failedAt+1)
	}
	if !containsAny(errMsg, []string{"不存在的表", "doesn't exist", "1146"}) {
		t.Fatalf("错误信息定位不到问题: %s", errMsg)
	}
}

func containsAny(s string, subs []string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && len(s) >= len(sub) && findFold(s, sub) {
			return true
		}
	}
	return false
}

func findFold(s, sub string) bool {
	ls, lsub := lower(s), lower(sub)
	for i := 0; i+len(lsub) <= len(ls); i++ {
		if ls[i:i+len(lsub)] == lsub {
			return true
		}
	}
	return false
}

func lower(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 32
		}
	}
	return string(b)
}

var _ = fmt.Sprint

// DDL 与事务控制语句的识别：handler 靠这两个判断拒绝执行，
// 判错一个就会让隐式提交漏进事务里。
func TestDDLAndTxnControlDetection(t *testing.T) {
	ddl := []string{
		"CREATE TABLE t (id INT)",
		"ALTER TABLE t ADD COLUMN c INT",
		"DROP TABLE t",
		"TRUNCATE TABLE t",
		"RENAME TABLE a TO b",
		"CREATE INDEX i ON t(c)",
		"DROP INDEX i ON t",
	}
	for _, q := range ddl {
		if !isDDL(q) {
			t.Errorf("没识别出 DDL: %s", q)
		}
	}
	notDDL := []string{
		"UPDATE t SET a=1 WHERE id=1",
		"INSERT INTO t VALUES (1)",
		"DELETE FROM t WHERE id=1",
		"SELECT * FROM t",
	}
	for _, q := range notDDL {
		if isDDL(q) {
			t.Errorf("把普通 DML 误判成 DDL: %s", q)
		}
	}

	txn := []string{"COMMIT", "commit;", "ROLLBACK", "rollback;", "START TRANSACTION", "BEGIN", "SET autocommit=1"}
	for _, q := range txn {
		if !isTxnControl(q) {
			t.Errorf("没识别出事务控制语句: %s", q)
		}
	}
	notTxn := []string{"UPDATE t SET a=1 WHERE id=1", "SELECT 1", "INSERT INTO t VALUES (1)"}
	for _, q := range notTxn {
		if isTxnControl(q) {
			t.Errorf("把普通语句误判成事务控制: %s", q)
		}
	}
}
