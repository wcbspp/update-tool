package main

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// 预检的硬约束：只读，绝不改动任何数据。
//
// 两个角度证明：
//   1) 执行前后把表里的数据整份 dump 出来比对，必须一字不差；
//   2) 用**只有 SELECT 权限**的账号跑预检，能跑通就说明它不需要写权限。
// 第二个角度更硬：真写了会直接报权限错误，而不是靠我们观察不到。

// dumpTable: 把整张表的行按主键顺序序列化成字符串，用于前后比对
func dumpTable(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	rows, err := db.Query("SELECT * FROM " + table + " ORDER BY 1")
	if err != nil {
		t.Fatalf("dump %s 失败: %v", table, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("读列名失败: %v", err)
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(cols, "|") + "\n")
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("扫描失败: %v", err)
		}
		for i, v := range vals {
			if i > 0 {
				sb.WriteByte('|')
			}
			switch x := v.(type) {
			case nil:
				sb.WriteString("<NULL>")
			case []byte:
				sb.Write(x)
			default:
				fmt.Fprintf(&sb, "%v", x)
			}
		}
		sb.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历失败: %v", err)
	}
	return sb.String()
}

// 预检一个会改数据的脚本，跑完数据必须原封不动
func TestPreflightWritesNothing(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)

	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	before := dumpTable(t, db, "smoke_t")

	// 三种写入都上：真写了必然看得出来
	stmts := []string{
		`UPDATE smoke_t SET v='CHANGED', n=999 WHERE id=1`,
		`INSERT INTO smoke_t VALUES (9,'new',90)`,
		`DELETE FROM smoke_t WHERE id=2`,
		`UPDATE smoke_t SET v=v WHERE id>=1`,
	}
	if _, _, err := preflight(ds, stmts, "", nil); err != nil {
		t.Fatalf("预检报错: %v", err)
	}

	after := dumpTable(t, db, "smoke_t")
	if before != after {
		t.Fatalf("预检改动了数据！\n前：%s\n后：%s", before, after)
	}
}

// 建一个只有 SELECT 权限的账号。返回的 DataSource 用它连库。
func makeReadOnlyDS(t *testing.T, ds DataSource) (DataSource, func()) {
	t.Helper()
	root, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer root.Close()

	const user = "mu_ro_test"
	const pass = "mu_ro_test_pw"
	// 建不出只读账号就无法证明「预检不需要写权限」，这里必须失败而不是跳过：
	// 跳过会让整条证明静默失效，看起来全绿其实什么都没验。
	if _, err := root.Exec("DROP USER IF EXISTS '" + user + "'@'%'"); err != nil {
		t.Fatalf("无法清理旧账号，预检只读性无法证明: %v", err)
	}
	if _, err := root.Exec("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + pass + "'"); err != nil {
		t.Fatalf("无法创建只读账号，预检只读性无法证明: %v", err)
	}
	if _, err := root.Exec("GRANT SELECT ON " + ds.Database + ".* TO '" + user + "'@'%'"); err != nil {
		root.Exec("DROP USER IF EXISTS '" + user + "'@'%'")
		t.Fatalf("无法授 SELECT 权限，预检只读性无法证明: %v", err)
	}
	cleanup := func() {
		if r2, err := openDB(ds); err == nil {
			r2.Exec("DROP USER IF EXISTS '" + user + "'@'%'")
			r2.Close()
		}
	}

	ro := ds
	ro.User = user
	ro.Pass = pass
	return ro, cleanup
}

// 用只有 SELECT 权限的账号跑预检：能跑通，就说明预检不需要任何写权限。
// 只要它偷偷写一句，这里就会拿到权限错误。
func TestPreflightWithReadOnlyAccount(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	ro, cleanup := makeReadOnlyDS(t, ds)
	defer cleanup()

	// 先确认这个账号真的写不了（防止用例本身失效）
	db, err := openDB(ro)
	if err != nil {
		t.Fatalf("只读账号连不上: %v", err)
	}
	if _, err := db.Exec(`UPDATE smoke_t SET v='x' WHERE id=1`); err == nil {
		db.Close()
		t.Fatal("只读账号竟然能写，这个用例证明不了什么")
	}
	db.Close()

	stmts := []string{
		`UPDATE smoke_t SET v='CHANGED' WHERE id=1`,
		`INSERT INTO smoke_t VALUES (9,'new',90)`,
		`DELETE FROM smoke_t WHERE id=2`,
	}
	rep, _, err := preflight(ro, stmts, "", nil)
	if err != nil {
		t.Fatalf("预检在只读账号下报错，说明它需要写权限: %v", err)
	}
	if len(rep) == 0 {
		t.Fatal("预检没有产出任何检查项")
	}
	for _, r := range rep {
		if fmt.Sprint(r["level"]) == "error" {
			t.Fatalf("只读账号下预检出现 error（可能是权限相关的写操作）：%v — %v", r["title"], r["detail"])
		}
	}
}

// 只读账号也要能跑完整预检：行数、判重、版本核对都只读
func TestPreflightReadOnlyCoversAllChecks(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	ro, cleanup := makeReadOnlyDS(t, ds)
	defer cleanup()

	// 先确认这个账号真的写不了，否则下面「跑通」毫无意义
	db, err := openDB(ro)
	if err != nil {
		t.Fatalf("只读账号连不上: %v", err)
	}
	if _, err := db.Exec(`UPDATE smoke_t SET v='x' WHERE id=1`); err == nil {
		db.Close()
		t.Fatal("只读账号竟然能写，这个用例证明不了什么")
	}
	db.Close()

	rep, _, err := preflight(ro, []string{
		`UPDATE smoke_t SET n=11 WHERE id=1`,
		`INSERT INTO smoke_t SELECT id+100, v, n FROM smoke_t WHERE id=1`,
		`DELETE FROM smoke_t WHERE id=3`,
	}, "", nil)
	if err != nil {
		t.Fatalf("预检失败: %v", err)
	}
	titles := map[string]bool{}
	for _, r := range rep {
		titles[fmt.Sprint(r["title"])] = true
	}
	// 这几项都要跑起来才算「完整预检」，不该因为只读而跳过
	for _, want := range []string{"目标表", "被删除表"} {
		if !titles[want] {
			t.Fatalf("只读账号下缺少检查项 %q，实际：%v", want, titles)
		}
	}
	// 不能把「权限不足」当正常结果糊过去：那样看着跑通了，其实什么都没查
	for _, r := range rep {
		msg := fmt.Sprint(r["title"]) + fmt.Sprint(r["detail"])
		if containsAny(msg, []string{"denied", "权限", "command denied", "1142"}) {
			t.Fatalf("预检把权限错误当成了检查结果：%v — %v", r["title"], r["detail"])
		}
	}
}
