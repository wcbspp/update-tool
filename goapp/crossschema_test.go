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
