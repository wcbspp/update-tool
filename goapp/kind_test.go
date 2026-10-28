package main

import "testing"

func TestStmtKind(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		{"INSERT INTO t SELECT 1", "insert"},
		{"  insert into t values (1)", "insert"},
		{"UPDATE t SET a=1 WHERE 1=1", "update"},
		{"delete from t where id=1", "delete"},
		{"SELECT 1", "query"},
		{"CREATE TABLE t (id int)", "ddl"},
		{"-- 注释\nUPDATE t SET a=1", "other"}, // 前置注释按原实现不识别
	}
	for _, c := range cases {
		if got := stmtKind(c.sql); got != c.want {
			t.Errorf("stmtKind(%q)=%s want %s", c.sql, got, c.want)
		}
	}
}

func TestDMLSecUnlimited(t *testing.T) {
	if got := dsDMLSec(DataSource{DMLSec: -1}); got != 0 {
		t.Errorf("DMLSec=-1 应表示不限(0)，实际 %d", got)
	}
	if got := dsDMLSec(DataSource{}); got != 1800 {
		t.Errorf("默认应为 1800，实际 %d", got)
	}
	if got := dsDMLSec(DataSource{DMLSec: 300}); got != 300 {
		t.Errorf("显式 300 应保留，实际 %d", got)
	}
	if got := dsDQLSec(DataSource{}); got != 60 {
		t.Errorf("DQL 默认应为 60，实际 %d", got)
	}
}
