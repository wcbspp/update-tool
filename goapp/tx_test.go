package main

import (
	"database/sql"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// 事务语义测试：只在设置了 MYSQL_TEST_DSN 时运行，避免误连生产库。
// 例：MYSQL_TEST_DSN='root:pass@tcp(127.0.0.1:3306)/medupdate_test' go test -run TestRunStatements -v
func dsFromEnv(t *testing.T, env string) DataSource {
	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skipf("未设置 %s，跳过数据库测试", env)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("DSN 解析失败: %v", err)
	}
	host, port := cfg.Addr, 3306
	if h, p, ok := splitHostPort(cfg.Addr); ok {
		host, port = h, p
	}
	return DataSource{Name: "test", Host: host, Port: port, User: cfg.User, Pass: cfg.Passwd, Database: cfg.DBName}
}

func splitHostPort(addr string) (string, int, bool) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 3306, false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return host, 3306, false
	}
	return host, port, true
}

func prepareTable(t *testing.T, ds DataSource) {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS tx_probe"); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE tx_probe (id INT PRIMARY KEY, v VARCHAR(20))"); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
}

func countProbe(t *testing.T, ds DataSource) int {
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM tx_probe").Scan(&n); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	return n
}

// 第 2 条失败 → 第 1 条必须被回滚
func TestRunStatementsRollback(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	prepareTable(t, ds)
	defer func() {
		if db, err := openDB(ds); err == nil {
			_, _ = db.Exec("DROP TABLE IF EXISTS tx_probe")
			db.Close()
		}
	}()

	stmts := []string{
		"INSERT INTO tx_probe (id, v) VALUES (1, 'a')",
		"INSERT INTO tx_probe (id, v) VALUES (1, 'dup')", // 主键冲突
	}
	_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if failedAt != 1 || errMsg == "" {
		t.Fatalf("期望第 2 条失败，实际 failedAt=%d err=%q", failedAt, errMsg)
	}
	if n := countProbe(t, ds); n != 0 {
		t.Fatalf("失败后应整体回滚，实际留下 %d 行", n)
	}
}

// 全部成功 → 必须提交
func TestRunStatementsCommit(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	prepareTable(t, ds)
	defer func() {
		if db, err := openDB(ds); err == nil {
			_, _ = db.Exec("DROP TABLE IF EXISTS tx_probe")
			db.Close()
		}
	}()

	stmts := []string{
		"INSERT INTO tx_probe (id, v) VALUES (1, 'a')",
		"INSERT INTO tx_probe (id, v) VALUES (2, 'b')",
	}
	_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if failedAt != -1 || errMsg != "" {
		t.Fatalf("期望全部成功，实际 failedAt=%d err=%q", failedAt, errMsg)
	}
	if n := countProbe(t, ds); n != 2 {
		t.Fatalf("期望提交 2 行，实际 %d", n)
	}
}

// 预演必须回滚
func TestRunStatementsDryRun(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	prepareTable(t, ds)
	defer func() {
		if db, err := openDB(ds); err == nil {
			_, _ = db.Exec("DROP TABLE IF EXISTS tx_probe")
			db.Close()
		}
	}()

	stmts := []string{"INSERT INTO tx_probe (id, v) VALUES (1, 'a')"}
	if _, _, errMsg, _ := runStatements(ds, stmts, true); errMsg != "" {
		t.Fatalf("预演不应报错: %s", errMsg)
	}
	if n := countProbe(t, ds); n != 0 {
		t.Fatalf("预演后表应为空，实际 %d 行", n)
	}
}

var _ = sql.ErrNoRows
