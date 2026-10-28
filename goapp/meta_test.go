package main

import (
	"database/sql"
	"errors"
	"testing"
)

// 模拟「中间件限制 information_schema」：所有走入口的查询一律失败，
// 断言各元数据函数确实退回到 SHOW INDEX / SHOW COLUMNS，而不是误报“没有索引/没有字段”。
func TestMetadataFallbackWhenInformationSchemaBlocked(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()

	stmts := []string{
		"DROP TABLE IF EXISTS meta_probe",
		"CREATE TABLE meta_probe (code VARCHAR(32) NOT NULL, begndate DATE NOT NULL, memo VARCHAR(20) NULL, CRTER_ID VARCHAR(16), PRIMARY KEY (code, begndate))",
	}
	for _, st := range stmts {
		if _, err := db.Exec(st); err != nil {
			t.Fatalf("准备表失败: %v", err)
		}
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS meta_probe") }()

	oldQ := infQuery
	infQuery = func(*sql.DB, string, ...any) (*sql.Rows, error) { return nil, errors.New("blocked by middleware") }
	defer func() { infQuery = oldQ }()

	// 索引：应回退 SHOW INDEX，拿到主键列
	cols, name := keyCols(db, ds.Database, "meta_probe")
	if len(cols) != 2 || name == "" {
		t.Fatalf("索引兜底失败：cols=%v name=%q", cols, name)
	}

	// 字段/可空：都应回退 SHOW COLUMNS，且只查一次就能拿到整表
	colsMap := fetchCols(db, ds.Database, "meta_probe")
	if _, ok := colsMap["CRTER_ID"]; !ok {
		t.Fatal("字段存在性兜底失败：CRTER_ID 被判为不存在")
	}
	if _, ok := colsMap["NO_SUCH_COL"]; ok {
		t.Fatal("字段存在性误判：不存在的列被判为存在")
	}
	if len(colsMap) != 4 {
		t.Fatalf("列数兜底失败：得到 %d，期望 4", len(colsMap))
	}
	if !colsMap["MEMO"] {
		t.Fatalf("可空兜底失败：memo 应可空，得到 %v", colsMap["MEMO"])
	}
	if colsMap["CODE"] {
		t.Fatalf("可空兜底误判：code 不可空，得到 %v", colsMap["CODE"])
	}
}

// 元数据缓存：同一张表第二次调用不应再发查询
func TestMetaCacheHits(t *testing.T) {
	ds := dsFromEnv(t, "MYSQL_TEST_DSN")
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	defer metaDrop(db)

	if _, err := db.Exec("DROP TABLE IF EXISTS meta_cache_probe"); err != nil {
		t.Fatalf("准备表失败: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE meta_cache_probe (id INT NOT NULL PRIMARY KEY, memo VARCHAR(8) NULL)"); err != nil {
		t.Fatalf("准备表失败: %v", err)
	}
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS meta_cache_probe") }()

	oldQ := infQuery
	calls := 0
	infQuery = func(db *sql.DB, q string, args ...any) (*sql.Rows, error) {
		calls++
		return oldQ(db, q, args...)
	}
	defer func() { infQuery = oldQ }()

	for i := 0; i < 3; i++ {
		mHasColumn(db, ds.Database, "meta_cache_probe", "memo")
		mCols(db, ds.Database, "meta_cache_probe")
	}
	if calls != 1 {
		t.Fatalf("同一张表应只查一次元数据，实际发了 %d 次", calls)
	}
}
