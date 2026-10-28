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

	oldQ, oldQR := infQuery, infQueryRow
	infQuery = func(*sql.DB, string, ...any) (*sql.Rows, error) { return nil, errors.New("blocked by middleware") }
	infQueryRow = func(db *sql.DB, _ string, _ ...any) *sql.Row {
		return db.QueryRow("SELECT 1 WHERE 1=0") // 始终 0 行 → Scan 返回 sql.ErrNoRows
	}
	defer func() { infQuery, infQueryRow = oldQ, oldQR }()

	// 索引：应回退 SHOW INDEX，拿到主键列
	cols, name := keyCols(db, ds.Database, "meta_probe")
	if len(cols) != 2 || name == "" {
		t.Fatalf("索引兜底失败：cols=%v name=%q", cols, name)
	}

	// 字段存在性：应回退 SHOW COLUMNS
	if !tableHasColumn(db, ds.Database, "meta_probe", "CRTER_ID") {
		t.Fatal("字段存在性兜底失败：CRTER_ID 被判为不存在")
	}
	if tableHasColumn(db, ds.Database, "meta_probe", "NO_SUCH_COL") {
		t.Fatal("字段存在性误判：不存在的列被判为存在")
	}

	// 列数：应回退 SHOW COLUMNS
	if n := tableColumnCount(db, ds.Database, "meta_probe"); n != 4 {
		t.Fatalf("列数兜底失败：得到 %d，期望 4", n)
	}

	// 可空判断：应回退 SHOW COLUMNS
	if nullable, ok := colNullable(db, ds.Database, "meta_probe", "memo"); !ok || !nullable {
		t.Fatalf("可空兜底失败：nullable=%v ok=%v", nullable, ok)
	}
	if nullable, ok := colNullable(db, ds.Database, "meta_probe", "code"); !ok || nullable {
		t.Fatalf("可空兜底误判：code 不可空，得到 nullable=%v ok=%v", nullable, ok)
	}
}
