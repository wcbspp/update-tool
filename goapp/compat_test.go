package main

// 版本兼容矩阵测试：同一批 SQL 用例，跑在多个 MySQL 版本上，逐条比对预检结论。
//
// 用法：
//   MYSQL_COMPAT_DSNS="8.4=root:pw@tcp(127.0.0.1:3306)/medupdate_test,5.7=root:pw@tcp(127.0.0.1:3357)/medupdate_test" \
//   go test -run TestSQLCompatMatrix -v
//
// 没有配置 DSN 时跳过，不影响常规 go test。

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
)

type compatCase struct {
	name string
	sql  string
	// 期望：这条语句在预检里必须出现的标题（任一命中即可）
	wantTitles []string
	// 期望：是否允许出现 error 级别（默认不允许）
	allowError bool
	// 只有这些版本才允许/要求跑（空表示所有版本）
	minMajor int
	minMinor int
}

var compatCases = []compatCase{
	{
		name:       "INSERT VALUES",
		sql:        `INSERT INTO compat_tgt (code, area) VALUES ('B1', '000000')`,
		wantTitles: []string{"目标表", "预计影响行数"},
	},
	{
		name:       "INSERT 省略 INTO",
		sql:        `INSERT compat_tgt (code, area) VALUES ('A2', '000000')`,
		wantTitles: []string{"目标表"},
	},
	{
		name:       "INSERT SET",
		sql:        `INSERT INTO compat_tgt SET code='A3', area='000000'`,
		wantTitles: []string{"目标表", "预计影响行数"},
	},
	{
		name:       "INSERT SET 值带逗号",
		sql:        `INSERT INTO compat_tgt SET code='A4', area='000000', memo='x,y'`,
		wantTitles: []string{"目标表", "无主键冲突"},
	},
	{
		name:       "INSERT VALUE 单数",
		sql:        `INSERT INTO compat_tgt VALUE ('A5', '000000', NULL)`,
		wantTitles: []string{"目标表"},
	},
	{
		name:       "INSERT IGNORE 冲突不报错",
		sql:        `INSERT IGNORE INTO compat_tgt (code, area) VALUES ('A1', '000000')`,
		wantTitles: []string{"主键已存在"},
	},
	{
		name:       "INSERT ODKU 冲突不报错",
		sql:        `INSERT INTO compat_tgt (code, area) VALUES ('A1', '000000') ON DUPLICATE KEY UPDATE area='000000'`,
		wantTitles: []string{"主键已存在"},
	},
	{
		name:       "INSERT 字符串里的 on duplicate 不能软化冲突",
		sql:        `INSERT INTO compat_tgt (code, area, memo) VALUES ('A1', '000000', 'on duplicate key update x')`,
		wantTitles: []string{"主键冲突"},
		allowError: true,
	},
	{
		name:       "REPLACE INTO",
		sql:        `REPLACE INTO compat_tgt (code, area) VALUES ('A1', '000000')`,
		wantTitles: []string{"目标表"},
	},
	{
		name:       "UPDATE 单表",
		sql:        `UPDATE compat_tgt SET area='000000' WHERE code='A1'`,
		wantTitles: []string{"被更新表", "预计影响行数"},
	},
	{
		name:       "UPDATE 多表 JOIN",
		sql:        `UPDATE compat_tgt a JOIN compat_upd b ON a.code=b.code SET a.area='000000' WHERE b.code='A1'`,
		wantTitles: []string{"被更新表", "数据来源表"},
	},
	{
		name:       "UPDATE STRAIGHT_JOIN",
		sql:        `UPDATE compat_upd b STRAIGHT_JOIN compat_tgt a ON a.code=b.old_code SET a.area='000000' WHERE b.code='A1'`,
		wantTitles: []string{"被更新表", "数据来源表"},
	},
	{
		name:       "UPDATE ON 条件里含 'set' 字符串",
		sql:        `UPDATE compat_tgt a JOIN compat_upd b ON a.code=b.code AND b.memo='a set b' SET a.area='000000' WHERE a.code='A1'`,
		wantTitles: []string{"被更新表"},
	},
	{
		name:       "UPDATE LIMIT 不算全表更新",
		sql:        `UPDATE compat_tgt SET area='000000' ORDER BY code LIMIT 5`,
		wantTitles: []string{"没有 WHERE 条件"},
	},
	{
		name:       "UPDATE 子查询派生表",
		sql:        `UPDATE compat_tgt a JOIN (SELECT code FROM compat_upd GROUP BY code) b ON a.code=b.code SET a.area='000000' WHERE a.code='A1'`,
		wantTitles: []string{"被更新表"},
	},
	{
		name:       "DELETE 带 WHERE",
		sql:        `DELETE FROM compat_tgt WHERE code='A1'`,
		wantTitles: []string{"被删除表", "预计影响行数"},
	},
	{
		name:       "DELETE 无 WHERE 必须拦",
		sql:        `DELETE FROM compat_tgt`,
		wantTitles: []string{"没有 WHERE 条件"},
		allowError: true,
	},
	{
		name:       "DELETE 带 LOW_PRIORITY 修饰也必须拦",
		sql:        `DELETE LOW_PRIORITY FROM compat_tgt`,
		wantTitles: []string{"没有 WHERE 条件"},
		allowError: true,
	},
	{
		name:       "DELETE 多表",
		sql:        `DELETE a FROM compat_tgt a JOIN compat_upd b ON a.code=b.code WHERE b.code='A1'`,
		wantTitles: []string{"被删除表"},
	},
	{
		name:       "DELETE USING",
		sql:        `DELETE FROM compat_tgt USING compat_tgt JOIN compat_upd ON compat_tgt.code=compat_upd.code WHERE compat_upd.code='A1'`,
		wantTitles: []string{"被删除表"},
	},
	{
		name:       "CTE UPDATE（8.0+）",
		sql:        `WITH w AS (SELECT 'A1' AS c) UPDATE compat_tgt a JOIN w ON a.code=w.c SET a.area='000000' WHERE a.code='A1'`,
		wantTitles: []string{"被更新表"},
		minMajor:   8, minMinor: 0,
	},
}

func compatSetup(db *sql.DB) error {
	stmts := []string{
		`DROP TABLE IF EXISTS compat_tgt`,
		`DROP TABLE IF EXISTS compat_upd`,
		`CREATE TABLE compat_tgt (
			code varchar(32) NOT NULL,
			area varchar(16) NOT NULL,
			memo varchar(200) NULL,
			PRIMARY KEY (code, area)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`CREATE TABLE compat_upd (
			code varchar(32) NOT NULL,
			old_code varchar(32) NULL,
			memo varchar(200) NULL,
			PRIMARY KEY (code)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		`INSERT INTO compat_tgt VALUES ('A1','000000',NULL)`,
		`INSERT INTO compat_upd VALUES ('A1','A1','m')`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("%s: %v", q, err)
		}
	}
	return nil
}

func TestSQLCompatMatrix(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("MYSQL_COMPAT_DSNS"))
	if raw == "" {
		t.Skip("未配置 MYSQL_COMPAT_DSNS")
	}
	type target struct {
		name string
		dsn  string
	}
	var targets []target
	for _, item := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(kv) != 2 {
			continue
		}
		targets = append(targets, target{kv[0], kv[1]})
	}

	for _, tg := range targets {
		tg := tg
		t.Run(tg.name, func(t *testing.T) {
			db, err := sql.Open("mysql", tg.dsn)
			if err != nil {
				t.Fatalf("连不上 %s: %v", tg.name, err)
			}
			defer db.Close()
			var rawVer string
			if err := db.QueryRow("SELECT VERSION()").Scan(&rawVer); err != nil {
				t.Fatalf("%s 取版本失败: %v", tg.name, err)
			}
			sv := parseServerVer(rawVer)
			if err := compatSetup(db); err != nil {
				t.Fatalf("%s 建表失败: %v", tg.name, err)
			}
			// DataSource 从 DSN 里解析
			ds := dsFromDSN(tg.dsn)

			pass, fail, skipped := 0, 0, 0
			for _, c := range compatCases {
				if c.minMajor > 0 && !sv.atLeast(c.minMajor, c.minMinor, 0) {
					skipped++
					continue
				}
				rep, _, err := preflight(ds, []string{c.sql}, "", nil)
				if err != nil {
					fail++
					t.Errorf("[%s] %s 预检直接报错: %v", tg.name, c.name, err)
					continue
				}
				var got []string
				hasErr := false
				for _, r := range rep {
					got = append(got, fmt.Sprint(r["title"]))
					if r["level"] == "error" {
						hasErr = true
					}
				}
				hit := false
				for _, w := range c.wantTitles {
					for _, g := range got {
						if g == w {
							hit = true
						}
					}
				}
				if !hit {
					fail++
					t.Errorf("[%s] %s：期望包含 %v，实际 %v", tg.name, c.name, c.wantTitles, got)
					continue
				}
				if hasErr && !c.allowError {
					fail++
					t.Errorf("[%s] %s：不该出现 error，实际 %v", tg.name, c.name, got)
					continue
				}
				if !hasErr && c.allowError && c.name == "DELETE 无 WHERE 必须拦" {
					fail++
					t.Errorf("[%s] %s：必须给 error 拦住", tg.name, c.name)
					continue
				}
				pass++
			}
			t.Logf("%-8s %-28s 通过 %d / 失败 %d / 跳过 %d", tg.name, rawVer, pass, fail, skipped)
		})
	}
}

// 从 DSN 还原 DataSource 字段（只支持测试用的简写格式）
func dsFromDSN(dsn string) DataSource {
	ds := DataSource{Port: 3306}
	rest := dsn
	if i := strings.LastIndex(rest, ")/"); i >= 0 {
		ds.Database = rest[i+2:]
		rest = rest[:i+1]
	}
	if i := strings.Index(rest, "@tcp("); i >= 0 {
		cred := rest[:i]
		hostport := strings.TrimSuffix(rest[i+5:], ")")
		if j := strings.Index(cred, ":"); j >= 0 {
			ds.User, ds.Pass = cred[:j], cred[j+1:]
		}
		if j := strings.LastIndex(hostport, ":"); j >= 0 {
			ds.Host = hostport[:j]
			fmt.Sscanf(hostport[j+1:], "%d", &ds.Port)
		}
	}
	ds.Env = "test"
	return ds
}
