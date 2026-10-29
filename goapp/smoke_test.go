package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// 端到端冒烟测试：真连数据库，走「预检 → 执行 → 核对结果」整条链路。
// 只在设置 MYSQL_TEST_DSN 时运行，且**只能指向测试库**：
//   MYSQL_TEST_DSN='root:pass@tcp(127.0.0.1:3306)/medupdate_test' go test -run TestSmoke -v
// 表名固定，不能并发跑（各用例互相 DROP）。

func smokeDS(t *testing.T) DataSource {
	t.Helper()
	return dsFromEnv(t, "MYSQL_TEST_DSN")
}

func smokeSetup(t *testing.T, ds DataSource) {
	t.Helper()
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	for _, q := range []string{
		`DROP TABLE IF EXISTS smoke_t`,
		`CREATE TABLE smoke_t (id INT PRIMARY KEY, v VARCHAR(20), n INT) ENGINE=InnoDB`,
		`INSERT INTO smoke_t VALUES (1,'a',10),(2,'b',20),(3,'c',30)`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("准备数据失败（%s）: %v", q, err)
		}
	}
}

func smokeRow(t *testing.T, ds DataSource, id int) (string, int, bool) {
	t.Helper()
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	var v string
	var n int
	if err := db.QueryRow("SELECT v, n FROM smoke_t WHERE id=?", id).Scan(&v, &n); err != nil {
		return "", 0, false
	}
	return v, n, true
}

func smokeCount(t *testing.T, ds DataSource) int {
	t.Helper()
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM smoke_t").Scan(&n); err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	return n
}

/* ---------------- 执行层（事务语义） ---------------- */

func TestSmokeFullFlow(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)

	stmts := []string{
		`UPDATE smoke_t SET v='A' WHERE id=1`,
		`INSERT INTO smoke_t VALUES (4,'d',40)`,
		`DELETE FROM smoke_t WHERE id=3`,
	}

	rep, _, err := preflight(ds, stmts, "", nil)
	if err != nil {
		t.Fatalf("预检报错: %v", err)
	}
	for _, r := range rep {
		if fmt.Sprint(r["level"]) == "error" {
			t.Fatalf("干净脚本的预检不该有 error: %v — %v", r["title"], r["detail"])
		}
	}

	if _, failedAt, errMsg, _ := runStatements(ds, stmts, false); errMsg != "" {
		t.Fatalf("执行失败于第 %d 条: %s", failedAt+1, errMsg)
	}

	if v, _, ok := smokeRow(t, ds, 1); !ok || v != "A" {
		t.Fatalf("id=1 应更新为 A，实际 %q（存在=%v）", v, ok)
	}
	if v, n, ok := smokeRow(t, ds, 4); !ok || v != "d" || n != 40 {
		t.Fatalf("id=4 应插入成功，实际 %q/%d（存在=%v）", v, n, ok)
	}
	if _, _, ok := smokeRow(t, ds, 3); ok {
		t.Fatal("id=3 应被删除")
	}
	if got := smokeCount(t, ds); got != 3 {
		t.Fatalf("最终应有 3 行，实际 %d", got)
	}
}

func TestSmokeRollbackOnFailure(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)

	stmts := []string{
		`UPDATE smoke_t SET v='CHANGED' WHERE id=1`,
		`INSERT INTO smoke_t VALUES (2,'dup',0)`, // 主键冲突
	}
	_, failedAt, errMsg, _ := runStatements(ds, stmts, false)
	if errMsg == "" {
		t.Fatal("主键冲突应该报错")
	}
	if failedAt != 1 {
		t.Fatalf("应失败在第 2 条（索引 1），实际 %d", failedAt)
	}
	if v, _, _ := smokeRow(t, ds, 1); v != "a" {
		t.Fatalf("第 1 条的修改没被回滚，id=1 变成了 %q", v)
	}
	if got := smokeCount(t, ds); got != 3 {
		t.Fatalf("回滚后行数应仍为 3，实际 %d", got)
	}
}

func TestSmokeDryRun(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)

	stmts := []string{
		`UPDATE smoke_t SET v='DRY' WHERE id=1`,
		`DELETE FROM smoke_t WHERE id=2`,
	}
	if _, _, errMsg, _ := runStatements(ds, stmts, true); errMsg != "" {
		t.Fatalf("演练不该报错: %s", errMsg)
	}
	if v, _, _ := smokeRow(t, ds, 1); v != "a" {
		t.Fatalf("演练改了数据：id=1 = %q", v)
	}
	if got := smokeCount(t, ds); got != 3 {
		t.Fatalf("演练后行数应仍为 3，实际 %d", got)
	}
}

/* ---------------- HTTP 入口（执行前的防线） ---------------- */

// 把测试库配成一个可执行的数据源，返回脚本 ID
func smokeFixture(t *testing.T, ds DataSource, sql string) string {
	t.Helper()
	setupHome(t)
	st := Store{
		DataSources: []DataSource{{
			ID: "d1", Name: "测试库", Host: ds.Host, Port: ds.Port, User: ds.User,
			Pass: ds.Pass, Database: ds.Database, Env: "test",
		}},
		Groups:  []Group{{ID: "g1", DSID: "d1", Name: "冒烟"}},
		Scripts: []Script{{ID: "s1", GroupID: "g1", Name: "冒烟脚本", SQL: sql}},
	}
	if err := saveStore(st); err != nil {
		t.Fatalf("写配置失败: %v", err)
	}
	return "s1"
}

func smokeRunHTTP(t *testing.T, scriptID string) (int, string) {
	t.Helper()
	rec := call(t, http.MethodPost, "/api/run", map[string]any{"scriptId": scriptID})
	return rec.Code, rec.Body.String()
}

// 没有 WHERE 的 UPDATE：执行入口必须拦住（预检能关，这条不能靠预检兜底）
func TestSmokeUnsafeUpdateBlocked(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	id := smokeFixture(t, ds, `UPDATE smoke_t SET v='ALL'`)

	code, body := smokeRunHTTP(t, id)
	if code == http.StatusOK {
		t.Fatalf("整表更新被执行了：%s", body)
	}
	if !strings.Contains(body, "没有 WHERE") && !strings.Contains(body, "WHERE 1=1") {
		t.Logf("拦截提示：%s", body)
	}
	if v, _, _ := smokeRow(t, ds, 1); v != "a" {
		t.Fatal("整表更新没有被拦住，数据被改了")
	}
}

// 没有 WHERE 的 DELETE 同样要拦
func TestSmokeUnsafeDeleteBlocked(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	id := smokeFixture(t, ds, `DELETE FROM smoke_t`)

	if code, body := smokeRunHTTP(t, id); code == http.StatusOK {
		t.Fatalf("整表删除被执行了：%s", body)
	}
	if got := smokeCount(t, ds); got != 3 {
		t.Fatalf("数据被删了，剩余 %d 行", got)
	}
}

// DDL 必须被拒（隐式提交，回滚保不住）
func TestSmokeDDLBlocked(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	id := smokeFixture(t, ds, `ALTER TABLE smoke_t ADD COLUMN extra INT`)

	code, body := smokeRunHTTP(t, id)
	if code != http.StatusBadRequest {
		t.Fatalf("DDL 应返回 400，实际 %d：%s", code, body)
	}
	if !strings.Contains(body, "DDL") {
		t.Fatalf("拦截提示里应说明是 DDL：%s", body)
	}
	// 真去库里确认列没被加上
	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='smoke_t' AND COLUMN_NAME='extra'`).Scan(&n); err != nil {
		t.Fatalf("查列失败: %v", err)
	}
	if n != 0 {
		t.Fatal("extra 列被加上了，DDL 没被真正拦住")
	}
}

// 脚本里写 COMMIT 必须被拒
func TestSmokeExplicitCommitBlocked(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)
	id := smokeFixture(t, ds, "UPDATE smoke_t SET v='x' WHERE id=1;\nCOMMIT;")

	code, body := smokeRunHTTP(t, id)
	if code == http.StatusOK {
		t.Fatalf("带 COMMIT 的脚本被执行了：%s", body)
	}
	if v, _, _ := smokeRow(t, ds, 1); v != "a" {
		t.Fatalf("带 COMMIT 的脚本改了数据：%q", v)
	}
}

// 非 InnoDB 表的写入不进事务：预检必须报 error
func TestSmokeNonTransactionalEngine(t *testing.T) {
	ds := smokeDS(t)
	smokeSetup(t, ds)

	db, err := openDB(ds)
	if err != nil {
		t.Fatalf("连接失败: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`DROP TABLE IF EXISTS smoke_myisam`); err != nil {
		t.Fatalf("清表失败: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE smoke_myisam (id INT PRIMARY KEY) ENGINE=MyISAM`); err != nil {
		t.Skipf("这个版本不支持 MyISAM（%v），跳过", err)
	}
	defer db.Exec(`DROP TABLE IF EXISTS smoke_myisam`)

	if _, err := db.Exec(`INSERT INTO smoke_myisam VALUES (1)`); err != nil {
		t.Fatalf("准备数据失败: %v", err)
	}

	// INSERT / UPDATE / DELETE 三种写入都要被拦
	for _, q := range []string{
		`INSERT INTO smoke_myisam VALUES (2)`,
		`UPDATE smoke_myisam SET id=3 WHERE id=1`,
		`DELETE FROM smoke_myisam WHERE id=1`,
	} {
		rep, _, err := preflight(ds, []string{q}, "", nil)
		if err != nil {
			t.Fatalf("预检报错: %v", err)
		}
		if !hasEngineError(rep) {
			t.Fatalf("写入 MyISAM 表没被拦（回滚保不住）：%s → %v", q, dumpChecks(rep))
		}
	}

	// 反向：MyISAM 只作为关联表被读、真正写的是 InnoDB 表 → 不能误报
	rep, _, err := preflight(ds, []string{
		`UPDATE smoke_t s JOIN smoke_myisam m ON s.id=m.id SET s.v='joined'`,
	}, "", nil)
	if err != nil {
		t.Fatalf("预检报错: %v", err)
	}
	if hasEngineError(rep) {
		t.Fatalf("只读 MyISAM 被误判成目标表：%v", dumpChecks(rep))
	}
}

func hasEngineError(rep []map[string]any) bool {
	for _, r := range rep {
		if fmt.Sprint(r["level"]) == "error" && strings.Contains(fmt.Sprint(r["title"]), "不是事务引擎") {
			return true
		}
	}
	return false
}

func dumpChecks(rep []map[string]any) []string {
	var out []string
	for _, r := range rep {
		out = append(out, fmt.Sprint(r["level"])+":"+fmt.Sprint(r["title"]))
	}
	return out
}

/* ---------------- 安全闸：拒绝连生产库 ---------------- */

func TestMain(m *testing.M) {
	// 在跑任何用例之前钉住，避免用例之间改全局变量引起数据竞争
	restartSelfHook = func() {}
	// 这些用例会 DROP TABLE，必须确认连的是测试库：
	// 只看 DSN 里解析出来的库名，不能因为口令或主机名里带 test 就放行。
	if dsn := os.Getenv("MYSQL_TEST_DSN"); dsn != "" {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "拒绝运行：MYSQL_TEST_DSN 解析失败（%v）\n", err)
			os.Exit(1)
		}
		if !strings.HasSuffix(strings.ToLower(cfg.DBName), "_test") {
			fmt.Fprintf(os.Stderr, "拒绝运行：库名 %q 不以 _test 结尾，测试会 DROP TABLE\n", cfg.DBName)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}
