package main

// SQL 执行台 —— 单文件、零运行时依赖的 Go 版
// 数据源 / 分组 / 脚本，校验（EXPLAIN）、预演（事务回滚）、执行

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

//go:embed index.html guide.html
var assets embed.FS

const maxRows = 500

// 编译时注入：-X main.buildTime=20261028-0245
var buildTime = "dev"

/* ---------------- 数据 ---------------- */

type DataSource struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Pass     string `json:"password"`
	Database string `json:"database"`
	Open     bool   `json:"open"`
	VerCol   string `json:"versionCol"`
	VerPfxs  string `json:"versionPrefixes"`
	Env      string `json:"env"` // prod | test
	// 超时配置（0 表示用默认值）
	ConnSec int `json:"connSec"` // 连接超时，秒，默认 10
	DQLSec  int `json:"dqlSec"`  // DQL（查询/统计）超时，秒，默认 60
	DMLSec  int `json:"dmlSec"`  // DML（执行写入）超时，秒，默认 1800；0 表示不限
	// 兼容旧字段
	IOSec  int `json:"ioSec,omitempty"`
	SelSec int `json:"selSec,omitempty"`
	SelMs  int `json:"selMs,omitempty"`
}

func dsConnSec(d DataSource) int {
	if d.ConnSec > 0 {
		return d.ConnSec
	}
	return 10
}

func dsDQLSec(d DataSource) int {
	if d.DQLSec > 0 {
		return d.DQLSec
	}
	if d.SelSec > 0 {
		return d.SelSec
	}
	if d.SelMs > 0 {
		return d.SelMs / 1000
	}
	return 60
}

func dsDMLSec(d DataSource) int {
	if d.DMLSec > 0 {
		return d.DMLSec
	}
	if d.DMLSec < 0 {
		return 0 // -1 = 不限
	}
	if d.IOSec > 0 { // 旧配置里它是执行阶段的实际上限
		return d.IOSec
	}
	return 1800
}

// 网络层超时按两者最大值再留 60 秒余量，用户不需要单独想它
func dsIOSec(d DataSource) int {
	io := dsDQLSec(d)
	v := dsDMLSec(d)
	if v == 0 { // DML 不限：网络层给 2 小时，避免把“不限”变成 2 分钟
		return 7200
	}
	if v > io {
		io = v
	}
	return io + 60
}

type Group struct {
	ID       string `json:"id"`
	DSID     string `json:"dsId"`     // 仅顶层文件夹有效
	ParentID string `json:"parentId"` // 空串 = 顶层文件夹
	Name     string `json:"name"`
	Open     bool   `json:"open"`
	Order    int    `json:"order"`

	// 以下仅顶层文件夹有效，子文件夹一律继承
	Preflight *bool  `json:"preflight"`
	VerCol    string `json:"versionCol"`
	VerPfxs   string `json:"versionPrefixes"`
}

type Script struct {
	ID      string `json:"id"`
	GroupID string `json:"groupId"` // 空串 = 根级单层脚本
	DSID    string `json:"dsId"`    // 仅根级脚本有效
	Name    string `json:"name"`
	SQL     string `json:"sql"`
	Order   int    `json:"order"`

	// 仅根级单层脚本有效
	Preflight *bool  `json:"preflight"`
	VerCol    string `json:"versionCol"`
	VerPfxs   string `json:"versionPrefixes"`
}

type Settings struct {
	Preflight *bool  `json:"preflight"` // nil = 默认开启（供根级脚本/new 文件夹使用）
	VerCol    string `json:"versionCol"`
	VerPfxs   string `json:"versionPrefixes"`
	UpdateURL string `json:"updateUrl"` // 升级源地址，例如 https://example.com/medupdate-xxx/
}

type Store struct {
	DataSources []DataSource `json:"datasources"`
	Groups      []Group      `json:"groups"`
	Scripts     []Script     `json:"scripts"`
	Settings    Settings     `json:"settings"`
}

func preflightEnabled(s Store) bool {
	return s.Settings.Preflight == nil || *s.Settings.Preflight
}

func defaultVerCol(s Store) string {
	if strings.TrimSpace(s.Settings.VerCol) != "" {
		return strings.TrimSpace(s.Settings.VerCol)
	}
	return "CRTER_ID"
}

func splitPrefixes(raw string) []string {
	out := make([]string, 0)
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		out = []string{"NV", "LC", "LT", "LM"}
	}
	return out
}

func defaultVerPfxs(s Store) []string {
	raw := strings.TrimSpace(s.Settings.VerPfxs)
	if raw == "" {
		raw = "NV,LC,LT,LM"
	}
	out := make([]string, 0)
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOf(d DataSource) string {
	if strings.TrimSpace(d.Env) == "" {
		return "prod"
	}
	return d.Env
}

func nextGroupOrder(s Store, parent string) int {
	max := 0
	for _, g := range s.Groups {
		if g.ParentID == parent && g.Order > max {
			max = g.Order
		}
	}
	return max + 1
}

func nextScriptOrder(s Store, gid string) int {
	max := 0
	for _, sc := range s.Scripts {
		if sc.GroupID == gid && sc.Order > max {
			max = sc.Order
		}
	}
	return max + 1
}

// 端口字段前后端可能传数字或字符串，统一收下
type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), "\"")
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*f = flexInt(n)
	return nil
}

// 原始报错 + 中文解释，两条都留着
func explainDBError(msg string) string {
	low := strings.ToLower(msg)
	switch {
	case strings.Contains(msg, "1130"):
		return "来源 IP 未被允许。请让 DBA 把这台机器的 IP 加进白名单；走 ZeroTier 时来源是 127.0.0.1"
	case strings.Contains(msg, "1045"):
		return "账号或口令不对。核对用户名、密码，并确认该账号允许从当前来源 IP 登录"
	case strings.Contains(msg, "2003") || strings.Contains(low, "connection refused") || strings.Contains(low, "no route to host"):
		return "连不上数据库。网络不通或端口没放开，检查 IP、端口、防火墙"
	case strings.Contains(msg, "1049"):
		return "库名不存在。检查拼写，或把库名留空后点「列出库 / 测试」选择"
	case strings.Contains(msg, "1044"):
		return "该账号没有这个库的权限，换账号或让 DBA 授权"
	case strings.Contains(msg, "1146"):
		return "表不存在。脚本里的表名写错了，或者连错了库"
	case strings.Contains(msg, "1054"):
		return "字段不存在。脚本里的列名写错了"
	case strings.Contains(msg, "1064"):
		return "SQL 语法错误。常见原因是少分号或关键字拼错，看报错里 near 后面那段"
	case strings.Contains(msg, "1062"):
		return "主键重复。这批数据之前已经插过，确认是不是重复执行"
	case strings.Contains(low, "dial tcp") && strings.Contains(low, "timeout"):
		return "连不上数据库。网络不通，或目标库做了 IP 白名单限制（生产环境常见）"
	case strings.Contains(low, "timeout"):
		return "连接超时。网络慢或被中间设备拦截"
	}
	return ""
}

func withHint(msg string) string {
	if h := explainDBError(msg); h != "" {
		return msg + "\n→ " + h
	}
	return msg
}

func dsByID(s Store, id string) (DataSource, bool) {
	for _, d := range s.DataSources {
		if d.ID == id {
			return d, true
		}
	}
	return DataSource{}, false
}

// 沿 parentId 一路向上，找到顶层文件夹
func topFolder(s Store, groupID string) (Group, bool) {
	cur, ok := Group{}, false
	for _, g := range s.Groups {
		if g.ID == groupID {
			cur, ok = g, true
			break
		}
	}
	if !ok {
		return Group{}, false
	}
	seen := map[string]bool{cur.ID: true}
	for cur.ParentID != "" {
		if seen[cur.ParentID] {
			break // 数据里有环，停在这里，别把请求卡死
		}
		seen[cur.ParentID] = true
		var parent Group
		found := false
		for _, g := range s.Groups {
			if g.ID == cur.ParentID {
				parent, found = g, true
				break
			}
		}
		if !found {
			break
		}
		cur = parent
	}
	return cur, true
}

// 脚本最终生效的：数据源 + 是否体检 + 版本字段 + 版本前缀
func effectiveFor(s Store, sc Script) (DataSource, bool, bool, string, []string) {
	pre := preflightEnabled(s)
	col := defaultVerCol(s)
	pfxs := defaultVerPfxs(s)

	if sc.GroupID != "" {
		if top, ok := topFolder(s, sc.GroupID); ok {
			if top.Preflight != nil {
				pre = *top.Preflight
			}
			if strings.TrimSpace(top.VerCol) != "" {
				col = strings.TrimSpace(top.VerCol)
			}
			if strings.TrimSpace(top.VerPfxs) != "" {
				pfxs = defaultVerPfxs(Store{Settings: Settings{VerPfxs: top.VerPfxs}})
			}
			if sc.Preflight != nil {
				pre = *sc.Preflight
			}
			if strings.TrimSpace(sc.VerCol) != "" {
				col = strings.TrimSpace(sc.VerCol)
			}
			if strings.TrimSpace(sc.VerPfxs) != "" {
				pfxs = splitPrefixes(sc.VerPfxs)
			}
			if d, ok2 := dsByID(s, top.DSID); ok2 {
				return d, true, pre, col, pfxs
			}
		}
		return DataSource{}, false, pre, col, pfxs
	}

	if sc.Preflight != nil {
		pre = *sc.Preflight
	}
	if strings.TrimSpace(sc.VerCol) != "" {
		col = strings.TrimSpace(sc.VerCol)
	}
	if strings.TrimSpace(sc.VerPfxs) != "" {
		pfxs = splitPrefixes(sc.VerPfxs)
	}
	if d, ok := dsByID(s, sc.DSID); ok {
		return d, true, pre, col, pfxs
	}
	return DataSource{}, false, pre, col, pfxs
}

var (
	mu       sync.Mutex
	appHome  string
	dataFile string
	logsDir  string
)

var storeCorrupt bool

func loadStore() Store {
	var s Store
	b, err := os.ReadFile(dataFile)
	if err != nil {
		storeCorrupt = false
		return s
	}
	if err := json.Unmarshal(b, &s); err != nil {
		// 文件存在但解析失败：标记为损坏，禁止后续写入覆盖
		storeCorrupt = true
		log.Printf("data.json 解析失败（%v），已进入只读保护，避免覆盖原文件", err)
		return Store{}
	}
	storeCorrupt = false
	return s
}

func saveStore(s Store) error {
	if storeCorrupt {
		return errors.New("data.json 已损坏，已拒绝写入以免覆盖原文件；请先修复或恢复备份")
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := dataFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dataFile)
}

func newID() string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}

/* ---------------- SQL 拆分 ---------------- */

// 语句类型分派：预检按这个决定走哪套检查
func stmtKind(st string) string {
	up := strings.ToUpper(strings.TrimSpace(st))
	switch {
	case strings.HasPrefix(up, "INSERT"):
		return "insert"
	case strings.HasPrefix(up, "UPDATE"):
		return "update"
	case strings.HasPrefix(up, "DELETE"):
		return "delete"
	}
	if isDDL(st) {
		return "ddl"
	}
	if isQuery(st) {
		return "query"
	}
	return "other"
}

func splitStatements(text string) []string {
	var out []string
	var buf strings.Builder
	inS, inD, inBt, inLine, inBlock := false, false, false, false, false
	rs := []rune(text)
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		var n rune
		if i+1 < len(rs) {
			n = rs[i+1]
		}
		if inLine {
			if c == '\n' {
				inLine = false
				buf.WriteRune(c)
			}
			continue
		}
		if inBlock {
			if c == '*' && n == '/' {
				inBlock = false
				i++
			}
			continue
		}
		if inS {
			if c == '\'' && n == '\'' {
				buf.WriteString("''")
				i++
				continue
			}
			if c == '\'' {
				inS = false
			}
			buf.WriteRune(c)
			continue
		}
		if inD {
			if c == '"' {
				inD = false
			}
			buf.WriteRune(c)
			continue
		}
		if inBt {
			if c == '`' {
				inBt = false
			}
			buf.WriteRune(c)
			continue
		}
		switch {
		case c == '-' && n == '-':
			inLine = true
			i++
			buf.WriteRune('\n')
		case c == '/' && n == '*':
			inBlock = true
			i++
		case c == '\'':
			inS = true
			buf.WriteRune(c)
		case c == '"':
			inD = true
			buf.WriteRune(c)
		case c == '`':
			inBt = true
			buf.WriteRune(c)
		case c == ';':
			if s := strings.TrimSpace(buf.String()); s != "" {
				out = append(out, s)
			}
			buf.Reset()
		default:
			buf.WriteRune(c)
		}
	}
	if s := strings.TrimSpace(buf.String()); s != "" {
		out = append(out, s)
	}
	return out
}

func isDDL(s string) bool {
	t := strings.ToUpper(strings.TrimSpace(s))
	for _, p := range []string{"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "GRANT", "REVOKE"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

func isQuery(s string) bool {
	t := strings.ToUpper(strings.TrimSpace(s))
	for _, p := range []string{"SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "WITH", "TABLE"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

func isTxnControl(s string) bool {
	t := strings.ToUpper(strings.TrimSpace(s))
	for _, p := range []string{"COMMIT", "ROLLBACK", "START TRANSACTION", "BEGIN", "SAVEPOINT", "SET AUTOCOMMIT"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

func idxList(idx []int) string {
	parts := make([]string, len(idx))
	for i, v := range idx {
		parts[i] = strconv.Itoa(v + 1)
	}
	return strings.Join(parts, "、")
}

// 按字符截断，超出补省略号
func clipRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func preview(s string) string {
	return clipRunes(strings.TrimSpace(s), 4000)
}

/* ---------------- 数据库 ---------------- */

func openDB(ds DataSource) (*sql.DB, error) {
	cfg := mysql.NewConfig()
	cfg.User = ds.User
	cfg.Passwd = ds.Pass
	cfg.Net = "tcp"
	port := ds.Port
	if port == 0 {
		port = 3306
	}
	cfg.Addr = net.JoinHostPort(ds.Host, strconv.Itoa(port))
	cfg.DBName = ds.Database
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	cfg.AllowNativePasswords = true
	cfg.Timeout = time.Duration(dsConnSec(ds)) * time.Second
	cfg.ReadTimeout = time.Duration(dsIOSec(ds)) * time.Second
	cfg.WriteTimeout = time.Duration(dsIOSec(ds)) * time.Second

	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// SELECT 超时由服务器中断（只影响 SELECT，不影响 DML）；执行事务时会被改成 0
	if _, err := db.Exec(fmt.Sprintf("SET SESSION max_execution_time = %d", dsDQLSec(ds)*1000)); err != nil {
		// 5.7.8 以下或代理不支持，忽略
		_ = err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

const appVersion = "1.0.6"

var runMu sync.Mutex

type cell = any

func scanRows(rows *sql.Rows) ([]string, [][]cell, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	out := [][]cell{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		line := make([]cell, len(cols))
		for i, v := range vals {
			switch t := v.(type) {
			case nil:
				line[i] = nil
			case []byte:
				line[i] = string(t)
			default:
				line[i] = fmt.Sprintf("%v", t)
			}
		}
		out = append(out, line)
		if len(out) >= maxRows {
			break
		}
	}
	return cols, out, rows.Err()
}

type stmtResult struct {
	Affected int64    `json:"affected"`
	Info     string   `json:"info,omitempty"`
	Columns  []string `json:"columns,omitempty"`
	Rows     [][]cell `json:"rows"`
	MS       int64    `json:"ms"`
}

func runStatements(ds DataSource, stmts []string, dry bool) (map[string]stmtResult, int, string, int64) {
	start := time.Now()
	res := map[string]stmtResult{}
	db, err := openDB(ds)
	if err != nil {
		return res, -1, err.Error(), time.Since(start).Milliseconds()
	}
	defer db.Close()
	// 执行阶段取消 DQL 的统计上限，DML 用 context 单独限时
	_, _ = db.Exec("SET SESSION max_execution_time = 0")

	ctx := context.Background()
	if sec := dsDMLSec(ds); sec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(sec)*time.Second)
		defer cancel()
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return res, -1, err.Error(), time.Since(start).Milliseconds()
	}

	fail := func(i int, e error) (map[string]stmtResult, int, string, int64) {
		_ = tx.Rollback()
		return res, i, withHint(e.Error()), time.Since(start).Milliseconds()
	}

	for i, st := range stmts {
		t0 := time.Now()
		if isQuery(st) {
			rows, err := tx.QueryContext(ctx, st)
			if err != nil {
				return fail(i, err)
			}
			cols, data, err := scanRows(rows)
			rows.Close()
			if err != nil {
				return fail(i, err)
			}
			if data == nil {
				data = [][]cell{}
			}
			res[strconv.Itoa(i)] = stmtResult{Columns: cols, Rows: data, MS: time.Since(t0).Milliseconds()}
		} else {
			r, err := tx.ExecContext(ctx, st)
			if err != nil {
				return fail(i, err)
			}
			n, _ := r.RowsAffected()
			res[strconv.Itoa(i)] = stmtResult{Affected: n, MS: time.Since(t0).Milliseconds()}
		}
	}

	if dry {
		_ = tx.Rollback()
	} else if err := tx.Commit(); err != nil {
		_ = tx.Rollback()
		return res, -1, err.Error(), time.Since(start).Milliseconds()
	}
	return res, -1, "", time.Since(start).Milliseconds()
}

func checkStatements(ds DataSource, stmts []string) ([]map[string]any, bool) {
	db, err := openDB(ds)
	if err != nil {
		return []map[string]any{{"i": 0, "ok": false, "error": withHint(err.Error())}}, false
	}
	defer db.Close()

	results := make([]map[string]any, 0, len(stmts))
	allOK := true
	for i, st := range stmts {
		if isTxnControl(st) {
			results = append(results, map[string]any{"i": i, "ok": true, "skipped": true, "note": "事务语句，已跳过"})
			continue
		}
		if isDDL(st) {
			results = append(results, map[string]any{"i": i, "ok": true, "skipped": true, "note": "DDL 无法静态校验"})
			continue
		}
		rows, err := db.Query("EXPLAIN " + st)
		if err != nil {
			allOK = false
			results = append(results, map[string]any{"i": i, "ok": false, "error": withHint(err.Error())})
			continue
		}
		rows.Close()
		results = append(results, map[string]any{"i": i, "ok": true})
	}
	return results, allOK
}

/* ---------------- 体检（DML 预检） ---------------- */

var (
	reInsert = regexp.MustCompile(`(?is)^\s*insert\s+(?:ignore\s+)?into\s+[\x60"]?([\w$.]+)[\x60"]?\s*(?:\(([^)]*)\))?\s*(select\b.*)$`)
	reFrom   = regexp.MustCompile(`(?is)\bfrom\s+[\x60"]?([\w$.]+)[\x60"]?`)
	reUpdate = regexp.MustCompile(`(?is)^\s*update\s+(.+?)\s+set\s+(.+)$`)
	reDelete = regexp.MustCompile(`(?is)^\s*delete\s+from\s+[\x60"]?([\w$.]+)`)
	reIdent  = regexp.MustCompile(`^[\x60"]?([\w$.]+)[\x60"]?`)
	reBatch  = regexp.MustCompile(`(?i)([A-Za-z0-9_]+)_update_([A-Za-z0-9]+)`)
)

// 5.6 / Linux 下表名区分大小写，先按不区分大小写解析出真实表名
func resolveTable(db *sql.DB, schema, name string) string {
	var actual string
	err := db.QueryRow(`SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) LIMIT 1`, schema, name).Scan(&actual)
	if err == nil && actual != "" {
		return actual
	}
	return name
}

func normTable(n string) string {
	n = strings.TrimSpace(n)
	n = strings.Trim(n, "`\"")
	if i := strings.LastIndex(n, "."); i >= 0 {
		n = strings.Trim(n[i+1:], "`\"")
	}
	return n
}
func quoteIdent(n string) string { return "`" + strings.ReplaceAll(n, "`", "``") + "`" }

func splitTopLevel(s string) []string {
	var out []string
	depth, last := 0, 0
	for i, c := range s {
		switch c {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, s[last:i])
				last = i + 1
			}
		}
	}
	out = append(out, s[last:])
	return out
}

func cutAtKeyword(s, kw string) string {
	low := strings.ToLower(s)
	depth := 0
	for i := 0; i < len(low); i++ {
		switch low[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && strings.HasPrefix(low[i:], " "+kw) {
			return s[:i]
		}
	}
	return s
}

func indexTopLevel(s string, target byte) int {
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && s[i] == target {
				return i
			}
		}
	}
	return -1
}

func parseTableRef(p string) (string, string) {
	p = strings.TrimSpace(p)
	fs := strings.Fields(p)
	if len(fs) == 0 {
		return "", ""
	}
	table := normTable(fs[0])
	alias := ""
	if len(fs) >= 3 && strings.EqualFold(fs[1], "as") {
		alias = trimIdent(fs[2])
	} else if len(fs) >= 2 {
		second := strings.ToLower(strings.Trim(fs[1], "`\""))
		switch second {
		case "join", "left", "right", "inner", "outer", "on", "use", "force", "ignore", "straight_join":
		default:
			alias = trimIdent(fs[1])
		}
	}
	if alias == "" {
		alias = table
	}
	return table, alias
}

type tblRef struct{ table, alias string }

// 反查某列属于哪些参与表：SET 没写别名时用它消歧，而不是猜
func tablesHavingCol(db *sql.DB, schema string, refs []tblRef, col string) []tblRef {
	out := make([]tblRef, 0)
	for _, r := range refs {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
			WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, schema, r.table, col).Scan(&n)
		if n > 0 {
			out = append(out, r)
		}
	}
	return out
}

// 表行数的近似值，来自 information_schema，不扫表

// 用执行计划估算影响行数：不执行查询，只看 EXPLAIN 的 rows
func estimateByExplain(db *sql.DB, st, target, alias string) (int64, bool) {
	rows, err := db.Query("EXPLAIN " + st)
	if err != nil {
		return 0, false
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return 0, false
	}
	idxTable, idxRows := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "table":
			idxTable = i
		case "rows":
			idxRows = i
		}
	}
	if idxRows < 0 {
		return 0, false
	}
	var bestMatch, bestAny int64
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows.Scan(ptrs...) != nil {
			continue
		}
		get := func(i int) string {
			if i < 0 || i >= len(vals) {
				return ""
			}
			switch v := vals[i].(type) {
			case []byte:
				return string(v)
			case nil:
				return ""
			default:
				return fmt.Sprintf("%v", v)
			}
		}
		n, err := strconv.ParseInt(strings.TrimSpace(get(idxRows)), 10, 64)
		if err != nil {
			continue
		}
		if n > bestAny {
			bestAny = n
		}
		tbl := ""
		if idxTable >= 0 {
			tbl = get(idxTable)
		}
		if strings.EqualFold(tbl, target) || (alias != "" && strings.EqualFold(tbl, alias)) {
			if n > bestMatch {
				bestMatch = n
			}
		}
	}
	if bestMatch > 0 {
		return bestMatch, true
	}
	if bestAny > 0 { // 别名也对不上时，退一步取最大扫描量
		return bestAny, true
	}
	return 0, false
}

func tableCount(db *sql.DB, t string) int64 {
	var n int64
	_ = db.QueryRow("SELECT COUNT(*) FROM " + quoteIdent(t)).Scan(&n)
	return n
}

// 某列是否存在于表里
func tableHasColumn(db *sql.DB, schema, table, col string) bool {
	var n int
	_ = db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND UPPER(COLUMN_NAME)=UPPER(?)`,
		schema, table, col).Scan(&n)
	return n > 0
}

// 列是否允许 NULL
func colNullable(db *sql.DB, schema, table, col string) (bool, bool) {
	var v string
	err := db.QueryRow(`SELECT IS_NULLABLE FROM information_schema.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND UPPER(COLUMN_NAME)=UPPER(?)`,
		schema, table, col).Scan(&v)
	if err != nil {
		return false, false
	}
	return strings.EqualFold(v, "YES"), true
}

func columnCount(db *sql.DB, schema, t string) int {
	var n int
	_ = db.QueryRow("SELECT COUNT(*) FROM information_schema.COLUMNS WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)", schema, t).Scan(&n)
	return n
}

// 唯一键：优先主键；没有主键就用“所有列都 NOT NULL”的唯一索引（唯一索引允许多个 NULL，不能用来判重）
func keyCols(db *sql.DB, schema, t string) ([]string, string) {
	rows, err := db.Query(`SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
		FROM information_schema.STATISTICS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND NON_UNIQUE=0
		ORDER BY (INDEX_NAME='PRIMARY') DESC, INDEX_NAME, SEQ_IN_INDEX`, schema, t)
	if err != nil {
		return nil, ""
	}
	defer rows.Close()

	type idx struct {
		name string
		cols []string
	}
	var list []idx
	for rows.Next() {
		var name, col string
		var seq int
		if rows.Scan(&name, &col, &seq) != nil {
			continue
		}
		if len(list) == 0 || list[len(list)-1].name != name {
			list = append(list, idx{name: name})
		}
		list[len(list)-1].cols = append(list[len(list)-1].cols, col)
	}

	for _, it := range list {
		if strings.EqualFold(it.name, "PRIMARY") {
			return it.cols, it.name
		}
	}
	for _, it := range list {
		ok := true
		for _, c := range it.cols {
			var nullable string
			_ = db.QueryRow(`SELECT IS_NULLABLE FROM information_schema.COLUMNS
				WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND UPPER(COLUMN_NAME)=UPPER(?)`,
				schema, t, c).Scan(&nullable)
			if !strings.EqualFold(nullable, "NO") {
				ok = false
				break
			}
		}
		if ok {
			return it.cols, it.name
		}
	}
	if len(list) > 0 {
		return list[0].cols, list[0].name
	}
	// 有些中间件对 information_schema 有限制，退回 SHOW INDEX
	return keyColsViaShowIndex(db, schema, t)
}

// 兜底：SHOW INDEX 直接读表，兼容性比 information_schema 好
func keyColsViaShowIndex(db *sql.DB, schema, t string) ([]string, string) {
	qualified := quoteIdent(t)
	if schema != "" {
		qualified = quoteIdent(schema) + "." + qualified
	}
	rows, err := db.Query("SHOW INDEX FROM " + qualified)
	if err != nil {
		return nil, ""
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, ""
	}
	idxName, idxCol, idxSeq, idxNonUniq, idxNull := -1, -1, -1, -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "key_name":
			idxName = i
		case "column_name":
			idxCol = i
		case "seq_in_index":
			idxSeq = i
		case "non_unique":
			idxNonUniq = i
		case "null":
			idxNull = i
		}
	}
	if idxName < 0 || idxCol < 0 || idxSeq < 0 {
		return nil, ""
	}

	type item struct {
		name     string
		cols     []string
		nullable bool
	}
	var got []item
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows.Scan(ptrs...) != nil {
			continue
		}
		cell := func(i int) string {
			if i < 0 || i >= len(vals) {
				return ""
			}
			switch v := vals[i].(type) {
			case []byte:
				return string(v)
			default:
				return fmt.Sprintf("%v", v)
			}
		}
		if idxNonUniq >= 0 && cell(idxNonUniq) != "0" {
			continue // 只要唯一索引
		}
		nm := cell(idxName)
		col := cell(idxCol)
		if nm == "" || col == "" {
			continue
		}
		if len(got) == 0 || got[len(got)-1].name != nm {
			got = append(got, item{name: nm})
		}
		it := &got[len(got)-1]
		it.cols = append(it.cols, col)
		if idxNull >= 0 && strings.EqualFold(cell(idxNull), "YES") {
			it.nullable = true
		}
	}

	for _, it := range got {
		if strings.EqualFold(it.name, "PRIMARY") {
			return it.cols, it.name
		}
	}
	for _, it := range got {
		if !it.nullable {
			return it.cols, it.name
		}
	}
	if len(got) > 0 {
		return got[0].cols, got[0].name
	}
	return nil, ""
}

// 这几列上有没有可用的索引（索引的前缀列包含它们即可）
// indexCoversJoin: 索引能否支撑按 joinCols 的等值连接。
// MySQL 只能用最左前缀，所以要求索引的前 len(joinCols) 列正好是这些列（顺序不限）。
func indexCoversJoin(indexCols, joinCols []string) bool {
	if len(joinCols) == 0 || len(indexCols) < len(joinCols) {
		return false
	}
	want := map[string]bool{}
	for _, c := range joinCols {
		want[strings.ToLower(c)] = true
	}
	seen := map[string]bool{}
	for _, c := range indexCols[:len(joinCols)] {
		lc := strings.ToLower(c)
		if !want[lc] || seen[lc] {
			return false
		}
		seen[lc] = true
	}
	return len(seen) == len(want)
}

// indexesOf: 取表上全部索引（含非唯一），保持出现顺序。优先 information_schema，失败退回 SHOW INDEX。
func indexesOf(db *sql.DB, schema, t string) (map[string][]string, []string) {
	out := map[string][]string{}
	var order []string
	rows, err := db.Query(`SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
		FROM information_schema.STATISTICS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, t)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var name, col string
			var seq int
			if rows.Scan(&name, &col, &seq) != nil {
				continue
			}
			if _, ok := out[name]; !ok {
				order = append(order, name)
			}
			out[name] = append(out[name], col)
		}
		if len(out) > 0 {
			return out, order
		}
	}
	// 中间件限制 information_schema 时的兜底
	qualified := quoteIdent(t)
	if schema != "" {
		qualified = quoteIdent(schema) + "." + qualified
	}
	srows, err := db.Query("SHOW INDEX FROM " + qualified)
	if err != nil {
		return out, order
	}
	defer srows.Close()
	cols, err := srows.Columns()
	if err != nil {
		return out, order
	}
	iName, iCol := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "key_name":
			iName = i
		case "column_name":
			iCol = i
		}
	}
	if iName < 0 || iCol < 0 {
		return out, order
	}
	type rowT struct {
		name, col string
		seq       int
	}
	var got []rowT
	for srows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if srows.Scan(ptrs...) != nil {
			continue
		}
		cell := func(i int) string {
			if i < 0 || i >= len(vals) {
				return ""
			}
			if b, ok := vals[i].([]byte); ok {
				return string(b)
			}
			return fmt.Sprintf("%v", vals[i])
		}
		got = append(got, rowT{cell(iName), cell(iCol), len(got)})
	}
	for _, r := range got {
		if r.name == "" || r.col == "" {
			continue
		}
		if _, ok := out[r.name]; !ok {
			order = append(order, r.name)
		}
		out[r.name] = append(out[r.name], r.col)
	}
	return out, order
}

// hasIndexFor: 是否存在以 joinCols 为最左前缀的索引；第二个返回值用于提示现有索引长什么样
func hasIndexFor(db *sql.DB, schema, t string, cols []string) (bool, string) {
	if len(cols) == 0 {
		return false, ""
	}
	idx, order := indexesOf(db, schema, t)
	sample := ""
	for _, name := range order {
		list := idx[name]
		if indexCoversJoin(list, cols) {
			return true, name
		}
		if sample == "" && len(list) > 0 {
			sample = name + "(" + strings.Join(list, ", ") + ")"
		}
	}
	return false, sample
}

// dupCount: 源表内按判重键统计“重复的组数”
func dupCount(db *sql.DB, table string, cols []string) (int64, error) {
	if len(cols) == 0 {
		return 0, nil
	}
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, quoteIdent(c))
	}
	q := "SELECT COUNT(*) FROM (SELECT 1 FROM " + quoteIdent(table) +
		" GROUP BY " + strings.Join(quoted, ",") + " HAVING COUNT(*)>1) t"
	var n int64
	err := db.QueryRow(q).Scan(&n)
	return n, err
}

func conflictCount(db *sql.DB, target, src string, pk []string) (int64, error) {
	var conds []string
	for _, c := range pk {
		conds = append(conds, "t."+quoteIdent(c)+"=s."+quoteIdent(c))
	}
	// 用小批次表驱动，避免优化器拿几千万行的目标表当驱动表去全表扫
	q := "SELECT COUNT(*) FROM " + quoteIdent(src) + " s STRAIGHT_JOIN " + quoteIdent(target) + " t ON " + strings.Join(conds, " AND ")
	var n int64
	err := db.QueryRow(q).Scan(&n)
	return n, err
}

// 每张批次表的版本号：期望前缀的值 + 不该出现的值（标红用）
func versionReport(db *sql.DB, schema string, tables []string, col string, pfxs []string) []map[string]any {
	out := make([]map[string]any, 0, len(tables))

	for _, rawT := range tables {
		t := resolveTable(db, schema, rawT)
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS
			WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND UPPER(COLUMN_NAME)=UPPER(?)`, schema, t, col).Scan(&n)
		if n == 0 {
			out = append(out, map[string]any{"table": t, "field": col, "missing": true})
			continue
		}

		conds := make([]string, 0, len(pfxs))
		for _, p := range pfxs {
			conds = append(conds, quoteIdent(col)+" LIKE '"+strings.ReplaceAll(p, "'", "''")+"%'")
		}
		match := "(" + strings.Join(conds, " OR ") + ")"

		pick := func(where string, limit int) []string {
			q := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s AND %s IS NOT NULL LIMIT %d",
				quoteIdent(col), quoteIdent(t), where, quoteIdent(col), limit)
			rows, err := db.Query(q)
			if err != nil {
				return nil
			}
			defer rows.Close()
			vals := make([]string, 0)
			for rows.Next() {
				var v sql.NullString
				if rows.Scan(&v) == nil && v.Valid {
					vals = append(vals, v.String)
				}
			}
			return vals
		}

		okVals := pick(match, 21)
		otherVals := pick("NOT "+match, 11)
		out = append(out, map[string]any{
			"table": t, "field": col, "ok": okVals, "other": otherVals, "missing": false,
		})
	}
	return out
}

var reWhereWord = regexp.MustCompile(`(?is)\bwhere\b`)

// 取最后一段顶层 WHERE 之后的条件
func whereTail(st string) string {
	loc := reWhereWord.FindAllStringIndex(st, -1)
	if len(loc) == 0 {
		return ""
	}
	return strings.TrimSpace(st[loc[len(loc)-1][1]:])
}

func whereOf(st string) string {
	w := whereTail(st)
	if w == "" {
		return ""
	}
	if len([]rune(w)) > 400 {
		w = string([]rune(w)[:400]) + "…"
	}
	return w
}

func trimIdent(s string) string { return strings.Trim(s, "`\"") }

func whereRaw(st string) string {
	w := whereTail(st)
	if w == "" {
		return ""
	}
	w = cutAtKeyword(w, "order by")
	w = cutAtKeyword(w, "limit")
	return strings.TrimSpace(w)
}

// 只读预估：把 UPDATE 的 FROM/WHERE 拿来 COUNT，不写任何数据
// explainIndexes: 只做 EXPLAIN（不执行），返回本次统计实际用到的索引，形如 "hilist_b(PRIMARY)"
func explainIndexes(db *sql.DB, q string) string {
	rows, err := db.Query("EXPLAIN " + q)
	if err != nil {
		return ""
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return ""
	}
	iTable, iKey := -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "table":
			iTable = i
		case "key":
			iKey = i
		}
	}
	if iTable < 0 || iKey < 0 {
		return ""
	}
	var parts []string
	seen := map[string]bool{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows.Scan(ptrs...) != nil {
			continue
		}
		cellStr := func(i int) string {
			if i < 0 || i >= len(vals) || vals[i] == nil {
				return ""
			}
			if b, ok := vals[i].([]byte); ok {
				return string(b)
			}
			return fmt.Sprintf("%v", vals[i])
		}
		t, k := cellStr(iTable), cellStr(iKey)
		if t == "" || k == "" || seen[t] {
			continue
		}
		seen[t] = true
		parts = append(parts, t+"("+k+")")
		if len(parts) >= 3 {
			break
		}
	}
	return strings.Join(parts, " + ")
}

func estimateUpdate(db *sql.DB, schema, st string) (int64, string, string, bool) {
	const capN = 300000

	m := reUpdate.FindStringSubmatch(st)
	if m == nil {
		return 0, "", "", false
	}
	tablesPart, setPart := m[1], m[2]
	setPart = cutAtKeyword(setPart, "where")
	setPart = cutAtKeyword(setPart, "order by")
	setPart = cutAtKeyword(setPart, "limit")

	alias := ""
	for _, asg := range splitTopLevel(setPart) {
		lhs := asg
		if eq := indexTopLevel(asg, '='); eq >= 0 {
			lhs = asg[:eq]
		}
		lhs = strings.TrimSpace(lhs)
		if dot := strings.Index(lhs, "."); dot > 0 {
			alias = trimIdent(lhs[:dot])
			break
		}
	}
	if alias == "" {
		return 0, "", "", false
	}

	var table string
	for _, part := range splitTopLevel(tablesPart) {
		if t, a := parseTableRef(part); t != "" && strings.EqualFold(a, alias) {
			table = resolveTable(db, schema, t)
		}
	}
	if table == "" {
		return 0, "", "", false
	}

	where := whereRaw(st)

	// 先做有界精确计数：最多数到 10 万行就停，不会因为表大而拖死
	q := "SELECT COUNT(*) FROM (SELECT 1 FROM " + tablesPart
	if where != "" {
		q += " WHERE " + where
	}
	q += fmt.Sprintf(" LIMIT %d) x", capN+1)
	var n int64
	if err := db.QueryRow(q).Scan(&n); err == nil {
		if n > capN {
			return capN, "cap", explainIndexes(db, q), true
		}
		return n, "", explainIndexes(db, q), true
	}

	// 数不出来（超时/太复杂）→ 用执行计划给个量级
	if n2, ok := estimateByExplain(db, st, table, alias); ok {
		return n2, "explain", "", true
	}
	return 0, "", "", false
}

// 语句完整性自检：缺分号、括号/引号不闭合、明显截断
func validateText(raw string) []map[string]any {
	out := make([]map[string]any, 0)
	add := func(level, title, detail string) {
		out = append(out, map[string]any{"stmt": -1, "level": level, "title": title, "detail": detail})
	}

	lines := strings.Split(raw, "\n")
	depth := 0
	var inQuote byte
	var inBlock bool
	clean := make([]string, len(lines)) // 去掉注释与字符串后的行
	for i, line := range lines {
		var b strings.Builder
		for j := 0; j < len(line); j++ {
			c := line[j]
			if inBlock {
				if c == '*' && j+1 < len(line) && line[j+1] == '/' {
					inBlock = false
					j++
				}
				continue
			}
			if inQuote != 0 {
				if c == inQuote {
					if j+1 < len(line) && line[j+1] == inQuote {
						j++
						continue
					}
					inQuote = 0
				}
				continue
			}
			if c == '-' && j+1 < len(line) && line[j+1] == '-' {
				break
			}
			if c == '/' && j+1 < len(line) && line[j+1] == '*' {
				inBlock = true
				j++
				continue
			}
			if c == '\'' || c == '"' || c == '`' {
				inQuote = c
				continue
			}
			if c == '(' {
				depth++
			} else if c == ')' {
				depth--
				if depth < 0 {
					add("error", "括号不配对", strings.TrimSpace(line))
					depth = 0
				}
			}
			b.WriteByte(c)
		}
		clean[i] = b.String()
	}
	if inQuote != 0 {
		add("error", "引号未闭合", "有单引号或双引号没有配对，后面的 SQL 会被吞掉")
	}
	if depth != 0 {
		add("error", "括号不配对", "有 "+strconv.Itoa(depth)+" 个左括号没有闭合")
	}

	// 缺分号：新语句开头紧跟在没有分号的上一行后面
	// 注意 INSERT ... SELECT、UPDATE ... SET 这类跨行写法不算缺分号
	starts := []string{"update", "insert", "delete", "select", "replace", "create", "alter", "drop", "truncate"}
	firstWord := func(t string) string {
		f := strings.Fields(strings.ToLower(t))
		if len(f) == 0 {
			return ""
		}
		return f[0]
	}
	curStmt := ""  // 当前未结束语句（上一处 ; 之后）
	prevLine := "" // 上一非空行
	for i := 0; i < len(lines); i++ {
		cur := strings.TrimSpace(clean[i])
		if cur == "" {
			continue
		}
		fw := firstWord(cur)
		isStart := false
		for _, k := range starts {
			if fw == k {
				isStart = true
				break
			}
		}

		if isStart && prevLine != "" && curStmt != "" && !strings.HasSuffix(prevLine, ";") {
			head := firstWord(curStmt)
			cont := false
			switch head {
			case "insert", "replace":
				cont = fw == "select" || fw == "values" || fw == "set" || fw == "table"
			case "update":
				cont = fw == "set"
			case "delete":
				cont = fw == "from"
			case "create", "alter":
				cont = fw == "select" || fw == "as"
			case "select":
				cont = fw == "from" || fw == "where" || fw == "union"
			}
			if !cont {
				add("warn", "疑似缺少分号",
					"第 "+strconv.Itoa(i+1)+" 行以 "+strings.Fields(cur)[0]+" 开头，但上一条语句没有以 ; 结尾，两条语句可能被当成一条")
				curStmt = cur
			}
		} else if curStmt == "" {
			curStmt = cur
		} else {
			curStmt += " " + cur
		}

		if strings.Contains(cur, ";") {
			// 本条结束，取分号之后的部分继续
			if idx := strings.LastIndex(cur, ";"); idx >= 0 && idx+1 < len(cur) {
				curStmt = strings.TrimSpace(cur[idx+1:])
			} else {
				curStmt = ""
			}
		}
		prevLine = cur
	}

	// 明显截断：最后一行以逗号/等号/AND/OR 结尾
	last := ""
	for i := len(clean) - 1; i >= 0; i-- {
		if t := strings.TrimSpace(clean[i]); t != "" {
			last = t
			break
		}
	}
	if last != "" {
		low := strings.ToLower(last)
		if strings.HasSuffix(last, ",") || strings.HasSuffix(last, "=") ||
			strings.HasSuffix(low, " and") || strings.HasSuffix(low, " or") {
			add("error", "语句疑似不完整", "最后一行以 "+last[len(last)-1:]+" 结尾，像是没写完")
		}
	}

	return out
}

// 检出库里已改数据但还没提交的事务——解释“我明明删了，工具还看得见”
func trxWarnings(db *sql.DB) []map[string]any {
	out := make([]map[string]any, 0)
	rows, err := db.Query(`SELECT trx_id, DATE_FORMAT(trx_started,'%H:%i:%s'), trx_rows_modified
		FROM information_schema.INNODB_TRX WHERE trx_rows_modified > 0`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id, started string
		var n int64
		if rows.Scan(&id, &started, &n) != nil {
			continue
		}
		out = append(out, map[string]any{
			"stmt": -1, "level": "warn", "title": "有未提交事务",
			"detail": "事务 " + id + " 已修改 " + strconv.FormatInt(n, 10) + " 行，开始于 " + started +
				"。如果这是你在客户端里的删改，记得 COMMIT；否则工具（另一个会话）看到的仍是提交前的数据",
		})
	}
	return out
}

func preflight(ds DataSource, stmts []string, verColName string, verPfxs []string) ([]map[string]any, []map[string]any, error) {
	db, err := openDB(ds)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()

	schema := ds.Database
	if schema == "" {
		_ = db.QueryRow("SELECT DATABASE()").Scan(&schema)
	}
	out := make([]map[string]any, 0)
	add := func(i int, level, title, detail string) {
		out = append(out, map[string]any{"stmt": i, "level": level, "title": title, "detail": detail})
	}
	// 超过 300ms 的检查在详情里标出耗时，方便定位哪一步慢
	addT := func(i int, level, title, detail string, t0 time.Time) {
		if d := time.Since(t0); d > 300*time.Millisecond {
			detail += fmt.Sprintf("（耗时 %d ms）", d.Milliseconds())
		}
		add(i, level, title, detail)
	}

	type stmtTarget struct {
		stmt  int
		where string
	}
	targetUse := map[string][]stmtTarget{}

	batchTables := make([]string, 0)
	seenTable := map[string]bool{}
	for i, st := range stmts {
		for _, m := range reBatch.FindAllStringSubmatch(st, -1) {
			full := strings.ToLower(m[0])
			if !seenTable[full] {
				seenTable[full] = true
				batchTables = append(batchTables, full)
			}
		}
		switch {
		case stmtKind(st) == "insert":
			m := reInsert.FindStringSubmatch(st)
			if m == nil {
				continue
			}
			target := resolveTable(db, schema, normTable(m[1]))
			colList := strings.TrimSpace(m[2])
			add(i, "ok", "目标表", target)
			fm := reFrom.FindStringSubmatch(m[3])
			if fm == nil {
				add(i, "warn", "逐个值插入", "INSERT ... VALUES：行数由常量列表决定，预检不做数据比对")
				continue
			}
			src := resolveTable(db, schema, normTable(fm[1]))
			if src == target {
				add(i, "error", "自己插自己", fmt.Sprintf("源表和目标表都是 %s", target))
				continue
			}
			// 带列清单的 INSERT 不做整表列数比对（列数本来就允许不同）
			if colList == "" {
				tc, scc := columnCount(db, schema, target), columnCount(db, schema, src)
				if tc > 0 && scc > 0 && tc != scc {
					add(i, "error", "列数不一致", fmt.Sprintf("%s 有 %d 列，%s 有 %d 列，INSERT ... SELECT 会直接报错", target, tc, src, scc))
				}
			}
			tCnt := time.Now()
			n := tableCount(db, src)
			if n == 0 {
				addT(i, "warn", "源表为空", fmt.Sprintf("%s 没有数据，这条 INSERT 不会插入任何行", src), tCnt)
			} else {
				addT(i, "ok", "源表非空", fmt.Sprintf("%s 共 %d 行", src, n), tCnt)
			}
			pk, idxName := keyCols(db, schema, target)
			if len(pk) == 0 {
				add(i, "warn", "目标表无主键或唯一索引", fmt.Sprintf("%s 既没有主键也没有唯一索引：重复执行会插入重复数据，且无法判断这批是否已经跑过", target))
				continue
			}
			if !strings.EqualFold(idxName, "PRIMARY") {
				add(i, "ok", "使用唯一索引判重", fmt.Sprintf("%s 无主键，改用唯一索引 %s（%s）", target, idxName, strings.Join(pk, ", ")))
			}
			for _, c := range pk {
				if nullable, ok := colNullable(db, schema, target, c); ok && nullable {
					add(i, "warn", "判重键里有可空列", fmt.Sprintf("%s.%s 允许 NULL：NULL 不参与唯一性比较，重复数据可能漏检", target, c))
					break
				}
			}
			if n > 0 {
				add(i, "ok", "预计影响行数", fmt.Sprintf("预计插入 %d 行（精确计数）", n))
			}
			if n > 0 {
				if dup, err := dupCount(db, src, pk); err == nil && dup > 0 {
					add(i, "error", "源表主键重复", fmt.Sprintf("%s 内有 %d 组重复主键，插入会失败", src, dup))
				}
				if has, sample := hasIndexFor(db, schema, target, pk); !has {
					msg := fmt.Sprintf("%s 上没有以 (%s) 为最左前缀的索引，按这些列比对会全表扫", target, strings.Join(pk, ", "))
					if sample != "" {
						msg += fmt.Sprintf("；现有索引 %s 的前缀对不上", sample)
					}
					msg += "。请先建索引再预检，否则无法判断这批数据是否已执行过"
					add(i, "error", "目标表缺少可用索引", msg)
				} else {
					tConf := time.Now()
					if conf, err := conflictCount(db, target, src, pk); err == nil {
						if conf > 0 {
							addT(i, "error", "主键冲突", fmt.Sprintf("%s 已存在 %d 行相同主键，INSERT 会失败（大概率这批已执行过）", target, conf), tConf)
						} else {
							addT(i, "ok", "无主键冲突", fmt.Sprintf("与 %s 比对无重复", target), tConf)
						}
					} else {
						add(i, "warn", "主键冲突检查未完成", "查询失败或超时（表可能很大）："+withHint(err.Error()))
					}
				}
			}
		case stmtKind(st) == "update":
			m := reUpdate.FindStringSubmatch(st)
			if m == nil {
				continue
			}
			tablesPart, setPart := m[1], m[2]
			setPart = cutAtKeyword(setPart, "where")
			setPart = cutAtKeyword(setPart, "order by")
			setPart = cutAtKeyword(setPart, "limit")

			updated := map[string]bool{}
			var unqual []string
			for _, asg := range splitTopLevel(setPart) {
				lhs := asg
				if eq := indexTopLevel(asg, '='); eq >= 0 {
					lhs = asg[:eq]
				}
				lhs = strings.TrimSpace(lhs)
				if dot := strings.Index(lhs, "."); dot > 0 {
					updated[strings.ToLower(strings.Trim(lhs[:dot], "`\""))] = true
				} else if lhs != "" {
					unqual = append(unqual, strings.Trim(lhs, "`\""))
				}
			}

			var refs []tblRef
			for _, part := range splitTopLevel(tablesPart) {
				if t, a := parseTableRef(part); t != "" {
					refs = append(refs, tblRef{resolveTable(db, schema, t), a})
				}
			}
			if len(refs) < 2 {
				// 单表 UPDATE：没有外部数据源，但目标表相关的检查仍然要做
				if len(refs) == 1 {
					r := refs[0]
					if !reWhereWord.MatchString(st) {
						add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请写成 WHERE 1=1）")
					}
					for _, col := range unqual {
						if !tableHasColumn(db, schema, r.table, col) {
							add(i, "error", "SET 列不存在", fmt.Sprintf("要更新的列 %s 在 %s 里找不到", col, r.table))
						}
					}
					add(i, "ok", "被更新表", fmt.Sprintf("%s（单表更新）", r.table))
					targetUse[r.table] = append(targetUse[r.table], stmtTarget{stmt: i, where: whereRaw(st)})
					if n, ok := estimateByExplain(db, st, r.table, r.alias); ok {
						tail := " · 未用索引（全表扫）"
						if keys := explainIndexes(db, st); keys != "" {
							tail = " · 索引 " + keys
						}
						add(i, "ok", "预计影响行数", fmt.Sprintf("预计更新 %d 行（按执行计划估算）%s", n, tail))
					} else {
						add(i, "warn", "预计影响行数未计算", "查询超时或语句太复杂（大表上常见），不影响执行")
					}
				}
				continue
			}

			// SET 左值没写别名 → 用元数据反查列归属，避免按位置瞎猜
			if len(updated) == 0 {
				for _, col := range unqual {
					owners := tablesHavingCol(db, schema, refs, col)
					switch len(owners) {
					case 1:
						updated[strings.ToLower(owners[0].alias)] = true
						add(i, "ok", "按列名定位被更新表", fmt.Sprintf("SET 左值 %s 只存在于 %s（别名 %s）", col, owners[0].table, owners[0].alias))
					case 0:
						add(i, "warn", "找不到列", fmt.Sprintf("SET 左值 %s 在参与的表里都不存在", col))
					default:
						names := make([]string, 0, len(owners))
						for _, o := range owners {
							names = append(names, o.table+"("+o.alias+")")
						}
						add(i, "error", "列名有歧义", fmt.Sprintf("%s 同时存在于 %s，MySQL 会报 ambiguous，请写成 别名.%s", col, strings.Join(names, "、"), col))
					}
				}
			}

			if !reWhereWord.MatchString(st) {
				add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请改成 WHERE 1=1 再跑）")
			}
			for _, col := range unqual {
				hit := false
				for _, r := range refs {
					if !updated[strings.ToLower(r.alias)] {
						continue
					}
					if tableHasColumn(db, schema, r.table, col) {
						hit = true
						break
					}
				}
				if !hit && len(updated) > 0 {
					add(i, "error", "SET 列不存在", fmt.Sprintf("要更新的列 %s 在目标表里找不到", col))
				}
			}

			tEst := time.Now()
			if n, note, idx, ok := estimateUpdate(db, schema, st); ok {
				tail := " · 未用索引（全表扫）"
				if idx != "" {
					tail = " · 索引 " + idx
				}
				switch note {
				case "cap":
					addT(i, "warn", "预计影响行数", fmt.Sprintf("≥ %d 行（数到 30 万就停了，实际可能更多）%s", n, tail), tEst)
				case "explain":
					addT(i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算）%s", n, tail), tEst)
				default:
					addT(i, "ok", "预计影响行数", fmt.Sprintf("预计更新 %d 行（精确计数，最多数 30 万）%s", n, tail), tEst)
				}
			} else {
				add(i, "warn", "预计影响行数未计算", "查询超时或语句太复杂（大表上常见），不影响执行")
			}
			for _, r := range refs {
				if updated[strings.ToLower(r.alias)] {
					add(i, "ok", "被更新表", fmt.Sprintf("%s（别名 %s）", r.table, r.alias))
					targetUse[r.table] = append(targetUse[r.table], stmtTarget{stmt: i, where: whereRaw(st)})
					continue
				}
				n := tableCount(db, r.table)
				if n == 0 {
					add(i, "warn", "源表为空", fmt.Sprintf("%s（别名 %s）没有数据，这条 UPDATE 不会命中任何行", r.table, r.alias))
				} else {
					add(i, "ok", "数据来源表", fmt.Sprintf("%s（别名 %s）共 %d 行", r.table, r.alias, n))
				}
			}
			if len(updated) == 0 {
				add(i, "warn", "无法判断被更新表", "SET 左值既没别名也无法按列名定位，已跳过源表为空检查")
			}

		case stmtKind(st) == "delete":
			m := reDelete.FindStringSubmatch(st)
			if m == nil {
				continue
			}
			target := resolveTable(db, schema, normTable(m[1]))
			add(i, "ok", "被删除表", target)
			targetUse[target] = append(targetUse[target], stmtTarget{stmt: i, where: whereRaw(st)})
			if !reWhereWord.MatchString(st) {
				add(i, "warn", "没有 WHERE 条件", "这条 DELETE 会清空整张表，请确认不是笔误")
			}
			if n, ok := estimateByExplain(db, st, target, ""); ok {
				add(i, "ok", "预计影响行数", fmt.Sprintf("预计删除 %d 行", n))
			} else {
				add(i, "warn", "预计影响行数未计算", "查询超时或语句太复杂（大表上常见），不影响执行")
			}
		}
	}

	for t, uses := range targetUse {
		if len(uses) < 2 {
			continue
		}
		parts := make([]string, 0, len(uses))
		for _, u := range uses {
			w := u.where
			if w == "" {
				w = "（无条件）"
			}
			parts = append(parts, fmt.Sprintf("第 %d 条 → %s", u.stmt+1, w))
		}
		wheres := make([]string, 0, len(uses))
		for _, u := range uses {
			wheres = append(wheres, u.where)
		}
		verdict, lvl := overlapVerdict(wheres)
		add(-1, lvl, fmt.Sprintf("同一张表被 %d 条语句更新：%s", len(uses), verdict),
			fmt.Sprintf("%s 出现在 %d 条语句里：\n%s\n结论：%s", t, len(uses), strings.Join(parts, "\n"), verdict))
	}

	out = append(out, trxWarnings(db)...)
	versions := versionReport(db, schema, batchTables, verColName, verPfxs)
	return out, versions, nil
}

/* ---------------- 工具 ---------------- */

// 保存失败时的统一响应（调用方负责先释放锁）
// 拉取升级清单，带 5 分钟缓存，避免前端频繁请求外部地址
var (
	updMu    sync.Mutex
	updAt    time.Time
	updBase  string
	updCache map[string]any
)

func fetchVersion(base string) (map[string]any, error) {
	updMu.Lock()
	defer updMu.Unlock()
	if updCache != nil && updBase == base && time.Since(updAt) < 5*time.Minute {
		return updCache, nil
	}
	c := &http.Client{Timeout: 8 * time.Second}
	resp, err := c.Get(base + "version.json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("升级源返回 %d", resp.StatusCode)
	}
	var v map[string]any
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&v); err != nil {
		return nil, err
	}
	updCache, updAt, updBase = v, time.Now(), base
	return v, nil
}

func saveFailed(w http.ResponseWriter, err error) {
	saveFailed(w, err)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("content-type", "application/json; charset=utf-8")
	w.Header().Set("cache-control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	b, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, v)
}

var logMu sync.Mutex

func appendLog(entry map[string]any) {
	b, err := json.Marshal(entry)
	if err != nil {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	f, err := os.OpenFile(filepath.Join(logsDir, "run-log.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(b, '\n'))
}

// 只读日志文件尾部，避免日志变大后每次全量解析
func readLogTail(maxBytes int64) ([]byte, error) {
	f, err := os.Open(filepath.Join(logsDir, "run-log.jsonl"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	start := int64(0)
	if st.Size() > maxBytes {
		start = st.Size() - maxBytes
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if start > 0 {
		if i := bytes.IndexByte(b, '\n'); i >= 0 {
			b = b[i+1:]
		}
	}
	return b, nil
}

func openBrowser(u string) {
	if os.Getenv("NO_OPEN") == "1" {
		return
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", u)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", u)
	default:
		cmd = exec.Command("xdg-open", u)
	}
	_ = cmd.Start()
}

func lineCount(sqlText string) int {
	n := 0
	for _, l := range strings.Split(sqlText, "\n") {
		t := strings.TrimSpace(l)
		if t != "" && !strings.HasPrefix(t, "--") {
			n++
		}
	}
	return n
}

/* ---------------- 路由 ---------------- */

func buildTree(s Store) map[string]any {
	dsList := make([]map[string]any, 0, len(s.DataSources))
	for _, d := range s.DataSources {
		dsList = append(dsList, map[string]any{
			"id": d.ID, "name": d.Name, "host": d.Host, "port": d.Port,
			"user": d.User, "database": d.Database, "hasPassword": d.Pass != "", "env": envOf(d),
			"connSec": dsConnSec(d), "dqlSec": dsDQLSec(d), "dmlSec": dsDMLSec(d),
		})
	}

	var build func(parent string) []map[string]any
	build = func(parent string) []map[string]any {
		out := make([]map[string]any, 0)
		gs := make([]Group, 0)
		for _, g := range s.Groups {
			if g.ParentID == parent {
				gs = append(gs, g)
			}
		}
		sort.SliceStable(gs, func(i, j int) bool { return gs[i].Order < gs[j].Order })
		for _, g := range gs {
			scs := make([]Script, 0)
			for _, sc := range s.Scripts {
				if sc.GroupID == g.ID {
					scs = append(scs, sc)
				}
			}
			sort.SliceStable(scs, func(i, j int) bool { return scs[i].Order < scs[j].Order })
			scripts := make([]map[string]any, 0)
			for _, sc := range scs {
				if sc.GroupID == g.ID {
					node := map[string]any{"id": sc.ID, "name": sc.Name, "lines": lineCount(sc.SQL), "dsId": sc.DSID,
						"versionCol": sc.VerCol, "versionPrefixes": sc.VerPfxs}
					if sc.Preflight != nil {
						node["preflight"] = *sc.Preflight
					} else {
						node["preflight"] = nil
					}
					scripts = append(scripts, node)
				}
			}
			node := map[string]any{
				"id": g.ID, "name": g.Name, "open": g.Open,
				"scripts": scripts, "folders": build(g.ID),
			}
			if parent == "" { // 顶层文件夹才有配置
				pre := preflightEnabled(s)
				if g.Preflight != nil {
					pre = *g.Preflight
				}
				col := g.VerCol
				if strings.TrimSpace(col) == "" {
					col = defaultVerCol(s)
				}
				node["dsId"] = g.DSID
				node["dsName"] = ""
				node["dsEnv"] = ""
				if d, ok := dsByID(s, g.DSID); ok {
					node["dsName"] = d.Name
					node["dsEnv"] = envOf(d)
				}
				node["preflight"] = pre
				node["versionCol"] = col
				node["versionPrefixes"] = g.VerPfxs
			}
			out = append(out, node)
		}
		return out
	}

	rootScripts := make([]map[string]any, 0)
	roots := make([]Script, 0)
	for _, sc := range s.Scripts {
		if sc.GroupID == "" {
			roots = append(roots, sc)
		}
	}
	sort.SliceStable(roots, func(i, j int) bool { return roots[i].Order < roots[j].Order })
	for _, sc := range roots {
		name := ""
		if d, ok := dsByID(s, sc.DSID); ok {
			name = d.Name
		}
		pre := preflightEnabled(s)
		if sc.Preflight != nil {
			pre = *sc.Preflight
		}
		col := sc.VerCol
		if strings.TrimSpace(col) == "" {
			col = defaultVerCol(s)
		}
		env := ""
		if d, ok := dsByID(s, sc.DSID); ok {
			env = envOf(d)
		}
		rootScripts = append(rootScripts, map[string]any{
			"id": sc.ID, "name": sc.Name, "lines": lineCount(sc.SQL),
			"dsId": sc.DSID, "dsName": name, "dsEnv": env,
			"preflight": pre, "versionCol": col, "versionPrefixes": sc.VerPfxs,
		})
	}

	return map[string]any{
		"datasources": dsList,
		"folders":     build(""),
		"rootScripts": rootScripts,
		"defaults": map[string]any{
			"preflight":       preflightEnabled(s),
			"versionCol":      defaultVerCol(s),
			"versionPrefixes": s.Settings.VerPfxs,
		},
	}
}

func findScriptByID(s Store, id string) (Script, bool) {
	for _, sc := range s.Scripts {
		if sc.ID == id {
			return sc, true
		}
	}
	return Script{}, false
}

func handler(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path

	// 页面
	// 下载三平台安装包（白名单，不接受任意路径）
	if r.Method == http.MethodGet && strings.HasPrefix(p, "/dl/") {
		files := map[string]string{
			"medupdate.exe":           "release-win/medupdate.exe",
			"medupdate-mac":           "release-mac/medupdate",
			"medupdate-linux":         "release-linux/medupdate",
			"medupdate-win-1.0.1.zip": "dist/medupdate-win-1.0.1.zip",
			"medupdate-mac-1.0.1.zip": "dist/medupdate-mac-1.0.1.zip",
			"medupdate-app.zip":       "dist/update-tool-mac-app.zip",
			"medupdate-lan.zip":       "dist/medupdate-lan-1.0.4.zip",
		}
		name := strings.TrimPrefix(p, "/dl/")
		rel, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		full := filepath.Join(appHome, rel)
		if _, err := os.Stat(full); err != nil {
			http.Error(w, "安装包还没生成，先在服务器上执行 build.sh", 404)
			return
		}
		w.Header().Set("content-disposition", "attachment; filename=\""+name+"\"")
		http.ServeFile(w, r, full)
		return
	}

	if r.Method == http.MethodGet && p == "/guide" {
		b, err := assets.ReadFile("guide.html")
		if err != nil {
			http.Error(w, "guide.html 缺失", 500)
			return
		}
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Header().Set("cache-control", "no-store")
		_, _ = w.Write(b)
		return
	}

	if r.Method == http.MethodGet && (p == "/" || p == "/index.html") {
		b, err := assets.ReadFile("index.html")
		if err != nil {
			http.Error(w, "index.html 缺失", 500)
			return
		}
		w.Header().Set("content-type", "text/html; charset=utf-8")
		w.Header().Set("cache-control", "no-store")
		_, _ = w.Write(b)
		return
	}

	switch {
	case r.Method == http.MethodGet && p == "/api/settings":
		mu.Lock()
		st := loadStore()
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"preflight": preflightEnabled(st), "updateUrl": st.Settings.UpdateURL})

	case r.Method == http.MethodPost && p == "/api/settings":
		var b struct {
			Preflight *bool   `json:"preflight"`
			UpdateURL *string `json:"updateUrl"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		st := loadStore()
		if b.Preflight != nil {
			st.Settings.Preflight = b.Preflight
		}
		if b.UpdateURL != nil {
			st.Settings.UpdateURL = strings.TrimSpace(*b.UpdateURL)
		}
		if err := saveStore(st); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "preflight": preflightEnabled(st)})

	case r.Method == http.MethodGet && p == "/api/inspect":
		// 诊断：看看这台库上的索引元数据能不能读到
		mu.Lock()
		st := loadStore()
		mu.Unlock()
		dsID := r.URL.Query().Get("ds")
		tbl := r.URL.Query().Get("table")
		var ds DataSource
		for _, d := range st.DataSources {
			if dsID == "" || d.ID == dsID {
				ds = d
				break
			}
		}
		if ds.Host == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "没有可用的数据源"})
			return
		}
		if tbl == "" {
			tbl = "hilist_b"
		}
		db, err := openDB(ds)
		if err != nil {
			writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		defer db.Close()
		schema := ds.Database
		if schema == "" {
			_ = db.QueryRow("SELECT DATABASE()").Scan(&schema)
		}
		resolved := resolveTable(db, schema, tbl)

		dump := func(q string, args ...any) []map[string]any {
			out := []map[string]any{}
			rows, err := db.Query(q, args...)
			if err != nil {
				out = append(out, map[string]any{"error": err.Error()})
				return out
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				if rows.Scan(ptrs...) != nil {
					continue
				}
				m := map[string]any{}
				for i, c := range cols {
					switch v := vals[i].(type) {
					case []byte:
						m[c] = string(v)
					default:
						m[c] = v
					}
				}
				out = append(out, m)
			}
			return out
		}

		stats := dump(`SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
			FROM information_schema.STATISTICS
			WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)
			ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, resolved)
		kcu := dump(`SELECT CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION
			FROM information_schema.KEY_COLUMN_USAGE
			WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)
			ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`, schema, resolved)
		cols, idxName := keyCols(db, schema, resolved)

		writeJSON(w, 200, map[string]any{
			"ok": true, "datasource": ds.Name, "schema_input": ds.Database,
			"schema_used": schema, "table_input": tbl, "table_resolved": resolved,
			"statistics": stats, "key_column_usage": kcu,
			"keyCols": cols, "index_name": idxName,
		})

	case r.Method == http.MethodGet && p == "/api/health":
		writeJSON(w, 200, map[string]any{"ok": true, "version": appVersion, "build": buildTime, "pid": os.Getpid()})

	case r.Method == http.MethodGet && p == "/api/update/check":
		mu.Lock()
		st := loadStore()
		mu.Unlock()
		base := strings.TrimSpace(st.Settings.UpdateURL)
		if base == "" {
			writeJSON(w, 200, map[string]any{"enabled": false})
			return
		}
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		ver, err := fetchVersion(base)
		if err != nil {
			writeJSON(w, 200, map[string]any{"enabled": true, "error": err.Error()})
			return
		}
		latest, _ := ver["version"].(string)
		out := map[string]any{
			"enabled": true, "current": appVersion, "latest": latest,
			"hasUpdate": latest != "" && latest != appVersion,
			"notes":     ver["notes"], "build": ver["build"], "base": base,
		}
		if files, ok := ver["files"].(map[string]any); ok {
			if f, ok := files["win"]; ok {
				out["win"] = f
			}
			if f, ok := files["mac"]; ok {
				out["mac"] = f
			}
		}
		writeJSON(w, 200, out)

	case r.Method == http.MethodGet && p == "/api/tree":
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		writeJSON(w, 200, buildTree(s))

	case r.Method == http.MethodPost && p == "/api/ds":
		var b struct {
			ID            string   `json:"id"`
			Name          *string  `json:"name"`
			Host          *string  `json:"host"`
			Port          *flexInt `json:"port"`
			User          *string  `json:"user"`
			Database      *string  `json:"database"`
			Password      *string  `json:"password"`
			ClearPassword bool     `json:"clearPassword"`
			VersionCol    *string  `json:"versionCol"`
			VersionPfxs   *string  `json:"versionPrefixes"`
			Env           *string  `json:"env"`
			ConnSec       *flexInt `json:"connSec"`
			IOSec         *flexInt `json:"ioSec"`
			DQLSec        *flexInt `json:"dqlSec"`
			DMLSec        *flexInt `json:"dmlSec"`
			SelSec        *flexInt `json:"selSec"` // 兼容旧字段
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		idx := -1
		for i := range s.DataSources {
			if s.DataSources[i].ID == b.ID && b.ID != "" {
				idx = i
			}
		}
		if idx < 0 {
			s.DataSources = append(s.DataSources, DataSource{ID: newID(), Name: "新数据源", Port: 3306})
			idx = len(s.DataSources) - 1
		}
		ds := &s.DataSources[idx]
		if b.Name != nil && strings.TrimSpace(*b.Name) != "" {
			ds.Name = strings.TrimSpace(*b.Name)
		}
		if b.Host != nil {
			ds.Host = strings.TrimSpace(*b.Host)
		}
		if b.Port != nil && int(*b.Port) > 0 {
			ds.Port = int(*b.Port)
		}
		if b.User != nil {
			ds.User = strings.TrimSpace(*b.User)
		}
		if b.Database != nil {
			ds.Database = strings.TrimSpace(*b.Database)
		}
		if b.Password != nil && *b.Password != "" {
			ds.Pass = *b.Password
		}
		if b.ClearPassword {
			ds.Pass = ""
		}
		if b.VersionCol != nil {
			ds.VerCol = strings.TrimSpace(*b.VersionCol)
		}
		if b.VersionPfxs != nil {
			ds.VerPfxs = strings.TrimSpace(*b.VersionPfxs)
		}
		if b.Env != nil {
			ds.Env = strings.TrimSpace(*b.Env)
		}
		if b.ConnSec != nil {
			ds.ConnSec = int(*b.ConnSec)
		}
		if b.IOSec != nil {
			ds.IOSec = int(*b.IOSec)
		}
		if b.DQLSec != nil {
			ds.DQLSec = int(*b.DQLSec)
		}
		if b.IOSec != nil && b.DMLSec == nil {
			ds.DMLSec = int(*b.IOSec)
		}
		if b.DMLSec != nil {
			v := int(*b.DMLSec)
			if v == 0 {
				v = -1 // 前端填 0 表示不限
			}
			ds.DMLSec = v
		}
		if ds.Env == "" {
			ds.Env = "prod" // 未选就按生产处理，宁可多一次确认
		}
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		id := ds.ID
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "id": id, "tree": buildTree(s)})

	case r.Method == http.MethodDelete && p == "/api/ds":
		id := r.URL.Query().Get("id")
		mu.Lock()
		s := loadStore()
		gids := map[string]bool{}
		var keepG []Group
		for _, g := range s.Groups {
			if g.DSID == id {
				gids[g.ID] = true
			} else {
				keepG = append(keepG, g)
			}
		}
		var keepS []Script
		for _, sc := range s.Scripts {
			if !gids[sc.GroupID] {
				keepS = append(keepS, sc)
			}
		}
		var keepD []DataSource
		for _, d := range s.DataSources {
			if d.ID != id {
				keepD = append(keepD, d)
			}
		}
		s.Groups, s.Scripts, s.DataSources = keepG, keepS, keepD
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "tree": buildTree(s)})

	case r.Method == http.MethodPost && p == "/api/ds/test":
		var b struct {
			ID       string   `json:"id"`
			Host     *string  `json:"host"`
			Port     *flexInt `json:"port"`
			User     *string  `json:"user"`
			Database *string  `json:"database"`
			Password *string  `json:"password"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		var saved DataSource
		for _, d := range s.DataSources {
			if d.ID == b.ID {
				saved = d
			}
		}
		ds := DataSource{Host: saved.Host, Port: saved.Port, User: saved.User, Database: saved.Database, Pass: saved.Pass}
		if b.Host != nil {
			ds.Host = strings.TrimSpace(*b.Host)
		}
		if b.Port != nil && int(*b.Port) > 0 {
			ds.Port = int(*b.Port)
		}
		if b.User != nil {
			ds.User = strings.TrimSpace(*b.User)
		}
		if b.Database != nil {
			ds.Database = strings.TrimSpace(*b.Database)
		}
		if b.Password != nil && *b.Password != "" {
			ds.Pass = *b.Password
		}
		if ds.Host == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "先填地址"})
			return
		}
		db, err := openDB(ds)
		if err != nil {
			writeJSON(w, 200, map[string]any{
				"ok":    false,
				"error": withHint(fmt.Sprintf("[%s@%s:%d] %v", ds.User, ds.Host, ds.Port, err)),
			})
			return
		}
		var ver, curDB string
		_ = db.QueryRow("SELECT VERSION(), IFNULL(DATABASE(),'(未选择库)')").Scan(&ver, &curDB)
		rows, err := db.Query("SHOW DATABASES")
		dbs := []string{}
		if err == nil {
			for rows.Next() {
				var name string
				if rows.Scan(&name) == nil {
					if name != "information_schema" && name != "performance_schema" {
						dbs = append(dbs, name)
					}
				}
			}
			rows.Close()
		}
		db.Close()
		writeJSON(w, 200, map[string]any{"ok": true, "output": ver + " / " + curDB, "databases": dbs})

	case r.Method == http.MethodPost && p == "/api/order":
		var b struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
			Dir  int    `json:"dir"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		st := loadStore()
		moved := false
		if b.Kind == "group" {
			parent := ""
			for _, g := range st.Groups {
				if g.ID == b.ID {
					parent = g.ParentID
				}
			}
			idxs := []int{}
			for i, g := range st.Groups {
				if g.ParentID == parent {
					idxs = append(idxs, i)
				}
			}
			sort.SliceStable(idxs, func(a, c int) bool { return st.Groups[idxs[a]].Order < st.Groups[idxs[c]].Order })
			for pos, i := range idxs {
				if st.Groups[i].ID != b.ID {
					continue
				}
				t := pos + b.Dir
				if t >= 0 && t < len(idxs) {
					idxs[pos], idxs[t] = idxs[t], idxs[pos]
					for n, gi := range idxs {
						st.Groups[gi].Order = n + 1
					}
					moved = true
				}
				break
			}
		} else if b.Kind == "script" {
			gid := ""
			for _, sc := range st.Scripts {
				if sc.ID == b.ID {
					gid = sc.GroupID
				}
			}
			idxs := []int{}
			for i, sc := range st.Scripts {
				if sc.GroupID == gid {
					idxs = append(idxs, i)
				}
			}
			sort.SliceStable(idxs, func(a, c int) bool { return st.Scripts[idxs[a]].Order < st.Scripts[idxs[c]].Order })
			for pos, i := range idxs {
				if st.Scripts[i].ID != b.ID {
					continue
				}
				t := pos + b.Dir
				if t >= 0 && t < len(idxs) {
					idxs[pos], idxs[t] = idxs[t], idxs[pos]
					for n, si := range idxs {
						st.Scripts[si].Order = n + 1
					}
					moved = true
				}
				break
			}
		}
		if moved {
			if err := saveStore(st); err != nil {
				mu.Unlock()
				saveFailed(w, err)
				return
			}
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": moved, "tree": buildTree(st)})

	case r.Method == http.MethodPost && p == "/api/group":
		var b struct {
			ID        string  `json:"id"`
			DSID      *string `json:"dsId"`
			ParentID  string  `json:"parentId"`
			Name      *string `json:"name"`
			Preflight *bool   `json:"preflight"`
			VerCol    *string `json:"versionCol"`
			VerPfxs   *string `json:"versionPrefixes"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		idx := -1
		for i := range s.Groups {
			if s.Groups[i].ID == b.ID && b.ID != "" {
				idx = i
			}
		}
		if idx < 0 {
			dsid := ""
			if b.DSID != nil {
				dsid = *b.DSID
			}
			s.Groups = append(s.Groups, Group{ID: newID(), DSID: dsid, ParentID: b.ParentID, Name: "新文件夹", Order: nextGroupOrder(s, b.ParentID)})
			idx = len(s.Groups) - 1
		}
		if b.Name != nil && strings.TrimSpace(*b.Name) != "" {
			s.Groups[idx].Name = strings.TrimSpace(*b.Name)
		}
		// 只有顶层文件夹保存连接与体检/版本配置，子文件夹一律继承
		if s.Groups[idx].ParentID == "" {
			if b.DSID != nil {
				s.Groups[idx].DSID = strings.TrimSpace(*b.DSID)
			}
			if b.Preflight != nil {
				s.Groups[idx].Preflight = b.Preflight
			}
			if b.VerCol != nil {
				s.Groups[idx].VerCol = strings.TrimSpace(*b.VerCol)
			}
			if b.VerPfxs != nil {
				s.Groups[idx].VerPfxs = strings.TrimSpace(*b.VerPfxs)
			}
		}
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		id := s.Groups[idx].ID
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "id": id, "tree": buildTree(s)})

	case r.Method == http.MethodDelete && p == "/api/group":
		id := r.URL.Query().Get("id")
		mu.Lock()
		s := loadStore()
		// 递归收集所有子文件夹
		dead := map[string]bool{id: true}
		for changed := true; changed; {
			changed = false
			for _, g := range s.Groups {
				if !dead[g.ID] && dead[g.ParentID] {
					dead[g.ID] = true
					changed = true
				}
			}
		}
		var keepG []Group
		for _, g := range s.Groups {
			if !dead[g.ID] {
				keepG = append(keepG, g)
			}
		}
		var keepS []Script
		for _, sc := range s.Scripts {
			if !dead[sc.GroupID] {
				keepS = append(keepS, sc)
			}
		}
		s.Groups, s.Scripts = keepG, keepS
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "tree": buildTree(s)})

	case r.Method == http.MethodPost && p == "/api/script":
		var b struct {
			ID            string  `json:"id"`
			GroupID       string  `json:"groupId"`
			DSID          *string `json:"dsId"`
			Name          *string `json:"name"`
			SQL           *string `json:"sql"`
			Preflight     *bool   `json:"preflight"`
			PreflightMode *string `json:"preflightMode"`
			VerCol        *string `json:"versionCol"`
			VerPfxs       *string `json:"versionPrefixes"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		idx := -1
		for i := range s.Scripts {
			if s.Scripts[i].ID == b.ID && b.ID != "" {
				idx = i
			}
		}
		if idx < 0 {
			s.Scripts = append(s.Scripts, Script{ID: newID(), GroupID: b.GroupID, Name: "新脚本", Order: nextScriptOrder(s, b.GroupID)})
			idx = len(s.Scripts) - 1
		}
		if b.Name != nil && strings.TrimSpace(*b.Name) != "" {
			s.Scripts[idx].Name = strings.TrimSpace(*b.Name)
		}
		if b.GroupID != "" {
			s.Scripts[idx].GroupID = b.GroupID
		}
		if b.SQL != nil {
			s.Scripts[idx].SQL = *b.SQL
		}
		if b.DSID != nil {
			s.Scripts[idx].DSID = strings.TrimSpace(*b.DSID)
		}
		if b.PreflightMode != nil {
			switch strings.ToLower(strings.TrimSpace(*b.PreflightMode)) {
			case "on":
				v := true
				s.Scripts[idx].Preflight = &v
			case "off":
				v := false
				s.Scripts[idx].Preflight = &v
			default:
				s.Scripts[idx].Preflight = nil
			}
		} else if b.Preflight != nil {
			s.Scripts[idx].Preflight = b.Preflight
		}
		if b.VerCol != nil {
			s.Scripts[idx].VerCol = strings.TrimSpace(*b.VerCol)
		}
		if b.VerPfxs != nil {
			s.Scripts[idx].VerPfxs = strings.TrimSpace(*b.VerPfxs)
		}
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		id := s.Scripts[idx].ID
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "id": id, "tree": buildTree(s)})

	case r.Method == http.MethodDelete && p == "/api/script":
		id := r.URL.Query().Get("id")
		mu.Lock()
		s := loadStore()
		var keep []Script
		for _, sc := range s.Scripts {
			if sc.ID != id {
				keep = append(keep, sc)
			}
		}
		s.Scripts = keep
		if err := saveStore(s); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "tree": buildTree(s)})

	case r.Method == http.MethodGet && p == "/api/script":
		id := r.URL.Query().Get("id")
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		sc, ok := findScriptByID(s, id)
		if !ok {
			writeJSON(w, 404, map[string]any{"error": "脚本不存在"})
			return
		}
		ds, hasDS, pre, col, pfxs := effectiveFor(s, sc)
		groupName := ""
		for _, g := range s.Groups {
			if g.ID == sc.GroupID {
				groupName = g.Name
			}
		}
		writeJSON(w, 200, map[string]any{"script": map[string]any{
			"id": sc.ID, "name": sc.Name, "sql": sc.SQL, "groupId": sc.GroupID,
			"groupName": groupName, "dsName": ds.Name, "dsId": ds.ID, "hasDs": hasDS,
			"preflight": pre, "versionCol": col, "versionPrefixes": strings.Join(pfxs, ","),
			"ownVersionCol": sc.VerCol, "ownVersionPrefixes": sc.VerPfxs,
			"ownPreflight": sc.Preflight, "env": envOf(ds),
		}})

	case r.Method == http.MethodPost && p == "/api/preflight":
		var b struct {
			ScriptID string `json:"scriptId"`
			SQL      string `json:"sql"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		sc, ok := findScriptByID(s, b.ScriptID)
		if !ok {
			writeJSON(w, 404, map[string]any{"ok": false, "error": "脚本不存在"})
			return
		}
		ds, hasDS, _, vcol, vpfxs := effectiveFor(s, sc)
		if !hasDS {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "这个脚本还没绑定数据源（顶层文件夹或脚本本身要选一个连接）"})
			return
		}
		text := b.SQL
		if text == "" {
			text = sc.SQL
		}
		stmts := splitStatements(text)
		if len(stmts) == 0 {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "没有 SQL"})
			return
		}
		if ds.Host == "" || ds.User == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "所属数据源还没配全"})
			return
		}
		pfStart := time.Now()
		checks, versions, err := preflight(ds, stmts, vcol, vpfxs)
		if err == nil {
			checks = append(validateText(text), checks...)
		}
		if err != nil {
			writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error(), "checks": []map[string]any{}, "versions": []map[string]any{}})
			return
		}
		hasErr := false
		for _, c := range checks {
			if c["level"] == "error" {
				hasErr = true
			}
		}
		previews := make([]string, len(stmts))
		for i, st := range stmts {
			previews[i] = preview(st)
		}
		writeJSON(w, 200, map[string]any{"ok": !hasErr, "checks": checks, "versions": versions, "count": len(stmts), "previews": previews, "ms": time.Since(pfStart).Milliseconds()})

	case r.Method == http.MethodPost && p == "/api/check":
		var b struct {
			ScriptID string `json:"scriptId"`
			SQL      string `json:"sql"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		sc, ok := findScriptByID(s, b.ScriptID)
		if !ok {
			writeJSON(w, 404, map[string]any{"ok": false, "error": "脚本不存在"})
			return
		}
		ds, hasDS, _, _, _ := effectiveFor(s, sc)
		if !hasDS {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "这个脚本还没绑定数据源（顶层文件夹或脚本本身要选一个连接）"})
			return
		}
		text := b.SQL
		if text == "" {
			text = sc.SQL
		}
		stmts := splitStatements(text)
		if len(stmts) == 0 {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "没有 SQL"})
			return
		}
		if ds.Host == "" || ds.User == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "所属数据源还没配全"})
			return
		}
		results, allOK := checkStatements(ds, stmts)
		previews := make([]string, len(stmts))
		for i, st := range stmts {
			previews[i] = preview(st)
		}
		writeJSON(w, 200, map[string]any{"ok": allOK, "results": results, "count": len(stmts), "previews": previews})

	case r.Method == http.MethodPost && p == "/api/run":
		var b struct {
			ScriptID string           `json:"scriptId"`
			SQL      string           `json:"sql"`
			DryRun   *bool            `json:"dryRun"`
			Versions []map[string]any `json:"versions"`
		}
		if err := readJSON(r, &b); err != nil {
			writeJSON(w, 400, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		mu.Lock()
		s := loadStore()
		mu.Unlock()
		sc, ok := findScriptByID(s, b.ScriptID)
		if !ok {
			writeJSON(w, 404, map[string]any{"ok": false, "error": "脚本不存在"})
			return
		}
		ds, hasDS, _, _, _ := effectiveFor(s, sc)
		if !hasDS {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "这个脚本还没绑定数据源（顶层文件夹或脚本本身要选一个连接）"})
			return
		}
		if ds.Host == "" || ds.User == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "所属数据源还没配全"})
			return
		}
		text := b.SQL
		if text == "" {
			text = sc.SQL
		}
		stmts := splitStatements(text)
		if len(stmts) == 0 {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "没有可执行的 SQL"})
			return
		}
		dry := true
		if b.DryRun != nil {
			dry = *b.DryRun
		}

		// 事务由工具统一管理：脚本里自带 COMMIT/ROLLBACK 会直接破坏预演语义
		var txnIdx, ddlIdx []int
		for i, st := range stmts {
			if isTxnControl(st) {
				txnIdx = append(txnIdx, i)
			}
			if isDDL(st) {
				ddlIdx = append(ddlIdx, i)
			}
		}
		if len(txnIdx) > 0 {
			writeJSON(w, 400, map[string]any{"ok": false,
				"error": "第 " + idxList(txnIdx) + " 条是 COMMIT/ROLLBACK/START TRANSACTION。事务由工具管理，请删掉这些语句再执行（否则预演无法回滚）"})
			return
		}
		if len(ddlIdx) > 0 {
			writeJSON(w, 400, map[string]any{"ok": false,
				"error": "第 " + idxList(ddlIdx) + " 条是 DDL（CREATE/ALTER/DROP/TRUNCATE 等）。MySQL 的 DDL 会隐式提交，一旦中断无法整体回滚，已拒绝执行。请把 DDL 单独拿出来在客户端执行"})
			return
		}

		if !runMu.TryLock() {
			writeJSON(w, 409, map[string]any{"ok": false, "error": "已有脚本正在执行，请等它结束再试"})
			return
		}
		results, failedAt, errMsg, ms := func() (map[string]stmtResult, int, string, int64) {
			defer runMu.Unlock()
			return runStatements(ds, stmts, dry)
		}()
		previews := make([]string, len(stmts))
		for i, st := range stmts {
			previews[i] = preview(st)
		}
		logPreviews := make([]string, len(previews))
		for i, pv := range previews {
			logPreviews[i] = clipRunes(pv, 200)
		}
		okRun := errMsg == ""
		errShort := errMsg
		if len([]rune(errShort)) > 600 {
			errShort = string([]rune(errShort)[:600])
		}
		var totalAffected int64
		for _, r := range results {
			totalAffected += r.Affected
		}
		appendLog(map[string]any{
			"ts": time.Now().Format(time.RFC3339), "script": sc.Name, "ds": ds.Name,
			"dryRun": dry, "stmts": len(stmts), "database": ds.Database,
			"ok": okRun, "ms": ms, "failedAt": failedAt, "error": errShort,
			"previews": logPreviews, "affected": totalAffected, "versions": b.Versions,
		})
		writeJSON(w, 200, map[string]any{
			"ok": okRun, "ms": ms, "count": len(stmts), "failedAt": failedAt,
			"previews": previews, "results": results, "error": errMsg, "affected": totalAffected,
		})

	case r.Method == http.MethodGet && p == "/api/logs":
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				limit = n
			}
		}
		wantOK := r.URL.Query().Get("ok")   // "" | true | false
		wantDry := r.URL.Query().Get("dry") // "" | true | false
		var logs []map[string]any
		b, err := readLogTail(2 << 20)
		if err == nil {
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			for i := len(lines) - 1; i >= 0 && len(logs) < limit; i-- {
				if lines[i] == "" {
					continue
				}
				var m map[string]any
				if json.Unmarshal([]byte(lines[i]), &m) != nil {
					continue
				}
				if wantOK != "" {
					if v, ok := m["ok"].(bool); !ok || v != (wantOK == "true") {
						continue
					}
				}
				if wantDry != "" {
					if v, ok := m["dryRun"].(bool); !ok || v != (wantDry == "true") {
						continue
					}
				}
				logs = append(logs, m)
			}
		}
		if logs == nil {
			logs = []map[string]any{}
		}
		writeJSON(w, 200, map[string]any{"logs": logs})

	case r.Method == http.MethodDelete && p == "/api/logs":
		if r.URL.Query().Get("confirm") != "1" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "清空日志需要显式确认（?confirm=1）"})
			return
		}
		if err := os.Remove(filepath.Join(logsDir, "run-log.jsonl")); err != nil && !os.IsNotExist(err) {
			writeJSON(w, 500, map[string]any{"ok": false, "error": "删除日志失败：" + err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})

	default:
		writeJSON(w, 404, map[string]any{"error": "not found"})
	}
}

// -mkrelease：由程序自己生成 version.json 与 SHA256SUMS，发版不依赖外部脚本
func makeRelease(args []string) {
	fs := flag.NewFlagSet("mkrelease", flag.ExitOnError)
	dir := fs.String("dir", ".", "包含安装包的目录")
	ver := fs.String("version", "", "版本号，例如 1.0.5")
	build := fs.String("build", buildTime, "构建号")
	notes := fs.String("notes", "", "更新说明")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if *ver == "" {
		fmt.Fprintln(os.Stderr, "缺少 -version")
		os.Exit(2)
	}
	files := map[string]any{}
	sums := make([]string, 0, 2)
	for _, osname := range []string{"win", "mac"} {
		// 支持随机后缀：medupdate-win-1.0.5-a8f3c2.zip
		matches, _ := filepath.Glob(filepath.Join(*dir, fmt.Sprintf("medupdate-%s-*.zip", osname)))
		if len(matches) != 1 {
			fmt.Fprintf(os.Stderr, "%s 目录下应当正好有一个 medupdate-%s-*.zip，实际 %d 个\n", *dir, osname, len(matches))
			os.Exit(2)
		}
		p := matches[0]
		name := filepath.Base(p)
		b, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "读不到 %s：%v\n", p, err)
			os.Exit(2)
		}
		sum := sha256.Sum256(b)
		hexs := hex.EncodeToString(sum[:])
		files[osname] = map[string]string{"url": name, "sha256": hexs}
		sums = append(sums, hexs+"  "+name)
	}
	man := map[string]any{"version": *ver, "build": *build, "notes": *notes, "files": files}
	mj, err := json.MarshalIndent(man, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := os.WriteFile(filepath.Join(*dir, "version.json"), append(mj, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if err := os.WriteFile(filepath.Join(*dir, "SHA256SUMS"), []byte(strings.Join(sums, "\n")+"\n"), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	fmt.Printf("已生成 %s/version.json（%s，%s）\n", *dir, *ver, *build)
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-mkrelease" {
		makeRelease(os.Args[2:])
		return
	}
	appHome = os.Getenv("SQLC_HOME")
	if appHome == "" {
		exe, err := os.Executable()
		if err == nil {
			appHome = filepath.Dir(exe)
		} else {
			appHome = "."
		}
	}
	dataFile = filepath.Join(appHome, "data.json")
	logsDir = filepath.Join(appHome, "logs")
	_ = os.MkdirAll(logsDir, 0o700)
	_ = os.Chmod(logsDir, 0o700) // 老版本建的是 0755，这里统一收紧

	port := os.Getenv("PORT")
	if port == "" {
		port = "8787"
	}
	host := os.Getenv("HOST")
	if host == "" {
		host = "0.0.0.0"
	}

	var h http.Handler = http.HandlerFunc(handler)
	if key := os.Getenv("ACCESS_KEY"); key != "" && key != "off" {
		next := h
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			given := r.URL.Query().Get("k")
			if given == "" {
				given = r.Header.Get("x-key")
			}
			if given != key {
				w.WriteHeader(401)
				_, _ = w.Write([]byte("401"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	addr := net.JoinHostPort(host, port)
	fmt.Println()
	fmt.Println("  update-tool v" + appVersion)
	fmt.Printf("  本机   http://127.0.0.1:%s/\n", port)
	for _, ip := range lanIPs() {
		fmt.Printf("  局域网 http://%s:%s/\n", ip, port)
	}
	fmt.Printf("  数据   %s\n\n", dataFile)

	openBrowser("http://127.0.0.1:" + port + "/")
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func lanIPs() []string {
	var out []string
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.To4() != nil && !ipnet.IP.IsLoopback() {
				out = append(out, ipnet.IP.String())
			}
		}
	}
	return out
}
