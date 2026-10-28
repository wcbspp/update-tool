package main

// SQL 执行台 —— 单文件、零运行时依赖的 Go 版
// 数据源 / 分组 / 脚本，校验（EXPLAIN）、预演（事务回滚）、执行

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
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

// 预检里的重查询（行数、重复、冲突、影响行数）各自限时。
// 等满 DQL 超时（可能是几分钟）会把“执行前预检”变成折磨；
// 超时立刻降级成提醒或执行计划估算，不会卡住更新。秒数可在数据源里配（pfSec）。
func pfCtx(sec int) (context.Context, context.CancelFunc) {
	if sec <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), time.Duration(sec)*time.Second)
}

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
	PFSec   int `json:"pfSec"`   // 预检里单条重查询超时，秒，默认 20；-1 表示不限（只受 DQL 约束）
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

// dsPFSec：预检里行数/重复/冲突/影响行数这类重查询的单条上限。
// 默认 20 秒；填 -1 表示不加这条上限，交给 DQL 超时控制。
func dsPFSec(d DataSource) int {
	if d.PFSec < 0 {
		return 0
	}
	if d.PFSec > 0 {
		return d.PFSec
	}
	return 20
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
	Preflight  *bool  `json:"preflight"` // nil = 默认开启（供根级脚本/new 文件夹使用）
	VerCol     string `json:"versionCol"`
	VerPfxs    string `json:"versionPrefixes"`
	UpdateURL  string `json:"updateUrl"`  // 升级源地址
	ListenMode string `json:"listenMode"` // local（默认，仅本机）| all（局域网）
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
			// MySQL 只有 “-- ”（后面跟空白）才是注释；a--b 是两个减号
			if i+2 < len(rs) && rs[i+2] != ' ' && rs[i+2] != '\t' && rs[i+2] != '\n' && rs[i+2] != '\r' {
				buf.WriteRune(c)
				break
			}
			inLine = true
			i++
			buf.WriteRune('\n')
		case c == '#':
			inLine = true
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
	for _, p := range []string{"CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "GRANT", "REVOKE",
		// 这些也会隐式提交（或自行控制事务），同样会让“失败整体回滚”失效
		"LOCK TABLES", "UNLOCK TABLES", "ANALYZE", "OPTIMIZE", "REPAIR", "CALL", "FLUSH", "INSTALL", "UNINSTALL"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

func isQuery(s string) bool {
	t := strings.ToUpper(strings.TrimSpace(s))
	if strings.HasPrefix(t, "WITH") {
		// CTE 后面接的可能是 UPDATE/DELETE，不能一律当查询（否则 affected 会报 0）
		return firstDMLIsSelect(s)
	}
	for _, p := range []string{"SELECT", "SHOW", "DESC", "DESCRIBE", "EXPLAIN", "TABLE"} {
		if strings.HasPrefix(t, p) {
			return true
		}
	}
	return false
}

// firstDMLIsSelect：跳过 CTE/括号，找到顶层第一个 DML 关键字，判断是不是 SELECT
func firstDMLIsSelect(s string) bool {
	rs := []rune(s)
	depth, inS, inD, inBt := 0, false, false, false
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		switch {
		case inS:
			if c == '\\' {
				i++
			} else if c == '\'' {
				inS = false
			}
			continue
		case inD:
			if c == '\\' {
				i++
			} else if c == '"' {
				inD = false
			}
			continue
		case inBt:
			if c == '`' {
				inBt = false
			}
			continue
		}
		switch c {
		case '\'':
			inS = true
			continue
		case '"':
			inD = true
			continue
		case '`':
			inBt = true
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 || c > 'z' || c < 'A' {
			continue
		}
		for _, kw := range []string{"SELECT", "UPDATE", "INSERT", "DELETE", "REPLACE"} {
			if i+len(kw) <= len(rs) && strings.EqualFold(string(rs[i:i+len(kw)]), kw) {
				prevOK := i == 0 || !isWordRune(rs[i-1])
				nextOK := i+len(kw) == len(rs) || !isWordRune(rs[i+len(kw)])
				if prevOK && nextOK {
					return kw == "SELECT"
				}
			}
		}
	}
	return true
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

var sessWarnOnce sync.Once

// sessInitConnector：每条新连接建好后设置 SELECT 超时（5.7.8 以下/代理不支持时忽略错误）
type sessInitConnector struct {
	base driver.Connector
	ms   int
}

func (c sessInitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if c.ms > 0 {
		if ex, ok := conn.(driver.ExecerContext); ok {
			if _, err := ex.ExecContext(ctx, fmt.Sprintf("SET SESSION max_execution_time = %d", c.ms), nil); err != nil {
				sessWarnOnce.Do(func() {
					log.Printf("提示：服务端不支持 max_execution_time（%v），预检将只靠客户端超时兜底", err)
				})
			}
		}
	}
	return conn, nil
}

func (c sessInitConnector) Driver() driver.Driver { return c.base.Driver() }

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

	// max_execution_time 是会话级设置：连接被丢弃后重新建连必须重设。
	// 用连接器在每次建连时执行，避免“超时一次之后就没保护了”。
	base, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(sessInitConnector{base: base, ms: dsDQLSec(ds) * 1000})
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

const appVersion = "1.0.17"

var runMu sync.Mutex

type cell = any

func scanRows(rows *sql.Rows) ([]string, [][]cell, bool, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, false, err
	}
	out := [][]cell{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, false, err
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
	truncated := false
	if len(out) >= maxRows && rows.Next() {
		truncated = true // 还有更多行，只是不展示
	}
	return cols, out, truncated, rows.Err()
}

type stmtResult struct {
	Affected   int64    `json:"affected"`
	RolledBack bool     `json:"rolledBack,omitempty"`
	Info       string   `json:"info,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	Rows       [][]cell `json:"rows"`
	Truncated  bool     `json:"truncated,omitempty"`
	MS         int64    `json:"ms"`
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

	// 超时按「每条语句」算：整段共用一个上下文时，一条慢语句会把
	// 前面已经成功的语句一起拖到超时，用户看到的是整批回滚。
	baseCtx := context.Background()
	dmlSec := dsDMLSec(ds)
	perStmt := func() (context.Context, context.CancelFunc) {
		if dmlSec > 0 {
			return context.WithTimeout(baseCtx, time.Duration(dmlSec)*time.Second)
		}
		return context.WithCancel(baseCtx)
	}

	tx, err := db.BeginTx(baseCtx, nil)
	if err != nil {
		return res, -1, err.Error(), time.Since(start).Milliseconds()
	}

	fail := func(i int, e error) (map[string]stmtResult, int, string, int64) {
		_ = tx.Rollback()
		// 整批已回滚：前面语句的“变更行数”不能留在结果和合计里
		for k, r := range res {
			r.Affected = 0
			r.RolledBack = true
			res[k] = r
		}
		return res, i, withHint(e.Error()), time.Since(start).Milliseconds()
	}

	for i, st := range stmts {
		t0 := time.Now()
		sctx, cancel := perStmt()
		if isQuery(st) {
			rows, err := tx.QueryContext(sctx, st)
			if err != nil {
				cancel()
				return fail(i, err)
			}
			cols, data, truncated, err := scanRows(rows)
			rows.Close()
			cancel()
			if err != nil {
				return fail(i, err)
			}
			if data == nil {
				data = [][]cell{}
			}
			res[strconv.Itoa(i)] = stmtResult{Columns: cols, Rows: data, Truncated: truncated, MS: time.Since(t0).Milliseconds()}
		} else {
			r, err := tx.ExecContext(sctx, st)
			if err != nil {
				cancel()
				return fail(i, err)
			}
			cancel()
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
	// DELETE t1, t2 FROM ... 与 DELETE FROM t USING ...
	reDeleteMulti = regexp.MustCompile(`(?is)^\s*delete\s+(?:low_priority\s+|quick\s+|ignore\s+)*(.+?)\s+from\s+`)
	// 单表：DELETE [修饰词] FROM t [AS a] 后面直接是 WHERE/ORDER/LIMIT/结尾，
	// 不能再有 from（否则是子查询，属于单表语句，不是多表 DELETE）
	reDeleteSingle = regexp.MustCompile(`(?is)^\s*delete\s+(?:low_priority\s+|quick\s+|ignore\s+)*from\s+[\x60"]?[\w$.]+[\x60"]?\s*(?:as\s+[\x60"]?[\w$]+[\x60"]?)?\s*(?:where\b|order\s+by\b|limit\b|;?\s*$)`)
	reDeleteUsing  = regexp.MustCompile(`(?is)^\s*delete\s+from\s+[\x60"]?([\w$.]+)[\x60"]?\s+using\s+`)
	reIdent        = regexp.MustCompile(`^[\x60"]?([\w$.]+)[\x60"]?`)
	reBatch        = regexp.MustCompile(`(?i)([A-Za-z0-9_]+)_update_([A-Za-z0-9]+)`)
)

// 5.6 / Linux 下表名区分大小写，先按不区分大小写解析出真实表名
func resolveTable(db *sql.DB, schema, name string) string {
	n, _ := resolveTableOK(db, schema, name)
	return n
}

// 第二返回值表示「查到了真实表名」；查不到（超时/中间件限制）时不要缓存，下次重试
func resolveTableOK(db *sql.DB, schema, name string) (string, bool) {
	var actual string
	err := db.QueryRow(`SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) LIMIT 1`, schema, name).Scan(&actual)
	if err == nil && actual != "" {
		return actual, true
	}
	return name, false
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
		// 关键字前可以是空格、换行、Tab（SQL 里换行很常见），后面必须是边界
		if depth == 0 && i > 0 && isSpaceByte(low[i-1]) && strings.HasPrefix(low[i:], kw) {
			rest := low[i+len(kw):]
			if rest == "" || isSpaceByte(rest[0]) || rest[0] == '(' {
				return s[:i]
			}
		}
	}
	return s
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
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

// countWhereBounded：按条件有界计数，最多数到 cap 行就停
func countWhereBounded(db *sql.DB, from, where string, cap int64, pfSec int) (int64, bool, error) {
	q := "SELECT COUNT(*) FROM (SELECT 1 FROM " + from
	if strings.TrimSpace(where) != "" {
		q += " WHERE " + where
	}
	q += fmt.Sprintf(" LIMIT %d) x", cap+1)
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, false, err
	}
	if n > cap {
		return cap, true, nil
	}
	return n, false, nil
}

// tableCountBounded：最多数到 cap 行就停，避免大表全表扫描。
// 第三个返回值区分「真的是 0 行」和「没数出来（超时/权限）」——
// 两者绝不能混为一谈：把超时当成空表会让用户去重灌数据。
func tableCountBounded(db *sql.DB, t string, cap int64, pfSec int) (int64, bool, error) {
	var n int64
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	q := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s LIMIT %d) x", quoteIdent(t), cap+1)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, false, err
	}
	if n > cap {
		return cap, true, nil
	}
	return n, false, nil
}

// 表名限定：schema.table
func qualTable(schema, table string) string {
	if schema == "" {
		return quoteIdent(table)
	}
	return quoteIdent(schema) + "." + quoteIdent(table)
}

// 唯一键：优先主键；没有主键就用“所有列都 NOT NULL”的唯一索引（唯一索引允许多个 NULL，不能用来判重）
// 元数据查询入口，测试里可替换以模拟「中间件限制 information_schema」
var infQuery = func(db *sql.DB, q string, args ...any) (*sql.Rows, error) {
	return db.Query(q, args...)
}
var infQueryRow = func(db *sql.DB, q string, args ...any) *sql.Row {
	return db.QueryRow(q, args...)
}

/* ---------------- 预检元数据缓存 ---------------- */
// 同一次预检里，一张表的元数据（真实表名/唯一键/索引/列）会被反复查询。
// 本机每次只几毫秒，DRDS 上每次几十毫秒，累积起来是预检慢的主因之一。
// 缓存挂在 *sql.DB 上，预检结束即删除，不跨请求、不共享。

var metaStore sync.Map

type metaCache struct {
	mu  sync.Mutex
	tbl map[string]*metaTable
}

type metaTable struct {
	cols  map[string]bool // 大写列名 → 是否可空
	gotC  bool
	idx   map[string][]string
	ord   []string
	gotI  bool
	keys  []string
	keyNm string
	gotK  bool
	name  string
	gotN  bool
}

func metaOf(db *sql.DB) *metaCache {
	if v, ok := metaStore.Load(db); ok {
		return v.(*metaCache)
	}
	v, _ := metaStore.LoadOrStore(db, &metaCache{tbl: map[string]*metaTable{}})
	return v.(*metaCache)
}

func metaDrop(db *sql.DB) { metaStore.Delete(db) }

func (mc *metaCache) entry(schema, table string) *metaTable {
	k := strings.ToUpper(schema) + "\x00" + strings.ToUpper(table)
	mc.mu.Lock()
	defer mc.mu.Unlock()
	e, ok := mc.tbl[k]
	if !ok {
		e = &metaTable{}
		mc.tbl[k] = e
	}
	return e
}

// 一次取回整表列的可空性；information_schema 报错或为空都退回 SHOW COLUMNS
func fetchCols(db *sql.DB, schema, table string) map[string]bool {
	out := map[string]bool{}
	rows, err := infQuery(db, `SELECT UPPER(COLUMN_NAME), UPPER(IS_NULLABLE) FROM information_schema.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)`, schema, table)
	if err == nil {
		for rows.Next() {
			var n, nul string
			if rows.Scan(&n, &nul) == nil {
				out[n] = strings.EqualFold(nul, "YES")
			}
		}
		rows.Close()
	}
	if len(out) > 0 {
		return out
	}
	rows2, err := db.Query("SHOW COLUMNS FROM " + qualTable(schema, table))
	if err != nil {
		return out
	}
	defer rows2.Close()
	names, err := rows2.Columns()
	if err != nil {
		return out
	}
	iField, iNull := -1, -1
	for i, c := range names {
		if strings.EqualFold(c, "Field") {
			iField = i
		}
		if strings.EqualFold(c, "Null") {
			iNull = i
		}
	}
	if iField < 0 {
		return out
	}
	for rows2.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if rows2.Scan(ptrs...) != nil {
			continue
		}
		var f, nul string
		if b, ok := vals[iField].([]byte); ok {
			f = string(b)
		}
		if iNull >= 0 {
			if b, ok := vals[iNull].([]byte); ok {
				nul = string(b)
			}
		}
		if f != "" {
			out[strings.ToUpper(f)] = strings.EqualFold(nul, "YES")
		}
	}
	return out
}

func mCols(db *sql.DB, schema, table string) map[string]bool {
	mc := metaOf(db)
	e := mc.entry(schema, table)
	mc.mu.Lock()
	if e.gotC {
		m := e.cols
		mc.mu.Unlock()
		return m
	}
	mc.mu.Unlock()
	m := fetchCols(db, schema, table)
	if len(m) > 0 { // 读失败（空）不缓存，下次重试，避免一次抖动把整次预检判成“没有列”
		mc.mu.Lock()
		e.cols, e.gotC = m, true
		mc.mu.Unlock()
	}
	return m
}

func mHasColumn(db *sql.DB, schema, table, col string) bool {
	_, ok := mCols(db, schema, table)[strings.ToUpper(col)]
	return ok
}

func mColNullable(db *sql.DB, schema, table, col string) (bool, bool) {
	v, ok := mCols(db, schema, table)[strings.ToUpper(col)]
	return v, ok
}

func mColumnCount(db *sql.DB, schema, table string) int { return len(mCols(db, schema, table)) }

func mIndexes(db *sql.DB, schema, table string) (map[string][]string, []string) {
	mc := metaOf(db)
	e := mc.entry(schema, table)
	mc.mu.Lock()
	if e.gotI {
		i, o := e.idx, e.ord
		mc.mu.Unlock()
		return i, o
	}
	mc.mu.Unlock()
	idx, ord := indexesOf(db, schema, table)
	if len(idx) > 0 { // 同上：读失败不缓存
		mc.mu.Lock()
		e.idx, e.ord, e.gotI = idx, ord, true
		mc.mu.Unlock()
	}
	return idx, ord
}

func mIndexNames(db *sql.DB, schema, table string) []string {
	idx, order := mIndexes(db, schema, table)
	out := make([]string, 0, 3)
	for _, name := range order {
		cols := idx[name]
		if len(cols) == 0 {
			continue
		}
		out = append(out, name+"("+strings.Join(cols, ",")+")")
		if len(out) >= 3 {
			break
		}
	}
	return out
}

func mKeyCols(db *sql.DB, schema, t string) ([]string, string) {
	mc := metaOf(db)
	e := mc.entry(schema, t)
	mc.mu.Lock()
	if e.gotK {
		c, n := e.keys, e.keyNm
		mc.mu.Unlock()
		return c, n
	}
	mc.mu.Unlock()
	cols, name := keyCols(db, schema, t)
	if len(cols) > 0 { // 同上：读失败不缓存
		mc.mu.Lock()
		e.keys, e.keyNm, e.gotK = cols, name, true
		mc.mu.Unlock()
	}
	return cols, name
}

func mResolveTable(db *sql.DB, schema, name string) string {
	mc := metaOf(db)
	e := mc.entry(schema, name)
	mc.mu.Lock()
	if e.gotN {
		n := e.name
		mc.mu.Unlock()
		return n
	}
	mc.mu.Unlock()
	n, ok := resolveTableOK(db, schema, name)
	if ok { // 失败不缓存，避免一次抖动把整次预检的表名固化错
		mc.mu.Lock()
		e.name, e.gotN = n, true
		mc.mu.Unlock()
	}
	return n
}

func mTablesHavingCol(db *sql.DB, schema string, refs []tblRef, col string) []tblRef {
	out := make([]tblRef, 0)
	for _, r := range refs {
		if mHasColumn(db, schema, r.table, col) {
			out = append(out, r)
		}
	}
	return out
}

func keyCols(db *sql.DB, schema, t string) ([]string, string) {
	rows, err := infQuery(db, `SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX
		FROM information_schema.STATISTICS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) AND NON_UNIQUE=0
		ORDER BY (INDEX_NAME='PRIMARY') DESC, INDEX_NAME, SEQ_IN_INDEX`, schema, t)
	if err != nil {
		// 华为 DRDS 等中间件可能不支持这类查询，直接走 SHOW INDEX
		return keyColsViaShowIndex(db, schema, t)
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
	// 一次性取该表所有列的可空性，避免逐列查询
	nullableCols := map[string]bool{}
	if rows, err := infQuery(db, `SELECT UPPER(COLUMN_NAME), UPPER(IS_NULLABLE) FROM information_schema.COLUMNS
		WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?)`, schema, t); err == nil {
		for rows.Next() {
			var name, nul string
			if rows.Scan(&name, &nul) == nil {
				nullableCols[name] = strings.EqualFold(nul, "YES")
			}
		}
		rows.Close()
	}

	for _, it := range list {
		ok := true
		for _, c := range it.cols {
			if nullableCols[strings.ToUpper(c)] {
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

// dupCount: 源表内按判重键统计“重复的组数”，最多数到 capCap 组就停
func dupCountCapped(db *sql.DB, table string, cols []string, capCap, pfSec int) (int64, bool, error) {
	if len(cols) == 0 {
		return 0, false, nil
	}
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, quoteIdent(c))
	}
	// 内层 LIMIT 让“重复很多”的情况提前收手，不必数完整个分组结果
	q := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s GROUP BY %s HAVING COUNT(*)>1 LIMIT %d) t",
		quoteIdent(table), strings.Join(quoted, ","), capCap+1)
	var n int64
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	err := db.QueryRowContext(ctx, q).Scan(&n)
	if n > int64(capCap) {
		return int64(capCap), true, err
	}
	return n, false, err
}

// 判重列映射：INSERT ... SELECT 里，目标判重列到底取自源表哪一列。
// 只有「SELECT 列表全是普通列引用，且与列清单按位置对得上」才给结论；
// 出现表达式（id+1）、函数、常量时实际插入值不是源表原值，不能据此判断冲突。
func insertKeyMap(sel, colList string, pk []string) (map[string]string, bool) {
	if len(pk) == 0 {
		return map[string]string{}, false
	}
	body := strings.TrimSpace(sel)
	if len(body) < 6 || !strings.EqualFold(body[:6], "select") {
		return map[string]string{}, false
	}
	body = strings.TrimSpace(body[6:])
	for {
		low := strings.ToLower(body)
		if strings.HasPrefix(low, "distinct ") {
			body = strings.TrimSpace(body[len("distinct "):])
			continue
		}
		if strings.HasPrefix(low, "all ") {
			body = strings.TrimSpace(body[len("all "):])
			continue
		}
		break
	}
	body = cutAtKeyword(body, "from")
	items := splitTopLevel(body)
	if len(items) == 0 {
		return map[string]string{}, false
	}

	// INSERT INTO t SELECT * FROM b：没写列清单且整表投影，值按位置对应同名判重列
	if strings.TrimSpace(colList) == "" && len(items) == 1 {
		it := strings.TrimSpace(items[0])
		if it == "*" || (strings.HasSuffix(it, ".*") && !strings.ContainsAny(it, " ()")) {
			out := map[string]string{}
			for _, c := range pk {
				u := strings.ToUpper(c)
				out[u] = u
			}
			return out, true
		}
	}

	var cols []string
	if strings.TrimSpace(colList) != "" {
		for _, c := range splitTopLevel(colList) {
			cols = append(cols, strings.ToUpper(trimIdent(strings.TrimSpace(c))))
		}
		if len(cols) != len(items) {
			return map[string]string{}, false
		}
	}

	out := map[string]string{}
	for idx, it := range items {
		col := plainColumnRef(it)
		if col == "" {
			return map[string]string{}, false
		}
		if cols != nil {
			out[cols[idx]] = col
		}
	}
	if cols == nil {
		for _, c := range pk {
			u := strings.ToUpper(c)
			out[u] = u
		}
	}
	for _, c := range pk {
		if _, ok := out[strings.ToUpper(c)]; !ok {
			return map[string]string{}, false
		}
	}
	return out, true
}

var rePlainCol = regexp.MustCompile("^[`\"]?[A-Za-z0-9_$]+[`\"]?(?:\\.[`\"]?[A-Za-z0-9_$]+[`\"]?)?$")
var reAsAlias = regexp.MustCompile(`(?is)^([\s\S]*\S)\s+as\s+[\x60"]?[\w$]+[\x60"]?$`)

// 普通列引用（可带表别名前缀、可带 AS 别名），返回不带限定符的列名；不是普通列就返回空
func plainColumnRef(item string) string {
	it := strings.TrimSpace(item)
	if m := reAsAlias.FindStringSubmatch(it); m != nil {
		it = strings.TrimSpace(m[1])
	}
	if !rePlainCol.MatchString(it) {
		return ""
	}
	if i := strings.LastIndex(it, "."); i >= 0 {
		it = it[i+1:]
	}
	return strings.ToUpper(strings.Trim(it, "`\""))
}

// conflictCountPairs 按「目标列 ↔ 源列」的对应关系数冲突行，最多数到 capN 行
func conflictCountPairs(db *sql.DB, target, src string, pairs [][2]string, capN, pfSec int) (int64, bool, error) {
	var conds []string
	for _, p := range pairs {
		conds = append(conds, "t."+quoteIdent(p[0])+"=s."+quoteIdent(p[1]))
	}
	q := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 FROM %s s STRAIGHT_JOIN %s t ON %s LIMIT %d) x",
		quoteIdent(src), quoteIdent(target), strings.Join(conds, " AND "), capN+1)
	var n int64
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	err := db.QueryRowContext(ctx, q).Scan(&n)
	if n > int64(capN) {
		return int64(capN), true, err
	}
	return n, false, err
}

func conflictCountCapped(db *sql.DB, target, src string, pk []string, capN, pfSec int) (int64, bool, error) {
	// 用小批次表驱动，避免优化器拿几千万行的目标表当驱动表去全表扫
	pairs := make([][2]string, 0, len(pk))
	for _, c := range pk {
		pairs = append(pairs, [2]string{c, c})
	}
	return conflictCountPairs(db, target, src, pairs, capN, pfSec)
}

// 每张批次表的版本号：期望前缀的值 + 不该出现的值（标红用）
func versionReport(db *sql.DB, schema string, tables []string, col string, pfxs []string, pfSec int) []map[string]any {
	out := make([]map[string]any, 0, len(tables))

	for _, rawT := range tables {
		t := mResolveTable(db, schema, rawT)
		if !mHasColumn(db, schema, t, col) {
			out = append(out, map[string]any{"table": t, "field": col, "missing": true})
			continue
		}

		if len(pfxs) == 0 {
			// 没配前缀：列出表里实际出现的值；读失败要单独标出来，不能和“没有值”混在一起
			vals, more, err := distinctVals(db, schema, t, col, 21, pfSec)
			item := map[string]any{
				"table": t, "field": col, "ok": vals, "other": []string{}, "missing": false,
				"note": "未配置版本前缀，仅列出该字段现有值",
			}
			if err != nil {
				item["err"] = "读取失败：" + err.Error()
				item["ok"] = []string{}
			} else if more {
				item["note"] = "未配置版本前缀，仅显示前 21 个值（还有更多）"
			}
			out = append(out, item)
			continue
		}
		conds := make([]string, 0, len(pfxs))
		for _, p := range pfxs {
			conds = append(conds, quoteIdent(col)+" LIKE '"+strings.ReplaceAll(p, "'", "''")+"%'")
		}
		match := "(" + strings.Join(conds, " OR ") + ")"

		pick := func(where string, limit int) []string {
			q := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s AND %s IS NOT NULL LIMIT %d",
				quoteIdent(col), qualTable(schema, t), where, quoteIdent(col), limit)
			ctx, cancel := pfCtx(pfSec)
			defer cancel()
			rows, err := db.QueryContext(ctx, q)
			if err != nil {
				return nil
			}
			defer rows.Close()
			return scanVals(rows)
		}

		okVals := pick(match, 21)
		otherVals := pick("NOT "+match, 11)
		out = append(out, map[string]any{
			"table": t, "field": col, "ok": okVals, "other": otherVals, "missing": false,
		})
	}
	return out
}

/* ---------------- 预检专用连接池 ---------------- */
// 每条连接都单独设置过 DQL 超时（openDB 里有 SET SESSION），最多 3 条并发，
// 避免为了并行把服务端超时保护丢掉，也不至于在 DRDS 上开太多连接。

type pfConns struct {
	ds   DataSource
	mu   sync.Mutex
	idle []*sql.DB
	all  []*sql.DB
	sem  chan struct{}
}

func newPfConns(ds DataSource) *pfConns {
	return &pfConns{ds: ds, sem: make(chan struct{}, 3)}
}

func (p *pfConns) take() *sql.DB {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		db := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return db
	}
	p.mu.Unlock()
	db, err := openDB(p.ds)
	if err != nil {
		return nil
	}
	p.mu.Lock()
	p.all = append(p.all, db)
	p.mu.Unlock()
	return db
}

func (p *pfConns) give(db *sql.DB) {
	if db == nil {
		return
	}
	p.mu.Lock()
	p.idle = append(p.idle, db)
	p.mu.Unlock()
}

func (p *pfConns) Close() {
	p.mu.Lock()
	all := p.all
	p.all, p.idle = nil, nil
	p.mu.Unlock()
	for _, db := range all {
		metaDrop(db)
		db.Close()
	}
}

// parallel 并发跑检查；开不出新连接就退回主连接，结果仍然正确，只是退化为串行
func (p *pfConns) parallel(primary *sql.DB, fns ...func(*sql.DB)) {
	var wg sync.WaitGroup
	for _, fn := range fns {
		if fn == nil {
			continue
		}
		wg.Add(1)
		go func(f func(*sql.DB)) {
			defer wg.Done()
			p.sem <- struct{}{}
			defer func() { <-p.sem }()
			c := p.take()
			if c == nil {
				f(primary)
				return
			}
			defer p.give(c)
			f(c)
		}(fn)
	}
	wg.Wait()
}

var reWhereWord = regexp.MustCompile(`(?is)\bwhere\b`)

// 取最后一段顶层 WHERE 之后的条件
// 只认顶层的 WHERE：子查询里的 WHERE、字符串里的 where 都不算
func whereTail(st string) string {
	rs := []rune(st)
	depth := 0
	inS, inD, inBt := false, false, false
	last := -1
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		switch {
		case inS:
			if c == '\\' {
				i++
				continue
			}
			if c == '\'' {
				inS = false
			}
			continue
		case inD:
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inD = false
			}
			continue
		case inBt:
			if c == '`' {
				inBt = false
			}
			continue
		}
		switch c {
		case '\'':
			inS = true
		case '"':
			inD = true
		case '`':
			inBt = true
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && (c == 'w' || c == 'W') && i+5 <= len(rs) && strings.EqualFold(string(rs[i:i+5]), "where") {
				prevOK := i == 0 || !isWordRune(rs[i-1])
				nextOK := i+5 == len(rs) || !isWordRune(rs[i+5])
				if prevOK && nextOK {
					last = i + 5
				}
			}
		}
	}
	if last < 0 {
		return ""
	}
	return strings.TrimSpace(string(rs[last:]))
}

func isWordRune(r rune) bool {
	return r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r > 127
}

var reDeleteLimit = regexp.MustCompile(`(?is)\blimit\s+(\d+)\s*;?\s*$`)

// DELETE 自带的 LIMIT n（顶层，在语句末尾）
func deleteLimit(st string) int64 {
	m := reDeleteLimit.FindStringSubmatch(strings.TrimSpace(st))
	if m == nil {
		return 0
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	return n
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

func estimateUpdate(db *sql.DB, schema, st string, pfSec int) (int64, string, string, bool) {
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
			table = mResolveTable(db, schema, t)
		}
	}
	if table == "" {
		return 0, "", "", false
	}

	where := whereRaw(st)

	// 先做有界精确计数：最多数到 30 万行就停，不会因为表大而拖死。
	q := "SELECT COUNT(*) FROM (SELECT 1 FROM " + tablesPart
	if where != "" {
		q += " WHERE " + where
	}
	q += fmt.Sprintf(" LIMIT %d) x", capN+1)
	var n int64
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	if err := db.QueryRowContext(ctx, q).Scan(&n); err == nil {
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
func trxWarnings(db *sql.DB, schema string, pfSec int) []map[string]any {
	out := make([]map[string]any, 0)
	// 只看当前库的事务：INNODB_TRX 不带库名，借 PROCESSLIST 关联。
	// p.db 为空（连库时没选默认库）的行也保留，并在文案里注明库未知，避免整类漏报。
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT t.trx_id, DATE_FORMAT(t.trx_started,'%H:%i:%s'), t.trx_rows_modified,
			IFNULL(p.db,''), IFNULL(p.user,'')
		FROM information_schema.INNODB_TRX t
		LEFT JOIN information_schema.PROCESSLIST p ON p.id = t.trx_mysql_thread_id
		WHERE t.trx_rows_modified > 0 AND (p.db = ? OR p.db IS NULL OR ? = '')`, schema, schema)
	if err != nil {
		// 关联失败（中间件/权限）时至少报个数，别静默当没有。
		// 用新 ctx：上面那条可能已经因超时把 ctx 取消了。
		fctx, fcancel := pfCtx(pfSec)
		defer fcancel()
		var n int
		if e := db.QueryRowContext(fctx, `SELECT COUNT(*) FROM information_schema.INNODB_TRX WHERE trx_rows_modified > 0`).Scan(&n); e == nil && n > 0 {
			out = append(out, map[string]any{
				"stmt": -1, "level": "warn", "title": "有未提交事务",
				"detail": fmt.Sprintf("库里有 %d 个事务改了行还没提交，但读不到它属于哪个库；工具看到的可能是提交前的数据", n),
			})
		}
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id, started, dbName, user string
		var n int64
		if rows.Scan(&id, &started, &n, &dbName, &user) != nil {
			continue
		}
		who := "会话"
		if user != "" {
			who = user
		}
		where := dbName
		if where == "" {
			where = "库未知"
		}
		out = append(out, map[string]any{
			"stmt": -1, "level": "warn", "title": "有未提交事务",
			"detail": "事务 " + id + " 由 " + who + " 在 " + where + " 已修改 " + strconv.FormatInt(n, 10) + " 行，开始于 " + started +
				"。如果这是你在客户端里的删改，记得 COMMIT；否则工具（另一个会话）看到的仍是提交前的数据",
		})
	}
	// 没有 PROCESSLIST 权限时 JOIN 会返回 0 行但不报错：再数一次总量，避免整类静默
	if len(out) == 0 {
		cctx, ccancel := pfCtx(pfSec)
		defer ccancel()
		var total int
		if e := db.QueryRowContext(cctx, `SELECT COUNT(*) FROM information_schema.INNODB_TRX WHERE trx_rows_modified > 0`).Scan(&total); e == nil && total > 0 {
			out = append(out, map[string]any{
				"stmt": -1, "level": "warn", "title": "有未提交事务",
				"detail": fmt.Sprintf("库里有 %d 个事务改了行还没提交，但读不到明细（可能没有 PROCESSLIST 权限）；工具看到的可能是提交前的数据", total),
			})
		}
	}
	return out
}

func preflight(ds DataSource, stmts []string, verColName string, verPfxs []string) ([]map[string]any, []map[string]any, error) {
	db, err := openDB(ds)
	if err != nil {
		return nil, nil, err
	}
	defer db.Close()
	defer metaDrop(db)

	conns := newPfConns(ds)
	defer conns.Close()
	pfSec := dsPFSec(ds)

	schema := ds.Database
	if schema == "" {
		_ = db.QueryRow("SELECT DATABASE()").Scan(&schema)
	}
	out := make([]map[string]any, 0)
	add := func(i int, level, title, detail string) {
		out = append(out, map[string]any{"stmt": i, "level": level, "title": title, "detail": detail})
	}
	// 超过 300ms 的检查在详情里标出耗时，方便定位哪一步慢
	addD := func(i int, level, title, detail string, d time.Duration) {
		if d > 300*time.Millisecond {
			detail += fmt.Sprintf("（耗时 %d ms）", d.Milliseconds())
		}
		add(i, level, title, detail)
	}

	type stmtTarget struct {
		stmt  int
		where string
	}
	targetUse := map[string][]stmtTarget{}

	type delTask struct {
		i     int
		stmt  string
		table string
		where string
		lim   int64
	}
	var delPending []delTask

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
			target := mResolveTable(db, schema, normTable(m[1]))
			colList := strings.TrimSpace(m[2])
			add(i, "ok", "目标表", target)
			fm := reFrom.FindStringSubmatch(m[3])
			if fm == nil {
				add(i, "warn", "逐个值插入", "INSERT ... VALUES：行数由常量列表决定，预检不做数据比对")
				continue
			}
			src := mResolveTable(db, schema, normTable(fm[1]))
			if src == target {
				add(i, "error", "自己插自己", fmt.Sprintf("源表和目标表都是 %s", target))
				continue
			}
			// 带列清单的 INSERT 不做整表列数比对（列数本来就允许不同）
			if colList == "" {
				tc, scc := mColumnCount(db, schema, target), mColumnCount(db, schema, src)
				if tc > 0 && scc > 0 && tc != scc {
					add(i, "error", "列数不一致", fmt.Sprintf("%s 有 %d 列，%s 有 %d 列，INSERT ... SELECT 会直接报错", target, tc, src, scc))
				}
			}
			pk, idxName := mKeyCols(db, schema, target)
			// 判重列在源表里对应哪一列：SELECT 里换过顺序、写过表达式就说不准，不做结论
			srcOf, mapOK := insertKeyMap(m[3], colList, pk)

			// 源表行数 / 源表重复键 / 与目标表的冲突互不依赖，且都是重查询：并发跑
			var (
				n, dup, conf                int64
				capped, dupTrunc, confTrunc bool
				cntErr, dupErr, confErr     error
				durCnt, durDup, durConf     time.Duration
			)
			tasks := []func(*sql.DB){func(c *sql.DB) {
				t0 := time.Now()
				n, capped, cntErr = tableCountBounded(c, src, 300000, pfSec)
				durCnt = time.Since(t0)
			}}
			if mapOK && len(pk) > 0 {
				srcCols := make([]string, 0, len(pk))
				pairs := make([][2]string, 0, len(pk))
				for _, c := range pk {
					srcCols = append(srcCols, srcOf[strings.ToUpper(c)])
					pairs = append(pairs, [2]string{c, srcOf[strings.ToUpper(c)]})
				}
				tasks = append(tasks, func(c *sql.DB) {
					t0 := time.Now()
					dup, dupTrunc, dupErr = dupCountCapped(c, src, srcCols, 1000, pfSec)
					durDup = time.Since(t0)
				})
				tasks = append(tasks, func(c *sql.DB) {
					t0 := time.Now()
					conf, confTrunc, confErr = conflictCountPairs(c, target, src, pairs, 1000, pfSec)
					durConf = time.Since(t0)
				})
			}
			conns.parallel(db, tasks...)

			switch {
			case cntErr != nil:
				addD(i, "warn", "行数统计失败", fmt.Sprintf("%s 的行数没数出来（超时或权限）：%s。不影响执行，判重照常进行", src, withHint(cntErr.Error())), durCnt)
			case n == 0:
				addD(i, "warn", "源表为空", fmt.Sprintf("%s 没有数据，这条 INSERT 不会插入任何行", src), durCnt)
			default:
				label := fmt.Sprintf("%s 共 %d 行", src, n)
				if capped {
					label = fmt.Sprintf("%s 共 ≥%d 行", src, n)
				}
				addD(i, "ok", "源表非空", label, durCnt)
			}
			if len(pk) == 0 {
				add(i, "warn", "目标表无主键或唯一索引", fmt.Sprintf("%s 既没有主键也没有唯一索引：重复执行会插入重复数据，且无法判断这批是否已经跑过", target))
				continue
			}
			if !strings.EqualFold(idxName, "PRIMARY") {
				add(i, "ok", "使用唯一索引判重", fmt.Sprintf("%s 无主键，改用唯一索引 %s（%s）", target, idxName, strings.Join(pk, ", ")))
			}
			for _, c := range pk {
				if nullable, ok := mColNullable(db, schema, target, c); ok && nullable {
					add(i, "warn", "判重键里有可空列", fmt.Sprintf("%s.%s 允许 NULL：NULL 不参与唯一性比较，重复数据可能漏检", target, c))
					break
				}
			}
			if cntErr == nil && n > 0 {
				if capped {
					add(i, "warn", "预计影响行数", fmt.Sprintf("至少 %d 行（数到 30 万就停了，实际可能更多）", n))
				} else {
					add(i, "ok", "预计影响行数", fmt.Sprintf("预计插入 %d 行（精确计数）", n))
				}
			}
			if !mapOK {
				add(i, "warn", "判重未做自动比对", "SELECT 列表里有表达式、函数，或与列清单按位置对不上，无法确定插入值取自源表哪一列；请人工确认这批是否已执行过")
				continue
			}
			if dupErr != nil {
				add(i, "warn", "源表重复检查未完成", "查询失败或超时："+withHint(dupErr.Error()))
			} else if dup > 0 {
				msg := fmt.Sprintf("%s 内有 %d 组重复主键，插入会失败", src, dup)
				if dupTrunc {
					msg = fmt.Sprintf("%s 内至少有 %d 组重复主键（数到 1000 就停了），插入会失败", src, dup)
				}
				addD(i, "error", "源表主键重复", msg, durDup)
			}
			if confErr != nil {
				add(i, "warn", "主键冲突检查未完成", "查询失败或超时（表可能很大）："+withHint(confErr.Error()))
			} else if conf > 0 {
				msg := fmt.Sprintf("%s 已存在 %d 行相同主键，INSERT 会失败（大概率这批已执行过）", target, conf)
				if confTrunc {
					msg = fmt.Sprintf("%s 至少已存在 %d 行相同主键（数到 1000 就停了），INSERT 会失败（大概率这批已执行过）", target, conf)
				}
				addD(i, "error", "主键冲突", msg, durConf)
			} else {
				addD(i, "ok", "无主键冲突", fmt.Sprintf("与 %s 比对无重复", target), durConf)
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
					refs = append(refs, tblRef{mResolveTable(db, schema, t), a})
				}
			}
			if len(refs) < 2 {
				// 单表 UPDATE：没有外部数据源，但目标表相关的检查仍然要做
				if len(refs) == 1 {
					r := refs[0]
					if whereTail(st) == "" {
						add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请写成 WHERE 1=1）")
					}
					for _, col := range unqual {
						if !mHasColumn(db, schema, r.table, col) {
							add(i, "error", "SET 列不存在", fmt.Sprintf("要更新的列 %s 在 %s 里找不到", col, r.table))
						}
					}
					add(i, "ok", "被更新表", fmt.Sprintf("%s（单表更新）", r.table))
					targetUse[r.table] = append(targetUse[r.table], stmtTarget{stmt: i, where: whereRaw(st)})
					if n, ok := estimateByExplain(db, st, r.table, r.alias); ok {
						tail := " · 全表扫（数据量小时优化器会这样选，不代表缺索引）"
						if keys := explainIndexes(db, st); keys != "" {
							tail = " · 索引 " + keys
						} else if names := mIndexNames(db, schema, r.table); len(names) > 0 {
							tail = " · 全表扫（表上有 " + strings.Join(names, "、") + "，数据少时可能不走）"
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
					owners := mTablesHavingCol(db, schema, refs, col)
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

			if whereTail(st) == "" {
				add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请改成 WHERE 1=1 再跑）")
			}
			for _, col := range unqual {
				hit := false
				for _, r := range refs {
					if !updated[strings.ToLower(r.alias)] {
						continue
					}
					if mHasColumn(db, schema, r.table, col) {
						hit = true
						break
					}
				}
				if !hit && len(updated) > 0 {
					add(i, "error", "SET 列不存在", fmt.Sprintf("要更新的列 %s 在目标表里找不到", col))
				}
			}

			targetTable := ""
			for _, ref := range refs {
				if updated[strings.ToLower(ref.alias)] {
					targetTable = ref.table
					break
				}
			}
			// 影响行数估算和各源表行数互不依赖：并发跑，再按原顺序输出
			type srcCnt struct {
				n      int64
				capped bool
				err    error
				dur    time.Duration
			}
			var (
				estN            int64
				estNote, estIdx string
				estOK           bool
				estDur          time.Duration
				cntMu           sync.Mutex
				cnts            = map[string]srcCnt{}
			)
			tasks := []func(*sql.DB){func(c *sql.DB) {
				t0 := time.Now()
				estN, estNote, estIdx, estOK = estimateUpdate(c, schema, st, pfSec)
				estDur = time.Since(t0)
			}}
			for _, r := range refs {
				if updated[strings.ToLower(r.alias)] {
					continue
				}
				rt := r.table
				tasks = append(tasks, func(c *sql.DB) {
					t0 := time.Now()
					n, capped, cerr := tableCountBounded(c, rt, 300000, pfSec)
					cntMu.Lock()
					cnts[rt] = srcCnt{n: n, capped: capped, err: cerr, dur: time.Since(t0)}
					cntMu.Unlock()
				})
			}
			conns.parallel(db, tasks...)

			if estOK {
				tail := " · 全表扫（数据量小时优化器会这样选，不代表缺索引）"
				if estIdx != "" {
					tail = " · 索引 " + estIdx
				} else if names := mIndexNames(db, schema, targetTable); len(names) > 0 {
					tail = " · 全表扫（表上有 " + strings.Join(names, "、") + "，数据少时可能不走）"
				}
				switch estNote {
				case "cap":
					addD(i, "warn", "预计影响行数", fmt.Sprintf("≥ %d 行（数到 30 万就停了，实际可能更多）%s", estN, tail), estDur)
				case "explain":
					addD(i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算）%s", estN, tail), estDur)
				default:
					addD(i, "ok", "预计影响行数", fmt.Sprintf("预计更新 %d 行（精确计数，最多数 30 万）%s", estN, tail), estDur)
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
				cr := cnts[r.table]
				if cr.err != nil {
					addD(i, "warn", "行数统计失败", fmt.Sprintf("%s（别名 %s）的行数没数出来（超时或权限）：%s", r.table, r.alias, withHint(cr.err.Error())), cr.dur)
				} else if cr.n == 0 {
					addD(i, "warn", "源表为空", fmt.Sprintf("%s（别名 %s）没有数据，这条 UPDATE 不会命中任何行", r.table, r.alias), cr.dur)
				} else {
					label := fmt.Sprintf("%s（别名 %s）共 %d 行", r.table, r.alias, cr.n)
					if cr.capped {
						label = fmt.Sprintf("%s（别名 %s）共 ≥%d 行", r.table, r.alias, cr.n)
					}
					addD(i, "ok", "数据来源表", label, cr.dur)
				}
			}
			if len(updated) == 0 {
				add(i, "warn", "无法判断被更新表", "SET 左值既没别名也无法按列名定位，已跳过源表为空检查")
			}

		case stmtKind(st) == "delete":
			lim := deleteLimit(st)
			where := whereRaw(st)

			// 三种写法：单表、DELETE t FROM t JOIN ...、DELETE FROM t USING t JOIN ...
			var targets []string
			countTable := ""
			multi := false
			switch {
			case reDeleteUsing.MatchString(st):
				mm := reDeleteUsing.FindStringSubmatch(st)
				targets = append(targets, mResolveTable(db, schema, normTable(mm[1])))
				multi = true // USING 的 WHERE 通常引用关联表，单表计数必然失败，走 EXPLAIN
			case reDeleteSingle.MatchString(st), reDelete.MatchString(st) && !reDeleteMulti.MatchString(st):
				// 单表（WHERE 里可以有子查询）
				mm := reDeleteUsing.FindStringSubmatch(st)
				_ = mm
				m := reDelete.FindStringSubmatch(st)
				if m == nil {
					continue
				}
				targets = append(targets, mResolveTable(db, schema, normTable(m[1])))
				countTable = targets[0]
			case reDeleteMulti.MatchString(st):
				mm := reDeleteMulti.FindStringSubmatch(st)
				if strings.Contains(mm[1], "(") {
					// 括号里的 from 是子查询，不是多表写法
					m := reDelete.FindStringSubmatch(st)
					if m == nil {
						continue
					}
					targets = append(targets, mResolveTable(db, schema, normTable(m[1])))
					countTable = targets[0]
					break
				}
				for _, part := range splitTopLevel(mm[1]) {
					if t, _ := parseTableRef(part); t != "" {
						targets = append(targets, mResolveTable(db, schema, t))
					}
				}
				multi = true
			default:
				m := reDelete.FindStringSubmatch(st)
				if m == nil {
					continue
				}
				targets = append(targets, mResolveTable(db, schema, normTable(m[1])))
				countTable = targets[0]
			}

			for _, tg := range targets {
				add(i, "ok", "被删除表", tg)
				targetUse[tg] = append(targetUse[tg], stmtTarget{stmt: i, where: where})
			}
			if multi {
				add(i, "warn", "多表 DELETE", "按关联条件删除，行数受关联结果影响，预检不做精确统计，请自行确认范围")
				if n, ok := estimateByExplain(db, st, targets[0], ""); ok {
					add(i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算）", n))
				}
			}
			if whereTail(st) == "" {
				if lim > 0 {
					add(i, "warn", "没有 WHERE 条件", fmt.Sprintf("这条 DELETE 没有 WHERE，按 LIMIT %d 最多删 %d 行，请确认不是笔误", lim, lim))
				} else {
					add(i, "warn", "没有 WHERE 条件", "这条 DELETE 会清空整张表，请确认不是笔误")
				}
			}
			if countTable != "" {
				delPending = append(delPending, delTask{i: i, stmt: st, table: countTable, where: where, lim: lim})
			}
		}
	}

	// DELETE 的行数统一并发跑，避免多条 DELETE 一条接一条等到超时
	if len(delPending) > 0 {
		type delRes struct {
			n      int64
			capped bool
			err    error
			dur    time.Duration
		}
		res := make([]delRes, len(delPending))
		tasks := make([]func(*sql.DB), len(delPending))
		for k := range delPending {
			k := k
			pp := delPending[k]
			tasks[k] = func(c *sql.DB) {
				t0 := time.Now()
				res[k].n, res[k].capped, res[k].err = countWhereBounded(c, qualTable(schema, pp.table), pp.where, 300000, pfSec)
				res[k].dur = time.Since(t0)
			}
		}
		conns.parallel(db, tasks...)
		for k, pp := range delPending {
			r := res[k]
			if r.err != nil {
				if n, ok := estimateByExplain(db, pp.stmt, pp.table, ""); ok {
					add(pp.i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算，行数没数完）", n))
				} else {
					add(pp.i, "warn", "预计影响行数未计算", "查询超时或语句太复杂（大表上常见），不影响执行")
				}
				continue
			}
			switch {
			case pp.lim > 0 && r.capped:
				addD(pp.i, "warn", "预计影响行数", fmt.Sprintf("至少 %d 行匹配，最多删 %d 行（LIMIT %d）", r.n, pp.lim, pp.lim), r.dur)
			case pp.lim > 0:
				hit := pp.lim
				if r.n < pp.lim {
					hit = r.n
				}
				addD(pp.i, "ok", "预计影响行数", fmt.Sprintf("预计删除 %d 行（LIMIT %d，精确计数）", hit, pp.lim), r.dur)
			case r.capped:
				addD(pp.i, "warn", "预计影响行数", fmt.Sprintf("至少 %d 行（数到 30 万就停了，实际可能更多）", r.n), r.dur)
			default:
				addD(pp.i, "ok", "预计影响行数", fmt.Sprintf("预计删除 %d 行（精确计数，最多数 30 万）", r.n), r.dur)
			}
		}
	}

	useTables := make([]string, 0, len(targetUse))
	for t := range targetUse {
		useTables = append(useTables, t)
	}
	sort.Strings(useTables)
	for _, t := range useTables {
		uses := targetUse[t]
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

	// 版本核对与前面的语句检查互不依赖，用另一条连接并发跑；
	// 没配版本字段时不做这项，免得界面上一堆“无  字段”
	if strings.TrimSpace(verColName) == "" {
		batchTables = nil
	}
	var versions []map[string]any
	var vwg sync.WaitGroup
	vwg.Add(1)
	go func() {
		defer vwg.Done()
		c := conns.take()
		if c == nil {
			versions = versionReport(db, schema, batchTables, verColName, verPfxs, pfSec)
			return
		}
		defer conns.give(c)
		versions = versionReport(c, schema, batchTables, verColName, verPfxs, pfSec)
	}()
	out = append(out, trxWarnings(db, schema, pfSec)...)
	vwg.Wait()
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

func fetchVersion(base string, fresh bool) (map[string]any, error) {
	updMu.Lock()
	defer updMu.Unlock()
	if !fresh && updCache != nil && updBase == base && time.Since(updAt) < 60*time.Second {
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
			"connSec": dsConnSec(d), "dqlSec": dsDQLSec(d), "dmlSec": dsDMLSec(d), "pfSec": d.PFSec,
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
		lm := st.Settings.ListenMode
		if lm == "" {
			lm = "local"
		}
		writeJSON(w, 200, map[string]any{"preflight": preflightEnabled(st), "updateUrl": st.Settings.UpdateURL, "listenMode": lm})

	case r.Method == http.MethodPost && p == "/api/settings":
		var b struct {
			Preflight  *bool   `json:"preflight"`
			UpdateURL  *string `json:"updateUrl"`
			ListenMode *string `json:"listenMode"`
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
		restart := false
		if b.ListenMode != nil {
			m := strings.TrimSpace(*b.ListenMode)
			if m != "all" {
				m = "local"
			}
			if m != st.Settings.ListenMode {
				st.Settings.ListenMode = m
				restart = true
			}
		}
		if err := saveStore(st); err != nil {
			mu.Unlock()
			saveFailed(w, err)
			return
		}
		mu.Unlock()
		writeJSON(w, 200, map[string]any{"ok": true, "preflight": preflightEnabled(st), "needRestart": restart})
		if restart {
			go func() {
				time.Sleep(500 * time.Millisecond)
				restartSelf()
			}()
		}

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
		mu.Lock()
		stH := loadStore()
		mu.Unlock()
		mode := stH.Settings.ListenMode
		if mode == "" {
			mode = "local"
		}
		writeJSON(w, 200, map[string]any{"ok": true, "version": appVersion, "build": buildTime,
			"pid": os.Getpid(), "listenMode": mode, "host": os.Getenv("HOST")})

	case r.Method == http.MethodPost && p == "/api/shutdown":
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		if host != "127.0.0.1" && host != "::1" {
			writeJSON(w, 403, map[string]any{"ok": false, "error": "只允许本机调用"})
			return
		}
		writeJSON(w, 200, map[string]any{"ok": true})
		go func() {
			time.Sleep(300 * time.Millisecond)
			os.Exit(0)
		}()

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
		ver, err := fetchVersion(base, r.URL.Query().Get("fresh") == "1")
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
			PFSec         *flexInt `json:"pfSec"`
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
		if b.PFSec != nil {
			ds.PFSec = int(*b.PFSec)
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
		if err := db.QueryRow("SELECT VERSION()").Scan(&ver); err != nil || ver == "" {
			ver = "未知版本"
		}
		if err := db.QueryRow("SELECT DATABASE()").Scan(&curDB); err != nil || curDB == "" {
			curDB = "(未选择库)"
		}
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
			// 预检失败（超时、连不上）也留一条记录，便于事后排查
			errShort := err.Error()
			if len([]rune(errShort)) > 600 {
				errShort = string([]rune(errShort)[:600])
			}
			appendLog(map[string]any{
				"ts": time.Now().Format(time.RFC3339), "type": "preflight",
				"script": sc.Name, "ds": ds.Name, "database": ds.Database,
				"dryRun": false, "stmts": len(stmts), "ok": false,
				"ms": time.Since(pfStart).Milliseconds(), "failedAt": -1, "error": errShort,
			})
			writeJSON(w, 200, map[string]any{"ok": false, "error": err.Error(), "checks": []map[string]any{}, "versions": []map[string]any{}})
			return
		}
		hasErr := false
		errN, warnN := 0, 0
		for _, c := range checks {
			switch c["level"] {
			case "error":
				hasErr = true
				errN++
			case "warn":
				warnN++
			}
		}
		previews := make([]string, len(stmts))
		for i, st := range stmts {
			previews[i] = preview(st)
		}
		// 每次预检都留一条记录（成功也记），便于回看当时结论
		appendLog(map[string]any{
			"ts": time.Now().Format(time.RFC3339), "type": "preflight",
			"script": sc.Name, "ds": ds.Name, "database": ds.Database,
			"dryRun": false, "stmts": len(stmts), "ok": !hasErr,
			"ms": time.Since(pfStart).Milliseconds(), "failedAt": -1,
			"errCount": errN, "warnCount": warnN,
		})
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
		chkStart := time.Now()
		results, allOK := checkStatements(ds, stmts)
		previews := make([]string, len(stmts))
		for i, st := range stmts {
			previews[i] = preview(st)
		}
		writeJSON(w, 200, map[string]any{"ok": allOK, "results": results, "count": len(stmts),
			"previews": previews, "ms": time.Since(chkStart).Milliseconds()})

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
				"error": "第 " + idxList(ddlIdx) + " 条是 DDL 或会隐式提交的语句（CREATE/ALTER/DROP/TRUNCATE/LOCK TABLES/ANALYZE/CALL 等）。它们会让“失败整体回滚”失效，已拒绝执行。请把这类语句单独拿出来在客户端执行"})
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

// 端口上是否已有本程序在跑；返回它对外的版本号
func runningVersion(port string) (string, bool) {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port + "/api/health")
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	var h struct {
		Version string `json:"version"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&h) != nil {
		return "", false
	}
	return h.Version, true
}

// ensureInstance：返回 true 表示应当由本进程提供服务
func ensureInstance(port string) bool {
	ver, alive := runningVersion(port)
	if !alive {
		return true
	}
	if ver == appVersion && os.Getenv("MEDUPDATE_RESTART") != "1" {
		fmt.Printf("  已有同版本实例在运行（v%s），直接打开浏览器\n", ver)
		return false
	}
	fmt.Printf("  检测到旧版本 v%s 正在运行，正在让它退出…\n", ver)
	c := &http.Client{Timeout: 3 * time.Second}
	if resp, err := c.Post("http://127.0.0.1:"+port+"/api/shutdown", "application/json", nil); err == nil {
		resp.Body.Close()
	}
	for i := 0; i < 60; i++ {
		if _, still := runningVersion(port); !still {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	return true
}

// restartSelf：起一个新进程接管，然后自己退出
func restartSelf() {
	exe, err := os.Executable()
	if err != nil {
		return
	}
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), "MEDUPDATE_RESTART=1")
	cmd.Dir = appHome
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Println("  自动重启失败，请手动重新打开：", err)
		return
	}
	os.Exit(0)
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
		mu.Lock()
		st := loadStore()
		mu.Unlock()
		if st.Settings.ListenMode == "all" {
			host = "0.0.0.0"
		} else {
			host = "127.0.0.1"
		}
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

	if !ensureInstance(port) {
		openBrowser("http://127.0.0.1:" + port + "/")
		return
	}
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

// distinctVals：某列的前 limit 个不同值（非空）。多取一条用来判断“还有更多”。
func distinctVals(db *sql.DB, schema, table, col string, limit, pfSec int) ([]string, bool, error) {
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL LIMIT %d",
		quoteIdent(col), qualTable(schema, table), quoteIdent(col), limit+1))
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	vals := scanVals(rows)
	if len(vals) > limit {
		return vals[:limit], true, nil
	}
	return vals, false, nil
}

func scanVals(rows *sql.Rows) []string {
	vals := make([]string, 0)
	for rows.Next() {
		var v sql.NullString
		if rows.Scan(&v) == nil && v.Valid {
			vals = append(vals, v.String)
		}
	}
	return vals
}
