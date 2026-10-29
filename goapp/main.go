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
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
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

//go:embed index.html guide.html icon.svg icon-32.png icon-180.png
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
		// 只有「文件不存在」才算第一次启动；读失败（权限、磁盘）必须进只读保护，
		// 否则下一次保存会把整份配置覆盖成空
		if errors.Is(err, fs.ErrNotExist) {
			storeCorrupt = false
			return s
		}
		storeCorrupt = true
		log.Printf("data.json 读取失败（%v），已进入只读保护，避免覆盖原文件", err)
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
	// 文件里是明文库口令：已存在的临时文件不会被 O_CREATE 改权限，这里强制收紧。
	// 收不紧不阻断保存——丢配置比权限宽松严重得多，但要留个痕迹
	if err := f.Chmod(0o600); err != nil {
		log.Printf("提示：无法把 %s 权限收紧到 600（%v），保存继续", tmp, err)
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
	if err := os.Rename(tmp, dataFile); err != nil {
		return err
	}
	// rename 之后同步目录项，否则断电可能回退到上一次保存
	if d, err := os.Open(filepath.Dir(dataFile)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
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

// MySQL 8.0 的 CTE：WITH a AS (...) [, b AS (...)] UPDATE/DELETE/...
// 预检要看的是主语句，CTE 定义本身不用分析（也不影响行数判断）
func stripCTE(st string) string {
	t := strings.TrimSpace(st)
	if len(t) < 4 || !strings.EqualFold(t[:4], "with") {
		return st
	}
	low := strings.ToLower(t)
	depth := 0
	inS, inD, inBt := false, false, false
	for i := 4; i < len(low); i++ {
		c := low[i]
		if inS {
			if c == '\\' {
				i++
				continue
			}
			if c == '\'' {
				inS = false
			}
			continue
		}
		if inD {
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inD = false
			}
			continue
		}
		if inBt {
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
			if depth != 0 || (c < 'a' || c > 'z') {
				continue
			}
			for _, kw := range []string{"select", "insert", "replace", "update", "delete"} {
				if strings.HasPrefix(low[i:], kw) {
					prevOK := i == 0 || !isWordRune(rune(low[i-1]))
					nextOK := i+len(kw) == len(low) || !isWordRune(rune(low[i+len(kw)]))
					if prevOK && nextOK {
						return strings.TrimSpace(t[i:])
					}
				}
			}
		}
	}
	return st
}

func stmtKind(st string) string {
	up := strings.ToUpper(strings.TrimSpace(stripCTE(st)))
	switch {
	case strings.HasPrefix(up, "INSERT"), strings.HasPrefix(up, "REPLACE"):
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
	text = strings.TrimPrefix(text, "\ufeff") // 从记事本/Excel 粘贴常带 BOM
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
			if c == '\\' {
				// MySQL 默认 NO_BACKSLASH_ESCAPES=off，'O\'Brien' 里的反斜杠是转义符
				buf.WriteRune(c)
				if i+1 < len(rs) {
					i++
					buf.WriteRune(rs[i])
				}
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

const appVersion = "1.0.19"

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
		rbErr := tx.Rollback()
		// 整批已回滚：前面语句的“变更行数”不能留在结果和合计里
		for k, r := range res {
			r.Affected = 0
			r.RolledBack = true
			res[k] = r
		}
		msg := withHint(e.Error())
		// 回滚本身失败时，绝不能告诉用户「已回滚」——那会让人放心重跑，数据被改两遍
		if rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			msg = "回滚可能未完成（" + rbErr.Error() + "）。请先人工核对前 " + strconv.Itoa(i) + " 条语句的影响，不要直接重跑。原始错误：" + msg
		}
		return res, i, msg, time.Since(start).Milliseconds()
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
		if isDDL(st) || isQuery(st) {
			// SHOW / DESC / TABLE 前面加 EXPLAIN 会被 MySQL 判成语法错误，这类不校验
			note := "DDL 无法静态校验"
			if !isDDL(st) {
				note = "SHOW / DESC 类语句无法用 EXPLAIN 校验"
			}
			results = append(results, map[string]any{"i": i, "ok": true, "skipped": true, "note": note})
			continue
		}
		ctxX, cancelX := explainCtx()
		defer cancelX()
		rows, err := db.QueryContext(ctxX, "EXPLAIN "+st)
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
	reInsert = regexp.MustCompile(`(?is)^\s*(?:insert|replace)\s+(?:low_priority\s+|delayed\s+|high_priority\s+)?(?:ignore\s+)?(?:into\s+)?[\x60"]?([\w$.]+)[\x60"]?\s*(?:partition\s*\([^)]*\)\s*)?(?:\(([^)]*)\)\s*)?((?:select|values?|set)\b.*)$`)
	reFrom   = regexp.MustCompile(`(?is)\bfrom\s+[\x60"]?([\w$.]+)[\x60"]?`)
	reDelete = regexp.MustCompile(`(?is)^\s*delete\s+from\s+[\x60"]?([\w$.]+)`)
	// DELETE t1, t2 FROM ... 与 DELETE FROM t USING ...
	reDeleteMulti = regexp.MustCompile(`(?is)^\s*delete\s+(?:low_priority\s+|quick\s+|ignore\s+)*(.+?)\s+from\s+`)
	// 单表：DELETE [修饰词] FROM t [AS a] 后面直接是 WHERE/ORDER/LIMIT/结尾，
	// 不能再有 from（否则是子查询，属于单表语句，不是多表 DELETE）
	reReleaseZip   = regexp.MustCompile(`^medupdate-(win|mac|linux)-\d+\.\d+\.\d+-[0-9a-f]{6}\.zip$`)
	reDeleteMods   = regexp.MustCompile(`(?is)^\s*delete\s+(?:(?:low_priority|quick|ignore)\s+)+`)
	reDeleteSingle = regexp.MustCompile(`(?is)^\s*delete\s+(?:low_priority\s+|quick\s+|ignore\s+)*from\s+[\x60"]?[\w$.]+[\x60"]?\s*(?:as\s+[\x60"]?[\w$]+[\x60"]?)?\s*(?:where\b|order\s+by\b|limit\b|;?\s*$)`)
	reDeleteUsing  = regexp.MustCompile(`(?is)^\s*delete\s+from\s+[\x60"]?([\w$.]+)[\x60"]?\s+using\s+`)
	reIdent        = regexp.MustCompile(`^[\x60"]?([\w$.]+)[\x60"]?`)
	reBatch        = regexp.MustCompile(`(?i)([A-Za-z0-9_]+)_update_([A-Za-z0-9]+)`)
	reIdentTok     = regexp.MustCompile("[A-Za-z_][A-Za-z0-9_$]*(?:\\.[A-Za-z_][A-Za-z0-9_$]*)?|,")
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
	n = strings.TrimSuffix(n, ".*") // DELETE t1.* FROM ... 里的 .*
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

// EXPLAIN 也要有界：服务端不支持 max_execution_time 时（5.6/MariaDB），
// 没有 context 就可能一直挂着并占住全局忙碌锁
func explainCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 20*time.Second)
}

type tblRef struct{ table, alias string }

// 用执行计划估算影响行数：不执行查询，只看 EXPLAIN 的 rows
func estimateByExplain(db *sql.DB, st, target, alias string) (int64, bool) {
	ctxX, cancelX := explainCtx()
	defer cancelX()
	rows, err := db.QueryContext(ctxX, "EXPLAIN "+st)
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

// SHOW INDEX 的统一解析：中间件限制 information_schema 时两个调用点共用
type showIdxRow struct {
	name, col             string
	nonUniq, hasNonUniq   bool
	nullable, hasNullable bool
}

func showIndexRows(db *sql.DB, schema, t string) ([]showIdxRow, bool) {
	qualified := quoteIdent(t)
	if schema != "" {
		qualified = quoteIdent(schema) + "." + qualified
	}
	rows, err := db.Query("SHOW INDEX FROM " + qualified)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, false
	}
	iName, iCol, iNonUniq, iNull := -1, -1, -1, -1
	for i, c := range cols {
		switch strings.ToLower(c) {
		case "key_name":
			iName = i
		case "column_name":
			iCol = i
		case "non_unique":
			iNonUniq = i
		case "null":
			iNull = i
		}
	}
	if iName < 0 || iCol < 0 {
		return nil, false
	}
	var got []showIdxRow
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
			if b, ok := vals[i].([]byte); ok {
				return string(b)
			}
			return fmt.Sprintf("%v", vals[i])
		}
		r := showIdxRow{name: cell(iName), col: cell(iCol)}
		if iNonUniq >= 0 {
			r.hasNonUniq = true
			r.nonUniq = cell(iNonUniq) != "0"
		}
		if iNull >= 0 {
			r.hasNullable = true
			r.nullable = strings.EqualFold(cell(iNull), "YES")
		}
		if r.name == "" || r.col == "" {
			continue
		}
		got = append(got, r)
	}
	return got, true
}

func keyColsViaShowIndex(db *sql.DB, schema, t string) ([]string, string) {
	rows, ok := showIndexRows(db, schema, t)
	if !ok {
		return nil, ""
	}
	type item struct {
		name     string
		cols     []string
		nullable bool
	}
	var got []item
	for _, r := range rows {
		if r.hasNonUniq && r.nonUniq {
			continue // 只要唯一索引
		}
		if len(got) == 0 || got[len(got)-1].name != r.name {
			got = append(got, item{name: r.name})
		}
		it := &got[len(got)-1]
		it.cols = append(it.cols, r.col)
		if r.hasNullable && r.nullable {
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
	rows2, ok := showIndexRows(db, schema, t)
	if !ok {
		return out, order
	}
	for _, r := range rows2 {
		if _, exists := out[r.name]; !exists {
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
		// 没写列清单、又不是 SELECT *：值按位置对应，名字对不上就说不清
		return map[string]string{}, false
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

		pick := func(where string, limit int) ([]string, error) {
			q := fmt.Sprintf("SELECT DISTINCT %s FROM %s WHERE %s AND %s IS NOT NULL LIMIT %d",
				quoteIdent(col), qualTable(schema, t), where, quoteIdent(col), limit)
			ctx, cancel := pfCtx(pfSec)
			defer cancel()
			rows, err := db.QueryContext(ctx, q)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			return scanVals(rows), nil
		}

		okVals, okErr := pick(match, 21)
		otherVals, _ := pick("NOT "+match, 11)
		item := map[string]any{
			"table": t, "field": col, "ok": okVals, "other": otherVals, "missing": false,
		}
		if okErr != nil {
			// 超时、无权限不能当成「这批没有版本号」
			item["err"] = "读取失败：" + okErr.Error()
			item["ok"] = []string{}
		}
		out = append(out, item)
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
// stripSQLComments 去掉 -- / # / /* */ 注释，只用于关键字判断
func stripSQLComments(st string) string {
	var b strings.Builder
	rs := []rune(st)
	inS, inD := false, false
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		switch {
		case inS:
			b.WriteRune(c)
			if c == '\\' && i+1 < len(rs) {
				i++
				b.WriteRune(rs[i])
			} else if c == '\'' {
				inS = false
			}
			continue
		case inD:
			b.WriteRune(c)
			if c == '\\' && i+1 < len(rs) {
				i++
				b.WriteRune(rs[i])
			} else if c == '"' {
				inD = false
			}
			continue
		}
		if c == '\'' {
			inS = true
			b.WriteRune(c)
			continue
		}
		if c == '"' {
			inD = true
			b.WriteRune(c)
			continue
		}
		if c == '-' && i+1 < len(rs) && rs[i+1] == '-' {
			for i < len(rs) && rs[i] != '\n' {
				i++
			}
			b.WriteRune(' ')
			continue
		}
		if c == '#' {
			for i < len(rs) && rs[i] != '\n' {
				i++
			}
			b.WriteRune(' ')
			continue
		}
		if c == '/' && i+1 < len(rs) && rs[i+1] == '*' {
			i += 2
			for i+1 < len(rs) && !(rs[i] == '*' && rs[i+1] == '/') {
				i++
			}
			i++
			b.WriteRune(' ')
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func deleteLimit(st string) int64 {
	m := reDeleteLimit.FindStringSubmatch(strings.TrimSpace(stripSQLComments(st)))
	if m == nil {
		return 0
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	return n
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

// explainIndexes: 只做 EXPLAIN（不执行），返回本次统计实际用到的索引，形如 "hilist_b(PRIMARY)"
func explainIndexes(db *sql.DB, q string) string {
	ctxX, cancelX := explainCtx()
	defer cancelX()
	rows, err := db.QueryContext(ctxX, "EXPLAIN "+q)
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

// DELETE t1, t2 FROM t1 JOIN t2 ... ：把别名映射回真实表名

var reCTEName = regexp.MustCompile("(?is)([A-Za-z_][A-Za-z0-9_$]*|\x60[^\x60]+\x60)\\s*(?:\\([^)]*\\)\\s*)?as\\s*\\(")

// CTE 定义里的名字不是真实表，后面解析表引用时要排除掉
func cteNames(st string) map[string]bool {
	out := map[string]bool{}
	main := stripCTE(st)
	if len(main) >= len(st) {
		return out
	}
	prefix := st[:len(st)-len(main)]
	mask := depthMask(prefix)
	for _, loc := range reCTEName.FindAllStringSubmatchIndex(prefix, -1) {
		if loc[2] < 0 || mask[loc[2]] != 0 {
			continue // 字符串里的 "x AS (" 不算
		}
		out[strings.ToLower(strings.Trim(prefix[loc[2]:loc[3]], "`\""))] = true
	}
	return out
}

var reJoinKw = regexp.MustCompile(`(?is)\s+(?:straight_join|inner\s+join|left\s+(?:outer\s+)?join|right\s+(?:outer\s+)?join|cross\s+join|join)\s+`)

// UPDATE 的多表部分既可能是 "t1 a, t2 b"，也可能是 "t1 a JOIN t2 b ON ..."，
// 统一切成单个表引用，并把 ON 条件丢掉

// 在顶层（跳过字符串和括号）找 SET，避免 ON 条件里的 'a set b' 把语句切坏
func splitUpdate(st string) (string, string, bool) {
	t := strings.TrimSpace(st)
	if len(t) < 6 || !strings.EqualFold(t[:6], "update") {
		return "", "", false
	}
	k := indexTopLevelPhrase(t, " set ")
	if k < 0 {
		return "", "", false
	}
	tables := strings.TrimSpace(t[len("update"):k])
	for _, mod := range []string{"low_priority", "ignore"} {
		for strings.HasPrefix(strings.ToLower(tables), mod) {
			tables = strings.TrimSpace(tables[len(mod):])
		}
	}
	return tables, strings.TrimSpace(t[k+len(" set "):]), true
}

// 每个字节位置的括号深度（引号里的括号不算）
func depthMask(s string) []int {
	m := make([]int, len(s)+1)
	depth := 0
	inS, inD, inBt := false, false, false
	for i := 0; i < len(s); i++ {
		c := s[i]
		m[i] = depth
		if inS {
			if c == '\\' {
				i++
				continue
			}
			if c == '\'' {
				inS = false
			}
			continue
		}
		if inD {
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inD = false
			}
			continue
		}
		if inBt {
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
		}
	}
	m[len(s)] = depth
	return m
}

func updateTableParts(s string) []string {
	var out []string
	for _, part := range splitTopLevel(s) {
		mask := depthMask(part)
		var cuts []int
		for _, loc := range reJoinKw.FindAllStringIndex(part, -1) {
			if mask[loc[0]] == 0 { // 只切顶层 JOIN，派生表里的不算
				cuts = append(cuts, loc[0], loc[1])
			}
		}
		var pieces []string
		start := 0
		for i := 0; i+1 < len(cuts); i += 2 {
			pieces = append(pieces, part[start:cuts[i]])
			start = cuts[i+1]
		}
		pieces = append(pieces, part[start:])
		for _, sub := range pieces {
			if k := indexTopLevelPhrase(sub, " on "); k >= 0 {
				sub = sub[:k]
			}
			sub = strings.TrimSpace(sub)
			if sub == "" || strings.HasPrefix(sub, "(") { // 派生表不是表名
				continue
			}
			out = append(out, sub)
		}
	}
	return out
}

func aliasToTable(st string) map[string]string {
	low := strings.ToLower(st)
	k := -1
	for off := 0; off < len(low); {
		j := strings.Index(low[off:], "from")
		if j < 0 {
			break
		}
		j += off
		prevOK := j > 0 && !isWordRune(rune(low[j-1]))
		nextOK := j+4 >= len(low) || !isWordRune(rune(low[j+4]))
		if prevOK && nextOK {
			k = j
			break
		}
		off = j + 4
	}
	if k < 0 {
		return nil
	}
	rest := st[k+4:]
	for _, kw := range []string{"where", "group", "order", "limit", "having"} {
		rest = cutAtKeyword(rest, kw)
	}
	out := map[string]string{}
	skip := map[string]bool{"join": true, "left": true, "right": true, "inner": true, "outer": true,
		"cross": true, "straight_join": true, "on": true, "using": true, "as": true, "natural": true}
	cur := ""
	for _, tk := range reIdentTok.FindAllString(rest, -1) {
		l := strings.ToLower(tk)
		if tk == "," || l == "join" {
			cur = ""
			continue
		}
		if skip[l] {
			continue
		}
		if cur == "" {
			cur = tk
			out[strings.ToUpper(trimIdent(tk))] = tk
			continue
		}
		out[strings.ToUpper(trimIdent(tk))] = cur
		cur = ""
	}
	return out
}

func estimateUpdate(db *sql.DB, schema, st string, pfSec int, hintTable, hintAlias string) (int64, string, string, bool) {
	const capN = 300000

	tablesPart, setPart, okUpd := splitUpdate(st)
	if !okUpd {
		return 0, "", "", false
	}
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
		alias = hintAlias // SET 左值没写别名时，用调用方已经解析出的目标表别名
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
		table = hintTable
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
	lim := deleteLimit(st)
	if err := db.QueryRowContext(ctx, q).Scan(&n); err == nil {
		if lim > 0 && n > lim {
			n = lim
		}
		if n > capN {
			return capN, "cap", explainIndexes(db, q), true
		}
		return n, "", explainIndexes(db, q), true
	}

	// 数不出来（超时/太复杂）→ 用执行计划给个量级
	if n2, ok := estimateByExplain(db, st, table, alias); ok {
		if lim > 0 && n2 > lim {
			n2 = lim
		}
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
// engineWarnings 检查要写入的表是不是事务引擎。
// MyISAM 之类的写入根本不进事务，ROLLBACK 会「成功」但数据已经落盘，
// 此时界面说「已回滚」就是在骗人——必须提前拦住。
func engineWarnings(db *sql.DB, schema string, tables []string, pfSec int) []map[string]any {
	out := make([]map[string]any, 0)
	if len(tables) == 0 {
		return out
	}
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	bad := make([]string, 0, len(tables))
	for _, t := range tables {
		var engine sql.NullString
		err := db.QueryRowContext(ctx, `SELECT ENGINE FROM information_schema.TABLES
			WHERE UPPER(TABLE_SCHEMA)=UPPER(?) AND UPPER(TABLE_NAME)=UPPER(?) LIMIT 1`, schema, t).Scan(&engine)
		if err != nil || !engine.Valid || engine.String == "" {
			continue // 查不到（中间件/权限/超时）时不要误报
		}
		if !strings.EqualFold(engine.String, "InnoDB") {
			bad = append(bad, t+"("+engine.String+")")
		}
	}
	if len(bad) > 0 {
		out = append(out, map[string]any{
			"i": 0, "level": "error", "title": "目标表不是事务引擎",
			"detail": "这些表写入后无法回滚：" + strings.Join(bad, "、") +
				"。中途失败时前面的改动会留在库里，请先确认影响范围再执行，失败后不要直接重跑。",
		})
	}
	return out
}

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

// 解析 INSERT ... VALUES 的常量元组（返回每个括号组里的顶层值文本）

// 按顶层逗号切值，字符串和括号里的逗号不算

// 只在括号外、引号外查找短语，避免把字符串里的内容当成语法
func indexTopLevelPhrase(s, phrase string) int {
	low := strings.ToLower(s)
	depth := 0
	inS, inD, inBt := false, false, false
	for i := 0; i < len(low); i++ {
		c := low[i]
		if inS {
			if c == '\\' {
				i++
				continue
			}
			if c == '\'' {
				inS = false
			}
			continue
		}
		if inD {
			if c == '\\' {
				i++
				continue
			}
			if c == '"' {
				inD = false
			}
			continue
		}
		if inBt {
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
			if depth == 0 && strings.HasPrefix(low[i:], phrase) {
				return i
			}
		}
	}
	return -1
}

func splitValues(s string) []string {
	var out []string
	var buf strings.Builder
	depth := 0
	inS, inD, inBt := false, false, false
	rs := []rune(s)
	for i := 0; i < len(rs); i++ {
		c := rs[i]
		if inS {
			buf.WriteRune(c)
			if c == '\\' && i+1 < len(rs) {
				i++
				buf.WriteRune(rs[i])
				continue
			}
			if c == '\'' {
				if i+1 < len(rs) && rs[i+1] == '\'' {
					i++
					buf.WriteRune(rs[i])
					continue
				}
				inS = false
			}
			continue
		}
		if inD {
			buf.WriteRune(c)
			if c == '\\' && i+1 < len(rs) {
				i++
				buf.WriteRune(rs[i])
				continue
			}
			if c == '"' {
				inD = false
			}
			continue
		}
		if inBt {
			buf.WriteRune(c)
			if c == '`' {
				inBt = false
			}
			continue
		}
		switch c {
		case '\'':
			inS = true
			buf.WriteRune(c)
		case '"':
			inD = true
			buf.WriteRune(c)
		case '`':
			inBt = true
			buf.WriteRune(c)
		case '(':
			depth++
			buf.WriteRune(c)
		case ')':
			if depth > 0 {
				depth--
			}
			buf.WriteRune(c)
		case ',':
			if depth == 0 {
				out = append(out, buf.String())
				buf.Reset()
				continue
			}
			buf.WriteRune(c)
		default:
			buf.WriteRune(c)
		}
	}
	out = append(out, buf.String())
	return out
}

func parseValuesTuples(rest string) [][]string {
	if k := indexTopLevelPhrase(rest, "on duplicate"); k >= 0 {
		rest = rest[:k]
	}
	low := strings.ToLower(rest)
	k, adv := strings.Index(low, "values"), len("values")
	if k < 0 { // INSERT ... VALUE (...) 单数写法
		k, adv = strings.Index(low, "value"), len("value")
	}
	if k < 0 {
		return nil
	}
	rs := []rune(rest[k+adv:])
	var out [][]string
	for i := 0; i < len(rs); {
		if rs[i] != '(' {
			i++
			continue
		}
		depth, inS, inD, inBt, closed, j := 0, false, false, false, false, i
		for ; j < len(rs); j++ {
			c := rs[j]
			if inS {
				if c == '\\' {
					j++
					continue
				}
				if c == '\'' {
					if j+1 < len(rs) && rs[j+1] == '\'' {
						j++
						continue
					}
					inS = false
				}
				continue
			}
			if inD {
				if c == '"' {
					inD = false
				}
				continue
			}
			if inBt {
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
				depth--
				if depth == 0 {
					closed = true
				}
			}
			if closed {
				break
			}
		}
		if !closed {
			break
		}
		out = append(out, splitValues(string(rs[i+1:j])))
		i = j + 1
	}
	return out
}

// INSERT ... VALUES 的预检：行数 + 主键冲突（列清单里要有完整主键才判）
// INSERT IGNORE / ON DUPLICATE KEY UPDATE 的语义是「冲突不报错」，预检结论要跟着软化
func insertTolerant(st string) (bool, string) {
	low := strings.ToLower(strings.TrimSpace(st))
	if strings.HasPrefix(low, "replace ") {
		return true, "REPLACE"
	}
	if strings.HasPrefix(low, "insert ignore") {
		return true, "IGNORE"
	}
	if indexTopLevelPhrase(st, "on duplicate key update") >= 0 {
		return true, "ON DUPLICATE KEY UPDATE"
	}
	return false, ""
}

// INSERT ... SET a=1, b=2 —— 拆成列名和一行的值
func parseSetAssignments(rest string) ([]string, [][]string) {
	low := strings.ToLower(strings.TrimSpace(rest))
	if !strings.HasPrefix(low, "set") {
		return nil, nil
	}
	body := strings.TrimSpace(rest)[3:]
	if k := indexTopLevelPhrase(body, "on duplicate"); k >= 0 {
		body = body[:k]
	}
	var cols, vals []string
	// 必须按值切分（认引号和括号），否则 b='x,y' 会被拆成两段
	for _, part := range splitValues(body) {
		eq := indexTopLevel(part, '=')
		if eq < 0 {
			continue
		}
		cols = append(cols, trimIdent(strings.TrimSpace(part[:eq])))
		vals = append(vals, strings.TrimSpace(part[eq+1:]))
	}
	if len(cols) == 0 {
		return nil, nil
	}
	return cols, [][]string{vals}
}

// INSERT ... SELECT 的源子句（从顶层 FROM 到末尾，去掉 ON DUPLICATE）
// 把连续空白压成一个空格，并记录每个字符在原文里的下标
func collapseWS(s string) (string, []int) {
	var b strings.Builder
	idx := make([]int, 0, len(s))
	prevSpace := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\n' || c == '\t' || c == '\r' {
			if prevSpace {
				continue
			}
			prevSpace = true
			b.WriteByte(' ')
			idx = append(idx, i)
			continue
		}
		prevSpace = false
		b.WriteByte(c)
		idx = append(idx, i)
	}
	return b.String(), idx
}

func selectFromTail(sel string) string {
	flat, idx := collapseWS(sel)
	if k := strings.Index(strings.ToLower(flat), " from "); k >= 0 {
		tail := strings.TrimSpace(sel[idx[k]:])
		if j := indexTopLevelPhrase(tail, " on duplicate"); j >= 0 {
			tail = strings.TrimSpace(tail[:j])
		}
		return tail
	}
	return ""
}

// 源子句带 JOIN/WHERE 时，按整段子查询计数才准
func countSourceRows(db *sql.DB, fromTail string, capN int64, pfSec int) (int64, bool, error) {
	// 源子句可能自带 ORDER BY / LIMIT，再套一层 LIMIT 会变成两条 LIMIT（语法错）
	tail := cutAtKeyword(fromTail, "order by")
	tail = cutAtKeyword(tail, "limit")
	q := fmt.Sprintf("SELECT COUNT(*) FROM (SELECT 1 %s LIMIT %d) x", strings.TrimSpace(tail), capN+1)
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, false, err
	}
	if n > capN {
		return capN, true, nil
	}
	return n, false, nil
}

func sourceClauseIsComplex(tail string) bool {
	flat, _ := collapseWS(tail)
	up := strings.ToUpper(flat)
	return strings.Contains(up, " JOIN ") || strings.Contains(up, " WHERE ")
}

func preflightInsertValues(db *sql.DB, schema string, i int, target, colList, rest string, pfSec int, tolerant bool, tolerantHow string, add func(int, string, string, string)) {
	var tuples [][]string
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(rest)), "set") {
		colsFromSet, one := parseSetAssignments(rest)
		if len(colsFromSet) == 0 {
			add(i, "warn", "无法解析 SET 赋值", "没能拆出列和值，只确认了目标表；请人工核对")
			return
		}
		colList = strings.Join(colsFromSet, ",")
		tuples = one
	} else {
		tuples = parseValuesTuples(rest)
	}
	if len(tuples) == 0 {
		add(i, "warn", "无法解析 VALUES", "没能拆出具体的值列表，只确认了目标表存在；请人工核对")
		return
	}
	add(i, "ok", "预计影响行数", fmt.Sprintf("常量列表共 %d 行", len(tuples)))

	pk, idxName := mKeyCols(db, schema, target)
	if len(pk) == 0 {
		add(i, "warn", "目标表无主键或唯一索引", fmt.Sprintf("%s 既没有主键也没有唯一索引，重复执行会插入重复数据", target))
		return
	}
	if strings.TrimSpace(colList) == "" {
		add(i, "warn", "判重未做", "这条 INSERT 没有列清单（INSERT INTO t VALUES ...），无法确定每列对应哪个字段；请确认这批是否已执行过")
		return
	}
	cols := splitTopLevel(colList)
	pos := map[string]int{}
	for idx, c := range cols {
		pos[strings.ToUpper(trimIdent(strings.TrimSpace(c)))] = idx
	}
	var missing []string
	for _, c := range pk {
		if _, ok := pos[strings.ToUpper(c)]; !ok {
			missing = append(missing, c)
		}
	}
	if len(missing) > 0 {
		add(i, "warn", "判重未做自动比对", fmt.Sprintf("列清单里没有主键/唯一键列 %s，无法判断这批是否已插入过", strings.Join(missing, ", ")))
		return
	}
	if !strings.EqualFold(idxName, "PRIMARY") {
		add(i, "ok", "使用唯一索引判重", fmt.Sprintf("%s 无主键，改用唯一索引 %s（%s）", target, idxName, strings.Join(pk, ", ")))
	}

	// 值个数要和列清单对得上，否则执行时会报错
	for n, t := range tuples {
		if len(t) != len(cols) {
			add(i, "error", "列数不匹配", fmt.Sprintf("第 %d 组值有 %d 个，列清单有 %d 个，执行会直接报错", n+1, len(t), len(cols)))
			return
		}
	}

	// 值列表内部自身重复
	seen := map[string]bool{}
	var dupInList int
	for _, t := range tuples {
		parts := make([]string, 0, len(pk))
		for _, c := range pk {
			parts = append(parts, strings.TrimSpace(t[pos[strings.ToUpper(c)]]))
		}
		k := strings.Join(parts, "\x00")
		if seen[k] {
			dupInList++
		}
		seen[k] = true
	}
	if dupInList > 0 {
		lvl, msg := "error", fmt.Sprintf("有 %d 组值的主键完全相同，执行会直接报主键冲突", dupInList)
		if tolerant {
			lvl = "warn"
			msg = fmt.Sprintf("有 %d 组值的主键重复，会走 %s 覆盖，不会报错", dupInList, tolerantHow)
		}
		add(i, lvl, "值列表内部重复", msg)
	}

	limit := len(tuples)
	if limit > 500 {
		limit = 500
	}
	plist := make([]string, 0, limit)
	for _, t := range tuples[:limit] {
		vals := make([]string, 0, len(pk))
		for _, c := range pk {
			vals = append(vals, strings.TrimSpace(t[pos[strings.ToUpper(c)]]))
		}
		plist = append(plist, "("+strings.Join(vals, ",")+")")
	}
	keys := make([]string, 0, len(pk))
	for _, c := range pk {
		keys = append(keys, quoteIdent(c))
	}
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE (%s) IN (%s)", qualTable(schema, target), strings.Join(keys, ","), strings.Join(plist, ","))
	ctx, cancel := pfCtx(pfSec)
	defer cancel()
	var exists int64
	if err := db.QueryRowContext(ctx, q).Scan(&exists); err != nil {
		add(i, "warn", "主键冲突检查未完成", withHint(err.Error()))
		return
	}
	note := ""
	if limit < len(tuples) {
		note = fmt.Sprintf("（只比对了前 %d 组）", limit)
	}
	if exists > 0 && tolerant {
		add(i, "warn", "主键已存在", fmt.Sprintf("%s 里已有 %d 行同主键的数据，会走 %s 覆盖/跳过，不会报错%s", target, exists, tolerantHow, note))
	} else if exists > 0 {
		add(i, "error", "主键冲突", fmt.Sprintf("%s 里已经有 %d 行同主键的数据，执行会失败并整批回滚%s", target, exists, note))
	} else {
		add(i, "ok", "无主键冲突", fmt.Sprintf("与 %s 比对无重复%s", target, note))
	}
}

// 服务端版本：不同版本能用的语法不一样，预检按版本给结论，而不是等执行报错
type serverVer struct {
	raw                 string
	major, minor, patch int
	ok                  bool
}

func parseServerVer(raw string) serverVer {
	v := serverVer{raw: raw}
	// MariaDB 为了兼容老客户端会在前面塞 5.5.5-
	if strings.Contains(strings.ToLower(raw), "mariadb") && strings.HasPrefix(raw, "5.5.5-") {
		raw = raw[len("5.5.5-"):]
	}
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == '.' || r == '-' || r == '+' || r == ' '
	})
	nums := make([]int, 0, 3)
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			break
		}
		nums = append(nums, n)
		if len(nums) == 3 {
			break
		}
	}
	if len(nums) < 2 {
		return v
	}
	v.major, v.minor = nums[0], nums[1]
	if len(nums) > 2 {
		v.patch = nums[2]
	}
	v.ok = true
	return v
}

func (v serverVer) atLeast(major, minor, patch int) bool {
	if !v.ok {
		return true // 认不出就别拦，交给服务端判断
	}
	if v.major != major {
		return v.major > major
	}
	if v.minor != minor {
		return v.minor > minor
	}
	return v.patch >= patch
}

func serverVersion(db *sql.DB) serverVer {
	var raw string
	if err := db.QueryRow("SELECT VERSION()").Scan(&raw); err != nil || raw == "" {
		return serverVer{}
	}
	return parseServerVer(raw)
}

// 只有 MySQL 8.0 才有的写法
// 返回特性名和它需要的最低版本
func usesMySQL8Only(st string) (string, int, int, int, bool) {
	low := strings.ToLower(strings.TrimSpace(st))
	if strings.HasPrefix(low, "with ") || strings.HasPrefix(low, "with(") {
		return "WITH（CTE）", 8, 0, 1, true
	}
	if strings.HasPrefix(low, "table ") {
		return "TABLE 语句", 8, 0, 19, true
	}
	if strings.HasPrefix(low, "values row") || strings.HasPrefix(low, "values(") {
		return "VALUES ROW", 8, 0, 19, true
	}
	return "", 0, 0, 0, false
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
	sv := serverVersion(db)
	out := make([]map[string]any, 0)
	// 记录每条语句是否真的产出过检查：UPDATE/DELETE 的检查是在循环之后
	// 统一补的，不能用「循环内有没有 append」来判断是否覆盖
	seen := make(map[int]bool, len(stmts))
	add := func(i int, level, title, detail string) {
		if i >= 0 {
			seen[i] = true
		}
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
		alias string // 目标表在这条语句里的别名；空表示未知（DELETE 不填）
		multi bool   // 多表写法（JOIN/USING）：WHERE 里的无限定列归属不明
	}
	targetUse := map[string][]stmtTarget{}

	type updTask struct {
		i       int
		stmt    string
		target  string
		alias   string
		refs    []tblRef
		updated map[string]bool
	}
	var updPending []updTask

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
	for i, stRaw := range stmts {
		st := stripCTE(stRaw)
		ctes := cteNames(stRaw)
		// 服务端版本不支持这个语法时，别等执行才报错
		if feat, mj, mi, pa, is8 := usesMySQL8Only(stRaw); is8 && !sv.atLeast(mj, mi, pa) {
			add(i, "error", "服务端不支持这个语法",
				fmt.Sprintf("这条用了 %s，需要 MySQL %d.%d.%d 及以上；当前服务端是 %s，执行会直接报语法错误", feat, mj, mi, pa, sv.raw))
		}
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
			if fm == nil || strings.HasPrefix(strings.ToLower(strings.TrimSpace(m[3])), "set") {
				tolerant, tolerantHow := insertTolerant(st)
				preflightInsertValues(db, schema, i, target, colList, m[3], pfSec, tolerant, tolerantHow, add)
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
			tolerant, tolerantHow := insertTolerant(st)
			// 判重列在源表里对应哪一列：SELECT 里换过顺序、写过表达式就说不准，不做结论
			srcOf, mapOK := insertKeyMap(m[3], colList, pk)

			// 源表行数 / 源表重复键 / 与目标表的冲突互不依赖，且都是重查询：并发跑
			var (
				n, dup, conf                int64
				capped, dupTrunc, confTrunc bool
				cntErr, dupErr, confErr     error
				durCnt, durDup, durConf     time.Duration
			)
			srcTail := selectFromTail(m[3])
			complexSrc := sourceClauseIsComplex(srcTail)
			tasks := []func(*sql.DB){func(c *sql.DB) {
				t0 := time.Now()
				if srcTail != "" {
					n, capped, cntErr = countSourceRows(c, srcTail, 300000, pfSec)
				} else {
					n, capped, cntErr = tableCountBounded(c, src, 300000, pfSec)
				}
				durCnt = time.Since(t0)
			}}
			// 源子句带 JOIN/WHERE 时，判重得按子查询重算，代价高且容易算错，
			// 这里明确跳过并告诉用户，而不是给出错的结论
			if mapOK && len(pk) > 0 && !complexSrc {
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
			if complexSrc && mapOK && len(pk) > 0 {
				add(i, "warn", "判重跳过", "源表带 JOIN 或 WHERE，判重需要按子查询重算，工具暂不自动判断；请确认这批是否已插入过")
			}

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
				lvl := "error"
				if tolerant {
					lvl, msg = "warn", fmt.Sprintf("%s 内有 %d 组重复主键，会走 %s 覆盖/跳过，不会报错", src, dup, tolerantHow)
				}
				addD(i, lvl, "源表主键重复", msg, durDup)
			}
			if confErr != nil {
				add(i, "warn", "主键冲突检查未完成", "查询失败或超时（表可能很大）："+withHint(confErr.Error()))
			} else if conf > 0 {
				msg := fmt.Sprintf("%s 已存在 %d 行相同主键，INSERT 会失败（大概率这批已执行过）", target, conf)
				if confTrunc {
					msg = fmt.Sprintf("%s 至少已存在 %d 行相同主键（数到 1000 就停了），INSERT 会失败（大概率这批已执行过）", target, conf)
				}
				lvl := "error"
				if tolerant {
					lvl, msg = "warn", fmt.Sprintf("%s 已有 %d 行同主键，会走 %s 覆盖/跳过，不会报错", target, conf, tolerantHow)
				}
				addD(i, lvl, "主键冲突", msg, durConf)
			} else if complexSrc {
				addD(i, "warn", "判重未执行", "源表带 JOIN 或 WHERE，未做自动比对；请人工确认这批是否已插入过", durConf)
			} else {
				addD(i, "ok", "无主键冲突", fmt.Sprintf("与 %s 比对无重复", target), durConf)
			}
		case stmtKind(st) == "update":
			tablesPart, setPart, okUpd := splitUpdate(st)
			if !okUpd {
				continue
			}
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
			for _, part := range updateTableParts(tablesPart) {
				if t, a := parseTableRef(part); t != "" {
					if ctes[strings.ToLower(trimIdent(t))] {
						continue // CTE 不是真实表
					}
					refs = append(refs, tblRef{mResolveTable(db, schema, t), a})
				}
			}
			if len(refs) < 2 {
				// 单表 UPDATE：没有外部数据源，但目标表相关的检查仍然要做
				if len(refs) == 1 {
					r := refs[0]
					if whereTail(st) == "" {
						if lim := deleteLimit(st); lim > 0 {
							add(i, "warn", "没有 WHERE 条件", fmt.Sprintf("这条 UPDATE 没有 WHERE，按 LIMIT %d 最多改 %d 行，请确认不是笔误", lim, lim))
						} else {
							add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请写成 WHERE 1=1）")
						}
					}
					for _, col := range unqual {
						if !mHasColumn(db, schema, r.table, col) {
							add(i, "error", "SET 列不存在", fmt.Sprintf("要更新的列 %s 在 %s 里找不到", col, r.table))
						}
					}
					add(i, "ok", "被更新表", fmt.Sprintf("%s（单表更新）", r.table))
					targetUse[r.table] = append(targetUse[r.table], stmtTarget{stmt: i, where: whereRaw(st), alias: r.alias})
					if n, ok := estimateByExplain(db, st, r.table, r.alias); ok {
						tail := " · 全表扫（数据量小时优化器会这样选，不代表缺索引）"
						if keys := explainIndexes(db, st); keys != "" {
							tail = " · 索引 " + keys
						} else if names := mIndexNames(db, schema, r.table); len(names) > 0 {
							tail = " · 全表扫（表上有 " + strings.Join(names, "、") + "，数据少时可能不走）"
						}
						add(i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（执行计划扫描行数估算，可能远大于实际命中；精确值需 COUNT）%s", n, tail))
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
				if lim := deleteLimit(st); lim > 0 {
					add(i, "warn", "没有 WHERE 条件", fmt.Sprintf("这条 UPDATE 没有 WHERE，按 LIMIT %d 最多改 %d 行，请确认不是笔误", lim, lim))
				} else {
					add(i, "error", "没有 WHERE 条件", "这条 UPDATE 会更新整张表，请确认不是笔误（确实要全表更新请改成 WHERE 1=1 再跑）")
				}
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
			targetAlias := ""
			for _, ref := range refs {
				if updated[strings.ToLower(ref.alias)] {
					targetTable = ref.table
					targetAlias = ref.alias
					targetUse[ref.table] = append(targetUse[ref.table], stmtTarget{stmt: i, where: whereRaw(st), alias: targetAlias, multi: true})
					break
				}
			}
			// 行数估算与各源表行数互不依赖，收集起来和整段脚本的其它语句一起并发跑
			updPending = append(updPending, updTask{
				i: i, stmt: stRaw, target: targetTable, alias: targetAlias,
				refs: refs, updated: updated,
			})

		case stmtKind(st) == "delete":
			st = reDeleteMods.ReplaceAllString(st, "delete ")
			lim := deleteLimit(st)
			where := whereRaw(st)

			// 三种写法：单表、DELETE t FROM t JOIN ...、DELETE FROM t USING t JOIN ...
			var targets []string
			countTable := ""
			multi := false
			multiDelete := false // 多表写法：WHERE 里无限定列的归属说不清
			switch {
			case reDeleteUsing.MatchString(st):
				mm := reDeleteUsing.FindStringSubmatch(st)
				targets = append(targets, mResolveTable(db, schema, normTable(mm[1])))
				multi = true // USING 的 WHERE 通常引用关联表，单表计数必然失败，走 EXPLAIN
				multiDelete = true
			case reDeleteSingle.MatchString(st), reDelete.MatchString(st) && !reDeleteMulti.MatchString(st):
				// 单表（WHERE 里可以有子查询）
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
				am := aliasToTable(st)
				for _, part := range splitTopLevel(mm[1]) {
					if t, _ := parseTableRef(part); t != "" {
						if real, ok := am[strings.ToUpper(trimIdent(t))]; ok {
							t = real
						}
						targets = append(targets, mResolveTable(db, schema, normTable(t)))
					}
				}
				multi = true
				multiDelete = true
			default:
				m := reDelete.FindStringSubmatch(st)
				if m == nil {
					continue
				}
				targets = append(targets, mResolveTable(db, schema, normTable(m[1])))
				countTable = targets[0]
			}

			for _, tg := range targets {
				if ctes[strings.ToLower(tg)] {
					continue // CTE 不是真实表
				}
				add(i, "ok", "被删除表", tg)
				targetUse[tg] = append(targetUse[tg], stmtTarget{stmt: i, where: where, multi: multiDelete})
			}
			if multi {
				add(i, "warn", "多表 DELETE", "按关联条件删除，行数受关联结果影响，预检不做精确统计，请自行确认范围")
				if n, ok := estimateByExplain(db, st, targets[0], ""); ok {
					add(i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算）", n))
				}
			}
			if whereTail(st) == "" {
				switch {
				case lim > 0:
					add(i, "warn", "没有 WHERE 条件", fmt.Sprintf("这条 DELETE 没有 WHERE，按 LIMIT %d 最多删 %d 行，请确认不是笔误", lim, lim))
				case multi || strings.Contains(strings.ToUpper(st), " JOIN "):
					add(i, "warn", "没有 WHERE 条件", "这条 DELETE 的删除范围由 JOIN 条件决定，请确认关联范围")
				default:
					add(i, "error", "没有 WHERE 条件", "这条 DELETE 会清空整张表，已阻止执行；确实要清空请加 LIMIT 或改成 TRUNCATE")
				}
			}
			if countTable != "" {
				delPending = append(delPending, delTask{i: i, stmt: stRaw, table: countTable, where: where, lim: lim})
			}
		}
	}

	// UPDATE 的估算与源表行数统一并发跑：脚本里多条 UPDATE 不再一条接一条等
	if len(updPending) > 0 {
		type estRes struct {
			n    int64
			note string
			idx  string
			ok   bool
			dur  time.Duration
		}
		type cntRes struct {
			n      int64
			capped bool
			err    error
			dur    time.Duration
		}
		ests := make([]estRes, len(updPending))
		cnts := make([]map[string]cntRes, len(updPending))
		var mu sync.Mutex
		var tasks []func(*sql.DB)
		for k := range updPending {
			k := k
			p := updPending[k]
			tasks = append(tasks, func(c *sql.DB) {
				t0 := time.Now()
				n, note, idx, ok := estimateUpdate(c, schema, p.stmt, pfSec, p.target, p.alias)
				mu.Lock()
				ests[k] = estRes{n: n, note: note, idx: idx, ok: ok, dur: time.Since(t0)}
				mu.Unlock()
			})
			m := map[string]cntRes{}
			cnts[k] = m
			for _, r := range p.refs {
				if p.updated[strings.ToLower(r.alias)] {
					continue
				}
				rt := r.table
				tasks = append(tasks, func(c *sql.DB) {
					t0 := time.Now()
					n, capped, cerr := tableCountBounded(c, rt, 300000, pfSec)
					mu.Lock()
					m[rt] = cntRes{n: n, capped: capped, err: cerr, dur: time.Since(t0)}
					mu.Unlock()
				})
			}
		}
		conns.parallel(db, tasks...)

		for k, p := range updPending {
			er := ests[k]
			if er.ok {
				tail := " · 全表扫（数据量小时优化器会这样选，不代表缺索引）"
				if er.idx != "" {
					tail = " · 索引 " + er.idx
				} else if names := mIndexNames(db, schema, p.target); len(names) > 0 {
					tail = " · 全表扫（表上有 " + strings.Join(names, "、") + "，数据少时可能不走）"
				}
				switch er.note {
				case "cap":
					addD(p.i, "warn", "预计影响行数", fmt.Sprintf("≥ %d 行（数到 30 万就停了，实际可能更多）%s", er.n, tail), er.dur)
				case "explain":
					addD(p.i, "warn", "预计影响行数", fmt.Sprintf("约 %d 行（按执行计划估算）%s", er.n, tail), er.dur)
				default:
					addD(p.i, "ok", "预计影响行数", fmt.Sprintf("预计更新 %d 行（精确计数，最多数 30 万）%s", er.n, tail), er.dur)
				}
			} else {
				add(p.i, "warn", "预计影响行数未计算", "查询超时或语句太复杂（大表上常见），不影响执行")
			}
			for _, r := range p.refs {
				if p.updated[strings.ToLower(r.alias)] {
					add(p.i, "ok", "被更新表", fmt.Sprintf("%s（别名 %s）", r.table, r.alias))
					continue
				}
				cr := cnts[k][r.table]
				if cr.err != nil {
					addD(p.i, "warn", "行数统计失败", fmt.Sprintf("%s（别名 %s）的行数没数出来（超时或权限）：%s", r.table, r.alias, withHint(cr.err.Error())), cr.dur)
				} else if cr.n == 0 {
					addD(p.i, "warn", "源表为空", fmt.Sprintf("%s（别名 %s）没有数据，这条 UPDATE 不会命中任何行", r.table, r.alias), cr.dur)
				} else {
					label := fmt.Sprintf("%s（别名 %s）共 %d 行", r.table, r.alias, cr.n)
					if cr.capped {
						label = fmt.Sprintf("%s（别名 %s）共 ≥%d 行", r.table, r.alias, cr.n)
					}
					addD(p.i, "ok", "数据来源表", label, cr.dur)
				}
			}
			if len(p.updated) == 0 {
				add(p.i, "warn", "无法判断被更新表", "SET 左值既没别名也无法按列名定位，已跳过源表为空检查")
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
		targets := make([]overlapTarget, 0, len(uses))
		for _, u := range uses {
			targets = append(targets, overlapTarget{where: u.where, alias: u.alias, multi: u.multi})
		}
		verdict, lvl := overlapVerdict(targets)
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
	out = append(out, engineWarnings(db, schema, batchTables, pfSec)...)
	out = append(out, trxWarnings(db, schema, pfSec)...)
	vwg.Wait()
	// 到这里 UPDATE/DELETE 的延迟检查也都产出了，再判断谁真的没被覆盖
	for i, st := range stmts {
		if seen[i] {
			continue
		}
		add(i, "warn", "预检未覆盖",
			fmt.Sprintf("这条语句（%s）工具暂时无法预检，请人工核对执行范围：%s", stmtKind(st), clipRunes(preview(st), 60)))
	}
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

// 升级源地址：老安装里存的是 dl.example.com，统一改指对象存储；
// 没配过的也走这个默认值，不再「静默不检查更新」。
const (
	legacyUpdateURL  = "dl.example.com"
	defaultUpdateURL = "https://oss-api.example.com/medupdate-tool/"
)

func effectiveUpdateURL(raw string) string {
	u := strings.TrimSpace(raw)
	if u == "" {
		return defaultUpdateURL
	}
	if p, err := url.Parse(u); err == nil && strings.EqualFold(p.Hostname(), legacyUpdateURL) {
		return defaultUpdateURL
	}
	return u
}

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

var (
	busyMu   sync.Mutex
	busyKind string
)

// 执行 / 预检 / 校验期间禁止改配置，避免删掉正在跑的脚本
// 占锁要原子：先判断再置位，两个请求同时进来只能有一个成功
func tryBeginBusy(kind string) (func(), bool) {
	busyMu.Lock()
	defer busyMu.Unlock()
	if busyKind != "" {
		return nil, false
	}
	busyKind = kind
	return func() {
		busyMu.Lock()
		busyKind = ""
		busyMu.Unlock()
	}, true
}

func busyNow() string {
	busyMu.Lock()
	defer busyMu.Unlock()
	return busyKind
}

func isConfigWrite(p, method string) bool {
	if method != http.MethodPost && method != http.MethodDelete {
		return false
	}
	switch p {
	case "/api/ds", "/api/group", "/api/script", "/api/order",
		"/api/settings", "/api/shutdown":
		// settings 可能在执行中被改成别的监听范围并触发重启；shutdown 会直接杀进程
		return true
	}
	return false
}

func saveFailed(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": "保存失败：" + err.Error()})
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

func appendLog(entry map[string]any) error {
	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("执行日志序列化失败：%v", err)
		return err
	}
	logMu.Lock()
	defer logMu.Unlock()
	path := filepath.Join(logsDir, "run-log.jsonl")
	// 超过 8MB 就轮转一份，避免日志无限增长
	if st, err := os.Stat(path); err == nil && st.Size() > 8<<20 {
		_ = os.Remove(path + ".1")
		_ = os.Rename(path, path+".1")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		log.Printf("执行日志写入失败（磁盘满/权限？）：%v", err)
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(b, '\n')); err != nil {
		log.Printf("执行日志写入失败：%v", err)
		return err
	}
	return nil
}

// 只读日志文件尾部，避免日志变大后每次全量解析
func readLogTail(maxBytes int64) ([]byte, error) {
	main := filepath.Join(logsDir, "run-log.jsonl")
	b, err := tailFile(main, maxBytes)
	if len(b) < int(maxBytes/2) {
		// 刚轮转过：主文件很小时，把上一份的尾部补在前面
		if rb, e2 := tailFile(main+".1", maxBytes-int64(len(b))); e2 == nil {
			b = append(rb, b...)
		}
	}
	if b == nil {
		return nil, err
	}
	return b, nil
}

func tailFile(path string, maxBytes int64) ([]byte, error) {
	f, err := os.Open(path)
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

// hostAllowed：只放行本机与局域网地址
func hostAllowed(host string, extra []string) bool {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	switch h {
	case "localhost", "127.0.0.1", "::1", "[::1]":
		return true
	}
	// 私网/回环 IP 字面量一律放行：覆盖 IPv6、换网换 IP 的情况；
	// 攻击者的域名（evil.example.com）ParseIP 不成立，仍然被挡。
	if ip := net.ParseIP(h); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}
	for _, name := range extra {
		if strings.EqualFold(name, h) {
			return true
		}
	}
	return false
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
	if err := cmd.Start(); err == nil {
		go func() { _ = cmd.Wait() }()
	}
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

// 执行入口的兜底：找出会清空整表/分区的 DELETE。返回 1-based 序号、提示、是否命中。
func findUnsafeStatement(stmts []string) (int, string, bool) {
	for idx, st := range stmts {
		plain := reDeleteMods.ReplaceAllString(st, "delete ")
		kind := stmtKind(plain)
		if kind == "update" {
			// 多表 UPDATE 用 JOIN ... ON 限定行，没有顶层 WHERE 不等于整表改写
			bare := strings.ToUpper(stripSQLComments(plain))
			limited := strings.Contains(bare, " JOIN ") || deleteLimit(plain) > 0
			if whereTail(plain) == "" && !limited {
				return idx + 1, "这条 UPDATE 没识别到 WHERE 条件；如果确实要改全表，请写成 WHERE 1=1 明确表达", true
			}
			continue
		}
		if kind != "delete" {
			continue
		}
		// 只看「有没有 WHERE / LIMIT」。多表写法（DELETE t1 FROM t1、… USING …）
		// 不带 WHERE 也是整表删除，不能放行
		if whereTail(plain) != "" || deleteLimit(plain) > 0 {
			continue
		}
		hint := "确实要清空请加 LIMIT，或者改用 TRUNCATE"
		if strings.Contains(strings.ToLower(plain), "partition") {
			hint = "按分区清空请改用 TRUNCATE TABLE … PARTITION (…)"
		}
		return idx + 1, hint, true
	}
	return 0, "", false
}

func handler(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path

	// 执行 / 预检进行中不允许改配置（删脚本、改数据源等），否则会动到正在跑的脚本
	if k := busyNow(); k != "" && isConfigWrite(p, r.Method) {
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "正在" + k + "，先等它结束再改脚本/文件夹/数据源"})
		return
	}

	// 页面
	// 下载三平台安装包（白名单，不接受任意路径）
	if r.Method == http.MethodGet && strings.HasPrefix(p, "/dl/") {
		files := map[string]string{
			"medupdate.exe":   "release-win/medupdate.exe",
			"medupdate-mac":   "release-mac/medupdate",
			"medupdate-linux": "release-linux/medupdate",
		}
		// 只放行发版脚本生成的包名（medupdate-平台-版本-6位hex.zip）。
		// 不能整目录 glob：dist 里可能有别处打出来的包，里面带 data.json（库口令）。
		if names, err := filepath.Glob(filepath.Join(appHome, "dist", "medupdate-*.zip")); err == nil {
			for _, full := range names {
				base := filepath.Base(full)
				if !reReleaseZip.MatchString(base) {
					continue
				}
				files[base] = filepath.Join("dist", base)
			}
		}
		name := strings.TrimPrefix(p, "/dl/")
		rel, ok := files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}
		full := filepath.Join(appHome, rel)
		// 用 Lstat：dist 里放一个名字合规的软链就能绕过白名单读到 data.json
		if fi, err := os.Lstat(full); err != nil || !fi.Mode().IsRegular() {
			http.Error(w, "安装包还没生成，先在服务器上执行 build.sh", 404)
			return
		}
		w.Header().Set("content-disposition", "attachment; filename=\""+name+"\"")
		http.ServeFile(w, r, full)
		return
	}

	// 浏览器标签图标：SVG 优先，另给 PNG 兜底（老 Safari、部分 Windows 浏览器不认 SVG favicon）
	if r.Method == http.MethodGet && (p == "/icon.svg" || p == "/icon-32.png" || p == "/icon-180.png" || p == "/favicon.ico") {
		name := strings.TrimPrefix(p, "/")
		ctype := "image/png"
		switch name {
		case "icon.svg":
			ctype = "image/svg+xml"
		case "favicon.ico", "icon-32.png":
			name = "icon-32.png"
		case "icon-180.png":
			name = "icon-180.png"
		}
		b, err := assets.ReadFile(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("content-type", ctype)
		w.Header().Set("cache-control", "public, max-age=3600")
		_, _ = w.Write(b)
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
		// 诊断端点：默认关闭，需要 DEBUG_API=1 才开，避免顺手暴露库结构
		if os.Getenv("DEBUG_API") != "1" {
			writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": "诊断接口已关闭（设置 DEBUG_API=1 可开启）"})
			return
		}
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
		base := effectiveUpdateURL(st.Settings.UpdateURL)
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
			if f, ok := files["linux"]; ok {
				out["linux"] = f
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
		if strings.TrimSpace(r.URL.Query().Get("id")) == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "缺少 id 参数，已拒绝（空 id 会匹配到全部顶层节点）"})
			return
		}
		id := r.URL.Query().Get("id")
		mu.Lock()
		s := loadStore()
		gids := map[string]bool{}
		for _, g := range s.Groups {
			if g.DSID == id {
				gids[g.ID] = true
			}
		}
		// 子文件夹继承父级的数据源，要沿着 parentId 一起删，否则留下孤儿
		for changed := true; changed; {
			changed = false
			for _, g := range s.Groups {
				if !gids[g.ID] && g.ParentID != "" && gids[g.ParentID] {
					gids[g.ID] = true
					changed = true
				}
			}
		}
		var keepG []Group
		for _, g := range s.Groups {
			if !gids[g.ID] {
				keepG = append(keepG, g)
			}
		}
		var keepS []Script
		for _, sc := range s.Scripts {
			if gids[sc.GroupID] {
				continue // 所属文件夹一起删
			}
			if sc.GroupID == "" && sc.DSID == id {
				continue // 根级单层脚本直接挂在这个数据源上
			}
			keepS = append(keepS, sc)
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
		if strings.TrimSpace(r.URL.Query().Get("id")) == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "缺少 id 参数，已拒绝（空 id 会匹配到全部顶层节点）"})
			return
		}
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
		if strings.TrimSpace(r.URL.Query().Get("id")) == "" {
			writeJSON(w, 400, map[string]any{"ok": false, "error": "缺少 id 参数，已拒绝（空 id 会匹配到全部顶层节点）"})
			return
		}
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
		doneBusy, okBusy := tryBeginBusy("预检")
		if !okBusy {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "正在" + busyNow() + "，先等它结束"})
			return
		}
		defer doneBusy()
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
		doneBusy, okBusy := tryBeginBusy("语法校验")
		if !okBusy {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "正在" + busyNow() + "，先等它结束"})
			return
		}
		defer doneBusy()
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
		doneBusy, okBusy := tryBeginBusy("执行")
		if !okBusy {
			writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": "正在" + busyNow() + "，先等它结束"})
			return
		}
		defer doneBusy()
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
		// 预检是能关的开关，这条防线必须在执行入口自己兜住
		if idx, hint, bad := findUnsafeStatement(stmts); bad {
			writeJSON(w, 400, map[string]any{"ok": false,
				"error": fmt.Sprintf("第 %d 条是无条件 DELETE，会清空整张表，已拒绝执行；%s", idx, hint)})
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

		// 互斥已由上面的 tryBeginBusy 保证，这里不需要第二把锁
		results, failedAt, errMsg, ms := runStatements(ds, stmts, dry)
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
		_ = os.Remove(filepath.Join(logsDir, "run-log.jsonl.1"))
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
	for _, osname := range []string{"win", "mac", "linux"} {
		// 支持随机后缀：medupdate-win-1.0.5-a8f3c2.zip
		matches, _ := filepath.Glob(filepath.Join(*dir, fmt.Sprintf("medupdate-%s-*.zip", osname)))
		if osname == "linux" && len(matches) == 0 {
			continue // linux 包可选，没打就不进清单
		}
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
	shutdownURL := "http://127.0.0.1:" + port + "/api/shutdown"
	if key := os.Getenv("ACCESS_KEY"); key != "" && key != "off" {
		shutdownURL += "?k=" + key
	}
	if resp, err := c.Post(shutdownURL, "application/json", nil); err == nil {
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
	// 只接受本机 / 局域网 Host，挡掉 DNS rebinding 把请求伪装成同源的玩法
	extraHosts := lanIPs()
	if hn, err := os.Hostname(); err == nil && hn != "" {
		extraHosts = append(extraHosts, hn, hn+".local")
	}
	base := h
	h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if hp, _, err := net.SplitHostPort(r.Host); err == nil {
			host = hp
		}
		// 防点击劫持：本机服务对任何网页都可访问，必须禁止被 iframe 嵌套
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		if !hostAllowed(host, extraHosts) {
			http.Error(w, "403 只允许本机或局域网访问", http.StatusForbidden)
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/api/") {
			ct := r.Header.Get("content-type")
			if !strings.HasPrefix(strings.ToLower(ct), "application/json") {
				http.Error(w, "415 需要 application/json", http.StatusUnsupportedMediaType)
				return
			}
		}
		base.ServeHTTP(w, r)
	})
	if key := os.Getenv("ACCESS_KEY"); key != "" && key != "off" {
		next := h
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 健康检查只回版本号，免鉴权：否则新版接管不了旧实例
			switch r.URL.Path {
			case "/api/health", "/icon.svg", "/icon-32.png", "/icon-180.png", "/favicon.ico":
				next.ServeHTTP(w, r)
				return
			}
			given := r.URL.Query().Get("k")
			if given == "" {
				given = r.Header.Get("x-key")
			}
			if given != key {
				w.WriteHeader(401)
				_, _ = w.Write([]byte("401 需要访问密钥，请在地址后加 ?k=你的密钥"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	addr := net.JoinHostPort(host, port)
	fmt.Println()
	fmt.Println("  update-tool v" + appVersion)
	fmt.Printf("  本机   http://127.0.0.1:%s/\n", port)
	if host == "0.0.0.0" {
		for _, ip := range lanIPs() {
			fmt.Printf("  局域网 http://%s:%s/\n", ip, port)
		}
	}
	fmt.Printf("  数据   %s\n\n", dataFile)

	openURL := "http://127.0.0.1:" + port + "/"
	if key := os.Getenv("ACCESS_KEY"); key != "" && key != "off" {
		openURL += "?k=" + key
	}
	if !ensureInstance(port) {
		openBrowser(openURL)
		return
	}
	openBrowser(openURL)
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second, // 防止 body 不发导致忙碌锁被永久占用
		WriteTimeout:      0,                // 执行可能很久，写超时交给业务层
		IdleTimeout:       120 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		// Windows 双击启动没有控制台，失败信息必须落盘，否则用户只看到「没反应」
		writeStartupLog(err)
		log.Fatal(err)
	}
}

// writeStartupLog 把启动失败写进 logs/startup.log，方便双击启动的用户排查
func writeStartupLog(err error) {
	_ = os.MkdirAll("logs", 0o755)
	line := time.Now().Format("2006-01-02 15:04:05") + " 启动失败: " + err.Error() + "\n"
	_ = os.WriteFile("logs/startup.log", []byte(line), 0o644)
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
