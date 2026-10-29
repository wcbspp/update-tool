package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 接口级集成测试：直接打 handler，不依赖真实数据库。
// 每个用例用独立的临时目录，避免碰到真实 data.json。

func setupHome(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	appHome = dir
	dataFile = filepath.Join(dir, "data.json")
	logsDir = filepath.Join(dir, "logs")
	if err := os.MkdirAll(logsDir, 0o700); err != nil {
		t.Fatalf("建 logs 目录失败: %v", err)
	}
	storeCorrupt = false
	busyMu.Lock()
	busyKind = ""
	busyMu.Unlock()
	t.Cleanup(func() {
		busyMu.Lock()
		busyKind = ""
		busyMu.Unlock()
		storeCorrupt = false
	})
	return dir
}

func call(t *testing.T, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf *bytes.Buffer
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("序列化请求失败: %v", err)
		}
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}
	req := httptest.NewRequest(method, path, buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("返回不是 JSON（%d）：%s", rec.Code, rec.Body.String())
	}
	return m
}

func mustOK(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d：%s", rec.Code, rec.Body.String())
	}
	return decode(t, rec)
}

/* ---------------- 页面与静态资源 ---------------- */

func TestAPIPages(t *testing.T) {
	setupHome(t)
	for _, p := range []string{"/", "/index.html", "/guide"} {
		rec := call(t, http.MethodGet, p, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s 期望 200，实际 %d", p, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "<html") {
			t.Fatalf("%s 返回的不是 HTML", p)
		}
	}
}

func TestAPIUnknownPath404(t *testing.T) {
	setupHome(t)
	rec := call(t, http.MethodGet, "/api/不存在", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("未知接口应 404，实际 %d", rec.Code)
	}
}

/* ---------------- health ---------------- */

func TestAPIHealth(t *testing.T) {
	setupHome(t)
	m := mustOK(t, call(t, http.MethodGet, "/api/health", nil))
	if m["ok"] != true {
		t.Fatalf("health 应 ok：%v", m)
	}
	if m["corrupt"] != false {
		t.Fatalf("空配置不该标记损坏：%v", m)
	}
	if m["listenMode"] != "local" {
		t.Fatalf("默认监听范围应为 local，实际 %v", m["listenMode"])
	}
}

func TestAPIHealthReportsCorrupt(t *testing.T) {
	dir := setupHome(t)
	// 必须放一个真解不开的文件：loadStore 会把全局状态复位
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte("MHJ1\x01garbage"), 0o600); err != nil {
		t.Fatalf("写坏文件失败: %v", err)
	}
	m := mustOK(t, call(t, http.MethodGet, "/api/health", nil))
	if m["corrupt"] != true {
		t.Fatalf("损坏时必须上报：%v", m)
	}
}

/* ---------------- 数据源 CRUD ---------------- */

func TestAPIDatasourceCRUD(t *testing.T) {
	setupHome(t)

	rec := call(t, http.MethodPost, "/api/ds", map[string]any{
		"name": "测试库", "host": "127.0.0.1", "port": 3306,
		"user": "root", "password": "secret", "database": "demo", "env": "test",
	})
	tree := mustOK(t, rec)
	ds := tree["tree"].(map[string]any)["datasources"].([]any)
	if len(ds) != 1 {
		t.Fatalf("应新增 1 个数据源，实际 %d", len(ds))
	}
	d := ds[0].(map[string]any)
	id := d["id"].(string)
	// 口令绝不能回传给前端
	if _, has := d["password"]; has {
		t.Fatal("接口回传了口令字段")
	}
	if d["hasPassword"] != true {
		t.Fatal("应标记已设口令")
	}

	// 列表（/api/tree）同样不能带口令
	raw := call(t, http.MethodGet, "/api/tree", nil).Body.String()
	if strings.Contains(raw, "secret") {
		t.Fatal("/api/tree 泄漏了口令明文")
	}

	// 改名字
	mustOK(t, call(t, http.MethodPost, "/api/ds", map[string]any{"id": id, "name": "改名后"}))
	raw = call(t, http.MethodGet, "/api/tree", nil).Body.String()
	if !strings.Contains(raw, "改名后") {
		t.Fatal("改名没生效")
	}

	// 删除
	mustOK(t, call(t, http.MethodDelete, "/api/ds?id="+id, nil))
	tree = mustOK(t, call(t, http.MethodGet, "/api/tree", nil))
	if len(tree["datasources"].([]any)) != 0 {
		t.Fatal("删除后仍有数据源")
	}
}

// 端口留空/为 0 时按默认 3306 处理（这是现有契约；越界值由前端拦）
func TestAPIDatasourcePortDefault(t *testing.T) {
	setupHome(t)
	m := mustOK(t, call(t, http.MethodPost, "/api/ds", map[string]any{
		"name": "x", "host": "h", "port": 0, "user": "u",
	}))
	ds := m["tree"].(map[string]any)["datasources"].([]any)[0].(map[string]any)
	if ds["port"].(float64) != 3306 {
		t.Fatalf("端口 0 应落成 3306，实际 %v", ds["port"])
	}
}

/* ---------------- 文件夹 / 脚本 CRUD ---------------- */

func createScript(t *testing.T) (dsID, groupID, scriptID string) {
	t.Helper()
	mustOK(t, call(t, http.MethodPost, "/api/ds", map[string]any{
		"name": "库", "host": "127.0.0.1", "port": 3306, "user": "root", "database": "d", "env": "test",
	}))
	tree := mustOK(t, call(t, http.MethodGet, "/api/tree", nil))
	dsID = tree["datasources"].([]any)[0].(map[string]any)["id"].(string)

	rec := call(t, http.MethodPost, "/api/group", map[string]any{"name": "文件夹", "dsId": dsID})
	gm := mustOK(t, rec)
	gs := gm["tree"].(map[string]any)["folders"].([]any)
	groupID = gs[0].(map[string]any)["id"].(string)

	rec = call(t, http.MethodPost, "/api/script", map[string]any{"groupId": groupID, "name": "脚本一"})
	sm := mustOK(t, rec)
	scriptID = sm["id"].(string)
	return
}

func TestAPIScriptCRUD(t *testing.T) {
	setupHome(t)
	_, groupID, scriptID := createScript(t)

	// 保存 SQL
	mustOK(t, call(t, http.MethodPost, "/api/script", map[string]any{
		"id": scriptID, "sql": "SELECT 1;",
	}))
	m := mustOK(t, call(t, http.MethodGet, "/api/script?id="+scriptID, nil))
	sc := m["script"].(map[string]any)
	if !strings.Contains(sc["sql"].(string), "SELECT 1") {
		t.Fatalf("SQL 没存住：%v", sc)
	}

	// 没 id 的删除必须被拒绝，不能误删别的
	if rec := call(t, http.MethodDelete, "/api/script?id=", nil); rec.Code == http.StatusOK {
		t.Fatal("空 id 删除应被拒绝")
	}

	mustOK(t, call(t, http.MethodDelete, "/api/script?id="+scriptID, nil))
	tree := mustOK(t, call(t, http.MethodGet, "/api/tree", nil))
	gs := tree["folders"].([]any)
	if len(gs[0].(map[string]any)["scripts"].([]any)) != 0 {
		t.Fatal("删除后脚本还在")
	}
	_ = groupID
}

/* ---------------- 设置 ---------------- */

func TestAPISettings(t *testing.T) {
	setupHome(t)

	m := mustOK(t, call(t, http.MethodGet, "/api/settings", nil))
	if m["listenMode"] != "local" {
		t.Fatalf("默认应是 local，实际 %v", m["listenMode"])
	}

	// 非法值统一归到 local；首次落库（原来是空）会被判为变化，触发一次重启
	m = mustOK(t, call(t, http.MethodPost, "/api/settings", map[string]any{"listenMode": "乱写"}))
	if m["needRestart"] != true {
		t.Fatalf("首次把监听范围落库应触发重启，实际 %v", m["needRestart"])
	}
	m2 := mustOK(t, call(t, http.MethodGet, "/api/settings", nil))
	if m2["listenMode"] != "local" {
		t.Fatalf("非法值应归一为 local，实际 %v", m2["listenMode"])
	}
}

func TestAPISettingsListenModeSwitch(t *testing.T) {
	setupHome(t)
	m := mustOK(t, call(t, http.MethodPost, "/api/settings", map[string]any{"listenMode": "all"}))
	if m["needRestart"] != true {
		t.Fatal("从 local 切到 all 应要求重启")
	}
	m2 := mustOK(t, call(t, http.MethodGet, "/api/settings", nil))
	if m2["listenMode"] != "all" {
		t.Fatalf("监听范围没存住：%v", m2["listenMode"])
	}
	// 同名值不该再触发重启
	if m := mustOK(t, call(t, http.MethodPost, "/api/settings", map[string]any{"listenMode": "all"})); m["needRestart"] != false {
		t.Fatal("同名值不该重启")
	}
}

/* ---------------- 导出 / 导入 ---------------- */

func TestAPIExportImport(t *testing.T) {
	setupHome(t)
	_, _, scriptID := createScript(t)
	mustOK(t, call(t, http.MethodPost, "/api/script", map[string]any{"id": scriptID, "sql": "UPDATE t SET a=1;"}))

	r := mustOK(t, call(t, http.MethodPost, "/api/export", map[string]any{
		"scope": "all", "includePassword": true,
	}))
	b64 := r["data"].(string)
	if b64 == "" || r["filename"] == "" {
		t.Fatalf("导出内容不对：%v", r)
	}
	if !strings.HasPrefix(r["filename"].(string), "medupdate-all-") {
		t.Fatalf("文件名不对：%v", r["filename"])
	}

	// 预览不应写盘
	before, err := os.ReadFile(dataFile)
	if err != nil {
		t.Fatalf("读配置失败: %v", err)
	}
	p := mustOK(t, call(t, http.MethodPost, "/api/import", map[string]any{
		"data": b64, "mode": "merge", "preview": true,
	}))
	if p["preview"] != true {
		t.Fatal("应标记为预览")
	}
	after, _ := os.ReadFile(dataFile)
	if !bytes.Equal(before, after) {
		t.Fatal("预览把配置写盘了")
	}

	// 真正导入（覆盖模式，不该新增脚本）
	res := mustOK(t, call(t, http.MethodPost, "/api/import", map[string]any{"data": b64, "mode": "overwrite"}))
	sum := res["summary"].(map[string]any)
	if sum["scripts"].(float64) != 0 {
		t.Fatalf("覆盖模式不该新增脚本：%v", sum)
	}
}

func TestAPIExportRefusesWhenCorrupt(t *testing.T) {
	dir := setupHome(t)
	if err := os.WriteFile(filepath.Join(dir, "data.json"), []byte("MHJ1\x01broken"), 0o600); err != nil {
		t.Fatalf("写坏文件失败: %v", err)
	}
	rec := call(t, http.MethodPost, "/api/export", map[string]any{"scope": "all"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("损坏时应拒绝导出，实际 %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "读不出来") {
		t.Fatalf("错误信息不清楚：%s", rec.Body.String())
	}
}

func TestAPIImportRejectsGarbage(t *testing.T) {
	setupHome(t)
	for _, bad := range []string{"", "!!!not base64!!!", "aGVsbG8="} {
		rec := call(t, http.MethodPost, "/api/import", map[string]any{"data": bad, "mode": "merge"})
		if rec.Code == http.StatusOK {
			t.Fatalf("垃圾内容 %q 被接受了", bad)
		}
	}
}

func TestAPIImportRejectsConfigFile(t *testing.T) {
	setupHome(t)
	_, _, _ = createScript(t) // 产生一个 data.json
	raw, err := os.ReadFile(dataFile)
	if err != nil {
		t.Fatalf("读配置失败: %v", err)
	}
	// 把 data.json 当导出文件导入，必须拒绝（魔数不同）
	rec := call(t, http.MethodPost, "/api/import", map[string]any{
		"data": base64.StdEncoding.EncodeToString(raw), "mode": "merge",
	})
	if rec.Code == http.StatusOK {
		t.Fatal("data.json 被当成导出文件导入了")
	}
}

/* ---------------- /dl/ 白名单（安全） ---------------- */

func TestAPIReleaseDownloadWhitelist(t *testing.T) {
	dir := setupHome(t)
	dist := filepath.Join(dir, "dist")
	if err := os.MkdirAll(dist, 0o700); err != nil {
		t.Fatalf("建 dist 失败: %v", err)
	}
	good := "medupdate-mac-1.0.4-abcdef.zip"
	if err := os.WriteFile(filepath.Join(dist, good), []byte("PK"), 0o600); err != nil {
		t.Fatalf("写包失败: %v", err)
	}
	// 不合规的包名不能下载
	bad := "medupdate-whatever.zip"
	if err := os.WriteFile(filepath.Join(dist, bad), []byte("PK"), 0o600); err != nil {
		t.Fatalf("写包失败: %v", err)
	}
	// 符号链接指向配置文件，必须 404
	link := "medupdate-win-1.0.4-abcdef.zip"
	if err := os.Symlink(filepath.Join(dir, "data.json"), filepath.Join(dist, link)); err != nil {
		// 建不起链接就等于这一项没测，不能让它白过
		t.Fatalf("建符号链接失败，无法验证软链防护: %v", err)
	}

	if rec := call(t, http.MethodGet, "/dl/"+good, nil); rec.Code != http.StatusOK {
		t.Fatalf("合规包名应可下载，实际 %d", rec.Code)
	}
	if rec := call(t, http.MethodGet, "/dl/"+bad, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("不合规包名应 404，实际 %d", rec.Code)
	}
	if rec := call(t, http.MethodGet, "/dl/../data.json", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("路径穿越应 404，实际 %d", rec.Code)
	}
	if rec := call(t, http.MethodGet, "/dl/"+link, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("符号链接应 404，实际 %d", rec.Code)
	}
}

/* ---------------- 忙碌锁 ---------------- */

func TestAPIBusyBlocksConfigWrites(t *testing.T) {
	setupHome(t)
	done, okBusy := tryBeginBusy("执行")
	if !okBusy {
		t.Fatal("空闲时应能占锁")
	}
	defer done()

	for _, tc := range []struct{ method, path string }{
		{http.MethodPost, "/api/script"},
		{http.MethodPost, "/api/ds"},
		{http.MethodPost, "/api/group"},
	} {
		rec := call(t, tc.method, tc.path, map[string]any{"name": "x"})
		if rec.Code != http.StatusConflict {
			t.Fatalf("%s 忙碌时应 409，实际 %d", tc.path, rec.Code)
		}
	}
	// 读操作不该被挡
	if rec := call(t, http.MethodGet, "/api/tree", nil); rec.Code != http.StatusOK {
		t.Fatalf("忙碌时读接口不该被挡，实际 %d", rec.Code)
	}
}

/* ---------------- 执行日志 ---------------- */

func TestAPILogsEmpty(t *testing.T) {
	setupHome(t)
	m := mustOK(t, call(t, http.MethodGet, "/api/logs", nil))
	if _, has := m["logs"]; !has {
		t.Fatalf("日志接口应返回 logs 数组：%v", m)
	}
	if n := len(m["logs"].([]any)); n != 0 {
		t.Fatalf("初始日志应为空，实际 %d 条", n)
	}
}

func TestAPILogsClear(t *testing.T) {
	setupHome(t)
	if err := appendLog(map[string]any{"at": "2026-10-30 00:00:00", "script": "x", "ok": true}); err != nil {
		t.Fatalf("写日志失败: %v", err)
	}
	m := mustOK(t, call(t, http.MethodGet, "/api/logs", nil))
	if n := len(m["logs"].([]any)); n != 1 {
		t.Fatalf("应读到 1 条日志，实际 %d 条", n)
	}
	// 不带确认必须拒绝，避免误点清空
	if rec := call(t, http.MethodDelete, "/api/logs", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("不带确认应 400，实际 %d", rec.Code)
	}
	mustOK(t, call(t, http.MethodDelete, "/api/logs?confirm=1", nil))
	m2 := mustOK(t, call(t, http.MethodGet, "/api/logs", nil))
	if n := len(m2["logs"].([]any)); n != 0 {
		t.Fatalf("清空后仍有 %d 条", n)
	}
}
