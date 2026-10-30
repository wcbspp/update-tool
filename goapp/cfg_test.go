package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestSealOpenCfg(t *testing.T) {
	plain := []byte(`{"datasources":[{"password":"secret"}]}`)
	sealed, err := sealCfg(plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(sealed, []byte("secret")) {
		t.Fatal("密文里出现了明文口令")
	}
	back, err := openCfg(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if !bytes.Equal(back, plain) {
		t.Fatalf("解出来的内容和原文不一致")
	}
	// 每次加密 nonce 不同，密文不能重复
	again, _ := sealCfg(plain)
	if bytes.Equal(sealed, again) {
		t.Fatal("两次加密得到相同密文，nonce 没有随机化")
	}
}

func TestOpenCfgTampered(t *testing.T) {
	sealed, err := sealCfg([]byte(`{"a":1}`))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	// 改密文最后一个字节
	bad := append([]byte(nil), sealed...)
	bad[len(bad)-1] ^= 0xff
	if _, err := openCfg(bad); err == nil {
		t.Fatal("密文被改动却解开了")
	}
	// 改格式版本
	bad2 := append([]byte(nil), sealed...)
	bad2[4] = 9
	if _, err := openCfg(bad2); err == nil {
		t.Fatal("格式版本不认识却解开了")
	}
	// 截断
	if _, err := openCfg(sealed[:len(sealed)-4]); err == nil {
		t.Fatal("截断的密文却解开了")
	}
}

func TestMagicSeparatesCfgAndExport(t *testing.T) {
	cfgBytes, _ := sealCfg([]byte(`{"a":1}`))
	if _, err := openExport(cfgBytes); err == nil {
		t.Fatal("配置文件的密文被当成导出文件接受了")
	}
	expBytes, _ := sealExport([]byte(`{"kind":"x"}`))
	if _, err := openCfg(expBytes); err == nil {
		t.Fatal("导出文件的密文被当成配置文件接受了")
	}
}

func TestLooksLikeJSON(t *testing.T) {
	if !looksLikeJSON([]byte("  \n{\"a\":1}")) {
		t.Fatal("缩进后的 JSON 没认出来")
	}
	if looksLikeJSON([]byte("MHJ1\x01\x00")) {
		t.Fatal("密文被当成 JSON")
	}
	if looksLikeJSON(nil) {
		t.Fatal("空内容被当成 JSON")
	}
}

func mkStore() Store {
	on := true
	return Store{
		DataSources: []DataSource{
			{ID: "d1", Name: "测试库", Host: "127.0.0.1", Port: 3306, Database: "demo_db", Pass: "p1"},
			{ID: "d2", Name: "备用库", Host: "127.0.0.2", Port: 3306, Database: "other_db", Pass: "p2"},
		},
		Groups: []Group{
			{ID: "g1", DSID: "d1", Name: "分组A", Order: 1},
			{ID: "g2", DSID: "", ParentID: "g1", Name: "子文件夹", Order: 1},
		},
		Scripts: []Script{
			{ID: "s1", GroupID: "g2", Name: "更新基础目录", SQL: "UPDATE a SET b=1;", Order: 1},
			{ID: "s2", GroupID: "g1", Name: "更新限价", SQL: "UPDATE c SET d=2;", Order: 1},
			{ID: "s3", GroupID: "", DSID: "d2", Name: "根脚本", SQL: "SELECT 1;", Order: 1},
		},
		Settings: Settings{Preflight: &on, VerCol: "CRTER_ID"},
	}
}

func TestBuildExportScopes(t *testing.T) {
	s := mkStore()

	all, err := buildExport(s, "all", "", true)
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all.Scripts) != 3 || len(all.Groups) != 2 || len(all.DataSources) != 2 {
		t.Fatalf("all 导出数量不对: %d 脚本 %d 文件夹 %d 连接", len(all.Scripts), len(all.Groups), len(all.DataSources))
	}
	if all.Settings == nil {
		t.Fatal("all 应该带上设置")
	}

	// 单个脚本：连父级链一起带，导入后位置不变
	one, err := buildExport(s, "script", "s1", true)
	if err != nil {
		t.Fatalf("script: %v", err)
	}
	if len(one.Scripts) != 1 || one.Scripts[0].ID != "s1" {
		t.Fatalf("单脚本导出不对: %+v", one.Scripts)
	}
	if len(one.Groups) != 2 {
		t.Fatalf("单脚本应带上两级父文件夹，实际 %d", len(one.Groups))
	}
	if len(one.DataSources) != 1 || one.DataSources[0].ID != "d1" {
		t.Fatalf("单脚本应只带被引用的连接，实际 %+v", one.DataSources)
	}

	// 整个文件夹
	grp, err := buildExport(s, "group", "g1", true)
	if err != nil {
		t.Fatalf("group: %v", err)
	}
	if len(grp.Scripts) != 2 || len(grp.Groups) != 2 {
		t.Fatalf("文件夹导出不对: %d 脚本 %d 文件夹", len(grp.Scripts), len(grp.Groups))
	}

	if _, err := buildExport(s, "script", "nope", true); err == nil {
		t.Fatal("不存在的脚本应该报错")
	}
	if _, err := buildExport(s, "bogus", "", true); err == nil {
		t.Fatal("未知范围应该报错")
	}
}

func TestExportStripsPassword(t *testing.T) {
	s := mkStore()
	b, err := buildExport(s, "all", "", false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	for _, d := range b.DataSources {
		if d.Pass != "" {
			t.Fatalf("%s 的口令没被清掉", d.Name)
		}
	}
	// 原 Store 不能被改动
	if s.DataSources[0].Pass != "p1" {
		t.Fatal("导出把内存里的配置改了")
	}
}

func TestPlanImportRemap(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "all", "", true)
	// 造一份"来自另一台机器"的导出：ID 全不同，连接指向同一个库
	b.DataSources[0].ID = "X1"
	b.DataSources[0].Name = "测试库"
	b.Groups[0].ID = "G1"
	b.Groups[0].DSID = "X1"
	b.Groups[1].ID = "G2"
	b.Groups[1].ParentID = "G1"
	b.Scripts[0].ID = "S1"
	b.Scripts[0].GroupID = "G2"
	b.Scripts[1].ID = "S2"
	b.Scripts[1].GroupID = "G1"
	b.Scripts[2].ID = "S3"
	b.Scripts[2].GroupID = ""
	b.Scripts[2].DSID = "X1"

	out, sum, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	// 连接按 host+port+database 匹配上，不新增
	if sum.AddedDS != 0 || sum.MatchedDS != 2 {
		t.Fatalf("连接匹配结果不对: 新增 %d 匹配 %d", sum.AddedDS, sum.MatchedDS)
	}
	if len(out.DataSources) != len(cur.DataSources) {
		t.Fatalf("连接应该没有增加，实际 %d", len(out.DataSources))
	}
	// 文件夹是复用的，ID 应该还是本机原有的
	gids := map[string]bool{}
	for _, g := range out.Groups {
		gids[g.ID] = true
		if g.ID == "G1" || g.ID == "G2" {
			t.Fatalf("文件夹 ID 没有换成本机的: %s", g.ID)
		}
	}
	for _, sc := range out.Scripts {
		if sc.ID == "S1" || sc.ID == "S2" || sc.ID == "S3" {
			t.Fatalf("脚本 ID 没有重新分配: %s", sc.ID)
		}
	}
	if sum.Groups != 0 || sum.Scripts != 3 {
		t.Fatalf("同配置再导入应该复用文件夹、只新增脚本，实际 %d 文件夹 %d 脚本", sum.Groups, sum.Scripts)
	}
	if len(out.Groups) != 2 {
		t.Fatalf("文件夹被重复创建了，实际 %d 个", len(out.Groups))
	}
	// 引用关系必须重建：脚本挂到新文件夹，新文件夹挂到新连接
	for _, g := range out.Groups {
		if g.Name == "分组A" {
			if g.DSID != "d1" {
				t.Fatalf("顶层文件夹的连接没对上: %q", g.DSID)
			}
			if g.ParentID != "" {
				t.Fatalf("顶层文件夹不该有父级")
			}
		}
		if g.Name == "子文件夹" {
			if g.ParentID == "" || g.ParentID == "G1" {
				t.Fatalf("子文件夹的父级没重建: %q", g.ParentID)
			}
		}
	}
	// 原 Store 不能被改动
	if len(cur.Groups) != 2 || len(cur.Scripts) != 3 {
		t.Fatal("planImport 改了传入的 Store")
	}
}

func TestPlanImportIntoEmpty(t *testing.T) {
	var empty Store
	b, _ := buildExport(mkStore(), "all", "", true)
	out, sum, err := planImport(empty, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum.AddedDS != 2 || sum.Groups != 2 || sum.Scripts != 3 {
		t.Fatalf("导入空配置结果不对: %+v", sum)
	}
	if len(out.DataSources[0].Pass) == 0 {
		t.Fatal("带口令导出，导入空配置后口令应该保留")
	}
}

func TestPlanImportNeedsPassword(t *testing.T) {
	cur := mkStore()
	cur.DataSources[0].Pass = "" // 本机没口令
	b, _ := buildExport(cur, "all", "", false)
	b.DataSources[0].Pass = ""
	out, sum, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	_ = out
	found := false
	for _, n := range sum.NeedPassword {
		if n == "测试库" {
			found = true
		}
	}
	if !found {
		t.Fatalf("没提示需要补录口令: %+v", sum.NeedPassword)
	}
}

func TestPlanImportOverwrite(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "all", "", true)
	// 模拟同事那边的同一个脚本改过内容
	for i := range b.Scripts {
		if b.Scripts[i].Name == "更新基础目录" {
			b.Scripts[i].SQL = "UPDATE changed SET x=9;"
		}
	}
	out, sum, err := planImport(cur, b, true)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum.Updated != 3 || sum.Scripts != 0 {
		t.Fatalf("覆盖模式应该全部按同名更新: %+v", sum)
	}
	hit := false
	for _, sc := range out.Scripts {
		if sc.Name == "更新基础目录" && sc.SQL == "UPDATE changed SET x=9;" {
			hit = true
		}
	}
	if !hit {
		t.Fatal("同名脚本的 SQL 没被更新")
	}
	if len(out.Scripts) != 3 {
		t.Fatalf("覆盖模式不应该新增脚本，实际 %d 条", len(out.Scripts))
	}
}

func TestPlanImportNameCollision(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "all", "", true)
	// 导入模式：同名脚本应该改名而不是覆盖
	out, sum, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum.Updated != 0 {
		t.Fatalf("合并模式不该有覆盖: %+v", sum)
	}
	if len(out.Scripts) != 6 {
		t.Fatalf("合并模式应该新增 3 条，实际总数 %d", len(out.Scripts))
	}
	names := map[string]int{}
	for _, sc := range out.Scripts {
		names[sc.Name]++
	}
	if names["更新基础目录"] != 1 || names["更新基础目录 (2)"] != 1 {
		t.Fatalf("同名脚本改名不对: %+v", names)
	}
}

func TestPlanImportRejectsWrongKind(t *testing.T) {
	cur := mkStore()
	if _, _, err := planImport(cur, ExportBundle{Kind: "something-else", FormatVersion: 1}, false); err == nil {
		t.Fatal("别的程序的导出文件应该被拒绝")
	}
	if _, _, err := planImport(cur, ExportBundle{Kind: exportKind, FormatVersion: 99}, false); err == nil {
		t.Fatal("不认识的格式版本应该被拒绝")
	}
}

func TestExportBundleRoundtripThroughSeal(t *testing.T) {
	s := mkStore()
	b, _ := buildExport(s, "all", "", true)
	plain, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sealed, err := sealExport(plain)
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if bytes.Contains(sealed, []byte("demo_db")) {
		t.Fatal("导出密文里出现了明文库名")
	}
	back, err := openExport(sealed)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	var got ExportBundle
	if err := json.Unmarshal(back, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Kind != exportKind || len(got.Scripts) != 3 {
		t.Fatalf("往返后内容不对: %+v", got)
	}
}

func TestPlanImportAppliesSettingsOnlyWhenEmpty(t *testing.T) {
	s := mkStore()
	b, _ := buildExport(s, "all", "", true)

	// 空配置：应当带上设置
	var empty Store
	out, sum, err := planImport(empty, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if !sum.Settings {
		t.Fatal("导入到空配置时应该应用设置")
	}
	if out.Settings.VerCol != "CRTER_ID" || out.Settings.Preflight == nil {
		t.Fatalf("设置没导入: %+v", out.Settings)
	}

	// 已有配置：不能覆盖本机设置
	cur := mkStore()
	on := false
	cur.Settings = Settings{Preflight: &on, VerCol: "LOCAL_ID"}
	out2, sum2, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum2.Settings {
		t.Fatal("已有配置时不该应用导入的设置")
	}
	if out2.Settings.VerCol != "LOCAL_ID" {
		t.Fatalf("本机设置被覆盖成 %q", out2.Settings.VerCol)
	}
}

// 导出子文件夹时，不能把上级文件夹里别的脚本一起带走
func TestExportGroupExcludesSiblingScripts(t *testing.T) {
	s := mkStore()
	// g1 里有 s2（兄弟脚本），g2 是 g1 的子文件夹，里面有 s1
	b, err := buildExport(s, "group", "g2", true)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(b.Scripts) != 1 || b.Scripts[0].ID != "s1" {
		t.Fatalf("只应带子文件夹里的 1 个脚本，实际 %d 个", len(b.Scripts))
	}
	if len(b.Groups) != 2 {
		t.Fatalf("上级链要带文件夹记录，实际 %d", len(b.Groups))
	}
}

// 同名文件夹绑的库不同时，必须另建一份，不能把脚本挂到别的库上
func TestImportGroupDSSConflict(t *testing.T) {
	cur := mkStore() // g1 绑 d1（demo_db）
	b, _ := buildExport(cur, "all", "", true)
	b.DataSources[0].ID = "X1"
	b.DataSources[0].Host = "10.0.0.9"
	b.Groups[0].ID = "G1"
	b.Groups[0].DSID = "X1"
	b.Groups[1].ID = "G2"
	b.Groups[1].ParentID = "G1"
	for i := range b.Scripts {
		b.Scripts[i].ID = "S" + string(rune('1'+i))
		b.Scripts[i].GroupID = "G2"
		b.Scripts[i].DSID = ""
	}

	out, sum, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum.AddedDS != 1 {
		t.Fatalf("新库应该新增 1 个连接，实际 %d", sum.AddedDS)
	}
	// 顶层应该有两个「分组A」：原来绑 d1 的，和新的绑新连接的
	var tops []Group
	for _, g := range out.Groups {
		if g.ParentID == "" && g.Name != "" {
			tops = append(tops, g)
		}
	}
	if len(tops) != 2 {
		t.Fatalf("应该有两个顶层文件夹，实际 %d 个: %+v", len(tops), tops)
	}
	names := map[string]bool{}
	for _, g := range tops {
		names[g.Name] = true
	}
	if !names["分组A"] || !names["分组A (2)"] {
		t.Fatalf("同绑不同库的文件夹应该加后缀，实际 %+v", names)
	}
	// 原文件夹必须保持绑 d1
	for _, g := range tops {
		if g.Name == "分组A" && g.DSID != "d1" {
			t.Fatalf("原文件夹的连接被改了: %q", g.DSID)
		}
	}
}

// 同一个库、不同账号，不能认成同一个连接
func TestImportMatchRequiresSameUser(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "ds", "d1", true)
	b.DataSources[0].User = "readonly"
	out, sum, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if sum.AddedDS != 1 || sum.MatchedDS != 0 {
		t.Fatalf("不同账号应该当成新连接: %+v", sum)
	}
	if len(out.DataSources) != len(cur.DataSources)+1 {
		t.Fatalf("连接数没变对")
	}
}

// 引用不存在的文件夹/连接，拒绝导入而不是默默挪位置
func TestImportRejectsDanglingRefs(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "all", "", true)

	bad := b
	bad.Scripts = append([]Script(nil), b.Scripts...)
	bad.Scripts[0].GroupID = "ghost"
	if _, _, err := planImport(cur, bad, false); err == nil {
		t.Fatal("脚本指向不存在的文件夹却导入成功了")
	}

	bad2 := b
	bad2.Groups = append([]Group(nil), b.Groups...)
	bad2.Groups[0].DSID = "ghost"
	if _, _, err := planImport(cur, bad2, false); err == nil {
		t.Fatal("文件夹指向不存在的连接却导入成功了")
	}
}

// 文件夹层级成环时要拒绝，不能死循环
func TestImportRejectsGroupCycle(t *testing.T) {
	cur := mkStore()
	b := ExportBundle{Kind: exportKind, FormatVersion: 1}
	b.Groups = []Group{
		{ID: "a", ParentID: "b", Name: "A"},
		{ID: "b", ParentID: "a", Name: "B"},
	}
	if _, _, err := planImport(cur, b, false); err == nil {
		t.Fatal("成环的文件夹层级应该被拒绝")
	}
}

// 同一份导出文件导入两次，不能越导越多
func TestImportDSConflictIdempotent(t *testing.T) {
	cur := mkStore()
	b, _ := buildExport(cur, "all", "", true)
	b.DataSources[0].ID = "X1"
	b.DataSources[0].Host = "10.0.0.9"
	b.Groups[0].ID = "G1"
	b.Groups[0].DSID = "X1"
	b.Groups[1].ID = "G2"
	b.Groups[1].ParentID = "G1"
	for i := range b.Scripts {
		b.Scripts[i].ID = "S" + string(rune('1'+i))
		b.Scripts[i].GroupID = "G2"
		b.Scripts[i].DSID = ""
	}

	out1, sum1, err := planImport(cur, b, true)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	out2, sum2, err := planImport(out1, b, true)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if sum1.Groups != 2 {
		t.Fatalf("第一次应新建 2 个文件夹，实际 %d", sum1.Groups)
	}
	if sum2.Groups != 0 || sum2.AddedDS != 0 {
		t.Fatalf("第二次导入不该再新建: %+v", sum2)
	}
	tops := 0
	for _, g := range out2.Groups {
		if g.ParentID == "" {
			tops++
		}
	}
	if tops != 2 {
		t.Fatalf("顶层文件夹应稳定在 2 个，实际 %d: %+v", tops, out2.Groups)
	}
	if len(out2.DataSources) != 3 {
		t.Fatalf("连接数应稳定在 3（原有 2 + 导入 1），实际 %d", len(out2.DataSources))
	}
}

// 本地文件夹没绑库时，不能被导入的连接填进去——否则该文件夹下原本无库的脚本会跑到导入的库上
func TestImportDoesNotFillExistingGroupDS(t *testing.T) {
	cur := mkStore()
	cur.Groups[0].DSID = "" // 本地「分组A」没绑库，但里面有脚本 s2

	b, _ := buildExport(cur, "all", "", true)
	b.Groups[0].DSID = "d2" // 导入的绑 d2
	for i := range b.Scripts {
		b.Scripts[i].ID = "S" + string(rune('1'+i))
	}
	out, _, err := planImport(cur, b, false)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	for _, g := range out.Groups {
		if g.ID == "g1" && g.DSID != "" {
			t.Fatalf("原文件夹被填上了连接 %q，会让原本没库的脚本跑到这个库上", g.DSID)
		}
	}
	// 应该另建一个绑 d2 的同名副本
	found := false
	for _, g := range out.Groups {
		if g.DSID == "d2" && g.ParentID == "" {
			found = true
		}
	}
	if !found {
		t.Fatal("没有为绑了库的版本新建文件夹")
	}
}
