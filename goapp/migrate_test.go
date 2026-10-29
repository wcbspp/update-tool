package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// 配置文件层面的测试：明文迁移、加密落盘、损坏保护。

func writeRaw(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatalf("写文件失败: %v", err)
	}
	return p
}

// 老版本的明文 data.json 必须能直接读，不能因为加了加密就读不出来
func TestLoadLegacyPlaintext(t *testing.T) {
	dir := setupHome(t)
	legacy := []byte(`{"datasources":[{"id":"d1","name":"老配置","host":"127.0.0.1","port":3306,"user":"root","password":"old","database":"db","env":"test"}],"groups":[],"scripts":[]}`)
	writeRaw(t, dir, "data.json", legacy)

	s := loadStore()
	if storeCorrupt {
		t.Fatal("明文配置被误判成损坏")
	}
	if len(s.DataSources) != 1 || s.DataSources[0].Name != "老配置" {
		t.Fatalf("明文配置没读出来: %+v", s.DataSources)
	}
	if s.DataSources[0].Pass != "old" {
		t.Fatal("口令没读出来")
	}
}

// 保存一次之后文件必须变成密文，且再读回来内容一致
func TestSaveMigratesToCiphertext(t *testing.T) {
	dir := setupHome(t)
	legacy := []byte(`{"datasources":[{"id":"d1","name":"老配置","host":"127.0.0.1","port":3306,"user":"root","password":"old","database":"db","env":"test"}],"groups":[],"scripts":[]}`)
	writeRaw(t, dir, "data.json", legacy)

	s := loadStore()
	if err := saveStore(s); err != nil {
		t.Fatalf("保存失败: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "data.json"))
	if err != nil {
		t.Fatalf("读文件失败: %v", err)
	}
	if !bytes.HasPrefix(raw, []byte(cfgMagic)) {
		head := raw
		if len(head) > 8 {
			head = head[:8]
		}
		t.Fatalf("保存后不是密文，文件头 = %q", string(head))
	}
	if bytes.Contains(raw, []byte("old")) || bytes.Contains(raw, []byte("老配置")) {
		t.Fatal("密文里还能看到明文内容")
	}

	// 重新读一遍，数据必须一模一样
	s2 := loadStore()
	if storeCorrupt {
		t.Fatal("刚写的密文读不出来")
	}
	if len(s2.DataSources) != 1 || s2.DataSources[0].Pass != "old" {
		t.Fatalf("往返后数据不一致: %+v", s2.DataSources)
	}

	// 权限必须是 600
	st, err := os.Stat(filepath.Join(dir, "data.json"))
	if err != nil {
		t.Fatalf("stat 失败: %v", err)
	}
	if perm := st.Mode().Perm(); perm != 0o600 {
		t.Fatalf("权限应为 600，实际 %o", perm)
	}
}

// 密文被改一个字节 → 必须判损坏，并且拒绝再写（否则等于把用户数据清空）
func TestCorruptCiphertextRefusesWrite(t *testing.T) {
	dir := setupHome(t)
	writeRaw(t, dir, "data.json", []byte(`{"datasources":[],"groups":[],"scripts":[]}`))
	if err := saveStore(loadStore()); err != nil {
		t.Fatalf("首次保存失败: %v", err)
	}

	p := filepath.Join(dir, "data.json")
	raw, _ := os.ReadFile(p)
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatalf("写坏文件失败: %v", err)
	}

	s := loadStore()
	if !storeCorrupt {
		t.Fatal("被改动的密文没有判成损坏")
	}
	if len(s.DataSources) != 0 {
		t.Fatal("损坏时不该返回任何数据")
	}

	// 关键：不能覆盖原文件
	s.DataSources = append(s.DataSources, DataSource{ID: "new", Name: "新加的"})
	if err := saveStore(s); err == nil {
		t.Fatal("损坏状态下保存应该被拒绝")
	}
	after, _ := os.ReadFile(p)
	if !bytes.Equal(after, raw) {
		t.Fatal("损坏的文件被覆盖了")
	}
}

// 既不是密文也不是 JSON 的文件：当损坏处理，不猜、不覆盖
func TestLoadRejectsUnknownFormat(t *testing.T) {
	dir := setupHome(t)
	writeRaw(t, dir, "data.json", []byte("\x00\x01\x02 random binary"))

	if s := loadStore(); len(s.DataSources) != 0 {
		t.Fatal("未知格式不应解析出数据")
	}
	if !storeCorrupt {
		t.Fatal("未知格式应标记损坏")
	}
	if err := saveStore(Store{}); err == nil {
		t.Fatal("未知格式下保存应被拒绝")
	}
}

// 空文件：当作损坏处理。文件存在却 0 字节只能是外部改动（保存是原子替换，
// 不会写出半截），此时拒写比默默当成空配置再覆盖安全。
func TestLoadEmptyFileIsCorrupt(t *testing.T) {
	dir := setupHome(t)
	writeRaw(t, dir, "data.json", nil)
	s := loadStore()
	if !storeCorrupt {
		t.Fatal("空的 data.json 应当判为损坏（拒写，避免覆盖）")
	}
	if len(s.DataSources) != 0 {
		t.Fatal("损坏时不该返回数据")
	}
	if err := saveStore(Store{}); err == nil {
		t.Fatal("损坏状态下保存应被拒绝")
	}
}

// 文件不存在：首次启动，正常空配置
func TestLoadMissingFile(t *testing.T) {
	setupHome(t)
	s := loadStore()
	if storeCorrupt {
		t.Fatal("文件不存在不该判损坏")
	}
	if len(s.DataSources) != 0 {
		t.Fatal("首次启动应为空配置")
	}
}

// 正常保存后不能留下 .tmp 垃圾
func TestSaveAtomicNoTempLeftover(t *testing.T) {
	dir := setupHome(t)
	if err := saveStore(Store{DataSources: []DataSource{{ID: "a", Name: "A"}}}); err != nil {
		t.Fatalf("保存失败: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "data.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("保存后残留了 .tmp 文件")
	}
}

// 保存失败时：原文件不能被改，也不能留下 .tmp
func TestSaveFailureKeepsOriginal(t *testing.T) {
	dir := setupHome(t)
	if err := saveStore(Store{DataSources: []DataSource{{ID: "a", Name: "A"}}}); err != nil {
		t.Fatalf("首次保存失败: %v", err)
	}
	path := filepath.Join(dir, "data.json")
	orig, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读原文件失败: %v", err)
	}

	// 把目录设成不可写，模拟磁盘/权限故障
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("改目录权限失败: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o700) })

	if err := saveStore(Store{DataSources: []DataSource{{ID: "b", Name: "B"}}}); err == nil {
		t.Skip("当前身份仍能写这个目录（可能是 root），无法构造失败路径")
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("原文件读不到了: %v", err)
	}
	if !bytes.Equal(orig, after) {
		t.Fatal("保存失败却改动了原文件")
	}

	os.Chmod(dir, 0o700) // 恢复后再检查残留
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("保存失败留下了 .tmp 文件")
	}
}

// 每次保存的密文都不同（nonce 随机），但解出来一样
func TestSaveNonceVaries(t *testing.T) {
	dir := setupHome(t)
	s := Store{DataSources: []DataSource{{ID: "a", Name: "A"}}}
	if err := saveStore(s); err != nil {
		t.Fatalf("第一次保存失败: %v", err)
	}
	first, _ := os.ReadFile(filepath.Join(dir, "data.json"))
	if err := saveStore(s); err != nil {
		t.Fatalf("第二次保存失败: %v", err)
	}
	second, _ := os.ReadFile(filepath.Join(dir, "data.json"))
	if bytes.Equal(first, second) {
		t.Fatal("两次保存产生相同密文，说明 nonce 没随机")
	}
	if len(second) != len(first) {
		t.Fatalf("同样内容长度应一致: %d vs %d", len(first), len(second))
	}
}
