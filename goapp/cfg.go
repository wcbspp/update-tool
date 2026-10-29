package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// 配置文件加密。
//
// 密钥编译在二进制里——**它不是安全边界**，只是让 data.json 无法被文本编辑器
// 或云盘/备份扫描直接读取。拿到程序的人仍能把内容解出来，这一点文档里已写明，
// 不要在这里假装它更安全。
var cfgKey = [32]byte{
	0x8a, 0x70, 0x85, 0x9f, 0x6e, 0xa7, 0xc8, 0x9c, 0x99, 0x3c, 0x40, 0xdb, 0xf2, 0xbb, 0x92, 0x3c,
	0x31, 0xc9, 0x0c, 0x4a, 0x3e, 0xc2, 0xfa, 0x06, 0x4d, 0xa6, 0x97, 0x14, 0x11, 0x0b, 0x61, 0x1a,
}

const (
	cfgMagic    = "MHJ1" // 配置文件
	exportMagic = "MHX1" // 导出文件（同一密钥，靠魔数区分，防止把 data.json 当导出文件导入）
	cfgFormat   = 1

	exportKind = "medupdate-export"
)

var errNotSealed = errors.New("不是本程序的加密文件")

// sealWith: [魔数][格式版本][nonce][密文]，魔数与版本号同时作为 AAD，改一个字节就解不开
func sealWith(key []byte, magic string, plain []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	hdr := make([]byte, 0, len(magic)+1)
	hdr = append(hdr, magic...)
	hdr = append(hdr, cfgFormat)

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(hdr)+len(nonce)+len(plain)+gcm.Overhead())
	out = append(out, hdr...)
	out = append(out, nonce...)
	return gcm.Seal(out, nonce, plain, hdr), nil
}

func openWith(key []byte, magic string, b []byte) ([]byte, error) {
	if !bytes.HasPrefix(b, []byte(magic)) {
		return nil, errNotSealed
	}
	rest := b[len(magic):]
	if len(rest) < 1 {
		return nil, errors.New("文件被截断，读不到格式版本")
	}
	if rest[0] != cfgFormat {
		return nil, fmt.Errorf("文件格式版本 %d 不认识，请升级程序", rest[0])
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	hdr := b[:len(magic)+1]
	body := rest[1:]
	if len(body) < gcm.NonceSize()+gcm.Overhead() {
		return nil, errors.New("文件被截断，内容不完整")
	}
	nonce, ct := body[:gcm.NonceSize()], body[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ct, hdr)
	if err != nil {
		return nil, errors.New("解开失败：文件被改动或损坏")
	}
	return plain, nil
}

func sealCfg(plain []byte) ([]byte, error)    { return sealWith(cfgKey[:], cfgMagic, plain) }
func openCfg(b []byte) ([]byte, error)        { return openWith(cfgKey[:], cfgMagic, b) }
func sealExport(plain []byte) ([]byte, error) { return sealWith(cfgKey[:], exportMagic, plain) }
func openExport(b []byte) ([]byte, error)     { return openWith(cfgKey[:], exportMagic, b) }

// looksLikeJSON: 老的明文配置一定以 { 开头
func looksLikeJSON(b []byte) bool {
	t := bytes.TrimLeft(b, " \t\r\n")
	return len(t) > 0 && t[0] == '{'
}

/* ---------------- 导出 / 导入 ---------------- */

// ExportBundle: 导出文件的内容。写进去之前整体加密，导出文件本身永远是密文。
type ExportBundle struct {
	Kind          string       `json:"kind"`
	FormatVersion int          `json:"formatVersion"`
	ExportedAt    string       `json:"exportedAt"`
	AppVersion    string       `json:"appVersion"`
	DataSources   []DataSource `json:"datasources,omitempty"`
	Groups        []Group      `json:"groups,omitempty"`
	Scripts       []Script     `json:"scripts,omitempty"`
	Settings      *Settings    `json:"settings,omitempty"`
}

// buildExport: scope = all | group | script
func buildExport(s Store, scope, id string, includePassword bool) (ExportBundle, error) {
	b := ExportBundle{
		Kind:          exportKind,
		FormatVersion: 1,
		ExportedAt:    time.Now().Format(time.RFC3339),
		AppVersion:    appVersion,
	}
	gset := map[string]bool{}
	sset := map[string]bool{}

	switch scope {
	case "all":
		for _, g := range s.Groups {
			gset[g.ID] = true
		}
		for _, sc := range s.Scripts {
			sset[sc.ID] = true
		}
		st := s.Settings
		b.Settings = &st
		b.DataSources = append([]DataSource(nil), s.DataSources...)

	case "group":
		found := false
		for _, g := range s.Groups {
			if g.ID == id {
				found = true
			}
		}
		if !found {
			return b, errors.New("找不到这个文件夹")
		}
		// 只收这个文件夹的子树；祖先只带文件夹记录，不带它们里面的脚本
		subtree := map[string]bool{}
		var walk func(string)
		walk = func(gid string) {
			if gid == "" || subtree[gid] {
				return
			}
			subtree[gid] = true
			for _, g := range s.Groups {
				if g.ParentID == gid {
					walk(g.ID)
				}
			}
		}
		walk(id)
		for _, sc := range s.Scripts {
			if subtree[sc.GroupID] {
				sset[sc.ID] = true
			}
		}
		for gid := range subtree {
			gset[gid] = true
		}
		// 祖先链加个数兜底：data.json 被手改成父子成环时不至于死循环
		for pid, n := parentOf(s, id), 0; pid != "" && n <= len(s.Groups); pid, n = parentOf(s, pid), n+1 {
			gset[pid] = true
		}

	case "script":
		var sc *Script
		for i := range s.Scripts {
			if s.Scripts[i].ID == id {
				sc = &s.Scripts[i]
			}
		}
		if sc == nil {
			return b, errors.New("找不到这个脚本")
		}
		sset[sc.ID] = true
		for pid, n := sc.GroupID, 0; pid != "" && n <= len(s.Groups); pid, n = parentOf(s, pid), n+1 {
			gset[pid] = true
		}

	case "ds":
		var found bool
		for _, d := range s.DataSources {
			if d.ID == id {
				b.DataSources = append(b.DataSources, d)
				found = true
			}
		}
		if !found {
			return b, errors.New("找不到这个连接")
		}

	default:
		return b, errors.New("导出范围不认识")
	}

	for _, g := range s.Groups {
		if gset[g.ID] {
			b.Groups = append(b.Groups, g)
		}
	}
	for _, sc := range s.Scripts {
		if sset[sc.ID] {
			b.Scripts = append(b.Scripts, sc)
		}
	}

	// 数据源：只带被引用到的（all 已经在上面全带了）
	if scope != "all" {
		need := map[string]bool{}
		for _, g := range b.Groups {
			if g.DSID != "" {
				need[g.DSID] = true
			}
		}
		for _, sc := range b.Scripts {
			if sc.DSID != "" {
				need[sc.DSID] = true
			}
		}
		for _, d := range s.DataSources {
			if need[d.ID] {
				b.DataSources = append(b.DataSources, d)
			}
		}
	}

	if !includePassword {
		for i := range b.DataSources {
			b.DataSources[i].Pass = ""
		}
	}
	return b, nil
}

// putGroup: 同名同父级就复用已有文件夹（并把空着的连接补齐），否则新建。返回本机 ID。
func putGroup(out *Store, g Group, newParent string, dsMap map[string]string, sum *ImportSummary) string {
	newDS := ""
	if g.DSID != "" {
		newDS = dsMap[g.DSID]
	}
	if id, ok := findGroupByName(out.Groups, newParent, g.Name); ok {
		curDS := ""
		for _, x := range out.Groups {
			if x.ID == id {
				curDS = x.DSID
				break
			}
		}
		// 只有「两边都没绑」或「绑的是同一个库」才复用。
		// 本地没绑、导入绑了的不算复用：把连接填进去会让这个文件夹里原本无库的
		// 本地脚本突然指向导入的库执行，比多一个文件夹危险得多。
		if curDS == newDS {
			return id
		}
		// 同绑不同库：先找已经建过的同库副本（重复导入同一份文件时幂等），
		// 找不到再新建一个带后缀的
		if newDS != "" {
			if id, ok := findGroupBySuffixed(out.Groups, newParent, g.Name, newDS); ok {
				return id
			}
		}
	}
	ng := g
	ng.ID = newID()
	ng.ParentID = newParent
	ng.DSID = newDS
	ng.Name = uniqueGroupName(out.Groups, newParent, g.Name)
	ng.Order = nextGroupOrder(*out, newParent)
	out.Groups = append(out.Groups, ng)
	sum.Groups++
	return ng.ID
}

// findGroupBySuffixed: 找同父级、同数据源、名字是 base 或 "base (n)" 的文件夹。
// 用于让同一份导出文件重复导入时不再多建副本。
func findGroupBySuffixed(list []Group, parent, base, dsid string) (string, bool) {
	base = strings.TrimSpace(base)
	for _, g := range list {
		if g.ParentID != parent || g.DSID != dsid {
			continue
		}
		n := strings.TrimSpace(g.Name)
		if n == base {
			return g.ID, true
		}
		if strings.HasPrefix(n, base+" (") && strings.HasSuffix(n, ")") {
			mid := n[len(base)+2 : len(n)-1]
			if mid != "" && strings.Trim(mid, "0123456789") == "" {
				return g.ID, true
			}
		}
	}
	return "", false
}

func uniqueGroupName(list []Group, parent, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "导入的文件夹"
	}
	if _, ok := findGroupByName(list, parent, name); !ok {
		return name
	}
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s (%d)", name, i)
		if _, ok := findGroupByName(list, parent, cand); !ok {
			return cand
		}
	}
}

func findGroupByName(list []Group, parent, name string) (string, bool) {
	name = strings.TrimSpace(name)
	for _, g := range list {
		if g.ParentID == parent && strings.TrimSpace(g.Name) == name {
			return g.ID, true
		}
	}
	return "", false
}

func parentOf(s Store, gid string) string {
	for _, g := range s.Groups {
		if g.ID == gid {
			return g.ParentID
		}
	}
	return ""
}

// ImportSummary: 导入结果，前端据此提示
type ImportSummary struct {
	AddedDS      int      `json:"addedDs"`
	MatchedDS    int      `json:"matchedDs"`
	Groups       int      `json:"groups"`
	Scripts      int      `json:"scripts"`
	Updated      int      `json:"updated"`
	Settings     bool     `json:"settings"`
	NeedPassword []string `json:"needPassword"`
}

// planImport: 纯函数，不改动传入的 Store。overwrite=true 时同名脚本覆盖 SQL。
func planImport(cur Store, b ExportBundle, overwrite bool) (Store, ImportSummary, error) {
	var sum ImportSummary
	if b.Kind != exportKind {
		return cur, sum, errors.New("这不是本程序的导出文件")
	}
	if b.FormatVersion != 1 {
		return cur, sum, fmt.Errorf("导出格式版本 %d 不认识，请升级程序", b.FormatVersion)
	}

	// 引用完整性：文件里引用的文件夹/连接必须都在文件里。
	// 查不到就当错报，否则脚本会被默默挪到根目录或丢掉连接。
	dsIDs := map[string]bool{}
	for _, d := range b.DataSources {
		dsIDs[d.ID] = true
	}
	gIDs := map[string]bool{}
	for _, g := range b.Groups {
		gIDs[g.ID] = true
	}
	for _, g := range b.Groups {
		if g.ParentID != "" && !gIDs[g.ParentID] {
			return cur, sum, fmt.Errorf("文件夹「%s」的上级不在导出文件里，已拒绝导入", g.Name)
		}
		if g.DSID != "" && !dsIDs[g.DSID] {
			return cur, sum, fmt.Errorf("文件夹「%s」引用的连接不在导出文件里，已拒绝导入", g.Name)
		}
	}
	for _, sc := range b.Scripts {
		if sc.GroupID != "" && !gIDs[sc.GroupID] {
			return cur, sum, fmt.Errorf("脚本「%s」所在的文件夹不在导出文件里，已拒绝导入", sc.Name)
		}
		if sc.DSID != "" && !dsIDs[sc.DSID] {
			return cur, sum, fmt.Errorf("脚本「%s」引用的连接不在导出文件里，已拒绝导入", sc.Name)
		}
	}

	out := cur
	out.DataSources = append([]DataSource(nil), cur.DataSources...)
	out.Groups = append([]Group(nil), cur.Groups...)
	out.Scripts = append([]Script(nil), cur.Scripts...)

	// 1) 数据源：按 host+port+database 匹配本机已有的
	dsMap := map[string]string{}
	for _, d := range b.DataSources {
		if id, ok := matchDS(out.DataSources, d); ok {
			dsMap[d.ID] = id
			sum.MatchedDS++
			if d.Pass != "" {
				for i := range out.DataSources {
					if out.DataSources[i].ID == id && out.DataSources[i].Pass == "" {
						out.DataSources[i].Pass = d.Pass
					}
				}
			} else {
				for i := range out.DataSources {
					if out.DataSources[i].ID == id && out.DataSources[i].Pass == "" {
						sum.NeedPassword = append(sum.NeedPassword, out.DataSources[i].Name)
					}
				}
			}
			continue
		}
		nd := d
		nd.ID = newID()
		nd.Name = uniqueDSName(out.DataSources, d.Name)
		dsMap[d.ID] = nd.ID
		out.DataSources = append(out.DataSources, nd)
		sum.AddedDS++
		if nd.Pass == "" {
			sum.NeedPassword = append(sum.NeedPassword, nd.Name)
		}
	}

	// 2) 文件夹：同名同父级就复用（绑的库不一致时另建一份），
	//    避免导入后多出一整套一模一样的目录
	pending := append([]Group(nil), b.Groups...)
	gMap := map[string]string{}
	for len(pending) > 0 {
		var rest []Group
		for _, g := range pending {
			newParent := ""
			if g.ParentID != "" {
				v, ok := gMap[g.ParentID]
				if !ok {
					rest = append(rest, g)
					continue
				}
				newParent = v
			}
			gMap[g.ID] = putGroup(&out, g, newParent, dsMap, &sum)
		}
		if len(rest) == len(pending) {
			return cur, sum, errors.New("导出文件里的文件夹层级有环，已拒绝导入")
		}
		pending = rest
	}

	// 3) 脚本
	for _, sc := range b.Scripts {
		ns := sc
		if sc.GroupID != "" {
			ns.GroupID = gMap[sc.GroupID]
		}
		if sc.DSID != "" {
			ns.DSID = dsMap[sc.DSID]
		}
		if overwrite {
			if old := findScriptByName(out.Scripts, ns.GroupID, ns.Name); old >= 0 {
				out.Scripts[old].SQL = ns.SQL
				sum.Updated++
				continue
			}
		}
		ns.ID = newID()
		ns.Name = uniqueScriptName(out.Scripts, ns.GroupID, ns.Name)
		ns.Order = nextScriptOrder(out, ns.GroupID)
		out.Scripts = append(out.Scripts, ns)
		sum.Scripts++
	}
	// 4) 设置：只在本机还是空的（换电脑/首次导入）时应用，
	//    避免把同事的监听范围、升级地址盖到已在用的机器上
	if b.Settings != nil && len(cur.DataSources) == 0 && len(cur.Groups) == 0 && len(cur.Scripts) == 0 {
		out.Settings = *b.Settings
		sum.Settings = true
	}
	sum.NeedPassword = uniqStrings(sum.NeedPassword)
	return out, sum, nil
}

func uniqStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := map[string]bool{}
	out := in[:0]
	for _, s := range in {
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// matchDS: 同一个库 + 同一个账号才算同一个连接。
// 账号也要比：同一个库上可能有只读和可写两个账号，认错就把脚本执行到别人的权限下了。
func matchDS(list []DataSource, d DataSource) (string, bool) {
	for _, x := range list {
		if strings.EqualFold(x.Host, d.Host) && x.Port == d.Port &&
			strings.EqualFold(strings.TrimSpace(x.User), strings.TrimSpace(d.User)) &&
			strings.EqualFold(strings.TrimSpace(x.Database), strings.TrimSpace(d.Database)) {
			return x.ID, true
		}
	}
	return "", false
}

func uniqueDSName(list []DataSource, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "导入的连接"
	}
	taken := func(n string) bool {
		for _, x := range list {
			if x.Name == n {
				return true
			}
		}
		return false
	}
	if !taken(name) {
		return name
	}
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s (%d)", name, i)
		if !taken(cand) {
			return cand
		}
	}
}

func findScriptByName(list []Script, gid, name string) int {
	for i := range list {
		if list[i].GroupID == gid && list[i].Name == name {
			return i
		}
	}
	return -1
}

func uniqueScriptName(list []Script, gid, name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "导入的脚本"
	}
	taken := func(n string) bool { return findScriptByName(list, gid, n) >= 0 }
	if !taken(name) {
		return name
	}
	for i := 2; ; i++ {
		cand := fmt.Sprintf("%s (%d)", name, i)
		if !taken(cand) {
			return cand
		}
	}
}
