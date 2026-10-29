package main

import "testing"

func TestCondsOverlap(t *testing.T) {
	cases := []struct {
		name        string
		a, b        string
		wantOverlap bool
		wantKnown   bool
	}{
		{"相同等值", "a.CODE = 'X'", "b.CODE = 'X'", true, true},
		{"不同等值", "a.CODE = 'X'", "b.CODE = 'Y'", false, true},
		{"区间相交", "a.BEGNDATE >= '2026-01-01'", "b.BEGNDATE >= '2026-06-01'", true, true},
		{"区间不相交", "a.BEGNDATE < '2026-01-01'", "b.BEGNDATE > '2026-06-01'", false, true},
		{"等值与区间上界", "a.D = '2026-03-01'", "b.D > '2026-06-01'", false, true},
		{"IN 与等值命中", "a.CODE IN ('X','Y')", "b.CODE = 'Y'", true, true},
		{"IN 与等值不命中", "a.CODE IN ('X','Y')", "b.CODE = 'Z'", false, true},
		{"等值与不等号边界", "a.D = '2026-01-01'", "b.D > '2026-01-01'", false, true},
		{"多列同时区分", "a.CODE='X' AND a.D>='2026-01-01'", "b.CODE='X' AND b.D<'2026-01-01'", false, true},
		{"没有共同列", "a.CODE='X'", "b.OTHER='Y'", false, false},
		{"含函数无法判定", "a.DATE(a.D)='2026-01-01'", "b.D='2026-01-01'", false, false},
		{"列比列无法判定", "a.CODE = b.CODE", "a.CODE = 'X'", false, false},
		{"同语句双别名同列", "a.CODE='X' AND b.CODE='Y'", "c.CODE='X'", false, false},
	}
	for _, c := range cases {
		ov, known := condsOverlap(c.a, c.b)
		if known != c.wantKnown || (known && ov != c.wantOverlap) {
			t.Errorf("%s: got overlap=%v known=%v, want overlap=%v known=%v",
				c.name, ov, known, c.wantOverlap, c.wantKnown)
		}
	}
}

func TestOverlapVerdict(t *testing.T) {
	cases := []struct {
		name    string
		targets []overlapTarget
		level   string
	}{
		{"两条互斥", []overlapTarget{{where: "CODE='X'"}, {where: "CODE='Y'"}}, "ok"},
		{"两条重叠", []overlapTarget{{where: "D>='2026-01-01'"}, {where: "D>='2026-06-01'"}}, "warn"},
		{"无法判定", []overlapTarget{{where: "a.CODE=b.CODE"}, {where: "CODE='X'"}}, "warn"},
		{"单条语句", []overlapTarget{{where: "CODE='X'"}}, "ok"},
		// 已知别名：关联表的条件不决定目标行，不该把结论搅成「无法判定」
		{"关联表条件不干扰", []overlapTarget{
			{where: "h.CODE='X' AND p.KIND='1'", alias: "h", multi: true},
			{where: "h.CODE='Y' AND p.KIND='2'", alias: "h", multi: true},
		}, "ok"},
		{"目标表条件仍能判重叠", []overlapTarget{
			{where: "h.CODE='X' AND p.KIND='1'", alias: "h", multi: true},
			{where: "h.CODE='X' AND p.KIND='2'", alias: "h", multi: true},
		}, "warn"},
		{"别名不同也按目标表列配对", []overlapTarget{
			{where: "a.CODE='X'", alias: "a"},
			{where: "b.CODE='Y'", alias: "b"},
		}, "ok"},
		{"多表语句里的无限定列不猜", []overlapTarget{
			{where: "CODE='X' AND p.KIND='1'", alias: "h", multi: true},
			{where: "CODE='Y'", alias: "h", multi: true},
		}, "warn"},
		{"多表语句全靠无限定列不判互斥", []overlapTarget{
			{where: "POOLAREA_NO='1'", alias: "h", multi: true},
			{where: "POOLAREA_NO='2'", alias: "h", multi: true},
		}, "warn"},
		{"单表语句无限定列照常判互斥", []overlapTarget{
			{where: "POOLAREA_NO='1'", alias: "h"},
			{where: "POOLAREA_NO='2'", alias: "h"},
		}, "ok"},
	}
	for _, c := range cases {
		if _, lvl := overlapVerdict(c.targets); lvl != c.level {
			t.Errorf("%s: level=%s want %s", c.name, lvl, c.level)
		}
	}
}

func TestCondsOverlapOn(t *testing.T) {
	cases := []struct {
		name                 string
		a, aliasA, b, aliasB string
		multiA, multiB       bool
		wantOverlap          bool
		wantKnown            bool
	}{
		{"关联表条件不影响目标行判定", "h.CODE='X' AND p.K='1'", "h", "h.CODE='Y' AND p.K='2'", "h", true, true, false, true},
		{"目标列相同仍重叠", "h.CODE='X' AND p.K='1'", "h", "h.CODE='X' AND p.K='2'", "h", true, true, true, true},
		{"别名不同按列名配对", "a.CODE='X'", "a", "b.CODE='Y'", "b", false, false, false, true},
		{"目标表没有可判定列", "h.DATE(d)='2026-01-01'", "h", "h.CODE='X'", "h", true, true, false, false},
		{"多表语句无限定列不猜", "CODE='X' AND p.K='1'", "h", "CODE='Y'", "h", true, true, false, false},
		{"单表无限定列正常判定", "CODE='X'", "h", "CODE='Y'", "h", false, false, false, true},
		{"多表语句纯无限定列不猜", "POOLAREA_NO='1'", "h", "POOLAREA_NO='2'", "h", true, true, false, false},
		// DELETE 走这条：多表写法但拿不到目标表别名，无限定列归属不明，不能判互斥
		{"多表语句别名未知不猜", "POOLAREA_NO='1'", "", "POOLAREA_NO='2'", "", true, true, false, false},
		{"多表语句别名未知但条件明确", "h.POOLAREA_NO='1'", "", "h.POOLAREA_NO='2'", "", true, true, false, false},
		// 单表且别名未知（旧路径）行为必须不变
		{"单表别名未知照常判定", "POOLAREA_NO='1'", "", "POOLAREA_NO='2'", "", false, false, false, true},
	}
	for _, c := range cases {
		ov, known := condsOverlapOn(c.a, c.aliasA, c.multiA, c.b, c.aliasB, c.multiB)
		if known != c.wantKnown || (known && ov != c.wantOverlap) {
			t.Errorf("%s: got overlap=%v known=%v, want overlap=%v known=%v",
				c.name, ov, known, c.wantOverlap, c.wantKnown)
		}
	}
}
