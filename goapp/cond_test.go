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
		name   string
		wheres []string
		level  string
	}{
		{"两条互斥", []string{"CODE='X'", "CODE='Y'"}, "ok"},
		{"两条重叠", []string{"D>='2026-01-01'", "D>='2026-06-01'"}, "warn"},
		{"无法判定", []string{"a.CODE=b.CODE", "CODE='X'"}, "warn"},
		{"单条语句", []string{"CODE='X'"}, "ok"},
	}
	for _, c := range cases {
		if _, lvl := overlapVerdict(c.wheres); lvl != c.level {
			t.Errorf("%s: level=%s want %s", c.name, lvl, c.level)
		}
	}
}
