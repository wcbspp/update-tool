package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// 条件重叠判断：纯文本解析，不连库、不读数据。
// 只处理常见的 `列 运算符 值` 组合；解析不了的条件会标记为「无法判定」，
// 不会假装给出结论。

type bound struct {
	v   string
	inc bool
	has bool
}

type cstr struct {
	lo, hi bound
	set    []string
	hasSet bool
}

type parsedWhere struct {
	cols    map[string]cstr // 同一列多条条件取交集后的结果
	unknown bool            // 有解析不了的条件
	atoms   int
}

var reDateLike = regexp.MustCompile(`^(\d{4})-(\d{1,2})-(\d{1,2})([ T].*)?$`)

func normVal(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "'\"`")
	// 数字统一成十进制形式，避免 '01' / '1' 被判成不同
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	// 日期/时间补零：从 Excel 粘过来的 '2025-1-15' 和 '2025-01-15' 是同一天
	if m := reDateLike.FindStringSubmatch(s); m != nil {
		y, _ := strconv.Atoi(m[1])
		mo, _ := strconv.Atoi(m[2])
		d, _ := strconv.Atoi(m[3])
		if mo >= 1 && mo <= 12 && d >= 1 && d <= 31 {
			return fmt.Sprintf("%04d-%02d-%02d%s", y, mo, d, m[4])
		}
	}
	return strings.ToLower(s)
}

func isLiteral(raw string) bool {
	r := strings.TrimSpace(raw)
	if r == "" {
		return false
	}
	if (r[0] == '\'' && r[len(r)-1] == '\'') || (r[0] == '"' && r[len(r)-1] == '"') {
		return true
	}
	if _, err := strconv.ParseFloat(r, 64); err == nil {
		return true
	}
	return strings.EqualFold(r, "null")
}

// normCol 保留表别名前缀（a.id 与 b.id 不是同一列），只去掉反引号与空白
func normCol(s string) string {
	s = strings.TrimSpace(s)
	s = strings.NewReplacer("`", "", "\"", "").Replace(s)
	s = strings.Join(strings.Fields(s), "")
	return strings.ToLower(s)
}

// splitAlias 把 a.id 拆成 (id, a)，没有别名时别名为空
func splitAlias(key string) (base, alias string) {
	if dot := strings.LastIndex(key, "."); dot >= 0 {
		return key[dot+1:], key[:dot]
	}
	return key, ""
}

// 按顶层 AND 切分，忽略括号、引号，以及 BETWEEN ... AND 里的 AND
func isSp(c byte) bool { return c == ' ' || c == '\n' || c == '\t' || c == '\r' }

func wordAt(s string, i int, w string) bool {
	if i+len(w) > len(s) {
		return false
	}
	return strings.EqualFold(s[i:i+len(w)], w)
}

func splitAnd(s string) []string {
	var out []string
	depth := 0
	inQ := byte(0)
	start := 0
	i := 0
	pendingBetween := false
	for i < len(s) {
		c := s[i]
		if inQ != 0 {
			if c == inQ {
				inQ = 0
			}
			i++
			continue
		}
		switch c {
		case '\'', '"', '`':
			inQ = c
			i++
			continue
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth == 0 && (c == 'A' || c == 'a' || c == 'B' || c == 'b') {
			leftOK := i == 0 || isSp(s[i-1])
			if leftOK && wordAt(s, i, "between") {
				right := i + 7
				if right >= len(s) || isSp(s[right]) {
					pendingBetween = true
					i = right
					continue
				}
			}
			if leftOK && wordAt(s, i, "and") {
				right := i + 3
				if right >= len(s) || isSp(s[right]) {
					if pendingBetween {
						pendingBetween = false
					} else {
						out = append(out, s[start:i])
						start = right
					}
					i = right
					continue
				}
			}
		}
		i++
	}
	out = append(out, s[start:])
	return out
}

func parseAtom(a string) (col string, c cstr, ok bool) {
	a = strings.TrimSpace(a)
	up := strings.ToUpper(a)
	// NOT IN / NOT LIKE / NOT BETWEEN 是补集，当前只做正向判断；
	// 误当成正向会给出「互不重叠」的错误绿灯，宁可判为未知
	if strings.Contains(up, " NOT ") {
		return "", cstr{}, false
	}
	switch {
	case strings.Contains(up, " BETWEEN "):
		parts := strings.SplitN(a, " ", 4)
		if len(parts) < 4 {
			return "", cstr{}, false
		}
		col = normCol(parts[0])
		// SplitN 把 "1 AND 10" 切成了 ["1", "AND 10"]，要拼回来再切 AND
		rest := strings.TrimSpace(parts[2] + " " + parts[3])
		bi := strings.Index(strings.ToUpper(rest), " AND ")
		if bi < 0 {
			return "", cstr{}, false
		}
		ra, rb := rest[:bi], rest[bi+5:]
		if !isLiteral(ra) || !isLiteral(rb) {
			return "", cstr{}, false
		}
		lo, hi := normVal(ra), normVal(rb)
		if col == "" || lo == "" || hi == "" {
			return "", cstr{}, false
		}
		return col, cstr{lo: bound{lo, true, true}, hi: bound{hi, true, true}}, true
	case strings.Contains(up, " IN "):
		op := strings.Index(up, " IN ")
		col = normCol(a[:op])
		rest := strings.TrimSpace(a[op+4:])
		if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
			return "", cstr{}, false
		}
		var vals []string
		for _, v := range strings.Split(rest[1:len(rest)-1], ",") {
			if !isLiteral(v) {
				return "", cstr{}, false
			}
			vals = append(vals, normVal(v))
		}
		if col == "" || len(vals) == 0 {
			return "", cstr{}, false
		}
		return col, cstr{set: vals, hasSet: true}, true
	case strings.Contains(up, " IS NULL"), strings.Contains(up, " IS NOT NULL"):
		return "", cstr{}, false // 空值条件不参与区间运算
	}
	for _, op := range []string{">=", "<=", "!=", "=", ">", "<"} {
		if idx := strings.Index(a, op); idx > 0 {
			col = normCol(a[:idx])
			raw := a[idx+len(op):]
			if !isLiteral(raw) {
				return "", cstr{}, false
			}
			val := normVal(raw)
			if col == "" || val == "" {
				return "", cstr{}, false
			}
			switch op {
			case "=":
				return col, cstr{lo: bound{val, true, true}, hi: bound{val, true, true}}, true
			case ">=":
				return col, cstr{lo: bound{val, true, true}}, true
			case ">":
				return col, cstr{lo: bound{val, false, true}}, true
			case "<=":
				return col, cstr{hi: bound{val, true, true}}, true
			case "<":
				return col, cstr{hi: bound{val, false, true}}, true
			case "!=":
				return "", cstr{}, false
			}
		}
	}
	if strings.Contains(up, " LIKE ") && strings.HasSuffix(strings.TrimSpace(a), "%'") {
		op := strings.Index(up, " LIKE ")
		col = normCol(a[:op])
		pre := strings.TrimSuffix(strings.Trim(normVal(a[op+6:]), "'\""), "%")
		// 前缀里还有 % 或 _ 就是通配符，不能当字面量比较，否则会误判「互不重叠」
		if col == "" || pre == "" || strings.ContainsAny(pre, "_%") {
			return "", cstr{}, false
		}
		return col, cstr{lo: bound{pre, true, true}, hi: bound{pre + "\uffff", true, true}}, true
	}
	return "", cstr{}, false
}

func cmpVal(a, b string) int {
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	if ea == nil && eb == nil {
		switch {
		case fa < fb:
			return -1
		case fa > fb:
			return 1
		}
		return 0
	}
	return strings.Compare(a, b)
}

func parseWhereStr(w string) parsedWhere {
	p := parsedWhere{cols: map[string]cstr{}}
	if strings.TrimSpace(w) == "" {
		return p
	}
	for _, atom := range splitAnd(w) {
		if strings.TrimSpace(atom) == "" {
			continue
		}
		col, c, ok := parseAtom(atom)
		if !ok {
			p.unknown = true
			continue
		}
		p.atoms++
		old, exists := p.cols[col]
		if !exists {
			p.cols[col] = c
			continue
		}
		merged, ok := mergeCstr(old, c)
		if !ok {
			p.unknown = true
			continue
		}
		p.cols[col] = merged
	}
	return p
}

func mergeCstr(a, b cstr) (cstr, bool) {
	if a.hasSet && b.hasSet {
		var both []string
		for _, x := range a.set {
			for _, y := range b.set {
				if x == y {
					both = append(both, x)
				}
			}
		}
		return cstr{set: both, hasSet: true}, len(both) > 0
	}
	if a.hasSet {
		a, b = b, a
	}
	if b.hasSet {
		var keep []string
		for _, v := range b.set {
			if inBounds(v, a) {
				keep = append(keep, v)
			}
		}
		return cstr{set: keep, hasSet: true}, len(keep) > 0
	}
	out := a
	if b.lo.has && (!out.lo.has || cmpVal(b.lo.v, out.lo.v) > 0 || (cmpVal(b.lo.v, out.lo.v) == 0 && !b.lo.inc)) {
		out.lo = b.lo
	}
	if b.hi.has && (!out.hi.has || cmpVal(b.hi.v, out.hi.v) < 0 || (cmpVal(b.hi.v, out.hi.v) == 0 && !b.hi.inc)) {
		out.hi = b.hi
	}
	if out.lo.has && out.hi.has {
		c := cmpVal(out.lo.v, out.hi.v)
		if c > 0 || (c == 0 && !(out.lo.inc && out.hi.inc)) {
			return cstr{}, false
		}
	}
	return out, true
}

func inBounds(v string, c cstr) bool {
	if c.lo.has {
		cmp := cmpVal(v, c.lo.v)
		if cmp < 0 || (cmp == 0 && !c.lo.inc) {
			return false
		}
	}
	if c.hi.has {
		cmp := cmpVal(v, c.hi.v)
		if cmp > 0 || (cmp == 0 && !c.hi.inc) {
			return false
		}
	}
	return true
}

func cstrOverlap(a, b cstr) bool {
	if a.hasSet && b.hasSet {
		for _, x := range a.set {
			for _, y := range b.set {
				if x == y {
					return true
				}
			}
		}
		return false
	}
	if a.hasSet {
		for _, v := range a.set {
			if inBounds(v, b) {
				return true
			}
		}
		return false
	}
	if b.hasSet {
		for _, v := range b.set {
			if inBounds(v, a) {
				return true
			}
		}
		return false
	}
	lo, loInc, hi, hiInc := bound{}, true, bound{}, true
	setLo := func(x bound) {
		if !x.has {
			return
		}
		if !lo.has || cmpVal(x.v, lo.v) > 0 || (cmpVal(x.v, lo.v) == 0 && !x.inc) {
			lo, loInc = x, x.inc
		}
	}
	setHi := func(x bound) {
		if !x.has {
			return
		}
		if !hi.has || cmpVal(x.v, hi.v) < 0 || (cmpVal(x.v, hi.v) == 0 && !x.inc) {
			hi, hiInc = x, x.inc
		}
	}
	setLo(a.lo)
	setLo(b.lo)
	setHi(a.hi)
	setHi(b.hi)
	if lo.has && hi.has {
		c := cmpVal(lo.v, hi.v)
		if c > 0 {
			return false
		}
		if c == 0 && !(loInc && hiInc) {
			return false
		}
	}
	return true
}

// targetCols 只挑出属于目标表的列条件。
// targetAlias 为空表示调用方不知道别名，退回旧行为（所有列都参与比较）。
// ok=false 表示语句里有归属不明的列（多表语句中的无限定列），此时不猜。
func targetCols(p parsedWhere, targetAlias string, multiStmt bool) (map[string][]cstr, bool) {
	g := map[string][]cstr{}
	if targetAlias == "" {
		// 多表语句但不知道目标表别名：无限定列究竟属于哪张表说不清，
		// 拿它判「互不重叠」会出假绿灯，宁可判未知
		if multiStmt {
			return nil, false
		}
		for k, v := range p.cols {
			base, _ := splitAlias(k)
			g[base] = append(g[base], v)
		}
		return g, true
	}
	// 语句里出现别的别名，或调用方已知这是多表语句 → 无限定列的归属不明
	multi := multiStmt
	for k := range p.cols {
		if _, alias := splitAlias(k); alias != "" && !strings.EqualFold(alias, targetAlias) {
			multi = true
			break
		}
	}
	for k, v := range p.cols {
		base, alias := splitAlias(k)
		switch {
		case alias == "":
			if multi {
				return nil, false
			}
			g[base] = append(g[base], v)
		case strings.EqualFold(alias, targetAlias):
			g[base] = append(g[base], v)
		}
	}
	return g, true
}

// condsOverlapOn: 两条 WHERE 是否可能改到同一行，只看目标表的条件。
// 关联表的列不决定目标行，不参与比较；known=false 表示解析不出来，不下结论。
func condsOverlapOn(whereA, aliasA string, multiA bool, whereB, aliasB string, multiB bool) (overlap, known bool) {
	a, b := parseWhereStr(whereA), parseWhereStr(whereB)
	if a.unknown || b.unknown || a.atoms == 0 || b.atoms == 0 {
		return false, false
	}
	groupA, okA := targetCols(a, aliasA, multiA)
	groupB, okB := targetCols(b, aliasB, multiB)
	if !okA || !okB {
		return false, false
	}
	shared := 0
	for base, cbs := range groupB {
		cas, ok := groupA[base]
		if !ok {
			continue
		}
		if len(cas) > 1 || len(cbs) > 1 {
			return false, false // 同一列出现多次，配不准，按未知处理
		}
		shared++
		if !cstrOverlap(cas[0], cbs[0]) {
			return false, true // 某一列明确不相交 → 两条语句不可能同时命中同一行
		}
	}
	if shared == 0 {
		return false, false // 没有共同列，无法判断
	}
	return true, true
}

// condsOverlap: 不知道目标表别名时的退化版本（所有列都参与比较）。
func condsOverlap(whereA, whereB string) (overlap, known bool) {
	return condsOverlapOn(whereA, "", false, whereB, "", false)
}

// overlapTarget: 同表的一条语句。alias 是目标表在这条语句里的别名，
// 空表示调用方不知道（如 DELETE），行为与旧版一致。
type overlapTarget struct {
	where string
	alias string
	multi bool // 这条语句是多表写法：WHERE 里的无限定列可能属于关联表
}

// overlapVerdict: 同一张表被多条语句更新时的结论。
// 任一对条件可能重叠 → 提示包含关系；全部可判定且不重叠 → 安全；
// 有任何一对无法判定 → 标注需人工核对。
func overlapVerdict(targets []overlapTarget) (verdict, level string) {
	overlapN, unknownN := 0, 0
	for i := 0; i < len(targets); i++ {
		for j := i + 1; j < len(targets); j++ {
			ov, known := condsOverlapOn(targets[i].where, targets[i].alias, targets[i].multi,
				targets[j].where, targets[j].alias, targets[j].multi)
			switch {
			case !known:
				unknownN++
			case ov:
				overlapN++
			}
		}
	}
	switch {
	case overlapN > 0:
		return "条件存在包含关系，暂未发现冲突，请人工核对", "warn"
	case unknownN > 0:
		return "条件无法自动比对，暂未发现冲突，请人工核对", "warn"
	}
	return "条件互不重叠，不会互相覆盖", "ok"
}
