package main

import (
	"reflect"
	"strings"
	"testing"
)

// splitStatements 是整个工具最脆弱的一环：分号切错，两条语句会被当成一条。
func TestSplitStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{
			name: "两条普通语句",
			in:   "select 1; select 2;",
			want: []string{"select 1", "select 2"},
		},
		{
			name: "结尾没有分号",
			in:   "select 1;\nselect 2",
			want: []string{"select 1", "select 2"},
		},
		{
			name: "字符串里的分号",
			in:   "select ';' as a; select 2",
			want: []string{"select ';' as a", "select 2"},
		},
		{
			name: "字符串里的双写单引号",
			in:   "select 'it''s; ok'; select 2",
			want: []string{"select 'it''s; ok'", "select 2"},
		},
		{
			name: "行注释里的分号",
			in:   "select 1 -- 注意 ; 这里\n; select 2",
			want: []string{"select 1", "select 2"},
		},
		{
			// 注释会被剥掉，这是设计：发到数据库前不留注释
			name: "块注释里的分号",
			in:   "select /* ; */ 1; select 2",
			want: []string{"select 1", "select 2"},
		},
		{
			name: "反引号里的分号",
			in:   "select `a;b` from t;",
			want: []string{"select `a;b` from t"},
		},
		{
			name: "空语句被跳过",
			in:   "select 1;;  ;select 2",
			want: []string{"select 1", "select 2"},
		},
		{
			name: "空文本",
			in:   "",
			want: nil,
		},
		{
			name: "只有注释",
			in:   "-- 只是说明\n",
			want: nil,
		},
	}

	norm := func(in []string) []string {
		out := make([]string, len(in))
		for i, s := range in {
			out[i] = strings.Join(strings.Fields(s), " ")
		}
		return out
	}

	for _, c := range cases {
		got := norm(splitStatements(c.in))
		if !reflect.DeepEqual(got, norm(c.want)) {
			t.Errorf("%s: 期望 %q，实际 %q", c.name, c.want, got)
		}
	}
}

// 跨行写法不该被判成“缺分号”
func TestValidateTextContinuation(t *testing.T) {
	ok := []string{
		"insert INTO hilist_b\nSELECT *\nFROM hilist_b_update_2107;",
		"update hilist_b a\nset a.ENDDATE = now()\nwhere 1=1;",
	}
	for _, sql := range ok {
		for _, c := range validateText(sql) {
			if c["title"] == "疑似缺少分号" {
				t.Errorf("跨行语句被误判缺分号: %q", sql)
			}
		}
	}

	bad := "update hilist_b set MEMO=MEMO\nselect 1;"
	found := false
	for _, c := range validateText(bad) {
		if c["title"] == "疑似缺少分号" {
			found = true
		}
	}
	if !found {
		t.Error("真缺分号没有检出")
	}
}
