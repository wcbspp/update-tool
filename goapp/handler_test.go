package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// 保存失败必须回 500，而不是把自己递归到栈溢出（曾导致整个进程消失）
func TestSaveFailedReturns500(t *testing.T) {
	rec := httptest.NewRecorder()
	saveFailed(rec, errors.New("磁盘只读"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("期望 500，实际 %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("返回不是 JSON：%v（%s）", err, rec.Body.String())
	}
	if body["ok"] != false || !strings.Contains(body["error"].(string), "磁盘只读") {
		t.Fatalf("错误信息不对：%v", body)
	}
}

// 执行/预检期间改配置要被 409 挡住
func TestBusyBlocksConfigWrites(t *testing.T) {
	done := beginBusy("执行")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/script?id=x", nil)
	handler(rec, req)
	done()
	if rec.Code != http.StatusConflict {
		t.Fatalf("忙碌时删除脚本应返回 409，实际 %d（%s）", rec.Code, rec.Body.String())
	}
}

// 非忙碌时不该被 409 误伤（这里落到找不到脚本）
func TestNotBusyPassesThrough(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/api/script?id=不存在", nil)
	handler(rec, req)
	if rec.Code == http.StatusConflict {
		t.Fatalf("空闲时不该返回 409")
	}
}

// 回滚修复：MySQL 的 \' 转义不能被当作字符串结尾
func TestSplitStatementsBackslashEscape(t *testing.T) {
	got := splitStatements(`UPDATE p SET a='O\'Brien;x' WHERE id=1; SELECT 1`)
	if len(got) != 2 {
		t.Fatalf("应切成 2 条，实际 %d：%#v", len(got), got)
	}
	if !strings.Contains(got[0], "Brien;x") {
		t.Fatalf("第一条被切坏了：%q", got[0])
	}
}

// INSERT ... VALUES 的常量元组解析
func TestParseValuesTuples(t *testing.T) {
	tuples := parseValuesTuples(`VALUES ('a,1', 2, CONCAT('x',',')), ('b', 3, 4) ON DUPLICATE KEY UPDATE x=1`)
	if len(tuples) != 2 {
		t.Fatalf("应解析出 2 组，实际 %d：%#v", len(tuples), tuples)
	}
	if len(tuples[0]) != 3 || strings.TrimSpace(tuples[0][0]) != "'a,1'" {
		t.Fatalf("第一组解析不对：%#v", tuples[0])
	}
	if len(tuples[1]) != 3 || strings.TrimSpace(tuples[1][2]) != "4" {
		t.Fatalf("第二组解析不对：%#v", tuples[1])
	}
}

// Host 白名单（挡 DNS rebinding）
func TestHostAllowList(t *testing.T) {
	if !hostAllowed("127.0.0.1", nil) || !hostAllowed("localhost", nil) {
		t.Fatal("本机 Host 应放行")
	}
	if hostAllowed("evil.example.com", nil) {
		t.Fatal("外部 Host 必须挡住")
	}
}

// 老的 dl 地址必须自动改指对象存储；没配过也用默认值（不再静默不检查更新）
func TestEffectiveUpdateURL(t *testing.T) {
	cases := map[string]string{
		"":                                  defaultUpdateURL,
		"https://dl.example.com/":           defaultUpdateURL,
		"http://dl.example.com":             defaultUpdateURL,
		"https://oss-api.example.com/x/":    "https://oss-api.example.com/x/",
		"https://self.example.com/updates/": "https://self.example.com/updates/",
	}
	for in, want := range cases {
		if got := effectiveUpdateURL(in); got != want {
			t.Fatalf("%q → %q，期望 %q", in, got, want)
		}
	}
}
