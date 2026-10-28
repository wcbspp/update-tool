package main

import "testing"

func TestIndexCoversJoin(t *testing.T) {
	cases := []struct {
		name  string
		index []string
		join  []string
		want  bool
	}{
		{"前缀命中单列", []string{"A", "B"}, []string{"A"}, true},
		{"非最左列不算", []string{"A", "B"}, []string{"B"}, false},
		{"完整两列", []string{"A", "B"}, []string{"A", "B"}, true},
		{"连接列顺序无关", []string{"A", "B"}, []string{"B", "A"}, true},
		{"三列索引撑两列", []string{"A", "B", "C"}, []string{"A", "B"}, true},
		{"三列索引撑后两列", []string{"A", "B", "C"}, []string{"B", "C"}, false},
		{"索引列不够", []string{"A"}, []string{"A", "B"}, false},
		{"大小写不敏感", []string{"a", "b"}, []string{"A"}, true},
		{"空连接列", []string{"A"}, nil, false},
	}
	for _, c := range cases {
		if got := indexCoversJoin(c.index, c.join); got != c.want {
			t.Errorf("%s: indexCoversJoin(%v,%v)=%v want %v", c.name, c.index, c.join, got, c.want)
		}
	}
}
