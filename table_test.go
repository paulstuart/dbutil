package dbutil

import (
	"testing"
)

func TestHTML(t *testing.T) {
	table, err := test_db.Table("select * from foo")
	if err != nil {
		t.Fatal(err)
	}
	table.SetLinks(0, "/x/%s/%s", 0, 1)
	for row := range table.HTML() {
		t.Log("ROW")
		for col := range row.Columns() {
			t.Log("COL", col)
		}
	}
}
