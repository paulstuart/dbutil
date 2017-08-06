package dbutil

import (
	"testing"
)

func TestTabular(t *testing.T) {
	db := fakeHammer(t, 5, 20)
	query := "select id as system_id, worker as system_work, counter as system_cntr, ts as system_modified from hammer"

	tw, table := Tabular(testout, true)
	if err := Stream(db, table, query); err != nil {
		t.Fatal(err)
	}
	tw.Flush()
}
