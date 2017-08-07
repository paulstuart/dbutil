package dbutil

import (
	"os"
	"testing"
)

func TestStrLen(t *testing.T) {
	u := unknownStruct{}
	l := strlen(u)
	if l != 0 {
		t.Fatalf("expected: %d, got:%d\n", 0, l)
	}
}

func TestTabular(t *testing.T) {
	if testing.Verbose() {
		testout = os.Stdout
	}
	db := structDb(t)
	prepare(db)
	query := `select * from structs`

	tw, table := Tabular(testout, true)
	if err := Stream(db, table, query); err != nil {
		t.Fatal(err)
	}
	tw.Flush()
}

func TestTabularWriter(t *testing.T) {
	if testing.Verbose() {
		testout = os.Stdout
	}
	db := structDb(t)
	prepare(db)
	query := `select * from structs`

	tw, table := Tabular(nil, true)
	if err := Stream(db, table, query); err != nil {
		t.Fatal(err)
	}
	tw.Flush()
}
