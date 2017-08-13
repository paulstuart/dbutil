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
	if err := NewStreamer(db).Stream(table, query); err != nil {
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
	if err := NewStreamer(db).Stream(table, query); err != nil {
		t.Fatal(err)
	}
	tw.Flush()
}

func TestPrintTable(t *testing.T) {
	db := structDb(t)
	prepare(db)
	const query = `select * from structs`
	if err := PrintTable(db, nil, true, query); err != nil {
		t.Fatal(err)
	}
}

func TestPrintTableError(t *testing.T) {
	db := structDb(t)
	prepare(db)
	if err := PrintTable(db, nil, true, queryBad); err == nil {
		t.Fatal("expected bad query error")
	} else {
		t.Log(err)
	}
}
