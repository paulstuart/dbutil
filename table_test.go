package dbutil

import (
	"os"
	"testing"
)

func TestTable(t *testing.T) {
	if testing.Verbose() {
		testout = os.Stdout
	}
	db := structDb(t)
	prepare(db)
	query := `select * from structs`

	if err := NewStreamer(db, query).Table(testout, true, nil); err != nil {
		t.Fatal(err)
	}
}

func TestTableError(t *testing.T) {
	db := structDb(t)
	prepare(db)
	if err := NewStreamer(db, queryBad).Table(testout, true, nil); err == nil {
		t.Fatal("expected bad query error")
	} else {
		t.Log(err)
	}
}
