package main

import (
	"database/sql/driver"
	"os"
	"testing"
	"time"

	dbu "github.com/paulstuart/dbutil"
)

const (
	test_file = "test.db"
)

var (
	test_db dbu.DBU
)

func (o *testStruct) IV() []driver.Value {
	return []driver.Value{o.Name, o.Kind, o.Data, o.Created}
}

func TestInit(t *testing.T) {
	var err error
	os.Remove(test_file)
	test_db, err = dbu.Open(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	_, err = test_db.DB.Exec(testSchema)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCreateExec(b *testing.B) {
	query := "insert into teststruct (name,kind,data,created) values(?,?,?,?)"
	args := []interface{}{
		"Grammatic, Bro",
		2001,
		[]byte("lorem ipsum"),
		time.Now(),
	}
	for i := 0; i < b.N; i++ {
		_, err := test_db.DB.Exec(query, args...)
		if err != nil {
			b.Log("EXEC ERR", err)
		}
	}
}
