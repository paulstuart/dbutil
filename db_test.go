package dbutil

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"
)

var (
	test_db   DBU
	test_file = "test.db"
)

type testStruct struct {
	ID      int64     `sql:"id" key:"true" table:"structs"`
	Name    string    `sql:"name"`
	Kind    int       `sql:"kind"`
	Data    []byte    `sql:"data"`
	Created time.Time `sql:"created" update:"false"`
}

const struct_sql = `create table structs (
        id integer not null primary key,
        name text,
        kind int,
        data blob,
        created     DATETIME DEFAULT CURRENT_TIMESTAMP
    );`

type testMap map[int64]testStruct

func init() {
	os.Remove(test_file)
}

func TestSqliteCreate(t *testing.T) {
	test_db, err := Open(test_file, true)
	if err != nil {
		log.Fatal(err)
	}
	defer test_db.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = test_db.Exec(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
		return
	}

	_, err = test_db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := test_db.Query("select id, name from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		fmt.Println(id, name)
	}
}

func TestSqliteDelete(t *testing.T) {
	test_db, _ = Open(test_file, true)
	cnt, err := test_db.Update("delete from foo where id=?", 13)
	if err != nil {
		fmt.Println("DELETE ERROR: ", err)
	}
	fmt.Println("DELETED: ", cnt)
}

func TestSqliteInsert(t *testing.T) {
	cnt, err := test_db.Update("insert into foo (id,name) values(?,?)", 13, "bakers")
	if err != nil {
		fmt.Println("INSERT ERROR: ", err)
	}
	fmt.Println("INSERTED: ", cnt)
}

func TestSqliteUpdate(t *testing.T) {
	cnt, err := test_db.Update("update foo set id=23 where id > ? and name like ?", "3", "bi%")
	if err != nil {
		fmt.Println("UPDATE ERROR: ", err)
	}
	fmt.Println("UPDATED: ", cnt)
}

func TestSqliteType(t *testing.T) {
	var cnt int
	cnt = -2
	test_db.GetType("select count(*) from foo where id > ? and name like ?", &cnt, "3", "b%")
	fmt.Println("COUNT: ", cnt)
}

func TestSqliteString(t *testing.T) {
	var name string
	test_db.GetType("select name from foo where id > ? and name like ?", &name, "3", "bi%")
	fmt.Println("NAME: ", name)
}

func TestSqliteTable(t *testing.T) {
	table, _ := test_db.Table("select id, name from foo where id > ? and name like ?", "3", "b%")
	table.Dumper(os.Stdout, true)
}

func TestObjects(t *testing.T) {
	_, err := test_db.Exec(struct_sql)
	if err != nil {
		t.Fatal(err)
	}
	s1 := testStruct{
		Name:    "Bobby Tables",
		Kind:    23,
		Data:    []byte("binary data"),
		Created: time.Now(),
	}
	s1.ID, err = test_db.ObjectInsert(s1)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: ", err)
	}
	s2 := testStruct{
		Name:    "Master Blaster",
		Kind:    999,
		Data:    []byte("whatever you like"),
		Created: time.Now(),
	}
	s2.ID, err = test_db.ObjectInsert(s2)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: ", err)
	}
	s3 := testStruct{
		Name:    "A, Keeper",
		Kind:    123,
		Data:    []byte("stick around"),
		Created: time.Now(),
	}
	s3.ID, err = test_db.ObjectInsert(s3)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: ", err)
	}
	/*
	   test_db.Update("update structs set kind=? where id=?", 99, s1.ID)
	   test_db.Update("update structs set name=? where id=?", "Master Update", s2.ID)
	*/
	s1.Kind = 99
	err = test_db.ObjectUpdate(s1)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: ", err)
	}
	s2.Name = "New Name"
	err = test_db.ObjectUpdate(s2)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: ", err)
	}
	err = test_db.ObjectDelete(s2)
	if err != nil {
		t.Errorf("OBJ DELETE ERROR: ", err)
	}
}

func TestLoadMap(t *testing.T) {
	results := test_db.LoadMap(testMap{}, "select * from structs").(testMap)
	for k, v := range results {
		fmt.Println("K:", k, "V:", v)
	}
}

func TestTable(t *testing.T) {
	query := "select id,name,kind from structs"
	table, err := test_db.Table(query)
	if err != nil {
		t.Fatal(err)
	}
	table.Print(true)
}

func myStream(columns []string, count int, buffer []sql.RawBytes) {
	fmt.Println("COLS:", columns)
	for _, b := range buffer {
		fmt.Println("V:", string(b))
	}
}

func TestStream(t *testing.T) {
	query := "select id,name,kind from structs"
	err := test_db.Stream(myStream, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamCSV(t *testing.T) {
	query := "select id,name,kind from structs"
	fmt.Println("\nCSV:")
	err := test_db.StreamCSV(os.Stdout, query)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println()
}

func TestStreamTab(t *testing.T) {
	query := "select id,name,kind from structs"
	fmt.Println("\nTAB:")
	err := test_db.StreamTab(os.Stdout, query)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println()
}

func TestBackup(t *testing.T) {
	err := test_db.Backup("test_backup.db")
	if err != nil {
		t.Fatal(err)
	}
}
