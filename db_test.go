package dbutil

import (
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

type testMap map[int64]testStruct

func init() {
	os.Remove(test_file)
}

func TestSqliteCreate(t *testing.T) {
	test_db, err := dbOpen(test_file)
	if err != nil {
		log.Fatal(err)
	}
	defer test_db.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	create table structs (id integer not null primary key, name text);
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
	test_db, _ = dbOpen(test_file)
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

func TestSqliteObj(t *testing.T) {
	sql := `create table structs (
        id integer not null primary key,
        name text,
        kind int,
        data blob,
        created     DATETIME DEFAULT CURRENT_TIMESTAMP
    );
	`
    _, err := test_db.Exec(sql)
	if err != nil {
		log.Printf("%q: %s\n", err, sql)
		return
	}

    query := "insert into structs (name,kind,data) values(?,?,?)"
	_, err = test_db.Update(query, "bob", 23, "bakers")
	if err != nil {
		fmt.Println("INSERT ERROR: ", err)
    }
	_, err = test_db.Update(query, "betty", 23, "bowers")
	if err != nil {
		fmt.Println("INSERT ERROR: ", err)
    }

    results := test_db.LoadMap(testMap{}, "select * from structs").(testMap)
    for k,v := range results {
        fmt.Println("K:",k,"V:",v)
    }
}

func TestTable(t *testing.T) {
    // TODO: Table chokes on time value
    query := "select id,name,kind from structs"
    table, err := test_db.Table(query)
	if err != nil {
		t.Fatal(err)
	}
    table.Print(true)
}
