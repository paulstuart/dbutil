package dbutil

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"testing/iotest"
	"time"
)

var (
	test_db   DBU
	test_file = "test.db"
	w         = iotest.NewWriteLogger("", os.Stderr)
)

func init() {
	fmt.Println("INIT")
}

type testStruct struct {
	ID      int64     `sql:"id" key:"true" table:"structs"`
	Name    string    `sql:"name"`
	Kind    int       `sql:"kind"`
	Data    []byte    `sql:"data"`
	Created time.Time //`sql:"created" update:"false"`
}

func (s *testStruct) TableName() string {
	return "structs"
}

func (s *testStruct) KeyField() string {
	return "id"
}

func (s *testStruct) InsertFields() string {
	return "name,kind,data"
}

func (s *testStruct) SelectFields() string {
	//return "id,name,kind,data,created"
	return "id,name,kind,data"
}

func (s *testStruct) UpdateValues() []interface{} {
	return []interface{}{s.Name, s.Kind, s.Data, s.ID}
}

func (s *testStruct) MemberPointers() []interface{} {
	//return []interface{}{&s.ID, &s.Name, &s.Kind, &s.Data, &s.Created}
	return []interface{}{&s.ID, &s.Name, &s.Kind, &s.Data}
}

func (s *testStruct) InsertValues() []interface{} {
	return []interface{}{s.Name, s.Kind, s.Data}
}

func (s *testStruct) SetID(id int64) {
	s.ID = id
}

func (s *testStruct) Key() int64 {
	return s.ID
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
	fmt.Println("CREATE")
	test_db, err := Open(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	defer test_db.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = test_db.Exec(sql)
	if err != nil {
		t.Log("%q: %s\n", err, sql)
		return
	}

	_, err = test_db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := test_db.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		t.Log(id, name)
	}
}

func TestSqliteDelete(t *testing.T) {
	test_db, _ = Open(test_file, true)
	cnt, err := test_db.Update("delete from foo where id=?", 13)
	if err != nil {
		t.Fatal("DELETE ERROR: ", err)
	}
	t.Log("DELETED: ", cnt)
}

func TestSqliteInsert(t *testing.T) {
	cnt, err := test_db.Update("insert into foo (id,name) values(?,?)", 13, "bakers")
	if err != nil {
		t.Log("INSERT ERROR: ", err)
	}
	t.Log("INSERTED: ", cnt)
}

func TestSqliteUpdate(t *testing.T) {
	cnt, err := test_db.Update("update foo set id=23 where id > ? and name like ?", "3", "bi%")
	if err != nil {
		t.Log("UPDATE ERROR: ", err)
	}
	t.Log("UPDATED: ", cnt)
}

func TestSqliteType(t *testing.T) {
	var cnt int
	cnt = -2
	test_db.GetType("select count(*) from foo where id > ? and name like ?", &cnt, "3", "b%")
	t.Log("COUNT: ", cnt)
}

func TestSqliteString(t *testing.T) {
	var name string
	test_db.GetType("select name from foo where id > ? and name like ?", &name, "3", "bi%")
	t.Log("NAME: ", name)
}

func TestSqliteTable(t *testing.T) {
	t.Skip("Need a way to test tables in non verbose fashion")
	table, _ := test_db.Table("select id, name from foo where id > ? and name like ?", "3", "b%")
	table.Dumper(w, true)
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

func TestFindBy(t *testing.T) {
	s := testStruct{}
	if err := test_db.FindBy(&s, "name", "Bobby Tables"); err != nil {
		t.Error(err)
	}
	t.Log("BY NAME", s)
	u := testStruct{}
	if err := test_db.FindBy(&u, "id", 1); err != nil {
		t.Error(err)
	}
	t.Log("BY ID", u)
}

func TestSelf(t *testing.T) {
	s := testStruct{ID: 1}
	if err := test_db.FindSelf(&s); err != nil {
		t.Error(err)
	}
	t.Log("BY SELF", s)
}

func TestVersionPre(t *testing.T) {
	v, err := test_db.Version()
	if err != nil {
		t.Error(err)
	}
	t.Log("VERSION PRE:", v)
}

func TestDBObject(t *testing.T) {
	s := &testStruct{
		Name: "Grammatic, Bro",
		Kind: 2001,
		Data: []byte("lorem ipsum"),
	}
	if err := test_db.Add(s); err != nil {
		t.Fatal(err)
	}
	s.Kind = 2015
	s.Name = "Void droid"
	if err := test_db.Save(s); err != nil {
		t.Fatal(err)
	}
	z := testStruct{}
	if err := test_db.Find(&z, QueryKeys{"kind": 2015}); err != nil {
		t.Fatal(err)
	}
	t.Log("FOUND", z)

	if err := test_db.Delete(s); err != nil {
		t.Fatal(err)
	}
}

func TestVersionPost(t *testing.T) {
	v, err := test_db.Version()
	if err != nil {
		t.Error(err)
	}
	t.Log("VERSION POST:", v)
}

func TestLoadMap(t *testing.T) {
	results := test_db.LoadMap(testMap{}, "select * from structs").(testMap)
	for k, v := range results {
		t.Log("K:", k, "V:", v)
	}
}

func TestStream(t *testing.T) {
	myStream := func(columns []string, count int, buffer []sql.RawBytes) {
		t.Log("STREAM COLS:", columns)
		for _, b := range buffer {
			t.Log("STREAM V:", string(b))
		}
	}
	query := "select id,name,kind from structs"
	err := test_db.Stream(myStream, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamCSV(t *testing.T) {
	query := "select id,name,kind from structs"
	t.Log("\nCSV:")
	err := test_db.StreamCSV(os.Stdout, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamTab(t *testing.T) {
	query := "select id,name,kind from structs"
	t.Log("\nTAB:")
	err := test_db.StreamTab(os.Stdout, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamJSON(t *testing.T) {
	query := "select id,name,kind from structs"
	err := test_db.StreamJSON(os.Stdout, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamObject(t *testing.T) {
	err := test_db.StreamObjects(os.Stdout, &testStruct{})
	if err != nil {
		t.Fatal(err)
	}
}

func numbChk(t *testing.T, s string) {
	t.Log(s, isNumber(s))
}

func TestIsNumb(t *testing.T) {
	numbChk(t, "10")
	numbChk(t, "10x")
	numbChk(t, "x10")
	numbChk(t, "10.1")
	numbChk(t, "10.1.2.3")
	numbChk(t, "1way")
	numbChk(t, "10 ")
	numbChk(t, " 1 ")
}

func TestBackup(t *testing.T) {
	err := test_db.Backup("test_backup.db")
	if err != nil {
		t.Fatal(err)
	}
}
