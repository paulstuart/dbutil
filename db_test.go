package dbutil

import (
	"database/sql"
	"log"
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

type testStruct struct {
	ID       int64     `sql:"id" key:"true" table:"structs"`
	Name     string    `sql:"name"`
	Kind     int       `sql:"kind"`
	Data     []byte    `sql:"data"`
	Modified time.Time `sql:"modified" update:"false"`
}

func (s *testStruct) Names() []string {
	return []string{
		"ID",
		"Name",
		"Kind",
		"Data",
		"Modified",
	}
}

func (s *testStruct) TableName() string {
	return "structs"
}

func (s *testStruct) KeyField() string {
	return "id"
}

func (s *testStruct) KeyName() string {
	return "ID"
}

func (s *testStruct) InsertFields() string {
	return "name,kind,data"
}

func (s *testStruct) SelectFields() string {
	return "id,name,kind,data,modified"
}

func (s *testStruct) UpdateValues() []interface{} {
	return []interface{}{s.Name, s.Kind, s.Data, s.ID}
}

func (s *testStruct) MemberPointers() []interface{} {
	return []interface{}{&s.ID, &s.Name, &s.Kind, &s.Data, &s.Modified}
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

func (s *testStruct) ModifiedBy(u int64, t time.Time) {
	s.Modified = t
}

const struct_sql = `create table if not exists structs (
        id integer not null primary key,
        name text,
        kind int,
        data blob,
        modified   DATETIME DEFAULT CURRENT_TIMESTAMP
    );`

type testMap map[int64]testStruct

func init() {
	os.Remove(test_file)
}

func TestFuncs(t *testing.T) {
	sqlInit(DriverName, "")
	db, err := sql.Open("dbutil", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const create = `create table if not exists iptest ( ip int )`
	const ins = `
insert into iptest values(fromIPv4('127.0.0.1'));
insert into iptest values(fromIPv4('192.168.1.1'));
`
	_, err = db.Exec(create)
	if err != nil {
		t.Logf("%q: %s\n", err, create)
		return
	}
	i, err := db.Exec(ins)
	t.Log("INSERT:", i)
	if err != nil {
		t.Logf("%q: %s\n", err, ins)
		return
	}
	var ip int64
	var ipv4 string
	dump(t, db, "select * from iptest", ip)
	dump(t, db, "select toIPv4(ip) as ipv4 from iptest", ipv4)
}

func TestSqliteCreate(t *testing.T) {
	test_db, err := Open(test_file, "", true)
	if err != nil {
		t.Fatal(err)
	}
	defer test_db.DB.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = test_db.DB.Exec(sql)
	if err != nil {
		t.Logf("%q: %s\n", err, sql)
		return
	}

	_, err = test_db.DB.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := test_db.DB.Query("select id, name from foo")
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
	test_db, _ = Open(test_file, "", true)
	cnt, err := test_db.Update("delete from foo where id=?", 13)
	if err != nil {
		t.Fatal("DELETE ERROR: ", err)
	}
	test_db.DB.Close()
	t.Log("DELETED: ", cnt)
}

func TestSqliteInsert(t *testing.T) {
	test_db, _ = Open(test_file, "", true)
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

func TestHTML(t *testing.T) {
	table, err := test_db.Table("select * from foo")
	if err != nil {
		t.Fatal(err)
	}
	table.SetLinks(0, "/x/%s/%s", 0, 1)
	for row := range table.HTMLRows() {
		t.Log("ROW")
		for col := range row.Columns() {
			t.Log("COL", col)
		}
	}
}

func TestObjects(t *testing.T) {
	_, err := test_db.DB.Exec(struct_sql)
	if err != nil {
		t.Fatal(err)
	}
	s1 := testStruct{
		Name:     "Bobby Tables",
		Kind:     23,
		Data:     []byte("binary data"),
		Modified: time.Now(),
	}
	s1.ID, err = test_db.ObjectInsert(s1)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s2 := testStruct{
		Name:     "Master Blaster",
		Kind:     999,
		Data:     []byte("whatever you like"),
		Modified: time.Now(),
	}
	s2.ID, err = test_db.ObjectInsert(s2)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s3 := testStruct{
		Name:     "A, Keeper",
		Kind:     123,
		Data:     []byte("stick around"),
		Modified: time.Now(),
	}
	s3.ID, err = test_db.ObjectInsert(s3)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s1.Kind = 99
	err = test_db.ObjectUpdate(s1)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	s2.Name = "New Name"
	err = test_db.ObjectUpdate(s2)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	err = test_db.ObjectDelete(s2)
	if err != nil {
		t.Errorf("OBJ DELETE ERROR: %s", err)
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
	myStream := func(columns []string, count int, buffer []interface{}) {
		t.Log("STREAM COLS:", columns)
		for _, b := range toString(buffer) {
			t.Log("STREAM V:", b)
		}
	}
	query := "select id,name,kind,modified from structs"
	err := test_db.Stream(myStream, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamCSV(t *testing.T) {
	query := "select id,name,kind from structs"
	t.Log("\nCSV:")
	out := (*twriter)(t)
	err := test_db.StreamCSV(out, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamTab(t *testing.T) {
	query := "select id,name,kind from structs"
	t.Log("\nTAB:")
	out := (*twriter)(t)
	err := test_db.StreamTab(out, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamJSON(t *testing.T) {
	query := "select id,name,kind from structs"
	out := (*twriter)(t)
	err := test_db.StreamJSON(out, query)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStreamObject(t *testing.T) {
	s := &testStruct{Modified: time.Now()}
	out := (*twriter)(t)
	err := test_db.StreamObjects(out, s)
	if err != nil {
		t.Fatal(err)
	}
	test_db.DB.Close()
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

type twriter testing.T

func NewTestlog(t *testing.T) *log.Logger {
	w := (*twriter)(t)
	return log.New(w, "", 0)
}

func (w *twriter) Write(p []byte) (int, error) {
	t := (*testing.T)(w)
	t.Logf("%s\n", string(p))
	return len(p), nil
}

func populate(db DBU) {
	db.Update(struct_sql)
	db.Insert("insert into structs(name, kind, data) values(?,?,?)", "abc", 23, "what ev er")
	db.Insert("insert into structs(name, kind, data) values(?,?,?)", "def", 69, "m'kay")
	db.Insert("insert into structs(name, kind, data) values(?,?,?)", "hij", 42, "meaning of life")
	db.Insert("insert into structs(name, kind, data) values(?,?,?)", "klm", 2, "of a kind")
}

func TestBackup(t *testing.T) {
	test_db, err := Open(test_file, "", true)
	if err != nil {
		t.Fatal(err)
	}
	defer test_db.DB.Close()

	populate(test_db)
	v1, _ := test_db.Version()
	t.Log("Version prior to backup:", v1)
	t.Log("Backed up:", test_db.BackedUp)
	t.Log("Changed prior to backup:", test_db.Changed())

	tlog := NewTestlog(t)
	err = test_db.Backup("test_backup.db", tlog)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Changed post backup:", test_db.Changed())
	v2, _ := DBVersion("test.db")
	t.Log("Version of backup:", v2)
	t.Log("Backed up:", test_db.BackedUp)
}

func dump(t *testing.T, db *sql.DB, query string, args ...interface{}) {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]interface{}, len(args))
	for i, f := range args {
		dest[i] = &f
	}
	for rows.Next() {
		rows.Scan(dest...)
		t.Log(args...)
	}
	rows.Close()
}
