package dbutil

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

const (
	default_query = "select id,name,kind,modified from structs"
)

var (
	test_file = "test.db"
	w         = iotest.NewWriteLogger("", os.Stderr)
)

type testStruct struct {
	ID       int64     `sql:"id" key:"true" table:"structs"`
	Name     string    `sql:"name"`
	Kind     int       `sql:"kind"`
	Data     []byte    `sql:"data"`
	Modified time.Time `sql:"modified" update:"false"`
	astring  string
	anint    int
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
insert into iptest values(atoip('127.0.0.1'));
insert into iptest values(atoip('192.168.1.1'));
`
	if _, err = db.Exec(create); err != nil {
		t.Fatalf("%q: %s\n", err, create)
	}

	if _, err = db.Exec(ins); err != nil {
		t.Fatalf("%q: %s\n", err, ins)
	}

	/*
		var ip int64
		var ipv4 string
		dump(t, db, "select * from iptest", ip)
		dump(t, db, "select iptoa(ip) as ipv4 from iptest", ipv4)
	*/
}

func TestSqliteCreate(t *testing.T) {
	db, err := NewDBU(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = db.DB.Exec(sql)
	if err != nil {
		t.Logf("%q: %s\n", err, sql)
		t.FailNow()
	}

	_, err = db.DB.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.DB.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		t.Log(id, name)
	}
	rows.Close()
}

func TestSqliteDelete(t *testing.T) {
	db, _ := NewDBU(test_file, true)
	cnt, err := db.Update("delete from foo where id=?", 13)
	if err != nil {
		t.Fatal("DELETE ERROR: ", err)
	}
	db.DB.Close()
	t.Log("DELETED: ", cnt)
}

func TestSqliteInsert(t *testing.T) {
	db, _ := NewDBU(test_file, true)
	cnt, err := db.Update("insert into foo (id,name) values(?,?)", 13, "bakers")
	if err != nil {
		t.Log("INSERT ERROR: ", err)
	}
	t.Log("INSERTED: ", cnt)
}

func TestSqliteUpdate(t *testing.T) {
	db, _ := NewDBU(test_file, true)
	cnt, err := db.Update("update foo set id=23 where id > ? and name like ?", "3", "bi%")
	if err != nil {
		t.Log("UPDATE ERROR: ", err)
	}
	t.Log("UPDATED: ", cnt)
}

func TestSqliteType(t *testing.T) {
	db, err := NewDBU(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	var cnt int64
	cnt = -2
	db.GetType("select count(*) from foo where id > ? and name like ?", &cnt, "3", "b%")
	t.Log("COUNT: ", cnt)
}

func TestSqliteString(t *testing.T) {
	var name string
	structDBU(t).GetType("select name from foo where id > ? and name like ?", &name, "3", "bi%")
	t.Log("NAME: ", name)
}

func structDb(t *testing.T) *sql.DB {
	db, err := Open(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	/*
		if _, _, err := Exec(db, struct_sql); err != nil {
			t.Fatal(err)
		}
	*/
	prepare(db)
	return db
}

func structDBU(t *testing.T) DBU {
	return DBU{DB: structDb(t)}
}

func TestHTML(t *testing.T) {
	db, err := NewDBU(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	table, err := db.Table("select * from foo")
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
	db := structDBU(t)
	s1 := testStruct{
		Name:     "Bobby Tables",
		Kind:     23,
		Data:     []byte("binary data"),
		Modified: time.Now(),
	}
	var err error
	s1.ID, err = db.ObjectInsert(s1)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s2 := testStruct{
		Name:     "Master Blaster",
		Kind:     999,
		Data:     []byte("whatever you like"),
		Modified: time.Now(),
	}
	s2.ID, err = db.ObjectInsert(s2)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s3 := testStruct{
		Name:     "A, Keeper",
		Kind:     123,
		Data:     []byte("stick around"),
		Modified: time.Now(),
	}
	s3.ID, err = db.ObjectInsert(s3)
	if err != nil {
		t.Errorf("OBJ INSERT ERROR: %s", err)
	}
	s1.Kind = 99
	err = db.ObjectUpdate(s1)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	s2.Name = "New Name"
	err = db.ObjectUpdate(s2)
	if err != nil {
		t.Errorf("OBJ UPDATE ERROR: %s", err)
	}
	err = db.ObjectDelete(s2)
	if err != nil {
		t.Errorf("OBJ DELETE ERROR: %s", err)
	}
}

func TestFindBy(t *testing.T) {
	db := structDBU(t)
	s := testStruct{}
	if err := db.FindBy(&s, "name", "Bobby Tables"); err != nil {
		t.Error(err)
	}
	t.Log("BY NAME", s)
	u := testStruct{}
	if err := db.FindBy(&u, "id", 1); err != nil {
		t.Error(err)
	}
	t.Log("BY ID", u)
}

func TestSelf(t *testing.T) {
	db := structDBU(t)
	s := testStruct{ID: 1}
	if err := db.FindSelf(&s); err != nil {
		t.Error(err)
	}
	t.Log("BY SELF", s)
}

func xTestVersionPre(t *testing.T) {
	db := structDBU(t)
	v, err := db.Version()
	if err != nil {
		t.Error(err)
	}
	t.Log("VERSION PRE:", v)
}

func TestDBObject(t *testing.T) {
	db := structDBU(t)
	s := &testStruct{
		Name: "Grammatic, Bro",
		Kind: 2001,
		Data: []byte("lorem ipsum"),
	}
	if err := db.Add(s); err != nil {
		t.Fatal(err)
	}
	s.Kind = 2015
	s.Name = "Void droid"
	if err := db.Save(s); err != nil {
		t.Fatal(err)
	}
	z := testStruct{}
	if err := db.Find(&z, QueryKeys{"kind": 2015}); err != nil {
		t.Fatal(err)
	}
	//t.Log("FOUND", z)

	if err := db.Delete(s); err != nil {
		t.Fatal(err)
	}
}

/*
func TestVersionPost(t *testing.T) {
	v, err := test_db.Version()
	if err != nil {
		t.Error(err)
	}
	t.Log("VERSION POST:", v)
}
*/

func TestLoadMap(t *testing.T) {
	db := structDBU(t)
	results := db.LoadMap(testMap{}, "select * from structs").(testMap)
	for k, v := range results {
		t.Log("K:", k, "V:", v)
	}
}

func TestStream(t *testing.T) {
	db := structDb(t)
	myStream := func(columns []string, count int, buffer []interface{}, err error) {
		if len(columns) == 0 {
			t.Error("no columns")
		}
		if false { // TODO: how to manage?
			t.Log("STREAM COLS:", columns)
			for _, b := range toString(buffer) {
				t.Log("STREAM V:", b)
			}
		}
	}
	query := "select id,name,kind,modified from structs"
	if err := Stream(db, myStream, query); err != nil {
		t.Fatal(err)
	}
}

func TestStreamInvalidQuery(t *testing.T) {
	db := structDb(t)
	myStream := func(columns []string, count int, buffer []interface{}, err error) {
		if len(columns) == 0 {
			t.Error("no columns")
		}
		if false { // TODO: how to manage?
			t.Log("STREAM COLS:", columns)
			for _, b := range toString(buffer) {
				t.Log("STREAM V:", b)
			}
		}
	}
	query := "THIS IS NOT SQL"
	if err := Stream(db, myStream, query); err == nil {
		t.Fatal("expected query error")
	}
}

type Writer struct {
	Prefix string
}

func (w *Writer) Write(p []byte) (n int, err error) {
	/*
		fmt.Print(w.Prefix)
		return fmt.Print(string(p))
	*/
	return fmt.Fprint(ioutil.Discard, string(p))
}

func TestStreamCSV(t *testing.T) {
	db := structDb(t)
	query := "select id,name,kind from structs"
	w := &Writer{"CSV:"}

	if err := StreamCSV(db, w, query); err != nil {
		t.Fatal(err)
	}
}

func TestStreamTab(t *testing.T) {
	db := structDb(t)
	query := "select id,name,kind from structs"
	w := &Writer{"TAB:"}
	if err := StreamTab(db, w, query); err != nil {
		t.Fatal(err)
	}
}

func TestStreamJSON(t *testing.T) {
	db := structDb(t)
	query := "select id,name,kind from structs"
	out := (*twriter)(t)
	err := StreamJSON(db, out, query)
	if err != nil {
		t.Fatal(err)
	}
}

func testDBU(t *testing.T) *sql.DB {
	return nil
}

func TestStreamObject(t *testing.T) {
	db := structDBU(t)
	s := &testStruct{Modified: time.Now()}
	out := (*twriter)(t)
	err := db.StreamObjects(out, s)
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

func prepare(db *sql.DB) {
	Exec(db, struct_sql)
	Exec(db, "insert into structs(name, kind, data) values(?,?,?)", "abc", 23, "what ev er")
	Exec(db, "insert into structs(name, kind, data) values(?,?,?)", "def", 69, "m'kay")
	Exec(db, "insert into structs(name, kind, data) values(?,?,?)", "hij", 42, "meaning of life")
	Exec(db, "insert into structs(name, kind, data) values(?,?,?)", "klm", 2, "of a kind")
}

func TestBackup(t *testing.T) {
	db, err := NewDBU(test_file, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.DB.Close()

	prepare(db.DB)
	v1, _ := db.Version()
	t.Log("Version prior to backup:", v1)
	t.Log("Backed up:", db.BackedUp)
	t.Log("Changed prior to backup:", db.Changed())

	tlog := NewTestlog(t)
	err = db.Backup("test_backup.db", tlog)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Changed post backup:", db.Changed())
	v2, _ := DBVersion("test.db")
	t.Log("Version of backup:", v2)
	t.Log("Backed up:", db.BackedUp)
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

func BenchmarkQueryAdHoc(b *testing.B) {
	db, err := Open(test_file, true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(db)
	query := "select id,name,kind,modified from structs where id > 0"

	if _, err := db.Query(query); err != nil {
		b.Error(err)
		return
	}
	queryAdHoc := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(query)
			if err != nil {
				b.Error(err)
				break
			}
			rows.Close()
		}
	}

	b.ResetTimer()
	b.Run("adhoc", queryAdHoc)
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Error(err)
	}
}

func TestMissingDB(t *testing.T) {
	_, err := Open("this_path_does_not_exist", false)
	if err == nil {
		t.Error("should have had error for missing file")
	}
}

func BenchmarkQueryPrepared(b *testing.B) {
	db, err := Open(test_file, true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(db)
	query := "select id,name,kind,modified from structs where id > ?"

	tx, err := db.Begin()
	if err != nil {
		b.Error(err)
		return
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		b.Error(err)
		return
	}

	queryPrepared := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := stmt.Query(0)
			if err != nil {
				b.Error(err)
				break
			}
			rows.Close()
		}
	}

	b.ResetTimer()
	b.Run("prepared", queryPrepared)
	stmt.Close()
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Error(err)
	}
}

func nullStream(columns []string, count int, buffer []interface{}, err error) {
	x := make([]int, 23)
	for _, buf := range toString(buffer) {
		fmt.Fprintln(ioutil.Discard, buf, x)
	}
}

func BenchmarkStream(b *testing.B) {
	dbs, err := Open("stest.db", true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(dbs)

	b.ResetTimer()
	if err := Stream(dbs, nullStream, default_query); err != nil {
		b.Error(err)
	}
}

func BenchmarkStreamJSON(b *testing.B) {
	dbs, err := Open("stest.db", true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(dbs)
	w := ioutil.Discard
	if testing.Verbose() {
		w = os.Stdout
	}
	/*
	 */

	b.ResetTimer()
	if err := StreamJSON(dbs, w, default_query); err != nil {
		b.Error(err)
	}
}

const (
	hammer_time = `
drop table if exists hammer;

create table hammer (
	id integer primary key,
	worker int,
	counter int,
	ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

pragma cache_size= 10485760;

PRAGMA journal_mode = WAL;

PRAGMA synchronous = 1;

`
	insert_hammer = `insert into hammer (worker, counter) values (?,?)`
)

func hammer(t *testing.T, workers, count int) {

	var wg sync.WaitGroup

	db, err := hammerDB("")
	if err != nil {
		t.Error(err)
		return
	}

	queue := make(chan int, count)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Log("start worker:", worker)
			for cnt := range queue {
				if _, err := db.Exec(insert_hammer, worker, cnt); err != nil {
					t.Errorf("worker:%d count:%d, error:%s\n", worker, cnt, err.Error())
				}
			}
			wg.Done()
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
}

func TestHammer(t *testing.T) {
	hammer(t, 4, 10000)
}

func hammerDB(name string) (*sql.DB, error) {
	if name == "" {
		name = "hammer.db"
	}
	db, err := Open(name, true)
	if err == nil {
		//return db, Commands(db, hammer_time, testing.Verbose())
		return db, Commands(db, hammer_time, false)
	}
	return nil, err
}

func errLogger(t *testing.T) chan error {
	e := make(chan error, 4096)
	go func() {
		for err := range e {
			t.Error(err)
		}
	}()
	return e
}

func TestServer(t *testing.T) {
	db, err := hammerDB("")
	if err != nil {
		t.Fatal(err)
	}
	r := make(chan Query, 4096)
	w := make(chan Action, 4096)
	e := errLogger(t)
	go Server(db, r, w, e)
	batter(t, r, w, 10, 100000)
	close(r)
	close(w)
	close(e)
}

/*
	reader := func(columns []string, row int, values []interface{}, err error) {
		if row == 0 {
			t.Log(columns)
		}
	}
*/

func batter(t *testing.T, r chan Query, w chan Action, workers, count int) {

	var wg sync.WaitGroup

	response := func(affected, last int64, err error) {
		//	t.Logf("aff:%d last:%d err:%v\n", affected, last, err)
		wg.Done()
	}

	queue := make(chan int, 4096)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Logf("worker:%d\n", worker)
			for cnt := range queue {
				wg.Add(1)
				w <- Action{
					Query:    insert_hammer,
					Args:     []interface{}{worker, cnt},
					Callback: response,
				}
			}
			wg.Done()
			t.Logf("done:%d\n", worker)
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
	t.Log("battered")
}

func TestInserterInvalidQuery(t *testing.T) {
	db, err := hammerDB("bulk.db")
	if err != nil {
		t.Error(err)
		return
	}

	fn := func(err error) {
		t.Log(err)
	}
	_, err = NewInserter(db, 4096, fn, "THIS IS NOT SQL")
	if err == nil {
		t.Error("expected query error")
	}
}

func TestInserter(t *testing.T) {
	db, err := hammerDB("bulk.db")
	if err != nil {
		t.Error(err)
		return
	}

	fn := func(err error) {
		t.Log(err)
	}
	inserter, err := NewInserter(db, 4096, fn, insert_hammer)
	if err != nil {
		t.Error(err)
		return
	}
	slam(t, inserter, 10, 1000000)
}

func slam(t *testing.T, inserter *Inserter, workers, count int) {
	t.Logf("slamming %d workers for %d iterations\n", workers, count)
	var wg sync.WaitGroup

	queue := make(chan int, 4096)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Logf("worker:%d\n", worker)
			for cnt := range queue {
				inserter.Insert(worker, cnt)
			}
			wg.Done()
			t.Logf("done:%d\n", worker)
		}(i)
	}
	for i := 0; i < count; i++ {
		queue <- i
	}
	close(queue)
	wg.Wait()
	i := inserter.Close()
	t.Logf("last: %d\n", i)
}

func TestGetResults(t *testing.T) {
	db, err := Open("hammer.db", true)
	if err != nil {
		t.Error(err)
		return
	}
	var i int64
	var ts string
	query := "select id,ts from hammer limit 1"
	_, err = GetResults(db, query, nil, &i, &ts)
	fmt.Printf("i = %d, ts = %s\n", i, ts)
}

func TestCreateQuery(t *testing.T) {
	db := structDb(t)
	s := testStruct{}
	query := createQuery(s, false)
	// select id,name,kind,data,modified from structs
	if _, err := GetResults(db, query, nil, &s.ID, &s.Name, &s.Kind, &s.Data, &s.Modified); err != nil {
		t.Error(err)
	}
	if !(s.ID > 0) {
		t.Errorf("ID is 0: %+v", s)
	}
}

func TestMapRow(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	args := []interface{}{"abc", 23}
	row, err := MapRow(db, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	data, ok := row["data"]
	if !ok {
		t.Fatal("missing data field")
	}
	if string(data.([]uint8)) != "what ev er" {
		t.Errorf("ROW: %+v\n", row)
	}

}

func TestMapRowInvalidQuery(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "THIS IS NOT SQL"
	_, err := MapRow(db, query)
	if err == nil {
		t.Fatal("expected query error")
	}
}

func TestRowStrings(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	args := []interface{}{"abc", 23}
	row, err := RowStrings(db, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	if len(row) < 5 {
		t.Fatalf("expected 5 fields, found: %d", len(row))
	}
	if row[3] != "what ev er" {
		t.Errorf("ROW: %+v\n", row)
	}
}

func TestDBUInsert(t *testing.T) {
	db := structDBU(t)
	query := "insert into structs(name, kind, data) values(?,?,?)"
	args := []interface{}{"Blur", 13, "bugman"}
	i, err := db.Insert(query, args...)
	if err != nil {
		t.Error(err)
	}
	if !(i > 0) {
		t.Errorf("expected last row to be greater than zero: %d", i)
	}
}

func TestObjectInsert(t *testing.T) {
	db := structDBU(t)
	s := testStruct{
		Name: "Blur",
		Kind: 13,
	}
	i, err := db.ObjectInsert(s)
	if err != nil {
		t.Error(err)
	}
	if !(i > 0) {
		t.Errorf("expected last row to be greater than zero: %d", i)
	}
}

func TestToString(t *testing.T) {
	const u8 = "8 uints"
	now := time.Now()
	raw := sql.RawBytes("raw meat")
	src := []interface{}{
		nil,
		"a string",
		[]uint8(u8),
		int32(32),
		int64(64),
		now,
		raw,
		3.1415926,
	}
	text := toString(src)
	if text[2] != u8 {
		t.Errorf("expected: %s got: %s\n", u8, text[2])
	}
}

func TestRowBadQuery(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "select * from Xstructs where name=? and kind=?"
	args := []interface{}{"abc", 23}
	_, _, err := Row(db, query, args...)
	if err == nil {
		t.Error("expected query error")
	}
}

func TestRowNoResults(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	args := []interface{}{"NOT MATCHING", 19182191212}
	_, row, err := Row(db, query, args...)
	if err != nil {
		t.Error(err)
	}
	if len(row) > 0 {
		t.Errorf("row (%d) should be empty: %v", len(row), row)
		id := row[0].(int64)
		if id > 0 {
			t.Errorf("unexpected query results: %v", id)
		}
	}
}

func TestExecEmpty(t *testing.T) {
	db := structDb(t)
	_, _, err := Exec(db, "")
	if err == nil {
		t.Error("expected query error")
	}
}

func TestRunMissingArgs(t *testing.T) {
	db := structDb(t)
	query := "insert into structs values(?,?,?,?,?)"
	_, err := Run(db, true, query)
	if err == nil {
		t.Error("expected query error")
	}
}

func TestRunInvalidQuery(t *testing.T) {
	db := structDb(t)
	query := "THIS IS NOT SQL"
	_, err := Run(db, true, query)
	if err == nil {
		t.Error("expected query error")
	}
}

func TestInsert(t *testing.T) {
	db := structDb(t)
	query := "insert into structs(name, kind, data) values(?,?,?)"
	args := []interface{}{"Blur", 13, "bugman"}
	i, err := Insert(db, query, args...)
	if err != nil {
		t.Error(err)
	}
	if !(i > 0) {
		t.Errorf("expected last row to be greater than zero: %d", i)
	}
}

func TestGenerator(t *testing.T) {
	db := structDb(t)

	query := "select * from structs"
	iter := Generator(db, query)
	record, ok := iter()
	if !ok {
		t.Fatal("no record found")
	}
	if len(record) == 0 {
		t.Fatal("empty record")
	}
	id := record[0].(int64)
	if id < 1 {
		t.Fatalf("invalid id: %d\n", id)
	}
}
