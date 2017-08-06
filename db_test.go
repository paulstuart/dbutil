package dbutil

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"
	"testing/iotest"
	"time"
)

const (
	querySelect = "select id,name,kind,modified from structs"
	queryBad    = "c e n'est pas une sql query"
	queryCreate = `create table if not exists structs (
    id integer not null primary key,
    name text,
    kind int,
    data blob,
    modified   DATETIME DEFAULT CURRENT_TIMESTAMP
);`
)

var (
	testFile = "test.db"
	Xw       = iotest.NewWriteLogger("", os.Stderr)
)

func init() {
	os.Remove(testFile)

}

func TestFuncs(t *testing.T) {
	sqlInit(DriverName, "", IPFuncs...)
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

	const testIP = "192.168.1.1"
	var ipv4 string
	args := []interface{}{testIP}
	GetResults(db, "select iptoa(ip) as ipv4 from iptest where ipv4 = ?", args, &ipv4)
	if ipv4 != testIP {
		t.Errorf("expected: %s but got: %s\n", testIP, ipv4)
	}
}

func TestSqliteBadHook(t *testing.T) {
	db, err := OpenSqliteWithHook(":memory:", DriverName, queryBad, true)
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	if _, err := DataVersion(db); err != nil {
		t.Fatal("expected error for bad hook")
	}
}

func TestSqliteCreate(t *testing.T) {
	db, err := Open(testFile, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sql := `
	create table foo (id integer not null primary key, name text);
	delete from foo;
	`
	_, err = db.Exec(sql)
	if err != nil {
		t.Fatalf("%q: %s\n", err, sql)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		t.Log(id, name)
	}
	rows.Close()
}

func TestSqliteDelete(t *testing.T) {
	db, _ := Open(testFile, true)
	cnt, err := Update(db, "delete from foo where id=?", 13)
	if err != nil {
		t.Fatal("DELETE ERROR: ", err)
	}
	t.Log("DELETED: ", cnt)
	db.Close()
}

func TestSqliteInsert(t *testing.T) {
	db, _ := Open(testFile, true)
	cnt, err := Update(db, "insert into foo (id,name) values(?,?)", 13, "bakers")
	if err != nil {
		t.Log("INSERT ERROR: ", err)
	}
	t.Log("INSERTED: ", cnt)
	db.Close()
}

func TestSqliteUpdate(t *testing.T) {
	db, _ := Open(testFile, true)
	cnt, err := Update(db, "update foo set id=23 where id > ? and name like ?", "3", "bi%")
	if err != nil {
		t.Log("UPDATE ERROR: ", err)
	}
	t.Log("UPDATED: ", cnt)
	db.Close()
}

func structDb(t *testing.T) *sql.DB {
	db, err := Open(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
	prepare(db)
	return db
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
func TestStream(t *testing.T) {
	db := structDb(t)
	myStream := func(columns []string, count int, buffer []interface{}) error {
		if len(columns) != 4 {
			t.Fatal("no columns")
		}
		if id, ok := buffer[0].(int64); !ok {
			t.Fatalf("expected numeric id: %v", buffer[0])
		} else if id == 0 {
			t.Fatalf("expected row id > 0")
		}
		return nil
	}
	query := "select id,name,kind,modified from structs"
	if err := Stream(db, myStream, query); err != nil {
		t.Fatal(err)
	}
}

func TestStreamBadQuery(t *testing.T) {
	db := structDb(t)
	myStream := func(columns []string, count int, buffer []interface{}) error {
		if len(columns) == 0 {
			t.Error("no columns")
		}
		return nil
	}
	if err := Stream(db, myStream, queryBad); err == nil {
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

func numbChk(t *testing.T, s interface{}) {
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
	const query = "insert into structs(name, kind, data) values(?,?,?)"

	Exec(db, queryCreate)
	Exec(db, query, "abc", 23, "what ev er")
	Exec(db, query, "def", 69, "m'kay")
	Exec(db, query, "hij", 42, "meaning of life")
	Exec(db, query, "klm", 2, "of a kind")
}

func TestIsNumberInvalid(t *testing.T) {
	if n := isNumber(nil); n {
		t.Errorf("expected to not be numeric")
	}

}

func TestBackup(t *testing.T) {
	db, err := Open(testFile, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	/*
		v1, _ := DataVersion(db)
		t.Log("Version prior to backup:", v1)

		tlog := NewTestlog(t)
		err = Backup(db, "test_backup.db", tlog)
		if err != nil {
			t.Fatal(err)
		}
		v2, _ := DBVersion("test.db")
		t.Log("Version of backup:", v2)
	*/
	tlog := NewTestlog(t)
	if err := Backup(db, "test_backup.db", tlog); err != nil {
		t.Fatal(err)
	}
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
	db, err := Open(testFile, true)
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
	db, err := Open(testFile, true)
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

func nullStream(columns []string, count int, buffer []interface{}) error {
	fmt.Fprintln(ioutil.Discard, buffer...)
	return nil
}

func BenchmarkStream(b *testing.B) {
	dbs, err := Open("stest.db", true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(dbs)

	b.ResetTimer()
	if err := Stream(dbs, nullStream, querySelect); err != nil {
		b.Error(err)
	}
}

func BenchmarkStreamToFile(b *testing.B) {
	f, err := os.Create("streamed.txt")
	if err != nil {
		b.Fatalf("error creating dest file:%v\n", err)
	}
	fStream := func(columns []string, count int, buffer []interface{}) error {
		tabs := len(buffer) - 1
		for i, item := range buffer {
			fmt.Fprint(f, item)
			if i < tabs {
				fmt.Fprint(f, "\t")
			}
		}
		fmt.Fprint(f, "\n")
		return nil
	}
	dbs, err := Open("stest.db", true)
	if err != nil {
		b.Error(err)
		return
	}
	prepare(dbs)

	//b.ResetTimer()
	if err := Stream(dbs, fStream, querySelect); err != nil {
		b.Fatal(err)
	}
	f.Close()
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

	b.ResetTimer()
	if err := StreamJSON(dbs, w, querySelect); err != nil {
		b.Error(err)
	}
}

const (
	hammerTime = `
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
	hammerInsert = `insert into hammer (worker, counter) values (?,?)`
)

func hammer(t *testing.T, workers, count int) {
	db, err := hammerDB("")
	if err != nil {
		t.Error(err)
		return
	}
	hammerDb(t, db, workers, count)
}

func hammerDb(t *testing.T, db *sql.DB, workers, count int) {
	var wg sync.WaitGroup
	queue := make(chan int, count)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Log("start worker:", worker)
			for cnt := range queue {
				if _, err := db.Exec(hammerInsert, worker, cnt); err != nil {
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
		//return db, Commands(db, hammerTime, testing.Verbose())
		return db, Commands(db, hammerTime, false)
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

func TestServerWrite(t *testing.T) {
	db, err := hammerDB("")
	if err != nil {
		t.Fatal(err)
	}
	r := make(chan Query, 4096)
	w := make(chan Action, 4096)
	e := errLogger(t)
	go Server(db, r, w)
	batter(t, w, 10, 100000)
	close(r)
	close(w)
	close(e)
}

func TestServerRead(t *testing.T) {
	db := fakeHammer(t, 10, 1000)
	r := make(chan Query, 4096)
	e := errLogger(t)
	go Server(db, r, nil)
	butter(t, r, 2, 10)
	close(r)
	close(e)
}

func batter(t *testing.T, w chan Action, workers, count int) {

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
					Query:    hammerInsert,
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

func Xbatter(t *testing.T, r chan Query, w chan Action, workers, count int) {

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
					Query:    hammerInsert,
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

func testReceiver(t *testing.T, callback chan int) RowFunc {
	var tally int
	go func() {
		callback <- tally
	}()

	return func(columns []string, row int, values []interface{}) error {
		if row == 0 {
			t.Logf("columns: %v\n", columns)
		}
		t.Logf("row:%d values:%v\n", row, values)
		tally++
		return nil
	}
}

func makeReader(t *testing.T, r chan Query, queue, workers, count int) func(query string, args ...interface{}) chan int {
	return func(query string, args ...interface{}) chan int {
		cb := make(chan int)
		tr := testReceiver(t, cb)
		qc := make(chan int, queue)
		ec := make(chan error, count)
		var wg sync.WaitGroup
		go func() {
			for err := range ec {
				fmt.Println("err back:", err)
				if err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}
		}()

		go func() {
			for i := 0; i < workers; i++ {
				fmt.Println("WORKER:", i)
				wg.Add(1)
				go func(worker int) {
					t.Logf("worker:%d\n", worker)
					for _ = range qc {
						r <- Query{
							Query: query,
							Args:  args,
							Reply: tr,
							Error: ec,
						}
						fmt.Println("added query:", worker)
					}
					wg.Done()
					t.Logf("done:%d\n", worker)
				}(i)
			}
			for i := 0; i < count; i++ {
				qc <- i
			}
			close(qc)
			wg.Wait()
		}()
		return cb
	}
}

func butter(t *testing.T, r chan Query, workers, count int) {

	limit := 100
	var wg sync.WaitGroup

	ec := make(chan error, count)
	var tally int
	replies := func(columns []string, row int, values []interface{}) error {
		if row == 0 {
			t.Logf("columns: %v\n", columns)
		}
		t.Logf("row:%d values:%v\n", row, values)
		tally++
		return nil
	}

	go func() {
		for err := range ec {
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}
	}()

	query := "select * from hammer limit ?"
	queue := make(chan int, 4096)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			t.Logf("worker:%d\n", worker)
			for _ = range queue {
				wg.Add(1)
				r <- Query{
					Query: query,
					Args:  []interface{}{limit},
					Reply: replies,
					Error: ec,
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
	limit *= count
	if tally != limit {
		t.Errorf("expected %d rows but got back %d\n", limit, tally)
	}
	t.Log("buttered")
}

func TestInserterBadQuery(t *testing.T) {
	db, err := hammerDB("bulk.db")
	if err != nil {
		t.Fatal(err)
	}

	fn := func(err error) {
		t.Log(err)
	}

	_, err = NewInserter(db, 4096, fn, queryBad)
	if err == nil {
		t.Error("expected query error")
	}
}

func TestInserterClosed(t *testing.T) {
	db, err := hammerDB("bulk.db")
	if err != nil {
		t.Fatal(err)
		return
	}

	fn := func(err error) {
		t.Log(err)
	}

	_, err = NewInserter(db, 4096, fn, queryBad)
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
	inserter, err := NewInserter(db, 4096, fn, hammerInsert)
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
	t.Logf("i = %d, ts = %s\n", i, ts)
}

func TestGetResultsEmpty(t *testing.T) {
	db, err := Open("hammer.db", true)
	if err != nil {
		t.Error(err)
		return
	}
	var i int64
	var ts string
	query := "select id,ts from hammer limit 1 where ts='now'"
	_, err = GetResults(db, query, nil, &i, &ts)
	t.Logf("i = %d, ts = %s\n", i, ts)
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

func TestMapRowBadQuery(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	_, err := MapRow(db, queryBad)
	if err == nil {
		t.Fatal("expected query error")
	}
}

func TestMapRowEmpty(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	args := []interface{}{"this does not exist", 666}
	_, err := MapRow(db, query, args...)
	if err != nil {
		t.Fatal(err)
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

func TestRowStringsBadQuery(t *testing.T) {
	db := structDb(t)
	if _, err := RowStrings(db, queryBad); err == nil {
		t.Fatalf("expected error for invalid query")
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
	text, err := toString(src)
	if err != nil {
		t.Fatal(err)
	}
	if text[2] != u8 {
		t.Errorf("expected: %s got: %s\n", u8, text[2])
	}
}

type unknownStruct struct{}

func TestToStringUnknownType(t *testing.T) {
	const u8 = "8 uints"
	unknown := unknownStruct{}
	src := []interface{}{
		nil,
		"a string",
		unknown,
	}
	_, err := toString(src)
	if err == nil {
		t.Fatal("expected error but got none")
	}
}

func TestRowBadQuery(t *testing.T) {
	db := structDb(t)
	// select id,name,kind,data,modified from structs
	args := []interface{}{"abc", 23}
	_, _, err := Row(db, queryBad, args...)
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

func TestExecBadQuery(t *testing.T) {
	db := structDb(t)
	_, _, err := Exec(db, queryBad)
	if err == nil {
		t.Fatalf("expected error for invalid query")
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

func TestFilename(t *testing.T) {
	db, err := Open(testFile, true)
	if err != nil {
		t.Fatal(err)
	}
	if name := path.Base(Filename(db)); name != testFile {
		t.Errorf("expected file name:%s got:%s\n", testFile, name)
	}
	db.Close()
}

func TestInsertMany(t *testing.T) {
	db := structDb(t)

	kind := 314159
	query := "insert into structs(name, kind, data) values(?,?,?)"
	args := [][]interface{}{
		{"many1", kind, "pie-hole"},
		{"many2", kind, "pie-hole"},
		{"many3", kind, "pie-hole"},
		{"many4", kind, "pie-hole"},
	}
	if err := InsertMany(db, query, args...); err != nil {
		t.Fatal(err)
	}

	query2 := "select count(*) as count from structs where kind=?"
	args2 := []interface{}{kind}
	var count int
	if _, err := GetResults(db, query2, args2, &count); err != nil {
		t.Fatal(err)
	}
	if count != len(args) {
		t.Errorf("expected %d rows but got %d rows instead\n", len(args), count)
	}
}

func TestInsertManyClosedDb(t *testing.T) {
	db := structDb(t)
	db.Close()

	kind := 314159
	query := "insert into structs(name, kind, data) values(?,?,?)"
	args := [][]interface{}{
		{"many1", kind, "pie-hole"},
		{"many2", kind, "pie-hole"},
		{"many3", kind, "pie-hole"},
		{"many4", kind, "pie-hole"},
	}
	if err := InsertMany(db, query, args...); err == nil {
		t.Fatalf("expected error that db was closed")
	}
}

func TestInsertManyMissingArgs(t *testing.T) {
	db := structDb(t)
	db.Close()

	query := "insert into structs(name, kind, data) values(?,?,?)"
	if err := InsertMany(db, query); err == nil {
		t.Fatalf("expected error for missing args")
	}
}

func TestInsertManyBadQuery(t *testing.T) {
	db := structDb(t)
	db.Close()

	if err := InsertMany(db, queryBad); err == nil {
		t.Fatalf("expected error for invalid query")
	}
}

func TestQueryClosed(t *testing.T) {
	db, err := Open(testFile, true)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	query := "select count(*) as count from structs"
	var count int
	if _, err = GetResults(db, query, nil, &count); err == nil {
		t.Fatal("expected query error")
	}
	t.Logf("got expected error: %v\n", err)
}

func fakeHammer(t *testing.T, workers, count int) *sql.DB {
	db, err := hammerDB("")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		worker := rand.Int() % workers
		if _, err := db.Exec(hammerInsert, worker, i); err != nil {
			t.Fatalf("worker:%d count:%d, error:%s\n", worker, i, err.Error())
		}
	}
	return db
}
