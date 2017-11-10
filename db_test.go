package dbutil

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	querySelect = "select id,name,kind,modified from structs"
	querySingle = "select id,name,kind,modified from structs limit 1"
	queryBad    = "c e n'est pas une sql query"
	queryCreate = `create table if not exists structs (
    id integer not null primary key,
    name text,
    kind int,
    data blob,
    modified   DATETIME DEFAULT CURRENT_TIMESTAMP
);`

	testDriver = "sqlite"
)

var (
	testFile = "test.db"
)

type testStruct struct {
	id       int
	name     string
	kind     int
	data     []byte
	modified time.Time
}

func (t *testStruct) Fields() []interface{} {
	return []interface{}{
		&(t.id),
		&(t.name),
		&(t.kind),
		&(t.data),
		&(t.modified),
	}
}

func init() {
	os.Remove(testFile)
	if testing.Verbose() {
		testout = os.Stdout
	}
	sql.Register(testDriver, &sqlite3.SQLiteDriver{})
}

func open(file string) (*sql.DB, error) {
	return sql.Open(testDriver, file)
}

func structDb(t *testing.T) *sql.DB {
	db := memDB(t)
	prepare(db)
	return db
}

func TestStream(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	myStream := func(columns []string, count int, buffer []interface{}) error {
		if len(columns) != 4 {
			t.Fatal("no columns")
		}
		if id, ok := buffer[0].(int64); !ok {
			t.Fatalf("expected numeric id: %v", buffer[0])
		} else if id == 0 {
			t.Fatalf("expected row id > 0")
		}
		t.Log(buffer...)
		return nil
	}
	if err := NewStreamer(db, querySelect).Stream(myStream); err != nil {
		t.Fatal(err)
	}
}

func TestStreamBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	myStream := func(columns []string, count int, buffer []interface{}) error {
		if len(columns) == 0 {
			t.Error("no columns")
		}
		return nil
	}
	if err := NewStreamer(db, queryBad).Stream(myStream); err == nil {
		t.Fatal("expected query error")
	}
}

func TestStreamBadFunc(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	myStream := func(columns []string, count int, buffer []interface{}) error {
		return fmt.Errorf("bad func, no biscuit")
	}
	if err := NewStreamer(db, querySelect).Stream(myStream); err == nil {
		t.Fatal("expected query error")
	}
}

func TestStreamCSV(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	if err := NewStreamer(db, querySelect).CSV(ioutil.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestStreamTab(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	if err := NewStreamer(db, querySelect).Tab(ioutil.Discard); err != nil {
		t.Fatal(err)
	}
}

func TestStreamJSON(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	err := NewStreamer(db, querySelect).JSON(ioutil.Discard)
	if err != nil {
		t.Fatal(err)
	}
}

func prepare(db *sql.DB) {
	const query = "insert into structs(name, kind, data) values(?,?,?)"

	if _, _, err := Exec(db, queryCreate); err != nil {
		panic(err)
	}
	Exec(db, query, "abc", 23, "what ev er")
	Exec(db, query, "def", 69, "m'kay")
	Exec(db, query, "hij", 42, "meaning of life")
	Exec(db, query, "klm", 2, "of a kind")
}

func BenchmarkQueryAdHoc(b *testing.B) {
	db, err := open(testFile)
	if err != nil {
		b.Fatal(err)
	}
	prepare(db)
	query := "select id,name,kind,modified from structs where id > 0"

	if _, err := db.Query(query); err != nil {
		b.Fatal(err)
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

func BenchmarkQueryAdHocOut(b *testing.B) {
	db, err := open(testFile)
	if err != nil {
		b.Fatal(err)
	}
	prepare(db)
	w := ioutil.Discard
	delimiter := "\t"
	query := "select id,name,kind,modified from structs where id > 0"
	queryAdHoc := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(query)
			if err != nil {
				b.Error(err)
				break
			}
			columns, _ := rows.Columns()
			buffer := make([]interface{}, len(columns))
			dest := make([]interface{}, len(columns))
			for k := 0; k < len(buffer); k++ {
				dest[k] = &buffer[k]
			}

			if !rows.Next() {
				b.Fatal("no rows found")
			}
			if err := rows.Scan(dest...); err != nil {
				b.Fatal(err)
			}
			for i, col := range buffer {
				fmt.Fprintf(w, "%v", col)
				if i < len(buffer)-1 {
					fmt.Fprint(w, delimiter)
				} else {
					fmt.Fprintln(w)
				}
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

func BenchmarkQueryPrepared(b *testing.B) {
	db, err := open(testFile)
	if err != nil {
		b.Fatal(err)
	}
	prepare(db)
	query := "select id,name,kind,modified from structs where id > ?"

	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		b.Fatal(err)
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
	/*
		f := ioutil.Discard
		tabs := len(buffer) - 1
		for i, item := range buffer {
			fmt.Fprint(f, item)
			if i < tabs {
				fmt.Fprint(f, "\t")
			}
		}
		fmt.Fprint(f, "\n")
	*/
	return nil
}

func BenchmarkStream(b *testing.B) {
	dbs, err := open("stest.db")
	if err != nil {
		b.Fatal(err)
	}
	prepare(dbs)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := stream(dbs, nullStream, querySingle); err != nil {
			b.Error(err)
		}
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
	dbs, err := open("stest.db")
	if err != nil {
		b.Fatal(err)
	}
	prepare(dbs)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := stream(dbs, fStream, querySingle); err != nil {
			b.Fatal(err)
		}
	}
	f.Close()
}

func BenchmarkStreamJSON(b *testing.B) {
	db, err := open("stest.db")
	if err != nil {
		b.Fatal(err)
	}
	prepare(db)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := NewStreamer(db, querySingle).JSON(testout); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkInsertSingle(b *testing.B) {
	db, err := open("file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	prepare(db)
	query := "insert into structs (name,kind) values('ziggy',1984)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Insert(db, query); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInsertTransactionNoArgs(b *testing.B) {
	db, err := open("file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	query := "insert into structs (name,kind) values('ziggy',1984)"
	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(); err != nil {
			b.Fatal(err)
		}
	}
	tx.Commit()
	stmt.Close()
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInsertTransactionWithArgs(b *testing.B) {
	db, err := open("file::memory:?cache=shared")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	query := "insert into structs (name,kind) values(?,?)"
	b.ResetTimer()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		b.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec("ziggy", 1984); err != nil {
			b.Fatal(err)
		}
	}
	tx.Commit()
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func TestColumnsEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	const query = "select name, kind from structs where name='this name does not exist'"
	rows, err := db.Query(query)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	rows.Next()
	if _, err := Columns(rows); err == nil {
		t.Fatal("expected empty row error")
	}
}

func TestRow(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	var name string
	var kind int64
	query := "select name, kind from structs limit 1"
	dest := []interface{}{&name, &kind}
	if err := Row(db, dest, query); err != nil {
		t.Fatal(err)
	}
	t.Logf("kind = %d, name = %s\n", kind, name)
}

func TestRowEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	var name string
	var kind int64
	query := "select name, kind from structs where name='this name does not exist'"
	dest := []interface{}{&name, &kind}
	if err := Row(db, dest, query); err == nil {
		t.Fatal("expected empty row error")
	} else if err != sql.ErrNoRows {
		t.Fatal("unexpected error:", err)
	}
}

func TestRowMap(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	query := "select * from structs where name=? and kind=?"
	row, err := RowMap(db, query, "abc", 23)
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
	for k, v := range row {
		t.Logf("col:%s val:%v (%T)", k, v, v)
	}
}

func TestRowMapBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	// select id,name,kind,data,modified from structs
	_, err := RowMap(db, queryBad)
	if err == nil {
		t.Fatal("expected query error")
	}
}

func TestRowMapEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	args := []interface{}{"this does not exist", 666}
	if _, err := RowMap(db, query, args...); err == nil {
		t.Fatal("error was expected")
	} else {
		t.Logf("got expected error: %v", err)
	}
}

func TestRowStrings(t *testing.T) {
	db := structDb(t)
	defer db.Close()
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

func TestRowStringsEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	query := "select * from structs where name='no such name'"
	if _, err := RowStrings(db, query); err == nil {
		t.Fatalf("expected error for invalid query")
	} else if err != sql.ErrNoRows {
		t.Fatalf("wrong error for empty query: %v", err)
	}
}

func TestRowStringsBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()
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
	defer db.Close()
	// select id,name,kind,data,modified from structs
	args := []interface{}{"abc", 23}
	if err := Row(db, nil, queryBad, args...); err == nil {
		t.Error("expected query error")
	}
}

func TestRowNoResults(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	// select id,name,kind,data,modified from structs
	query := "select * from structs where name=? and kind=?"
	if err := Row(db, nil, query, "NOT MATCHING", 19182191212); err == nil {
		t.Fatal("expected error")
	} else {
		t.Logf("got expected error: %v", err)
	}
}

func TestExecEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	_, _, err := Exec(db, "")
	if err == nil {
		t.Error("expected query error")
	}
}

func TestExecBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	_, _, err := Exec(db, queryBad)
	if err == nil {
		t.Fatalf("expected error for invalid query")
	}
}

func TestInsert(t *testing.T) {
	db := structDb(t)
	defer db.Close()
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

/*
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
*/

func TestInsertMany(t *testing.T) {
	db := structDb(t)
	defer db.Close()

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

	/*
		FIX THIS
		query2 := "select count(*) as count from structs where kind=?"
		args2 := []interface{}{kind}
		var count int
		if err := Row(db, query2, args2, &count); err != nil {
			t.Fatal(err)
		}
		if count != len(args) {
			t.Errorf("expected %d rows but got %d rows instead\n", len(args), count)
		}
	*/
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
	defer db.Close()

	kind := 314159
	args := [][]interface{}{
		{"many1", kind},
		{"many2", kind},
		{"many3", kind},
	}
	query := "insert into structs(name, kind, data) values(?,?,?)"
	if err := InsertMany(db, query, args...); err == nil {
		t.Fatalf("expected error for missing args")
	} else {
		t.Log(err)
	}
}

func TestInsertManyBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	if err := InsertMany(db, queryBad); err == nil {
		t.Fatalf("expected error for invalid query")
	} else {
		t.Log(err)
	}
}

func TestQueryClosed(t *testing.T) {
	db, err := open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	query := "select count(*) as count from structs"
	var count int
	if _, _, err = Get(db, query, nil, &count); err == nil {
		t.Fatal("expected query error")
	}
	t.Logf("got expected error: %v\n", err)
}

func memDB(t *testing.T) *sql.DB {
	db, err := open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGet(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	q := "select * from structs limit 1"
	_, _, err := Get(db, q)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
}

func TestGetEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	query := "select name, kind from structs where name='this name does not exist'"
	if _, _, err := Get(db, query); err == nil {
		t.Fatal("expected empty row error")
	} else if err != sql.ErrNoRows {
		t.Fatal("unexpected error:", err)
	}
}

func TestDBStrings(t *testing.T) {
	db := structDb(t)
	q := "select * from structs limit 1"
	cols, err := RowStrings(db, q)
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log(cols)
	}
}
