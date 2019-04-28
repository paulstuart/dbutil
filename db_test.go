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
	querySelect = "select id,name,kind,data,modified from structs"
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
	testout  = ioutil.Discard

	testData = [][]interface{}{
		{"abc", 23, "what ev er"},
		{"def", 69, "m'kay"},
		{"hij", 42, "meaning of life"},
		{"klm", 2, "of a kind, to a point"},
	}
)

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

func emptyTable(t *testing.T) *sql.DB {
	db := memDB(t)
	if _, err := db.Exec(queryCreate); err != nil {
		t.Fatal(err)
	}
	return db
}

func structDb(t *testing.T) *sql.DB {
	db := emptyTable(t)
	prepare(db)
	return db
}

func TestStream(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	myStream := func(columns []string, count int, buffer []interface{}) error {
		if len(columns) != 5 {
			t.Fatalf("expected %d columns but got: %d", 5, len(columns))
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

	if testing.Verbose() {
		testout = os.Stdout
	}
	if err := NewStreamer(db, querySelect).CSV(testout, true); err != nil {
		t.Fatal(err)
	}
}

func TestStreamTSV(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	if testing.Verbose() {
		testout = os.Stdout
	}
	if err := NewStreamer(db, querySelect).TSV(testout, true); err != nil {
		t.Fatal(err)
	}
}

func TestStreamJSON(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	if testing.Verbose() {
		testout = os.Stdout
	}
	if err := NewStreamer(db, querySelect).JSON(testout); err != nil {
		t.Fatal(err)
	}
}

func prepare(db *sql.DB) {
	const query = "insert into structs(name, kind, data) values(?,?,?)"
	for _, data := range testData {
		db.Exec(query, data...)
	}
}

func benchDb(b *testing.B) *sql.DB {
	//db, err := open(":memory:")
	db, err := open("bench.db")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := db.Exec(queryCreate); err != nil {
		b.Fatal(err)
	}
	prepare(db)
	return db
}

func BenchmarkQueryAdHoc(b *testing.B) {
	db := benchDb(b)
	defer db.Close()

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
	db := benchDb(b)
	defer db.Close()

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
	db := benchDb(b)
	defer db.Close()

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
	return nil
}

func BenchmarkStream(b *testing.B) {
	db := benchDb(b)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := stream(db, nullStream, querySingle); err != nil {
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
	db := benchDb(b)
	defer db.Close()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := stream(db, fStream, querySingle); err != nil {
			b.Fatal(err)
		}
	}
	f.Close()
}

func BenchmarkStreamJSON(b *testing.B) {
	db := benchDb(b)
	defer db.Close()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := NewStreamer(db, querySingle).JSON(testout); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkStreamCSV(b *testing.B) {
	db := benchDb(b)
	defer db.Close()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := NewStreamer(db, querySingle).CSV(testout, true); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkStreamTSV(b *testing.B) {
	db := benchDb(b)
	defer db.Close()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := NewStreamer(db, querySingle).TSV(testout, true); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkInsertSingle(b *testing.B) {
	db := benchDb(b)
	defer db.Close()

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
	db := benchDb(b)
	defer db.Close()

	query := "insert into structs (name,kind) values('ziggy',1984)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		stmt, err := tx.Prepare(query)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		if _, err := stmt.Exec(); err != nil {
			b.Fatal(err)
		}
		tx.Commit()
		stmt.Close()
	}
	b.StopTimer()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkInsertTransactionWithArgs(b *testing.B) {
	db := benchDb(b)
	defer db.Close()

	query := "insert into structs (name,kind) values(?,?)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
		if _, err := stmt.Exec("ziggy", 1984); err != nil {
			b.Fatal(err)
		}
		tx.Commit()
	}
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
	if string(data.(string)) != "what ev er" {
		t.Errorf("ROW: %+v\n", row)
	}
	for k, v := range row {
		t.Logf("col:%s val:%v (%T)", k, v, v)
	}
}

func TestRowMapBadQuery(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	_, err := RowMap(db, queryBad)
	if err == nil {
		t.Fatal("expected query error")
	}
}

func TestRowMapEmpty(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	query := "select * from structs where name=? and kind=?"
	if _, err := RowMap(db, query, "this does not exist", 666); err == nil {
		t.Fatal("error was expected")
	} else {
		t.Logf("got expected error: %v", err)
	}
}

func TestRowStrings(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	query := "select * from structs where name=? and kind=?"
	row, err := RowStrings(db, query, "abc", 23)
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
	text := toString(src)
	if text[2] != u8 {
		t.Errorf("expected: %s got: %s\n", u8, text[2])
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

func TestUpdate(t *testing.T) {
	db := structDb(t)
	defer db.Close()
	query := "update structs set name=? where kind > ?"
	cnt, err := Update(db, query, "bigger", 40)
	if err != nil {
		t.Fatal(err)
	}
	if cnt != 2 {
		t.Errorf("expected count to be 2 but got: %d", cnt)
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

func TestInsertMany(t *testing.T) {
	db := structDb(t)
	defer db.Close()

	kind := 314159
	query := "insert into structs(name, kind, data) values(?,?,?)"
	args := [][]interface{}{
		{"many1", kind, "pie-hole"},
		{"many2", kind, "pie-hole"},
		{"many3", kind, "pie-hole"},
	}
	if err := InsertMany(db, query, args...); err != nil {
		t.Fatal(err)
	}

	query2 := "select count(*) as cnt from structs where kind=?"
	var count int
	if err := Row(db, []interface{}{&count}, query2, kind); err != nil {
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

func TestInserter(t *testing.T) {
	db := emptyTable(t)
	const q1 = "insert into structs(name, kind, data) values(?,?,?)"
	insert, err := NewInserter(db, q1)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range testData {
		insert.Insert(row...)
	}
	if err := insert.Close(); err != nil {
		t.Fatal(err)
	}
	var cnt int
	dest := []interface{}{&cnt}
	if err := Row(db, dest, "select count(*) as cnt from structs"); err != nil {
		t.Fatal(err)
	}
	if cnt != 4 {
		t.Errorf("expected count to be 4 but got: %d", cnt)
	}
}

func TestInserterClosed(t *testing.T) {
	db := emptyTable(t)
	db.Close()
	const q1 = "insert into structs(name, kind, data) values(?,?,?)"
	_, err := NewInserter(db, q1)
	if err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestInserterBadQuery(t *testing.T) {
	db := emptyTable(t)
	defer db.Close()
	const q1 = "insert into tabledoesnotexist(name, kind, data) values(?,?,?)"
	_, err := NewInserter(db, q1)
	if err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestInserterMissingArgs(t *testing.T) {
	db := emptyTable(t)
	defer db.Close()
	const q1 = "insert into structs(name, kind, data) values(?,?,?)"
	insert, err := NewInserter(db, q1)
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if err := insert.Insert("myname", 99); err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}
