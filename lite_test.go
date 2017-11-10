package dbutil

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"testing"

	sqlite3 "github.com/mattn/go-sqlite3"
)

const (
	badPath = "/path/does/not/exist/database.db"
)

func TestFuncs(t *testing.T) {
	db, err := Open(":memory:", ConfigFuncs(ipFuncs...), ConfigDriverName("funky"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const create = `create table iptest ( ip int )`
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

	if err := Row(db, []interface{}{&ipv4}, "select iptoa(ip) as ipv4 from iptest where ipv4 = ?", testIP); err != nil {
		t.Fatal(err)
	}

	if ipv4 != testIP {
		t.Errorf("expected: %s but got: %s\n", testIP, ipv4)
	}

	var ip32 int32
	if err := Row(db, []interface{}{&ip32}, "select atoip('8.8.8') as ipv4"); err != nil {
		t.Fatal(err)
	} else {
		if ip32 != -1 {
			t.Fatalf("expected: %d but got: %d\n", -1, ip32)
		}
	}
}

func TestSqliteBadHook(t *testing.T) {
	const badDriver = "badhook"
	_, err := Open(":memory:", ConfigDriverName(badDriver), ConfigHook(queryBad))

	if err == nil {
		t.Fatal("expected error for bad hook")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func simpleQuery(db *sql.DB) error {
	var one int
	dest := []interface{}{&one}
	if err := Row(db, dest, "select 1", nil); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("expected: %d but got %d", 1, one)
	}
	return nil
}

func TestSqliteFuncsBad(t *testing.T) {
	u := &unknownStruct{}
	badFuncs := []SqliteFuncReg{
		{"", u, true},
	}
	const driver = "badfunc"
	const hook = "select 1"
	sqlInit(driver, hook, badFuncs...)
	db, err := sql.Open(driver, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err := simpleQuery(db); err == nil {
		t.Fatal("expected error for bad func")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadPath(t *testing.T) {
	sqlInit(DefaultDriver, "")
	_, err := Open(badPath)
	if err == nil {
		t.Fatal("expected error for bad path")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadURI(t *testing.T) {
	sqlInit(DefaultDriver, "")
	_, err := Open("test.db ! % # mode ro bad=")
	if err == nil {
		t.Fatal("expected error for bad uri")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestVersion(t *testing.T) {
	_, i, _ := Version()
	if i < 3017000 {
		t.Errorf("old version: %d\n", i)
	} else {
		t.Log(i)
	}
}

func TestBackup(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	//if err := backup(db, "test_backup.db", 1024, testout); err != nil {
	if err := Backup(db, "test_backup.db"); err != nil {
		t.Fatal(err)
	}
}

func TestBackupBadDir(t *testing.T) {
	db, err := Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	if err := backup(db, "/this/path/does/not/exist/test_backup.db", 1024, testout); err == nil {
		t.Fatal("expected backup error")
	} else {
		t.Log(err)
	}
}

func TestFile(t *testing.T) {
	db := memDB(t)
	if err := os.Chdir("sql"); err != nil {
		t.Fatal(err)
	}
	if err := File(db, "test.sql", true, testout); err != nil {
		t.Fatal(err)
	}
	limit := 3
	var total int64
	dest := []interface{}{&total}
	if err := Row(db, dest, "select total from summary where country=? limit ?", "USA", limit); err != nil {
		t.Fatal(err)
	}
	if total != 3 {
		t.Fatalf("expected count of: %d but got %d\n", limit, total)
	}
	db.Close()
}

func TestFileDoesNotExit(t *testing.T) {
	db := memDB(t)
	if err := File(db, "this_file_does_not_exist.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for missing file")
	} else {
		t.Log(err)
	}
}

func TestFileReadMissing(t *testing.T) {
	db := memDB(t)
	if err := File(db, "sql/test3.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for missing file")
	} else {
		t.Log(err)
	}
}

func TestFileBadExec(t *testing.T) {
	db := memDB(t)
	if err := File(db, "sql/test4.sql", testing.Verbose(), testout); err == nil {
		t.Fatal("expected error for invalid sql")
	} else {
		t.Log(err)
	}
}

func TestPragmas(t *testing.T) {
	db := memDB(t)
	Pragmas(db, testout)
}

func TestCommandsBadQuery(t *testing.T) {
	db := memDB(t)
	query := "select asdf xyz m'kay;"
	if err := Commands(db, query, false, nil); err == nil {
		t.Fatal("expected error for bad query")
	} else {
		t.Log(err)
	}
}

func TestCommandsReadMissingFile(t *testing.T) {
	db := memDB(t)
	cmd := `.read /this/file/does/not/exist.sql`
	if err := Commands(db, cmd, false, nil); err == nil {
		t.Fatal("expected error for reading command file")
	} else {
		t.Log(err)
	}
}

func TestCommandsTrigger(t *testing.T) {
	db := structDb(t)
	const (
		query1 = `create table if not exists inserted (id integer, msg text)`
		query2 = `
CREATE TRIGGER structs_insert AFTER INSERT ON structs 
BEGIN
    insert or replace into inserted (id) values(NEW.id);
    insert or replace into inserted (msg) values('ack!');
END;
`
	)
	if _, _, err := Exec(db, query1); err != nil {
		t.Fatal(err)
	}
	if err := Commands(db, query2, testing.Verbose(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestDataVersion(t *testing.T) {
	db := structDb(t)

	i, err := DataVersion(db)
	if err != nil {
		t.Fatal(err)
	}
	if i < 1 {
		t.Fatalf("expected version to be greater than zero but instead is: %d\n", i)
	}
}

func TestConnQueryOk(t *testing.T) {
	name := "connQuery01"
	query := "select 23;"

	fn := func(columns []string, row int, values []driver.Value) error {
		return nil
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return ConnQuery(conn, fn, query)
		},
	}
	sql.Register(name, drvr)
	_, err := sql.Open(name, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
}

func TestConnQueryBad(t *testing.T) {
	name := "connQuery02"
	fn := func(columns []string, row int, values []driver.Value) error {
		return nil
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return ConnQuery(conn, fn, queryBad)
		},
	}
	sql.Register(name, drvr)
	db, _ := sql.Open(name, ":memory:")
	_, err := db.Query(querySelect)
	if err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestConnQueryFuncBad(t *testing.T) {
	file := "test.db"
	os.Remove(file)
	db, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	prepare(db)
	Close(db)

	name := "connQuery03"
	fn := func(columns []string, row int, values []driver.Value) error {
		return fmt.Errorf("function had an error")
	}
	drvr := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return ConnQuery(conn, fn, querySelect)
		},
	}
	sql.Register(name, drvr)
	db, _ = sql.Open(name, file)

	if _, err = db.Query(querySelect); err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}

func TestOpenBadFile(t *testing.T) {
	if _, err := Open("/path/does/:mem not/ory: exist/:memory:/abc123"); err == nil {
		t.Fatal("expected error but got none")
	} else {
		t.Log("got expected error:", err)
	}
}
