package dbutil

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
)

const (
	badPath = "/path/does/not/exist/database.db"
)

func TestFuncs(t *testing.T) {
	sqlInit("funky", "", ipFuncs...)
	db, err := sql.Open("funky", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	//const create = `create table if not exists iptest ( ip int )`
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
	args := []interface{}{testIP}

	if _, err := Get(db, "select iptoa(ip) as ipv4 from iptest where ipv4 = ?", args, &ipv4); err != nil {
		t.Fatal(err)
	}

	if ipv4 != testIP {
		t.Errorf("expected: %s but got: %s\n", testIP, ipv4)
	}

	var ip32 int32
	if _, err := Get(db, "select atoip('8.8.8') as ipv4", nil, &ip32); err != nil {
		t.Fatal(err)
	} else {
		if ip32 != -1 {
			t.Fatalf("expected: %d but got: %d\n", -1, ip32)
		}
	}
}

func TestSqliteBadHook(t *testing.T) {
	const badDriver = "badhook"
	sqlInit(badDriver, queryBad)
	db, err := sql.Open(badDriver, ":memory:")
	defer db.Close()

	if err != nil {
		t.Fatal(err)
	}
	if _, err := DataVersion(db); err == nil {
		t.Fatal("expected error for bad hook")
	} else {
		t.Logf("got expected hook error: %v\n", err)
	}
}

func simpleQuery(db *sql.DB) error {
	var one int
	if _, err := Get(db, "select 1", nil, &one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("expected: %d but got %d\n", 1, one)
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
	sqlInit(DriverName, "")
	_, err := Open(badPath)
	if err == nil {
		t.Fatal("expected error for bad path")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadURI(t *testing.T) {
	sqlInit(DriverName, "")
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
	if err := backup(db, "test_backup.db", 1024, testout); err != nil {
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
	args := []interface{}{"USA", limit}
	var total int64
	_, err := Get(db, "select total from summary where country=? limit ?", args, &total)
	if err != nil {
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
