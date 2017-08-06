package dbutil

import (
	"database/sql"
	"os"
	"testing"
)

func TestFuncs(t *testing.T) {
	sqlInit("funky", "", IPFuncs...)
	db, err := sql.Open("funky", ":memory:")
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

/*
func TestSqliteFilename(t *testing.T) {
	sqlInit(DriverName, "")
	db, err := Open(":memory:", true)
	if err != nil {
		t.Fatal(err)
	}
}
*/
func TestSqliteBadPath(t *testing.T) {
	sqlInit(DriverName, "")
	_, err := Open("/PATH/DOES/NOT/EXIST/DUDE.db", true)
	if err == nil {
		t.Fatal("expected error for bad path")
	} else {
		t.Logf("got expected error: %v\n", err)
	}
}

func TestSqliteBadURI(t *testing.T) {
	sqlInit(DriverName, "")
	_, err := Open("test.db ! % # mode ro bad=", true)
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

func TestBackupBadDir(t *testing.T) {
	db, err := Open(testFile, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	prepare(db)
	tlog := NewTestlog(t)
	if err := Backup(db, "/this/path/does/not/exist/test_backup.db", tlog); err == nil {
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
	if err := File(db, "test.sql", testing.Verbose(), testout); err != nil {
		t.Fatal(err)
	}
	limit := 3
	args := []interface{}{"USA", limit}
	var total int64
	_, err := GetResults(db, "select total from summary where country=? limit ?", args, &total)
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

func TestPragmas(t *testing.T) {
	db := memDB(t)
	Pragmas(db, testout)
}
